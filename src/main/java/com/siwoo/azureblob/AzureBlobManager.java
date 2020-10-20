package com.siwoo.azureblob;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobType;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.ByteArrayOutputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.util.*;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Blob 메인 컴포넌트
 *  1. Storage Account
 *  2. Blob Client
 *  3. Blob Containers
 *  4. Blob Blocks (ListBlobItem)
 *      -> NAME, URI
 *      - CloudBlockBlob
 *      - CloudAppendBlob
 *      - CloudPageBlob
 *      - CloudBlobDirectory
 *
 *      ListItemBlob (전체 결과) vs ResultSegment (부분 결과) - Max - 5000
 *      BlobContinuationToken
 *  연산
 *      read, write, delete
 *
 *  Blob 읽기시
 *      Blob 을 서버에 다운받지 말고, 바로 storage uri 로 참고할 수 있는 방법은 없을지 확인.
 *
 *  Soft Delete?
 *      삭제 이후 recovering 기능 제공하는 삭제. Retain Day 로 복제 파일 retain 기간 조절 가능.
 *      삭제된 파일 확인시 BlobListingDetails.Deleted 을 이용해 쿼리.
 *
 */
public class AzureBlobManager {

    private final String CONNECTION_STRING;
    private final CloudStorageAccount storageAccount;
    private final CloudBlobClient blobClient;

    public AzureBlobManager(String connectionString) throws URISyntaxException, InvalidKeyException {
        CONNECTION_STRING = connectionString;
        storageAccount = CloudStorageAccount.parse(connectionString);
        blobClient = storageAccount.createCloudBlobClient();
    }

    private CloudBlobContainer container(String containerName) throws URISyntaxException, StorageException {
        CloudBlobContainer c = blobClient.getContainerReference(containerName);
        if (!c.exists())
            throw new ResourceNotFoundException(String.format("Container [%s] doesn't exists.", containerName));
        return c;
    }

    @SneakyThrows   //Heap issue?
    public List<AzureBlob> blobsSlow(String container) {
        CloudBlobContainer c = container(container);
        List<AzureBlob> result = new ArrayList<>();
        blobsSlow(c.listBlobs(), result);
        return result;
    }

    @SneakyThrows   // 2 times faster but not recognize CloudBlockDirectory
    public List<AzureBlob> blobs(String container, String path, Predicate<CloudBlockBlob> condition) {
        CloudBlobContainer c = container(container);
        List<AzureBlob> blobs = new ArrayList<>();
        ResultContinuation token = null;
        do {
            ResultSegment<ListBlobItem> segment =
                    c.listBlobsSegmented(path, true, null, 5000, token, null, null);
                    // 파티션 단위로 읽음 & listBlob 보다 좀 더 최신 데이터와 동기화
            token = segment.getContinuationToken();
            blobs.addAll(segment.getResults().stream()
                    .map(e -> (CloudBlockBlob) e)
                    .filter(condition)
                    .filter(e -> e.getProperties().getDeletedTime() == null)
                    .map(AzureBlob::from).collect(Collectors.toList()));
        } while (token != null);
        return blobs;
    }

    @SneakyThrows
    public byte[] download(String containerName, String blobName) {
        checkNotNull(containerName, blobName);
        CloudBlobContainer c = container(containerName);
        CloudBlockBlob blob = c.getBlockBlobReference(blobName);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        blob.download(out);
        byte[] bytes = out.toByteArray();
        out.write(bytes);
        return bytes;
    }

    @SneakyThrows   // 2 times faster but not recognize CloudBlockDirectory
    public List<AzureBlob> blobs(String container, String path) {
        return blobs(container, path, e -> true);
    }

    public List<AzureBlob> blobs(String container) {
        return blobs(container, null, (e) -> true);
    }

    @SneakyThrows
    private void blobsSlow(Iterable<ListBlobItem> items, List<AzureBlob> result) {
        if (items == null) return;
        for (ListBlobItem item: items) {
            if (item instanceof CloudBlobDirectory) {
                result.add(AzureBlob.toDir((CloudBlobDirectory) item));
                blobsSlow(((CloudBlobDirectory) item).listBlobs(), result);
            } else if (item instanceof CloudBlockBlob)
                result.add(AzureBlob.toFile((CloudBlockBlob) item));
        }
    }

    @SneakyThrows
    public int setTier(List<AzureBlob> blobs, AccessTier tier) {
        checkNotNull(blobs, tier);
        int unit = blobs.size() / 10;
        integer = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(unit);
        int result = parallalTiering(0, blobs.size(), unit, blobs, tier, latch);
        latch.await();
        System.out.println(integer.get());
        return result;
    }

    private static AtomicInteger integer;

    /**
     * 느리니까 병렬 연산하자
     */
    @SneakyThrows
    private int parallalTiering(int start, int end, int unit, List<AzureBlob> blobs, AccessTier tier, CountDownLatch latch) {
        if (end - start <= unit) {
            //do work
            if (start > end) return 0;
            integer.incrementAndGet();
            System.out.printf("from %d - to %d%n", start, end);
            List<AzureBlob> sublist = blobs.subList(start, end);
            new Thread(() -> {
                try {
                    for (AzureBlob file : sublist) {
                        BlobClient blobClient = new BlobClientBuilder()
                                .connectionString(CONNECTION_STRING)
                                .containerName(file.getContainer())
                                .blobName(file.getName())
                                .buildClient();
                        if (blobClient.getProperties().getAccessTier() != tier
                                && blobClient.getProperties().getBlobSize() != 0) //todo: find out how to diff btw dir and blob
                            blobClient.setAccessTier(tier);
                        latch.countDown();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    //ignore
                }
            }).start();
            return end - start;
        }
        int mid = (end + start) / 2;
        return parallalTiering(start, mid, unit, blobs, tier, latch) + parallalTiering(mid, end, unit, blobs, tier, latch);
    }

    @SneakyThrows
    private void createTestBlobs(String container) {
        CloudBlobContainer c = container(container);
        byte[] dummy = "테스트 입니다.".getBytes(StandardCharsets.UTF_8);
        String PARTITION1_FORMAT = "root=%03d";
        String PARTITION2_FORMAT = "20%02d-%02d";
        String FILE_FORMAT = "test%03d.txt";
        String FINAL = "%s/%s/%s";
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i=1; i<10; i++) {
            final String part1 = String.format(PARTITION1_FORMAT, i);
            executorService.submit(() -> {
                for (int year=15; year<=20; year++) {
                    for (int month=1; month<=12; month++) {
                        String part2 = String.format(PARTITION2_FORMAT, year, month);
                        for (int f=1; f<=10; f++) {
                            try {
                                String file = String.format(FILE_FORMAT, f);
                                String path = String.format(FINAL, part1, part2, file);
                                CloudBlockBlob blob = c.getBlockBlobReference(path);
                                blob.uploadFromByteArray(dummy, 0, dummy.length);
                            } catch (Exception e) {
                                //ignore
                            }
                        }
                    }
                }
            });
        }
        executorService.awaitTermination(3, TimeUnit.MINUTES);
        System.out.println("finish job.");
        executorService.shutdownNow();
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.load(Main.class.getClassLoader().getResourceAsStream("application.properties"));
        AzureBlobManager blobManager = new AzureBlobManager(props.getProperty(Main.CONNECTION_STRING));
        //blobManager.createTestBlobs(props.getProperty(Main.BLOB_CONTAINER));
    }
}
