package com.siwoo.azureblob;

import org.apache.commons.lang3.time.StopWatch;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class Main {
    public static final String CONNECTION_STRING = "azure.blob.connection_string";
    public static final String BLOB_CONTAINER = "azure.blob.container";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.load(Main.class.getClassLoader().getResourceAsStream("application.properties"));
        AzureBlobManager blobManager = new AzureBlobManager(props.getProperty(CONNECTION_STRING));
        List<AzureBlob> result = null;

//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        result = blobManager.blobsSlow(props.getProperty(BLOB_CONTAINER));  //1 min - 1936
//        System.out.println(result);
//        System.out.println(result.size());
//
//        stopWatch.stop();
//        System.out.println(stopWatch.toString());
//        stopWatch.reset();
//        stopWatch.start();
//        result = blobManager.blobsSlow(props.getProperty(BLOB_CONTAINER));    //26 sec - 1936
//        System.out.println(result);
//        System.out.println(result.size());
//
//        stopWatch.stop();
//        System.out.println(stopWatch.toString());

//        result = blobManager.blobs(props.getProperty(BLOB_CONTAINER), "root=001", e -> true);
//        System.out.println(result);
//        System.out.println(result.size());
//
        LocalDateTime time = LocalDateTime.of(2020, 10, 20, 14, 04, 00);
        result = blobManager.blobs(props.getProperty(BLOB_CONTAINER), "root=001", e -> time.isBefore(AzureBlob.fromDate(e.getProperties().getLastModified())));
        System.out.println(result);
        System.out.println(result.size());

        String url = blobManager.blobs(props.getProperty(BLOB_CONTAINER), "root=001/2015-01/test001.txt").get(0).getUrl();
        System.out.printf("Go to resource: %s%n", url);

        byte[] bytes = blobManager.download(props.getProperty(BLOB_CONTAINER), "root=001/2015-01/test001.txt");
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
    }
}
