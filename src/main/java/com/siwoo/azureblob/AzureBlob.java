package com.siwoo.azureblob;

import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

@Getter @ToString
@AllArgsConstructor
public abstract class AzureBlob {
    private final String name;
    private final String url;
    private final String container;

    @SneakyThrows
    public static AzureBlobFile toFile(CloudBlockBlob blob) {
        checkNotNull(blob);
        return new AzureBlobFile(blob.getName(),
                blob.getUri().toString(),
                blob.getContainer().getName(),
                fromDate(blob.getProperties().getCreatedTime()),
                fromDate(blob.getProperties().getLastModified()));
    }

    public static AzureBlob from(ListBlobItem e) {
        if (e instanceof CloudBlockBlob)
            return toFile((CloudBlockBlob) e);
        else if (e instanceof CloudBlobDirectory)
            return toDir((CloudBlobDirectory) e);
        throw new UnsupportedOperationException();
    }

    @SneakyThrows
    public static AzureBlobDirectory toDir(CloudBlobDirectory dir) {
        checkNotNull(dir);
        return new AzureBlobDirectory(dir.getPrefix(), dir.getUri().toString(), dir.getContainer().getName());
    }

    public static LocalDateTime fromDate(Date date) {
        checkNotNull(date);
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    @Getter
    public static final class AzureBlobFile extends AzureBlob {
        private final LocalDateTime creation;
        private final LocalDateTime modification;

        public AzureBlobFile(String name, String url, String containerName, LocalDateTime creation, LocalDateTime modification) {
            super(name, url, containerName);
            this.creation = creation;
            this.modification = modification;
        }

        @Override
        public String toString() {
            return "AzureBlobFile{" +
                    "name='" + super.getName() + '\'' +
                    ", url='" + super.getUrl() + '\'' +
                    ", creation=" + creation +
                    ", modification=" + modification +
                    '}';
        }
    }

    @Getter
    public static final class AzureBlobDirectory extends AzureBlob {
        public AzureBlobDirectory(String name, String url, String containerName) {
            super(name, url, containerName);
        }

        @Override
        public String toString() {
            return "AzureBlobDirectory{" +
                    "name='" + super.getName() + '\'' +
                    ", url='" + super.getUrl() + '\'' +
                    '}';
        }
    }

}
