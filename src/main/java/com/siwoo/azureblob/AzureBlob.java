package com.siwoo.azureblob;

import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Getter @ToString
@AllArgsConstructor
public abstract class AzureBlob {
    private final String name;
    private final String url;

    public static AzureBlobFile toFile(CloudBlockBlob blob) {
        checkNotNull(blob);
        return new AzureBlobFile(blob.getName(), blob.getUri().toString(),
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

    public static AzureBlobDirectory toDir(CloudBlobDirectory dir) {
        checkNotNull(dir);
        return new AzureBlobDirectory(dir.getPrefix(), dir.getUri().toString());
    }

    public static LocalDateTime fromDate(Date date) {
        checkNotNull(date);
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    @Getter
    public static final class AzureBlobFile extends AzureBlob {
        private final LocalDateTime creation;
        private final LocalDateTime modification;

        public AzureBlobFile(String name, String url, LocalDateTime creation, LocalDateTime modification) {
            super(name, url);
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
        public AzureBlobDirectory(String name, String url) {
            super(name, url);
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
