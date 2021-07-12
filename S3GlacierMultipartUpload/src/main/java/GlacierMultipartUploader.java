import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glacier.GlacierClient;
import software.amazon.awssdk.services.glacier.model.*;
import software.amazon.awssdk.utils.BinaryUtils;

public class GlacierMultipartUploader {

    //For changing part size of multipart upload change value of this variable
    private String  partSize ;
    private Region region;
    private GlacierClient client;

    public GlacierMultipartUploader(String partSize,Region region){
        this.partSize = partSize;
        this.region = region;
        this.client = GlacierClient.builder()
                .region(region)
                .build();
    }

    public static void main(String[] args) throws Exception {
        GlacierMultipartUploader uploader = new GlacierMultipartUploader("1073741824",Region.AP_SOUTH_1);
        String filePath = "";
        GlacierUploadResponse res = uploader.glacierUpload(filePath, "test");
        System.out.println(res.archiveId + ":" + res.checksum + ":" + res.location);

    }

    /**
     * Uploads file on given path to AWS Glacier in given vault using multi part upload
     * @param archiveFilePath
     * @param vaultName
     * @return GlacierUploadResponse containing location , archiveId and checksum
     * @throws Exception
     */
    public GlacierUploadResponse glacierUpload(String archiveFilePath, String vaultName) throws Exception {

        GlacierUploadResponse response = new GlacierUploadResponse();
        System.out.println("Uploading request received for file:" + archiveFilePath + " to vault:" + vaultName);
        File myFile = new File(archiveFilePath);
        Path path = Paths.get(archiveFilePath);


        try {
            String uploadId = initiateMultipartUpload(client, vaultName);
            String checksum = uploadParts(uploadId, archiveFilePath, vaultName);
            CompleteMultipartUploadResponse uploadResponse = completeMultiPartUpload(uploadId, checksum, archiveFilePath, vaultName);
            response.checksum = uploadResponse.checksum();
            response.location = uploadResponse.location();
            response.archiveId = uploadResponse.archiveId();
            System.out.println("The ID of the archived item is " + uploadResponse.archiveId());
            client.close();

            return response;

        } catch (GlacierException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
            throw e;
        }

    }


    /**
     * Uploads file on given path to AWS Glacier in given vault using multi part upload in parallel
     * @param archiveFilePath
     * @param vaultName
     * @return GlacierUploadResponse containing location , archiveId and checksum
     * @throws Exception
     */
    public GlacierUploadResponse glacierUploadParallel(String archiveFilePath, String vaultName) throws Exception {

        GlacierUploadResponse response = new GlacierUploadResponse();
        System.out.println("Uploading request received for file:" + archiveFilePath + " to vault:" + vaultName);
        File myFile = new File(archiveFilePath);
        Path path = Paths.get(archiveFilePath);


        try {
            String uploadId = initiateMultipartUpload(client, vaultName);
            String checksum = uploadParts(uploadId, archiveFilePath, vaultName);
            CompleteMultipartUploadResponse uploadResponse = completeMultiPartUpload(uploadId, checksum, archiveFilePath, vaultName);
            response.checksum = uploadResponse.checksum();
            response.location = uploadResponse.location();
            response.archiveId = uploadResponse.archiveId();
            System.out.println("The ID of the archived item is " + uploadResponse.archiveId());
            client.close();

            return response;

        } catch (GlacierException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
            throw e;
        }

    }


    private String initiateMultipartUpload(GlacierClient client, String vaultName) {
        // Initiate
        InitiateMultipartUploadRequest request = InitiateMultipartUploadRequest.builder()
                .vaultName(vaultName)
                .partSize(this.partSize)
                .build();

        InitiateMultipartUploadResponse result = client.initiateMultipartUpload(request);
        return result.uploadId();
    }

    private String uploadParts(String uploadId, String archiveFilePath, String vaultName) throws NoSuchAlgorithmException, IOException, Exception {

        int filePosition = 0;
        long currentPosition = 0;
        byte[] buffer = new byte[Integer.valueOf(this.partSize)];
        List<byte[]> binaryChecksums = new LinkedList<byte[]>();

        File file = new File(archiveFilePath);
        FileInputStream fileToUpload = new FileInputStream(file);
        String contentRange;
        int read = 0;
        int i = 1;
        while (currentPosition < file.length()) {
            System.out.println("Inside part:" + i);
            i++;
            read = fileToUpload.read(buffer, filePosition, buffer.length);
            if (read == -1) {
                break;
            }
            byte[] bytesRead = Arrays.copyOf(buffer, read);

            contentRange = String.format("bytes %s-%s/*", currentPosition, currentPosition + read - 1);
            String checksum = TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(bytesRead));
            byte[] binaryChecksum = BinaryUtils.fromHex(checksum);
            binaryChecksums.add(binaryChecksum);
            System.out.println(contentRange);


            //Upload part.
            UploadMultipartPartRequest partRequest = UploadMultipartPartRequest.builder()
                    .vaultName(vaultName)
                    .checksum(checksum)
                    .range(contentRange)
                    .uploadId(uploadId)
                    .build();

            UploadMultipartPartResponse partResult = client.uploadMultipartPart(partRequest, RequestBody.fromBytes(bytesRead));
            System.out.println("Part uploaded, checksum: " + partResult.checksum());

            currentPosition = currentPosition + read;
        }
        fileToUpload.close();
        String checksum = TreeHashGenerator.calculateTreeHash(binaryChecksums); //TODO
        return checksum;
    }

    private  CompleteMultipartUploadResponse completeMultiPartUpload(String uploadId, String checksum, String archiveFilePath, String vaultName) throws NoSuchAlgorithmException, IOException {

        File file = new File(archiveFilePath);

        CompleteMultipartUploadRequest compRequest = CompleteMultipartUploadRequest.builder()
                .vaultName(vaultName)
                .uploadId(uploadId)
                .checksum(checksum)
                .archiveSize(String.valueOf(file.length()))
                .build();

        CompleteMultipartUploadResponse compResult = this.client.completeMultipartUpload(compRequest);
        return compResult;
    }

}

class GlacierUploadResponse {

    String archiveId;
    String checksum;
    String location;
}



