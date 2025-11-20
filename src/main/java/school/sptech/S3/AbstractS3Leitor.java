package school.sptech.S3;

import school.sptech.Auditoria;
import school.sptech.JDBC.ConexaoBanco;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.InputStream;

public abstract class AbstractS3Leitor {

    protected final String bucket = "s3-java-excel";
    protected final Region region = Region.US_EAST_1;

    protected final ConexaoBanco conexao = new ConexaoBanco();
    protected final Auditoria auditoria = new Auditoria(conexao.getJdbcTemplate());

    protected S3Client createS3Client() {
        return S3Client.builder()
                .region(region)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
    }

    protected InputStream getS3Object(S3Client s3Client, String objectKey) {
        GetObjectRequest req = GetObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .build();
        return s3Client.getObject(req);
    }
}
