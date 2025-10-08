package school.sptech.S3;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class S3LeituraMunicipios {
    public static void main(String[] args) {
        String bucket = "s3-java-excel";
        String key = "IDHM_Municipios.xlsx";
        String keyRelatorio = "RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xlsx";


        Region region = Region.US_EAST_1;

        try (S3Client s3 = S3Client.builder()
                .region(region)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build()) {

            InputStream inputStream = s3.getObject(
                    GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build()
            );

            InputStream inputStreamRelatorio = s3.getObject(
                    GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(keyRelatorio)
                            .build()
            );

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
