package school.sptech.S3;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.MockedStatic;
import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class S3LeituraEnemTest {

    @Mock
    JdbcTemplate jdbcTemplate;

    @DisplayName("Deve processar o arquivo do S3 quando ele ainda não existir no banco de dados")
    @Test
    void deveProcessarArquivoNovoSemCredenciaisReais() throws IOException {
        // cria mock do S3Client e do builder
        S3Client s3ClientMock = mock(S3Client.class);
        @SuppressWarnings("unchecked")
        S3ClientBuilder builderMock = mock(S3ClientBuilder.class);

        S3Object arquivoNovo = S3Object.builder().key("planilhas_enem/NOTAS_ENEM.xlsx").build();
        ListObjectsResponse listResponse = ListObjectsResponse.builder()
                .contents(List.of(arquivoNovo))
                .build();

        when(s3ClientMock.listObjects(any(ListObjectsRequest.class))).thenReturn(listResponse);


        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builderMock);

            when(builderMock.region(any(Region.class))).thenReturn(builderMock);
            when(builderMock.credentialsProvider(any(ProfileCredentialsProvider.class))).thenReturn(builderMock);

            when(builderMock.build()).thenReturn(s3ClientMock);

            S3LeituraEnem sut = new S3LeituraEnem(jdbcTemplate);
            when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), any()))
                    .thenReturn(0);

            S3LeituraEnem spy = spy(sut);
            doNothing().when(spy).processarArquivo(any(S3Client.class), anyString());

            // Act
            spy.leituraArquivos();

            // Assert: verificamos que o S3Client.build() foi invocado e que processarArquivo foi chamado com o mock
            verify(builderMock, atLeastOnce()).build();
            verify(spy, times(1)).processarArquivo(s3ClientMock, "planilhas_enem/NOTAS_ENEM.xlsx");
        }
    }
}