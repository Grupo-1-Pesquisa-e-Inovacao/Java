package school.sptech.S3;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
class S3LeituraEstadosTest {

    @DisplayName("Deve ler corretamente o arquivo de estados e retornar a lista de siglas")
    @Test
    void testProcessarArquivoEstados() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        S3Client s3ClientMock = mock(S3Client.class);
        S3ClientBuilder builderMock = mock(S3ClientBuilder.class);

        // cria workbook com cabeçalho + 3 estados
        byte[] estadosBytes;
        try (Workbook wb = new XSSFWorkbook()) {
            Sheet sheet = wb.createSheet();
            sheet.createRow(0); // cabeçalho
            sheet.createRow(1).createCell(0).setCellValue("SP");
            sheet.createRow(2).createCell(0).setCellValue("RJ");
            sheet.createRow(3).createCell(0).setCellValue("MG");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            wb.write(baos);
            estadosBytes = baos.toByteArray();
        }

        when(s3ClientMock.getObject(any(GetObjectRequest.class))).thenAnswer(invocation -> {
            GetObjectRequest req = invocation.getArgument(0);
            byte[] chosen = "IDHM_Estados.xlsx".equals(req.key()) ? estadosBytes : new byte[0];
            GetObjectResponse resp = GetObjectResponse.builder().build();
            return new ResponseInputStream<>(resp, new ByteArrayInputStream(chosen));
        });

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builderMock);
            lenient().when(builderMock.region(any(Region.class))).thenReturn(builderMock);
            lenient().when(builderMock.credentialsProvider(any(ProfileCredentialsProvider.class))).thenReturn(builderMock);
            lenient().when(builderMock.build()).thenReturn(s3ClientMock);

            S3LeituraEstados sut = new S3LeituraEstados(jdbcTemplate);

            Method m = S3LeituraEstados.class.getDeclaredMethod("processarArquivoEstados", S3Client.class);
            m.setAccessible(true);

            @SuppressWarnings("unchecked")
            List<String> estados = (List<String>) m.invoke(sut, s3ClientMock);

            assertNotNull(estados);
            assertEquals(3, estados.size());
            assertEquals(List.of("SP", "RJ", "MG"), estados);
        }
    }

    @DisplayName("Deve processar o relatório e mapear corretamente os códigos das UFs existentes")
    @Test
    void testProcessarArquivoRelatorio() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        S3Client s3ClientMock = mock(S3Client.class);
        S3ClientBuilder builderMock = mock(S3ClientBuilder.class);

        // cria workbook em memória (relatório, começa linha 7)
        byte[] relatorioBytes;
        try (Workbook wb = new XSSFWorkbook()) {
            Sheet sheet = wb.createSheet();
            for (int i = 0; i < 7; i++) sheet.createRow(i); // até linha 6
            Row r7 = sheet.createRow(7);
            r7.createCell(0).setCellValue("1");
            r7.createCell(1).setCellValue("SP");
            Row r8 = sheet.createRow(8);
            r8.createCell(0).setCellValue("2");
            r8.createCell(1).setCellValue("RJ");
            Row r9 = sheet.createRow(9);
            r9.createCell(0).setCellValue("3");
            r9.createCell(1).setCellValue("BA");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            wb.write(baos);
            relatorioBytes = baos.toByteArray();
        }

        when(s3ClientMock.getObject(any(GetObjectRequest.class))).thenAnswer(invocation -> {
            GetObjectRequest req = invocation.getArgument(0);
            byte[] chosen = "RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xlsx".equals(req.key()) ? relatorioBytes : new byte[0];
            GetObjectResponse resp = GetObjectResponse.builder().build();
            return new ResponseInputStream<>(resp, new ByteArrayInputStream(chosen));
        });

        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builderMock);
            lenient().when(builderMock.region(any(Region.class))).thenReturn(builderMock);
            lenient().when(builderMock.credentialsProvider(any(ProfileCredentialsProvider.class))).thenReturn(builderMock);
            lenient().when(builderMock.build()).thenReturn(s3ClientMock);

            S3LeituraEstados sut = new S3LeituraEstados(jdbcTemplate);

            Method m = S3LeituraEstados.class.getDeclaredMethod("processarArquivoRelatorio", S3Client.class, List.class);
            m.setAccessible(true);

            @SuppressWarnings("unchecked")
            Map<String, String> idUf = (Map<String, String>) m.invoke(sut, s3ClientMock, List.of("SP", "RJ"));

            assertNotNull(idUf);
            assertEquals(2, idUf.size());
            assertEquals("SP", idUf.get("1"));
            assertEquals("RJ", idUf.get("2"));
            assertFalse(idUf.containsValue("BA"));
        }
    }
}
