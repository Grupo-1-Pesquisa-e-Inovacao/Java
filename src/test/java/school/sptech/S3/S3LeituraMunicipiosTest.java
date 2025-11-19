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
class S3LeituraMunicipiosTest {

    @DisplayName("Deve processar corretamente o arquivo de municípios e extrair nomes e UFs")
    @Test
    void testProcessarArquivoMunicipio() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        S3Client s3ClientMock = mock(S3Client.class);
        S3ClientBuilder builderMock = mock(S3ClientBuilder.class);

        // Cria workbook simulado de municípios (coluna 0: "Município (UF)")
        byte[] municipiosBytes;
        try (Workbook wb = new XSSFWorkbook()) {
            Sheet sheet = wb.createSheet();
            sheet.createRow(0); // Cabeçalho
            sheet.createRow(1).createCell(0).setCellValue("Campinas (SP)");
            sheet.createRow(2).createCell(0).setCellValue("Niterói (RJ)");
            sheet.createRow(3).createCell(0).setCellValue("Salvador (BA)");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            wb.write(baos);
            municipiosBytes = baos.toByteArray();
        }

        when(s3ClientMock.getObject(any(GetObjectRequest.class))).thenAnswer(invocation -> {
            GetObjectRequest req = invocation.getArgument(0);
            byte[] chosen = "IDHM_Municipios.xlsx".equals(req.key()) ? municipiosBytes : new byte[0];
            GetObjectResponse resp = GetObjectResponse.builder().build();
            return new ResponseInputStream<>(resp, new ByteArrayInputStream(chosen));
        });

        // Mocka o builder do S3Client
        try (MockedStatic<S3Client> s3Static = mockStatic(S3Client.class)) {
            s3Static.when(S3Client::builder).thenReturn(builderMock);
            lenient().when(builderMock.region(any(Region.class))).thenReturn(builderMock);
            lenient().when(builderMock.credentialsProvider(any(ProfileCredentialsProvider.class))).thenReturn(builderMock);
            lenient().when(builderMock.build()).thenReturn(s3ClientMock);

            S3LeituraMunicipios sut = new S3LeituraMunicipios(jdbcTemplate);

            Method m = S3LeituraMunicipios.class.getDeclaredMethod("processarArquivoMunicipio", S3Client.class);
            m.setAccessible(true);

            @SuppressWarnings("unchecked")
            Map<String, String> result = (Map<String, String>) m.invoke(sut, s3ClientMock);

            assertNotNull(result);
            assertEquals(3, result.size());
            assertTrue(result.containsKey("campinas|SP"));
            assertTrue(result.containsKey("niteroi|RJ"));
            assertTrue(result.containsKey("salvador|BA"));
        }
    }

    @DisplayName("Deve mapear corretamente os códigos dos municípios a partir do relatório do IBGE")
    @Test
    void testProcessarArquivoRelatorio() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        S3Client s3ClientMock = mock(S3Client.class);
        S3ClientBuilder builderMock = mock(S3ClientBuilder.class);

        // Workbook simulado de relatório (linhas a partir da 7)
        byte[] relatorioBytes;
        try (Workbook wb = new XSSFWorkbook()) {
            Sheet sheet = wb.createSheet();
            for (int i = 0; i < 7; i++) sheet.createRow(i);
            Row r7 = sheet.createRow(7);
            r7.createCell(1).setCellValue("São Paulo"); // Nome do estado
            r7.createCell(7).setCellValue("3550308");  // ID do município
            r7.createCell(8).setCellValue("Campinas"); // Nome do município

            Row r8 = sheet.createRow(8);
            r8.createCell(1).setCellValue("Rio de Janeiro");
            r8.createCell(7).setCellValue("3303302");
            r8.createCell(8).setCellValue("Niterói");

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

            S3LeituraMunicipios sut = new S3LeituraMunicipios(jdbcTemplate);

            // Municipios esperados (como se viessem do IDHM_Municipios)
            Map<String, String> municipiosComUf = Map.of(
                    "campinas|SP", "Campinas (SP)",
                    "niteroi|RJ", "Niterói (RJ)"
            );

            Method m = S3LeituraMunicipios.class.getDeclaredMethod("processarArquivoRelatorio", S3Client.class, Map.class);
            m.setAccessible(true);

            @SuppressWarnings("unchecked")
            Map<String, String> result = (Map<String, String>) m.invoke(sut, s3ClientMock, municipiosComUf);

            assertNotNull(result);
            assertEquals(2, result.size());
            assertEquals("3550308", result.get("campinas|SP"));
            assertEquals("3303302", result.get("niteroi|RJ"));
        }
    }

    @DisplayName("Deve mapear corretamente os códigos das UFs a partir do relatório do IBGE")
    @Test
    void testProcessarArquivoRelatorioIdUf() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        S3Client s3ClientMock = mock(S3Client.class);
        S3ClientBuilder builderMock = mock(S3ClientBuilder.class);

        // Workbook simulado de relatório com código de UF (coluna 0)
        byte[] relatorioBytes;
        try (Workbook wb = new XSSFWorkbook()) {
            Sheet sheet = wb.createSheet();
            for (int i = 0; i < 7; i++) sheet.createRow(i);
            Row r7 = sheet.createRow(7);
            r7.createCell(0).setCellValue("35"); // Código UF
            r7.createCell(1).setCellValue("São Paulo");
            r7.createCell(8).setCellValue("Campinas");

            Row r8 = sheet.createRow(8);
            r8.createCell(0).setCellValue("33");
            r8.createCell(1).setCellValue("Rio de Janeiro");
            r8.createCell(8).setCellValue("Niterói");

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            wb.write(baos);
            relatorioBytes = baos.toByteArray();
        }

        // Mocka getObject do S3
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

            S3LeituraMunicipios sut = new S3LeituraMunicipios(jdbcTemplate);

            Map<String, String> municipiosComUf = Map.of(
                    "campinas|SP", "Campinas (SP)",
                    "niteroi|RJ", "Niterói (RJ)"
            );

            Method m = S3LeituraMunicipios.class.getDeclaredMethod("processarArquivoRelatorioIdUf", S3Client.class, Map.class);
            m.setAccessible(true);

            @SuppressWarnings("unchecked")
            Map<String, String> result = (Map<String, String>) m.invoke(sut, s3ClientMock, municipiosComUf);

            assertNotNull(result);
            assertEquals(2, result.size());
            assertEquals("35", result.get("campinas|SP"));
            assertEquals("33", result.get("niteroi|RJ"));
        }
    }
}
