package school.sptech.S3;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import school.sptech.Auditoria;
import school.sptech.JDBC.ConexaoBanco;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;


import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.*;

public class S3LeituraEstados {
    ConexaoBanco conexao = new ConexaoBanco();
    private static final Logger logger = LoggerFactory.getLogger(S3LeituraEstados.class);
    private final String bucket = "s3-java-excel";
    private final String key = "IDHM_Estados.xlsx";
    private final String keyRelatorio = "RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xlsx";
    private final Region region = Region.US_EAST_1;
    private final JdbcTemplate jdbcTemplate;
    private final Auditoria auditoria = new Auditoria(conexao.getJdbcTemplate());

    public S3LeituraEstados(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void processarArquivos() {
        try (S3Client s3Client = S3Client.builder()
                .region(region)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build()) {

            logger.info("--------------------- INICIO PROCESSAMENTO ESTADOS ---------------------");
            List<String> estados = processarArquivoEstados(s3Client);
            Map<String, String> idUf = processarArquivoRelatorio(s3Client, estados);
            inserirDadosNoBanco(s3Client, idUf);
            logger.info("--------------------- FIM PROCESSAMENTO ESTADOS ---------------------");

        } catch (S3Exception e) {
            logger.error("Erro ao acessar o S3: {}", e.getMessage(), e);
            throw new RuntimeException("Falha ao processar arquivos do S3", e);
        } catch (Exception e) {
            logger.error("Erro inesperado: {}", e.getMessage(), e);
            throw new RuntimeException("Erro ao processar arquivos", e);
        }
    }

    private List<String> processarArquivoEstados(S3Client s3Client) throws IOException {
        List<String> estados = new ArrayList<>();
        try (InputStream inputStream = getS3Object(s3Client, key);
             Workbook workbook = new XSSFWorkbook(inputStream)) {

            Sheet sheet = workbook.getSheetAt(0);
            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                if (row != null && row.getCell(0) != null) {
                    estados.add(row.getCell(0).getStringCellValue());
                }
            }
        }
        return estados;
    }

    private Map<String, String> processarArquivoRelatorio(S3Client s3Client, List<String> estados) throws IOException {
        Map<String, String> idUf = new HashMap<>();
        try (InputStream inputStream = getS3Object(s3Client, keyRelatorio);
             Workbook workbook = new XSSFWorkbook(inputStream)) {
            Sheet sheet = workbook.getSheetAt(0);
            for (int i = 7; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                if (row != null && row.getCell(0) != null && row.getCell(1) != null) {
                    String uf = row.getCell(1).getStringCellValue();
                    if (estados.contains(uf) && !idUf.containsValue(uf)) {
                        idUf.put(row.getCell(0).getStringCellValue(), uf);
                    }
                }
            }
        }
        return idUf;
    }

    private void inserirDadosNoBanco(S3Client s3Client, Map<String, String> idUf) throws IOException {
        try (InputStream inputStream = getS3Object(s3Client, key);
             Workbook workbook = new XSSFWorkbook(inputStream)) {

            Sheet sheet = workbook.getSheetAt(0);
            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                LocalDate dataAcao = LocalDate.now();
                Row row = sheet.getRow(i);
                if (row == null) continue;

                try {
                    String estadoNome = row.getCell(0).getStringCellValue();
                    String estadoId = null;
                    for (Map.Entry<String, String> entry : idUf.entrySet()) {
                        if (entry.getValue().equals(estadoNome)) {
                            estadoId = entry.getKey();
                            break;
                        }
                    }

                    if (jdbcTemplate.queryForObject("SELECT COUNT(*) FROM estado WHERE idUF = ?", Integer.class, Integer.parseInt(estadoId)) == 0) {
                        if (estadoId != null) {
                            jdbcTemplate.update(
                                    "INSERT INTO estado (idUF, nomeUf, posicaoIDHM, idhm, posicaoIDHM_educacao, idhmEducacao) VALUES (?, ?, ?, ?, ?, ?)",
                                    Integer.parseInt(estadoId),
                                    estadoNome,
                                    row.getCell(1).getNumericCellValue(),
                                    row.getCell(2).getNumericCellValue(),
                                    row.getCell(5).getNumericCellValue(),
                                    row.getCell(6).getNumericCellValue()
                            );
                            auditoria.auditoriaUpdate("INSERT", dataAcao, "Sucesso", key, i);
                            logger.info("Inserido estado: {} com ID: {}", estadoNome, estadoId);
                        } else {
                            logger.warn("ID não encontrado para o estado: {}", estadoNome);
                            auditoria.auditoriaUpdate("INSERT", dataAcao, "Erro", key, i);
                        }
                    } else{
                        logger.warn("ID já existe no banco de dados : {}", estadoId);
                    }
                } catch (Exception e) {
                    auditoria.auditoriaUpdate("INSERT", dataAcao, "Erro", key, i);
                    logger.error("Erro ao inserir linha {}: {}", i, e.getMessage(), e);
                }
            }
        }
    }

    private InputStream getS3Object(S3Client s3Client, String objectKey) {
        return s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .build());
    }
}

