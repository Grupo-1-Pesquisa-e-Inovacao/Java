package school.sptech.S3;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import school.sptech.JDBC.ConexaoBanco;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.*;

public class S3LeituraEnem extends AbstractS3Leitor {
    private final JdbcTemplate jdbcTemplate;

    public S3LeituraEnem(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private static final Logger logger = LoggerFactory.getLogger(S3LeituraEnem.class);
    private final String bucket = "s3-java-excel";
    private final String pasta = "planilhas_enem/";

    public void leituraArquivos() {
        try (S3Client s3Client = createS3Client()) {
            ListObjectsRequest listObjects = ListObjectsRequest.builder()
                    .bucket(bucket)
                    .build();
            List<S3Object> objects = s3Client.listObjects(listObjects).contents();
            for (S3Object object : objects) {
                if (object.key().contains(pasta) && !object.key().equals(pasta)) {
                    if (jdbcTemplate.queryForObject("SELECT COUNT(*) from processamento_planilha WHERE nome_arquivo = ?", Integer.class, object.key()) == 0) {
                        try {
                            processarArquivo(s3Client, object.key());
                        } catch (IOException e) {
                            logger.error("Erro ao processar o arquivo {}: {}", object.key(), e.getMessage());
                        }
                    } else {
                        logger.warn("Arquivo já processado: {}", object.key());
                    }
                }
            }
        }
    }

    public void processarArquivo(S3Client s3Client, String objectKey) throws IOException {

        logger.info("--------------------- INICIO PROCESSAMENTO ENEM ---------------------");
        logger.info("Processando arquivo: {}", objectKey);
        auditoria.auditoriaInsertProcessamento(objectKey, LocalDateTime.now(), 0, "Processando");

        int count = 0;
        int totalInserido = 0;

        try (InputStream inputStream = getS3Object(s3Client, objectKey);
             Workbook workbook = new XSSFWorkbook(inputStream);
             Connection conn = new ConexaoBanco().getBasicDataSource().getConnection()) {
            
            conn.setAutoCommit(false);
            String sql = "INSERT INTO media_aluno_enem (idEstado, idMunicipio, inscricao_enem, nota_candidato) VALUES (?, ?, ?, ?)";
            Set<String> idMunicipiosValidos = new HashSet<>(jdbcTemplate.query("SELECT idMunicipio from municipio",
                    (rs, rowNum) -> rs.getString("idMunicipio")
            ));

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                Sheet sheet = workbook.getSheetAt(0);
                Iterator<Row> rowIterator = sheet.iterator();
                if (rowIterator.hasNext()) {
                    rowIterator.next();
                }
                
                int naoInseridos = 0;
                while (rowIterator.hasNext()) {
                    LocalDateTime dataAcao = LocalDateTime.now();
                    Row row = rowIterator.next();
                    Cell cell = row.getCell(0, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                    
                    if (cell != null) {
                        String[] linha = cell.getStringCellValue().split(",");
                        if (linha.length >= 6 && linha[1] != null && linha[3] != null && linha[5] != null
                                && !linha[1].isEmpty() && !linha[3].isEmpty() && !linha[5].isEmpty()
                                && linha[3].length() >= 2) {
                            try {
                                if (idMunicipiosValidos.contains(linha[3])) {
                                    ps.setInt(1, Integer.parseInt(linha[3].substring(0, 2)));
                                    ps.setInt(2, Integer.parseInt(linha[3]));
                                    ps.setString(3, linha[1]);
                                    ps.setDouble(4, Double.parseDouble(linha[5]));
                                    ps.addBatch();
                                    if (++count % 20000 == 0) {
                                        int[] batchResult = ps.executeBatch();
                                        conn.commit();
                                        ps.clearBatch();
                                        totalInserido += Arrays.stream(batchResult).sum();
                                        logger.info("Inseridos {} registros (lote de 20000)", totalInserido);
                                    }
                                } else {
                                    auditoria.auditoriaUpdate("INSERT", dataAcao, "Erro", objectKey, row.getRowNum());
                                    logger.warn("Valor não inserido pois o idMunicipio {} não está presente no banco.", linha[3]);
                                    naoInseridos += 1;
                                }
                            } catch (NumberFormatException e) {
                                auditoria.auditoriaUpdate("INSERT", dataAcao, "Erro", objectKey, row.getRowNum());
                                logger.warn("Formato inválido na linha: " + String.join(",", linha), e);
                            }
                        }
                    }
                }

                int[] finalBatch = ps.executeBatch();
                logger.info("Inseridos {} registros (lote final)", finalBatch.length);
                conn.commit();
                totalInserido += Arrays.stream(finalBatch).sum();

                auditoria.auditoriaUpdateProcessamento(objectKey, LocalDateTime.now(), totalInserido, "Concluído");
                logger.info("Processamento concluído. \nTotal de registros inseridos: {}\nRegistros não inseridos: {}", totalInserido, naoInseridos);

                logger.info("--------------------- FIM PROCESSAMENTO ENEM ---------------------");

            } catch (Exception e) {
                auditoria.auditoriaUpdateProcessamento(objectKey, LocalDateTime.now(), totalInserido, "Erro");
                logger.error("Erro ao processar o arquivo: " + objectKey, e);
            }
        } catch (Exception e) {
            logger.error("Erro ao processar o arquivo: " + objectKey, e);
            auditoria.auditoriaUpdateProcessamento(objectKey, LocalDateTime.now(), totalInserido, "Erro");
            throw new IOException("Falha ao processar o arquivo " + objectKey, e);
        }
    }
}
