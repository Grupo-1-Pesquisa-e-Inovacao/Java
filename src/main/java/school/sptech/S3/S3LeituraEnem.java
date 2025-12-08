package school.sptech.S3;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import school.sptech.JDBC.ConexaoBanco;
import school.sptech.Slack.Slack;
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

    public S3LeituraEnem(JdbcTemplate jdbcTemplate, Slack slack) {
        this.jdbcTemplate = jdbcTemplate;
        this.slack = slack;
    }

    private static final Logger logger = LoggerFactory.getLogger(S3LeituraEnem.class);
    private final String bucket = "s3-java-excel1";
    private final String pasta = "planilhas_enem/";
    private Slack slack;

    public void leituraArquivos() throws InterruptedException{
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

    public void processarArquivo(S3Client s3Client, String objectKey) throws IOException, InterruptedException {

        logger.info("--------------------- INICIO PROCESSAMENTO ENEM ---------------------");
        logger.info("Processando arquivo: {}", objectKey);
        auditoria.auditoriaInsertProcessamento(objectKey, LocalDateTime.now(), 0, 0, "Processando");

        String processandoMsg = ":hourglass_flowing_sand: *Processando Arquivo*\n" +
                "```" + objectKey + "```";
        slack.enviarNotificacao(processandoMsg, "dados-enem");
        int count = 0;
        int totalInserido = 0;
        int naoInseridos = 0;

        try (InputStream inputStream = getS3Object(s3Client, objectKey);
             Workbook workbook = new XSSFWorkbook(inputStream);
             Connection conn = new ConexaoBanco().getBasicDataSource().getConnection()) {
             String mensagem = "";

            conn.setAutoCommit(false);
            String sql = "INSERT INTO media_aluno_enem (ano, idEstado, idMunicipio, inscricao_enem, nota_candidato) VALUES (?, ?, ?, ?, ?)";
            Set<String> idMunicipiosValidos = new HashSet<>(jdbcTemplate.query("SELECT idMunicipio from municipio",
                    (rs, rowNum) -> rs.getString("idMunicipio")
            ));

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                Sheet sheet = workbook.getSheetAt(0);
                Iterator<Row> rowIterator = sheet.iterator();
                if (rowIterator.hasNext()) {
                    rowIterator.next();
                }

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
                                    ps.setInt(1, Integer.parseInt(linha[0]));
                                    ps.setInt(2, Integer.parseInt(linha[3].substring(0, 2)));
                                    ps.setInt(3, Integer.parseInt(linha[3]));
                                    ps.setString(4, linha[1]);
                                    ps.setDouble(5, Double.parseDouble(linha[5]));
                                    ps.addBatch();
                                    if (++count % 20000 == 0) {
                                        int[] batchResult = ps.executeBatch();
                                        conn.commit();
                                        ps.clearBatch();
                                        totalInserido += Arrays.stream(batchResult).sum();
                                        logger.info("Inseridos {} registros (lote de 20000)", totalInserido);
                                    }
                                } else {
                                    mensagem = String.format("Valor não inserido pois o idMunicipio %s não está presente no banco.", linha[3]);
                                    auditoria.auditoriaUpdate("INSERT", dataAcao, "Erro", objectKey, row.getRowNum(), mensagem);
                                    logger.warn("Valor não inserido pois o idMunicipio {} não está presente no banco.", linha[3]);
                                    naoInseridos += 1;
                                }
                            } catch (NumberFormatException e) {
                                mensagem = String.format("Formato inválido na linha: %s", String.join(",", linha));
                                auditoria.auditoriaUpdate("INSERT", dataAcao, "Erro", objectKey, row.getRowNum(), mensagem);
                                naoInseridos += 1;
                                logger.warn("Formato inválido na linha: " + String.join(",", linha), e);
                            }
                        }
                    }
                }

                int[] finalBatch = ps.executeBatch();
                logger.info("Inseridos {} registros (lote final)", finalBatch.length);
                conn.commit();
                totalInserido += Arrays.stream(finalBatch).sum();

                logger.info("Total de linhas processadas: {}", totalInserido + naoInseridos);
                logger.info("Total de linhas inseridas: {}", totalInserido);
                logger.info("Total de linhas não inseridas: {}", naoInseridos);

                int totalLinhas = totalInserido + naoInseridos;
                double percentualSucesso = totalLinhas > 0 ? (double) totalInserido / totalLinhas * 100 : 0;

                String emoji = percentualSucesso >= 90 ? ":white_check_mark:" : ":warning:";
                String status = percentualSucesso >= 90 ? "*SUCESSO*" : "*ATENÇÃO*";
                
                String mensagemConclusao = String.format(
                    "%s %s\n" +
                    "*Arquivo Processado*\n" +
                    "```%s```\n\n" +
                    "*Status:* %s\n" +
                    "*Precisão:* %.2f%%\n" +
                    "*Total de Linhas:* %d\n" +
                    "*Linhas Processadas com Sucesso:* %d\n" +
                    "*Linhas com Erro:* %d",
                    emoji, status,
                    objectKey,
                    status,
                    percentualSucesso,
                    totalLinhas,
                    totalInserido,
                    naoInseridos
                );

                slack.enviarNotificacao(mensagemConclusao, "dados-enem");

                auditoria.auditoriaUpdateProcessamento(objectKey, LocalDateTime.now(), naoInseridos, totalInserido, "Concluído");
                logger.info("--------------------- FIM PROCESSAMENTO ENEM ---------------------");

            } catch (Exception e) {
                auditoria.auditoriaUpdateProcessamento(objectKey, LocalDateTime.now(), naoInseridos, totalInserido, "Erro");
                logger.error("Erro ao processar o arquivo: " + objectKey, e);
            }
        } catch (Exception e) {
            logger.error("Erro ao processar o arquivo: " + objectKey, e);
            auditoria.auditoriaUpdateProcessamento(objectKey, LocalDateTime.now(), naoInseridos, totalInserido, "Erro");
            throw new IOException("Falha ao processar o arquivo " + objectKey, e);
        }
    }
}
