package school.sptech.S3;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class S3LeituraMunicipios {
    private static final Logger logger = LoggerFactory.getLogger(S3LeituraMunicipios.class);
    private final JdbcTemplate jdbcTemplate;
    private final String bucket = "s3-java-excel";
    private final String key = "IDHM_Municipios.xlsx";
    private final String keyRelatorio = "RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xlsx";
    private final Region region = Region.US_EAST_1;

    private static final Map<String, String> UF_NOME_TO_SIGLA = new HashMap<>() {{
        put("Rondônia", "RO");
        put("Acre", "AC");
        put("Amazonas", "AM");
        put("Roraima", "RR");
        put("Pará", "PA");
        put("Amapá", "AP");
        put("Tocantins", "TO");
        put("Maranhão", "MA");
        put("Piauí", "PI");
        put("Ceará", "CE");
        put("Rio Grande do Norte", "RN");
        put("Paraíba", "PB");
        put("Pernambuco", "PE");
        put("Alagoas", "AL");
        put("Sergipe", "SE");
        put("Bahia", "BA");
        put("Minas Gerais", "MG");
        put("Espírito Santo", "ES");
        put("Rio de Janeiro", "RJ");
        put("São Paulo", "SP");
        put("Paraná", "PR");
        put("Santa Catarina", "SC");
        put("Rio Grande do Sul", "RS");
        put("Mato Grosso do Sul", "MS");
        put("Mato Grosso", "MT");
        put("Goiás", "GO");
        put("Distrito Federal", "DF");
    }};

    public S3LeituraMunicipios(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Normalizes municipality names to handle variations between files
     * - Removes accents and diacritical marks
     * - Replaces hyphens with spaces (Grão-Pará -> Grao Para)
     * - Normalizes apostrophes and quotes
     * - Removes extra whitespace
     * - Converts to lowercase for consistent matching
     */
    private String normalizarNomeMunicipio(String nome) {
        if (nome == null) {
            return "";
        }

        String normalized = java.text.Normalizer.normalize(nome, java.text.Normalizer.Form.NFD)
            .replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
        return normalized
            .replace("-", " ")        // Replace hyphens with spaces
            .replace("'", "")         // Remove single quotes
            .replace("\"", "")        // Remove double quotes
            .replace("º", "")         // Remove ordinal indicators
            .replace("°", "")          // Remove degree symbols
            .replaceAll("\\s+", " ")  // Normalize multiple spaces to single space
            .toLowerCase()
            .trim();
    }

    public void processarArquivos() {
        try (S3Client s3Client = S3Client.builder()
                .region(region)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build()) {

            Map<String, String> municipiosComUf = processarArquivoMunicipio(s3Client);
            logger.info("IDHM: {} municípios carregados", municipiosComUf.size());
            Map<String, String> idMunicipio = processarArquivoRelatorio(s3Client, municipiosComUf);
            logger.info("Processou relatório IdMunicipio: " + idMunicipio.size());
            Map<String, String> idUf = processarArquivoRelatorioIdUf(s3Client, municipiosComUf);
            logger.info("Processou relatório IdUf: " + idUf.size());
            inserirDadosNoBanco(s3Client, idMunicipio, idUf, municipiosComUf);
            logger.info("Processou no banco");

            logger.info("--------------------- FIM PROCESSAMENTO ---------------------");

        } catch (S3Exception e) {
            logger.error("Erro ao acessar o S3: {}", e.getMessage(), e);
            throw new RuntimeException("Falha ao processar arquivos do S3", e);
        } catch (Exception e) {
            logger.error("Erro inesperado: {}", e.getMessage(), e);
            throw new RuntimeException("Erro ao processar arquivos", e);
        }
    }

    private Map<String, String> processarArquivoMunicipio(S3Client s3Client) throws IOException {
        Map<String, String> municipioComUf = new HashMap<>();
        try (InputStream inputStream = getS3Object(s3Client, key);
             Workbook workbook = new XSSFWorkbook(inputStream)) {
            jdbcTemplate.update("DELETE from municipio where idMunicipio > 1");
            Sheet sheet = workbook.getSheetAt(0);
            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                if (row != null && row.getCell(0) != null) {
                    String municipioCompleto = row.getCell(0).getStringCellValue().trim();
                    String municipio = municipioCompleto.replaceAll("\\s*\\([A-Z]{2}\\)$", "").trim();
                    String ufSigla = municipioCompleto.replaceAll(".*\\(([A-Z]{2})\\)$", "$1").trim();
                    String chave = normalizarNomeMunicipio(municipio) + "|" + ufSigla;
                    municipioComUf.put(chave, municipioCompleto);
                }
            }
        }
        return municipioComUf;
    }

    private Map<String, String> processarArquivoRelatorio(S3Client s3Client, Map<String, String> municipiosComUf) throws IOException {
        Map<String, String> chaveMunicipioId = new HashMap<>();
        int naoEncontrados = 0;
        try (InputStream inputStream = getS3Object(s3Client, keyRelatorio);
             Workbook workbook = new XSSFWorkbook(inputStream)) {

            Sheet sheet = workbook.getSheetAt(0);
            for (int i = 7; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                if (row != null && row.getCell(1) != null && row.getCell(7) != null && row.getCell(8) != null) {
                    String municipio = row.getCell(8).getStringCellValue().trim();
                    String ufNome = row.getCell(1).getStringCellValue().trim();
                    String ufSigla = UF_NOME_TO_SIGLA.getOrDefault(ufNome, ufNome);
                    String chave = normalizarNomeMunicipio(municipio) + "|" + ufSigla;
                    if (municipiosComUf.containsKey(chave)) {
                        String codigoCompleto = row.getCell(7).getStringCellValue();
                        chaveMunicipioId.put(chave, codigoCompleto);
                    }
                }
            }
        }
        return chaveMunicipioId;
    }


    private Map<String, String> processarArquivoRelatorioIdUf(S3Client s3Client, Map<String, String> municipiosComUf) throws IOException {
        Map<String, String> chaveUfCode = new HashMap<>();
        try (InputStream inputStream = getS3Object(s3Client, keyRelatorio);
             Workbook workbook = new XSSFWorkbook(inputStream)) {

            Sheet sheet = workbook.getSheetAt(0);
            for (int i = 7; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                if (row != null && row.getCell(0) != null && row.getCell(1) != null && row.getCell(8) != null) {
                    String municipio = row.getCell(8).getStringCellValue().trim();
                    String ufNome = row.getCell(1).getStringCellValue().trim();
                    String ufSigla = UF_NOME_TO_SIGLA.getOrDefault(ufNome, ufNome);
                    String ufCode = row.getCell(0).getStringCellValue().trim();
                    String chave = normalizarNomeMunicipio(municipio) + "|" + ufSigla;

                    if (municipiosComUf.containsKey(chave)) {
                        chaveUfCode.put(chave, ufCode);
                    }
                }
            }
            logger.info("RELATORIO UF: {} códigos de UF mapeados", chaveUfCode.size());
        }
        return chaveUfCode;
    }

    private void inserirDadosNoBanco(S3Client s3Client, Map<String, String> chaveMunicipioId, Map<String, String> chaveUfCode, Map<String, String> municipiosComUf) throws IOException {
        try (InputStream inputStream = getS3Object(s3Client, key);
             Workbook workbook = new XSSFWorkbook(inputStream)) {
            Sheet sheet = workbook.getSheetAt(0);
            int inseridos = 0;
            int erros = 0;

            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                LocalDate dataAcao = LocalDate.now();
                Row row = sheet.getRow(i);
                if (row == null) continue;

                try {
                    String municipioNomeCompleto = row.getCell(0).getStringCellValue();
                    String municipioNome = municipioNomeCompleto.replaceAll("\\s*\\([A-Z]{2}\\)$", "").trim();
                    String ufSigla = municipioNomeCompleto.replaceAll(".*\\(([A-Z]{2})\\)$", "$1").trim();
                    String chave = normalizarNomeMunicipio(municipioNome) + "|" + ufSigla;

                    String municipioId = chaveMunicipioId.get(chave);
                    String ufCode = chaveUfCode.get(chave);

                    if (municipioId != null && ufCode != null) {
                        jdbcTemplate.update(
                                "INSERT INTO municipio (idMunicipio, idEstado, nome_municipio, posicaoIDHM, IDHM, posicaoIDHM_educacao, idhmEducacao)" +
                                        " VALUES (?, ?, ?, ?, ?, ?, ?)",
                                Integer.parseInt(municipioId), // idMunicipio
                                Integer.parseInt(ufCode),      // idUf
                                municipioNomeCompleto,  // nome_municipio
                                (int) row.getCell(1).getNumericCellValue(),  // posicaoIDHM
                                row.getCell(2).getNumericCellValue(),  // IDHM
                                (int) row.getCell(5).getNumericCellValue(),  // posicaoIDHM_educacao
                                row.getCell(6).getNumericCellValue());   // idhmEducacao

                        // auditoria

                        jdbcTemplate.update("INSERT INTO auditoria (tipo_acao, data_acao, status_acao) VALUES (?, ?, ?)", "INSERT", dataAcao, "Sucesso");
                        logger.info("Inserido município: {} com ID: {} e UF: {}", municipioNomeCompleto, municipioId, ufCode);
                        inseridos++;
                    } else {
                        erros++;
                        jdbcTemplate.update("INSERT INTO auditoria (tipo_acao, data_acao, status_acao) VALUES (?, ?, ?)", "INSERT", dataAcao, "Falha");
                        logger.warn("ID ou UF não encontrado para o município: {} (Chave: {}, ID: {}, UF: {})", municipioNomeCompleto, chave, municipioId, ufCode);
                    }
                } catch (Exception e) {
                    erros++;
                    logger.error("Erro ao inserir linha {}: {}", i, e.getMessage(), e);
                }
            }
            logger.info("Resumo: {} inseridos, {} erros, Total processado: {}", inseridos, erros, inseridos + erros);
        }
    }

    private InputStream getS3Object(S3Client s3Client, String objectKey) {
        return s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .build());
    }
}

