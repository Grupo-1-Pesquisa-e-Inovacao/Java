package school.sptech;

import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDate;

public class Auditoria {
    private final JdbcTemplate jdbcTemplate;
    public Auditoria(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void auditoriaUpdate(String tipo_acao, LocalDate data_acao, String status_acao, String classe_acao, int linha_excel) {
        jdbcTemplate.update("INSERT INTO auditoria (tipo_acao, data_acao, status_acao, classe_acao, linha_excel) VALUES (?, ?, ?, ?, ?)", tipo_acao, data_acao, status_acao, classe_acao, linha_excel);
    }


    public void auditoriaInsertProcessamento(String nome_arquivo, LocalDate data_acao, Integer linhas_processadas, String status) {
        jdbcTemplate.update("INSERT INTO processamento_planilha (nome_arquivo, data_processamento, linhas_processadas, status) VALUES (?, ?, ?, ?)", nome_arquivo, data_acao, linhas_processadas, status);
    }


    public void auditoriaUpdateProcessamento(String nome_arquivo, LocalDate data_acao, Integer linhas_processadas, String status) {
        jdbcTemplate.update("UPDATE processamento_planilha SET data_processamento = ?, linhas_processadas = ?, status = ? WHERE nome_arquivo = ?", data_acao, linhas_processadas, status, nome_arquivo);
    }
}
