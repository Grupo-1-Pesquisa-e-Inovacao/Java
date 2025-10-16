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
}
