package school.sptech.Relatorios;

import org.springframework.jdbc.core.JdbcTemplate;
import school.sptech.Slack.SlackEventos;

import java.io.*;
import java.util.List;

public class RelatorioNotasService {

    private JdbcTemplate jdbcTemplate = null;

    public RelatorioNotasService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<RelatorioNotas> getRelatorio() {
        String sql = "SELECT\n" +
                "    e.idUF,\n" +
                "    e.nomeUf,\n" +
                "    m.idMunicipio,\n" +
                "    m.nome_municipio,\n" +
                "    me.ano,\n" +
                "    me.nota_candidato\n" +
                "FROM\n" +
                "    estado e\n" +
                "JOIN\n" +
                "    municipio m ON e.idUF = m.idEstado\n" +
                "JOIN\n" +
                "    media_aluno_enem me ON me.idMunicipio = m.idMunicipio\n" +
                "WHERE\n" +
                "    me.ano = (SELECT MAX(ano) FROM media_aluno_enem)\n" +
                "LIMIT 3000;";
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            RelatorioNotas notas = new RelatorioNotas();
            notas.setIdUF(rs.getInt("idUF"));
            notas.setNomeEstado(rs.getString("nomeUf"));
            notas.setIdMunicipio(rs.getInt("idMunicipio"));
            notas.setNomeMunicipio(rs.getString("nome_municipio"));
            notas.setAno(rs.getInt("ano"));
            notas.setNota(rs.getDouble("nota_candidato"));
            return notas;
        });
    }

    public void exportarRelatorio() throws IOException {
        List<RelatorioNotas> relatorio = getRelatorio();
        String caminhoArquivo = "relatorio_notas_enem.csv";
        String SEPARATOR = ";";

        try (
                FileWriter fw = new FileWriter(caminhoArquivo);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw)
        ) {

            out.println("RELATÓRIO GERADO DE FORMA AUTOMÁTICA COM INTENÇÃO EM VERIFICAR MÉDIA DE NOTAS");
            out.println("ID_UF" + SEPARATOR + "Nome_Estado" + SEPARATOR +
                    "ID_Municipio" + SEPARATOR + "Nome_Municipio" + SEPARATOR +
                    "Ano" + SEPARATOR + "Nota_Candidato");

            for (RelatorioNotas nota : relatorio) {
                out.println(
                        nota.getIdUF() + SEPARATOR +
                                "\"" + nota.getNomeEstado() + "\"" + SEPARATOR +
                                nota.getIdMunicipio() + SEPARATOR +
                                "\"" + nota.getNomeMunicipio() + "\"" + SEPARATOR +
                                nota.getAno() + SEPARATOR +
                                String.format("%.2f", nota.getNota())
                );
            }

            out.println("QUANTIDADE DE LINHAS NESTE RELATÓRIO: " + relatorio.size());

            System.out.println("Relatório CSV gerado com sucesso em: " + caminhoArquivo);

        } catch (IOException e) {
            System.err.println("Erro ao escrever o arquivo CSV: " + e.getMessage());
            throw e;
        }
    }


}
