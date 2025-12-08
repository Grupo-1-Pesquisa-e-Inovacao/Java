package school.sptech;
import school.sptech.JDBC.ConexaoBanco;
import school.sptech.S3.S3LeituraEnem;
import school.sptech.S3.S3LeituraEstados;
import school.sptech.S3.S3LeituraMunicipios;
import school.sptech.Slack.Slack;
import school.sptech.Relatorios.RelatorioNotasService;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws InterruptedException, IOException {
        ConexaoBanco conexao = new ConexaoBanco();
        Slack slack = new Slack(conexao.getJdbcTemplate());

        S3LeituraEstados s3LeituraEstados = new S3LeituraEstados(conexao.getJdbcTemplate());
        s3LeituraEstados.processarArquivos();

        S3LeituraMunicipios s3LeituraMunicipios = new S3LeituraMunicipios(conexao.getJdbcTemplate());
        s3LeituraMunicipios.processarArquivos();

        S3LeituraEnem s3LeituraEnem = new S3LeituraEnem(conexao.getJdbcTemplate(), slack);
        s3LeituraEnem.leituraArquivos();

        RelatorioNotasService relatorio = new RelatorioNotasService(conexao.getJdbcTemplate());
        relatorio.exportarRelatorio();
    }
}