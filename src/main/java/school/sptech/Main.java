package school.sptech;
import school.sptech.JDBC.ConexaoBanco;
import school.sptech.S3.S3LeituraEstados;
import school.sptech.S3.S3LeituraMunicipios;

import java.io.File;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.Scanner;
public class Main {
    public static void main(String[] args) {
        ConexaoBanco conexao = new ConexaoBanco();
        S3LeituraEstados s3LeituraEstados = new S3LeituraEstados(conexao.getJdbcTemplate());
        s3LeituraEstados.processarArquivos();

        S3LeituraMunicipios s3LeituraMunicipios = new S3LeituraMunicipios(conexao.getJdbcTemplate());
        s3LeituraMunicipios.processarArquivos();
    }
}