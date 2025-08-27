package school.sptech;
import java.io.File;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.Scanner;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws InterruptedException {
        String caminho = "C:/sistema-de-logs";

        Random aleatorio = new Random();

        Scanner input = new Scanner(System.in);
        LocalDateTime horarioAgora = LocalDateTime.now();
        File caminhoCsv = new File(caminho);
        File[] arquivosGerais = caminhoCsv.listFiles();

        Boolean validAnswer = false;
        Integer tamanhoItens = arquivosGerais.length;

        if(tamanhoItens == 0){
            System.out.println("Não existem logs nesse local.");
        } else{
            for(int i = 0; i < arquivosGerais.length; i ++){
                System.out.println("Arquivo " + (i) + ": " + arquivosGerais[i].getName());
            }
            System.out.println("Escolha uma data para análise dos dados.");
            while(!validAnswer){
                try{
                    Integer resposta = input.nextInt();
                    if(resposta < 0 || resposta > tamanhoItens - 1){
                        throw new IllegalArgumentException();
                    } else{
                        validAnswer = true;
                    }
                } catch (IllegalArgumentException e) {
                    System.out.println("Insira o número de um arquivo válido.");
                }
            }
            int numeroAleatorio = aleatorio.nextInt(5, 30);
            System.out.println("[A#001] " + horarioAgora + "[SYSTEM] Server starting");
            System.out.println("[A#001] " + horarioAgora + "[SYSTEM] Console Mode Activated");
            Thread.sleep(1000);
            for(int i = 0; i < 30; i++){
                horarioAgora = LocalDateTime.now();
                System.out.println("[A#001] " + horarioAgora + " [SYSTEM] Reading archive 27-08-2025"  + "." + i + ".csv");
                if(i == numeroAleatorio){
                    Thread.sleep(1000);
                    for (int j = 1; j <= 5; j++) {
                        horarioAgora = LocalDateTime.now();
                        System.out.println("[ERROR] " + horarioAgora + " [SYSTEM] Cannot read file: 27-08-2025"  + "." + i + ".csv" + " TRY (" + j + "/5)");
                        Thread.sleep(1000);

                    }
                    throw new RuntimeException("Limite de tempo para leitura do arquivo excedido.");
                }
                Thread.sleep(1000);
            }
        }
    }
}