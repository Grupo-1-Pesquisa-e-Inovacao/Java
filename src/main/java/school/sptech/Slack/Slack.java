package school.sptech.Slack;

import org.json.JSONObject;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

public class Slack {
    private static final HttpClient client = HttpClient.newHttpClient();
    private JdbcTemplate jdbcTemplate = null;

    public Slack(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<SlackEventos> getListaSlacks() {
        String sql = "SELECT * FROM slack_evento JOIN slack_config ON slack_evento.idSlackEvento = slack_config.fkIdSlackEvento";
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            SlackEventos evento = new SlackEventos();
            evento.setIdSlackEvento(rs.getInt("idSlackEvento"));
            evento.setNomeEvento(rs.getString("nome_evento"));
            evento.setDescricao(rs.getString("descricao"));
            evento.setLigado(rs.getBoolean("ligado"));
            evento.setWebhook(rs.getString("webhook_url"));
            return evento;
        });
    }

    public void enviarNotificacao(String mensagem, String nomeEvento) throws IOException, InterruptedException {
        for (SlackEventos listaSlack : getListaSlacks()) {
            if (listaSlack.getLigado()) {
                JSONObject json = new JSONObject();
                if (listaSlack.getNomeEvento().equals("nomeEvento")) {
                    json.put("text", mensagem);
                    enviarMsg(json, listaSlack.getWebhook());
                }
            }
        }
    }

    public void enviarMsg(JSONObject message, String url) throws IOException, InterruptedException {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(message.toString()))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                System.out.println("Mensagem enviada com sucesso para o Slack!");
            } else {
                System.err.println("Erro ao enviar mensagem para o Slack. Status: " + response.statusCode());
                System.err.println("Resposta: " + response.body());
            }
        } catch (Exception e) {
            System.err.println("Erro ao enviar mensagem para o Slack:");
            e.printStackTrace();
            throw e;
        }
    }
}
