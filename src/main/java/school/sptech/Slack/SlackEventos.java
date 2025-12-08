package school.sptech.Slack;

public class SlackEventos {
    private Integer idSlackEvento;
    private String nomeEvento;
    private String descricao;
    private Boolean ligado;
    private String webhook;

    public String getWebhook() {
        return webhook;
    }

    public void setWebhook(String webhook) {
        this.webhook = webhook;
    }

    public SlackEventos(){}

    public Integer getIdSlackEvento() {
        return idSlackEvento;
    }

    public void setIdSlackEvento(Integer idSlackEvento) {
        this.idSlackEvento = idSlackEvento;
    }

    public String getNomeEvento() {
        return nomeEvento;
    }

    public void setNomeEvento(String nomeEvento) {
        this.nomeEvento = nomeEvento;
    }

    public String getDescricao() {
        return descricao;
    }

    public void setDescricao(String descricao) {
        this.descricao = descricao;
    }

    public Boolean getLigado() {
        return ligado;
    }

    public void setLigado(Boolean ligado) {
        this.ligado = ligado;
    }
}
