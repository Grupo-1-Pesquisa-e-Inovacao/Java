package school.sptech.Relatorios;

public class RelatorioNotas {
    private String nomeEstado;
    private int idUF;
    private String nomeMunicipio;
    private int idMunicipio;

    public int getIdUF() {
        return idUF;
    }

    public void setIdUF(int idUF) {
        this.idUF = idUF;
    }

    private double nota;
    private int ano;

    public RelatorioNotas() {
    }

    public String getNomeEstado() {
        return nomeEstado;
    }

    public void setNomeEstado(String nomeEstado) {
        this.nomeEstado = nomeEstado;
    }
    public String getNomeMunicipio() {
        return nomeMunicipio;
    }

    public void setNomeMunicipio(String nomeMunicipio) {
        this.nomeMunicipio = nomeMunicipio;
    }

    public int getIdMunicipio() {
        return idMunicipio;
    }

    public void setIdMunicipio(int idMunicipio) {
        this.idMunicipio = idMunicipio;
    }

    public double getNota() {
        return nota;
    }

    public void setNota(double nota) {
        this.nota = nota;
    }

    public int getAno() {
        return ano;
    }

    public void setAno(int ano) {
        this.ano = ano;
    }
}
