
## GeoEduca - Java System
Sistema contemplando o back-end do projeto da empresa fictícia GeoEduca, com um projeto de categorização de notas de enem por estado para análise posterior. 

Acesse: https://github.com/Grupo-1-Pesquisa-e-Inovacao
## Tecnologias utilizadas

**Back-end:** Java 21

**Banco de Dados:** MySQL 5.7


## Requisitos

**Java:** Versão 8+

**MySQL Server:** Versão 5.7+
## Instalação

Instale as dependências com Maven

```bash
git clone https://github.com/Grupo-1-Pesquisa-e-Inovacao/Java
cd Java
mvn clean install
```
    
## Uso

Estruturar a sua conexão com o banco

```bash
    public ConexaoBanco() {
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        basicDataSource.setUrl("jdbc:mysql://ipBancoDeDados/nomeBancoDeDados?useSSL=false&serverTimezone=UTC");
        basicDataSource.setUsername("seuUsuario");
        basicDataSource.setPassword("suaSenha");
        
        this.basicDataSource = basicDataSource;
        this.jdbcTemplate = new JdbcTemplate(basicDataSource);
    }
```

Estruturar os arquivos S3, junto com o nome deles e o nome do bucket
```bash
# classe S3LeituraEstados.java
    private final String bucket = "nomeSeuBucket";
    private final String key = "nomeSeuArquivo";
```
## Funcionalidades

- Leitura de arquivos em um bucket S3
- Conexão com banco MySQL Server
- Inserção de dados no banco
- Tratamento de erros e sistema de logs


## Autores

- [@maath-soares]https://github.com/Matheus-Carvalho-Soares

