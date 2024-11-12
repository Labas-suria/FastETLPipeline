# FastETLPipeline

O FastETLPipeline é uma aplicação python desenvolvida com o objetivo de fornecer uma plataforma para criação e execução de pipelines ETL para dados de forma fácil e simplificada.

Ferramenta ideal para pequenos times de dados que têm seu foco na análise, simplificando todo o processo de extração, transformação e carga dos dados que precede a criação das visualizações e dashboards.

# Features
1. TXT (extração)
2. CSV (extração e load)
3. Google Sheets
## TXT
A aplicação 
## CSV
A aplicação 
## Google Sheets
A aplicação permite a extração e carga de dados em planilhas no Google Sheets via API. Para a utilização da classe é necessário configurar o ambiente para a autenticação via OAuth (Consulte a [documentação da API](https://developers.google.com/docs/api/quickstart/python?hl=pt-br)).

Uma vez que o arquivo "credentials.json" foi gerado, o salve na pasta raíz do projeto. Por padrão os arquivos  "credentials.json" e "token.json" serão procurados pelo programa na raíz, mas é possível alterar os caminhos na opção "Set Variables" da aplicação, ou diretamente no arquivo "variables_paths.json" do projeto.

### EXTRAINDO dados de uma planilha:
Para extrair os dados de uma planilha no Google Sheets é necessário que o nó de extração no arquivo de configuração da pipeline (.json) tenha a seguinte configuração:

```JSON
 (1)"extract_node_name": {
            (2)"class": "extract", 
            (3)"type": "g_sheets",
            "params": {
                   (4)"sheet_id": "sheetidstring123",
                   (5)"extract_range": "pagename!A:AZ"
            }
```

1. **"extract_node_name"** - A string que dá nome ao nó.
2. **"class": "extract"** - Esta descrição determina para o orquestrador que o nó é da classe de extração. 
3. **"type": "g_sheets"** - Esta descrição determina para o orquestrador que o o tipo da extração é "g_sheets". Ou seja, a extração será realizada de uma planilha no Google Sheets.
4.  **"sheet_id"** - Aqui deverá ser passada a string com o id da planilha, que se encontra na URL da planilha. 

> Ex: *"https:/docs.google.com/spreadsheets/d/**sheetidstring123**/edit?"*
5. **"extract_range"** - String com o nome da página de o intervalo de onde os dados serão extraídos: "*{nome da página}*!*{intervalo}*"

Se a extração correr corretamente os dados da página serão armazenados em um "pandas.Dataframe".


