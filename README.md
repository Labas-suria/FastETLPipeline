# FastETLPipeline

O FastETLPipeline é uma aplicação python desenvolvida com o objetivo de fornecer uma plataforma para criação e execução de pipelines ETL para dados de forma fácil e simplificada.

Ferramenta ideal para pequenos times de dados que têm seu foco na análise, simplificando todo o processo de extração, transformação e carga dos dados que precede a criação das visualizações e dashboards.

# Features
1. [TXT (extração)](https://github.com/Labas-suria/FastETLPipeline/tree/dev_list_to_dataframe?tab=readme-ov-file#txt)
2. [CSV (extração e load)](https://github.com/Labas-suria/FastETLPipeline/tree/dev_list_to_dataframe?tab=readme-ov-file#csv)
3. [Google Sheets (extração e load)](https://github.com/Labas-suria/FastETLPipeline/tree/dev_list_to_dataframe?tab=readme-ov-file#google-sheets)
4. [MySQL (extração e load)](https://github.com/Labas-suria/FastETLPipeline/tree/dev_list_to_dataframe?tab=readme-ov-file#mysql)
5. [PostgreSQL (extração e load)](https://github.com/Labas-suria/FastETLPipeline/tree/dev_list_to_dataframe?tab=readme-ov-file#postgresql)
6. Conectores (extração, load e transformação)
	1. Conector para extração
	2. Conector para transformação
	3. Conector para carga
## TXT
A aplicação permite a extração de dados de arquivos ".txt", desde que os dados estejam em um padrão tabular, permitindo a extração dos valores de cada coluna dado um separador determinado na configuração do nó.
> Ex: 
> 
> a, b, c
> 
> 1, 2, 3
> 
> z, x, y
> 
> É possível extrair os dados do .txt acima, configurando o separador como uma vírgula.
> 
### EXTRAINDO dados de um txt:
Para extrair dados de um arquivo ".txt" é necessário que o nó de extração no arquivo de configuração da pipeline (.json) tenha a seguinte configuração:
```JSON
 "(1)extract_node_name": {
            "(2)class": "extract", 
            "(3)type": "txt",
            "params": {
                   "(4)path": "path\\to\\file.txt",
                   "(5)separator": "",
                   "(6)first_line": "",
                   "(7)last_line": "",
                   "(8)header_line": true
            }
 }
```

1. **"extract_node_name"** - A string que dá nome ao nó.
2. **"class": "extract"** - Esta descrição determina para o orquestrador que o nó é da classe de extração. 
3. **"type": "txt"** - Esta descrição determina para o orquestrador que o o tipo da extração será "txt". Ou seja, os dados serão extraídos de um arquivo ".txt".
4.  **"path"** - Aqui deverá ser passada a string com o caminho até o arquivo txt de onde os dados serão extraídos. 
5.  **"separator"** - Aqui será passada o caractere que define onde começa e termina o valor de cada coluna. O separador dos valores no arquivo (vírgula, ponto e vírgula, tabulação, espaço...).
6.  **"first_line"** - ***Valor não obrigatório.*** Inteiro que determina a partir de qual linha o programa extrairá os valores. Caso o valor não seja fornecido, o programa pegará a primeira linha do arquivo.
7.  **"last_line"** - ***Valor não obrigatório.*** Inteiro que determina a última linha que será extraída do arquivo. Caso o valor não seja fornecido, o programa extraírá até a ultima linha do arquivo.
8.  **"header_line"** - ***Valor não obrigatório.*** Booleano que determina se a primeira linha do arquivo possue os nomes de cada coluna. 
Se o valor for "true", o programa irá retirar a primeira linha do arquivo, a considerando como a linha de cabeçalho. Logo, para o programa, a nova primeira linha do arquivo (a linha 0) será a que segue a linha de cabeçalho, e a última terá como novo índice o valor antigo -1.  

## CSV
A aplicação permite a extração e carga de dados de arquivos ".csv". Extraindo de ".csv" para um pandas.DataFrame e carregando dados de um pandas.DataFrame para um arquivo ".csv".

### EXTRAINDO dados de um csv:
Para extrair dados de um arquivo ".csv" é necessário que o nó de extração no arquivo de configuração da pipeline (.json) tenha a seguinte configuração:
```JSON
 "(1)extract_node_name": {
            "(2)class": "extract", 
            "(3)type": "csv",
            "params": {
                   "(4)file_path": "path\\to\\file.csv",
            }
}
```

1. **"extract_node_name"** - A string que dá nome ao nó.
2. **"class": "extract"** - Esta descrição determina para o orquestrador que o nó é da classe de extração. 
3. **"type": "csv"** - Esta descrição determina para o orquestrador que o o tipo da extração será "csv". Ou seja, os dados serão extraídos de um arquivo ".csv".
4.  **"file_path"** - Aqui deverá ser passada a string com o caminho até o arquivo csv de onde os dados serão extraídos. 

### CARREGANDO dados em um csv:
Para carregar os dados em um arquivo ".csv" é necessário que o nó de carga no arquivo de configuração da pipeline (.json) tenha a seguinte configuração:
```JSON
 "(1)load_node_name": {
            "(2)class": "load", 
            "(3)type": "csv",
            "params": {
                   "(4)file_path": "path\\to\\file.csv",
            }
}
```

1. **"load_node_name"** - A string que dá nome ao nó.
2. **"class": "load"** - Esta descrição determina para o orquestrador que o nó é da classe de carga. 
3. **"type": "csv"** - Esta descrição determina para o orquestrador que o o tipo da carga será "csv". Ou seja, a carga será realizada em um arquivo ".csv".
4.  **"file_path"** - Aqui deverá ser passada a string com o caminho até o arquivo csv onde os dados serão armazenados. 

## Google Sheets
A aplicação permite a extração e carga de dados em planilhas no Google Sheets via API. Para a utilização da classe é necessário configurar o ambiente para a autenticação via OAuth (Consulte a [documentação da API](https://developers.google.com/docs/api/quickstart/python?hl=pt-br)).

Uma vez que o arquivo "credentials.json" foi gerado, o salve na pasta raíz do projeto. Por padrão os arquivos  "credentials.json" e "token.json" serão procurados pelo programa na raíz, mas é possível alterar os caminhos na opção "Set Variables" da aplicação, ou diretamente no arquivo "variables_paths.json" do projeto.

### EXTRAINDO dados de uma planilha:
Para extrair os dados de uma planilha no Google Sheets é necessário que o nó de extração no arquivo de configuração da pipeline (.json) tenha a seguinte configuração:

```JSON
 "(1)extract_node_name": {
            "(2)class": "extract", 
            "(3)type": "g_sheets",
            "params": {
                   "(4)sheet_id": "sheetidstring123",
                   "(5)extract_range": "pagename!A:AZ"
            }
}
```

1. **"extract_node_name"** - A string que dá nome ao nó.
2. **"class": "extract"** - Esta descrição determina para o orquestrador que o nó é da classe de extração. 
3. **"type": "g_sheets"** - Esta descrição determina para o orquestrador que o o tipo da extração é "g_sheets". Ou seja, a extração será realizada de uma planilha no Google Sheets.
4.  **"sheet_id"** - Aqui deverá ser passada a string com o id da planilha, que se encontra na URL da planilha. 

> Ex: *"https:/docs.google.com/spreadsheets/d/**sheetidstring123**/edit?"*
5. **"extract_range"** - String com o nome da página de o intervalo de onde os dados serão extraídos: "*{nome da página}*!*{intervalo}*"

Se a extração correr corretamente os dados da página serão armazenados em um "pandas.Dataframe".

### CARREGANDO dados em uma planilha:
Para carregar os dados em uma planilha no Google Sheets é necessário que o nó de extração no arquivo de configuração da pipeline (.json) tenha a seguinte configuração:

```JSON
 "(1)load_node_name": {
            "(2)class": "load", 
            "(3)type": "g_sheets",
            "params": {
                   "(4)sheet_id": "sheetidstring123",
                   "(5)update_range": "pagename!A:AZ"
            }
}
```

1. **"extract_node_name"** - A string que dá nome ao nó.
2. **"class": "load"** - Esta descrição determina para o orquestrador que o nó é da classe de carga. 
3. **"type": "g_sheets"** - Esta descrição determina para o orquestrador que o o tipo da carga é "g_sheets". Ou seja, a carga será realizada em uma planilha no Google Sheets.
4.  **"sheet_id"** - Aqui deverá ser passada a string com o id da planilha, que se encontra na URL da planilha. 
> Ex: *"https:/docs.google.com/spreadsheets/d/**sheetidstring123**/edit?"*
5. **"update_range"** - String com o nome da página de o intervalo onde os dados serão carregados: "*{nome da página}*!*{intervalo}*"

Os dados serão carregados de "pandas.Dataframe" tratado conforme a configuração da pipeline e armazenados na planilha determinada pelo usuário.

## MySQL
A aplicação permite a extração e carga de dados de bancos de dados MySQL. Para a utilização da classe é necessário o arquivo *mysql_config.json* com as credenciais de acesso para o banco de dados.
O arquivo *mysql_config.json* deverá ter as seguintes informações:
```JSON
{
	"user": "user_name",
	"password": "userpassword",
	"host": "iptodatabase",
	"database": "databasename"
}
```
Uma vez que o arquivo *mysql_config.json* foi criado, é necessário que seja configurado na opção "Set Variables" da aplicação ou diretamente no arquivo "variables_paths.json" do projeto a variável que possuirá o caminho para o arquivo *mysql_config.json*.
>variables_paths.json:
>"mysql_config.json": "C:\\todo\\caminho\\ate\\mysql_config.json"

>Opção Set Variables:
> **Na primeira caixa**: mysql_config.json, **na segunda caixa**: C:\\todo\\caminho\\ate\\mysql_config.json

### EXTRAINDO dados de um banco de dados MySQL:
Para extrair dados de uma tabela em um banco de dados MySQL é necessário que o nó de extração no arquivo de configuração da pipeline (.json) tenha a seguinte configuração:
```JSON
 "(1)extract_node_name": {
	 "(2)class": "extract",
	 "(3)type": "mysql",
	 "params": {
		 "(4)sql_query": "SELECT * FROM table_name;"
	 }
 }
```

1. **"extract_node_name"** - A string que dá nome ao nó.
2. **"class": "extract"** - Esta descrição determina para o orquestrador que o nó é da classe de extração. 
3. **"type": "mysql"** - Esta descrição determina para o orquestrador que o o tipo da extração será "mysql". Ou seja, os dados serão extraídos de um arquivo banco de dados MySQL.
4.  **"sql_query"** - Aqui deverá ser passada a string com a query do tipo select que extrairá os dados da tabela.

### CARREGANDO dados em um banco de dados MySQL:
Para carregar dados em uma tabela em um banco de dados MySQL é necessário que o nó de carga no arquivo de configuração da pipeline (.json) tenha a seguinte configuração:
```JSON
 "(1)load_node_name": {
	 "(2)class": "load",
	 "(3)type": "mysql",
	 "params": {
		 "(4)table_name": "tablename",
	 }
 }
```

1. **"extract_node_name"** - A string que dá nome ao nó.
2. **"class": "extract"** - Esta descrição determina para o orquestrador que o nó é da classe de extração. 
3. **"type": "mysql"** - Esta descrição determina para o orquestrador que o o tipo da extração será "mysql". Ou seja, os dados serão extraídos de um arquivo banco de dados MySQL.
4.  **"table_name"** - Aqui deverá ser passada a string com o nome de tabela onde os dados serão carregados.

## PostgreSQL
A aplicação permite a extração e carga de dados de bancos de dados PostgreSQL. Para a utilização da classe é necessário o arquivo *postgresql_config.json* com as credenciais de acesso para o banco de dados.
O arquivo *postgresql_config.json* deverá ter as seguintes informações:
```JSON
{
	"user": "user_name",
	"password": "userpassword",
	"host": "iptodatabase",
	"database": "databasename"
}
```
Uma vez que o arquivo *postgresql_config.json* foi criado, é necessário que seja configurado na opção "Set Variables" da aplicação ou diretamente no arquivo "variables_paths.json" do projeto a variável que possuirá o caminho para o arquivo *postgresql_config.json*.
>variables_paths.json:
>"postgresql_config.json": "C:\\todo\\caminho\\ate\\postgresql_config.json"

>Opção Set Variables:
> **Na primeira caixa**: postgresql_config.json, **na segunda caixa**: C:\\todo\\caminho\\ate\\postgresql_config.json

### EXTRAINDO dados de um banco de dados PostgreSQL:
Para extrair dados de uma tabela em um banco de dados PostgreSQL é necessário que o nó de extração no arquivo de configuração da pipeline (.json) tenha a seguinte configuração:
```JSON
 "(1)extract_node_name": {
	 "(2)class": "extract",
	 "(3)type": "postgresql",
	 "params": {
		 "(4)sql_query": "SELECT * FROM table_name;"
	 }
 }
```

1. **"extract_node_name"** - A string que dá nome ao nó.
2. **"class": "extract"** - Esta descrição determina para o orquestrador que o nó é da classe de extração. 
3. **"type": "postgresql"** - Esta descrição determina para o orquestrador que o o tipo da extração será "postgresql". Ou seja, os dados serão extraídos de um arquivo banco de dados PostgreSQL.
4.  **"sql_query"** - Aqui deverá ser passada a string com a query do tipo select que extrairá os dados da tabela.

### CARREGANDO dados em um banco de dados PostgreSQL:
Para carregar dados em uma tabela em um banco de dados PostgreSQL é necessário que o nó de carga no arquivo de configuração da pipeline (.json) tenha a seguinte configuração:
```JSON
 "(1)load_node_name": {
	 "(2)class": "load",
	 "(3)type": "postgresql",
	 "params": {
		 "(4)table_name": "tablename",
	 }
 }
```

1. **"extract_node_name"** - A string que dá nome ao nó.
2. **"class": "extract"** - Esta descrição determina para o orquestrador que o nó é da classe de extração. 
3. **"type": "postgresql"** - Esta descrição determina para o orquestrador que o o tipo da extração será "postgresql". Ou seja, os dados serão extraídos de um arquivo banco de dados PostgreSQL.
4.  **"table_name"** - Aqui deverá ser passada a string com o nome de tabela onde os dados serão carregados.
## Conectores
A aplicação permite que o usuário possa acoplar código Python em qualquer etapa do ETL, seja para extração, transformação ou carga.

Um “conector” é uma classe escrita pelo usuário, que herda de uma das classes abstratas (conforme a operação que for realizar: AbstractExtract para extração, AbstractTransform para transformação e AbstractLoad para carga) e implementa o único método abstrato assinado em cada uma.

Para isso, o script com o código deve estar dentro da pasta “CONNECTOR_SOURCE” na raiz do projeto
### Conector para extração
Para acoplar na pipeline um conector para extração, o script implementado deve ter o seguinte padrão:
```Python
from abstract_connectors.interfaces import AbstractExtract

class ClassName(AbstractExtract):  
    def __init__(self, **kwargs):  
        ""Aqui você deve inicializar os atributos necessários à sua classe. 
        É uma boa prática que os parâmetros sejam passados dentro do 
        dicionário “params” no nó correspondente no arquivo de configuração 
        da pipeline (.json).""  
        
        self.atributo = kwargs['meu_atributo'] # exemplo, variavel receberá "params": { ..., "meu_atributo": "valor", ...}.
  
    def extract(self) -> DataFrame:
	    ""Aqui o código para extração será escrito. É obrigatório que, 
	    mesmo que não seja utilizado na extração, método deverá retornar 
	    os dados como um pandas.Dataframe""
```

Uma vez que o seu código esteja na pasta “CONNECTOR_SOURCE” e implemente "AbstractExtract", é necessário que o nó de extração no arquivo de configuração da pipeline (.json) tenha a seguinte configuração:

```JSON
 "(1)connector_node_name": {
	 "(2)class": "extract",
	 "(3)type": "connector",
	 "params": {
		 "(4)script_import": "CONNECTORS_SOURCE.script_name", 
		 "(5)class_name": "ClassName", 
		 "(6)atributo_1": "valor_atributo_1",
		 "(6)atributo_1": "valor_atributo_2",
		 "(6)atributo_n": "valor_atributo_n"
	}
 }
```
1. **"connector_node_name"** - A string que dá nome ao nó.
2. **"class": "extract"** - Esta descrição determina para o orquestrador que o nó é da classe de extração. 
3. **"type": "connector"** - Esta descrição determina para o orquestrador que o o tipo da extração será "connector". Ou seja, os dados serão extraídos da fonte/forma que o usuário implementar. 
4.  **"script_import"** - Aqui deverá ser passada a string com o nome do script (.py) dentro da pasta "CONNECTORS_SOURCE".
5. **"class_name"** - Aqui deverá passada a string com o nome da classe escrita no script pelo usuário.
6. **Atributos** - Poderão ser adicionados aqui os atributos para inicialização da classe (um caminho para um arquivo de credenciais, uma url, entre outros). 

### Conector para transformação
>**Importante:** O seu método para transformação receberá um pandas.Dataframe.
>
Para acoplar na pipeline um conector para transformação, o script implementado deve ter o seguinte padrão:
```Python
from abstract_connectors.interfaces import AbstractTransform

class ClassName(AbstractTransform):  
    def __init__(self, data: DataFrame, **kwargs):  
        ""Aqui você deve inicializar os atributos necessários à sua classe. 
        É uma boa prática que os parâmetros sejam passados dentro do 
        dicionário “params” no nó correspondente no arquivo de configuração 
        da pipeline (.json).""  
        
       self.data = data # Variável que guarda o Dataframe recebido da pipeline.
  
    def apply(self) -> DataFrame:
	    ""Aqui o código para transformação será escrito. É obrigatório que, 
	    mesmo que não seja utilizado na transformação, método deverá retornar 
	    os dados como um pandas.Dataframe""
```

Uma vez que o seu código esteja na pasta “CONNECTOR_SOURCE” e implemente "AbstractTransform", é necessário que o nó de transformação no arquivo de configuração da pipeline (.json) tenha a seguinte configuração:

```JSON
 "(1)connector_node_name": {
	 "(2)class": "transform",
	 "(3)type": "connector",
	 "params": {
		 "(4)script_import": "CONNECTORS_SOURCE.script_name", 
		 "(5)class_name": "ClassName", 
		 "(6)atributo_1": "valor_atributo_1",
		 "(6)atributo_1": "valor_atributo_2",
		 "(6)atributo_n": "valor_atributo_n"
	}
 }
```
1. **"connector_node_name"** - A string que dá nome ao nó.
2. **"class": "transform"** - Esta descrição determina para o orquestrador que o nó é da classe de transformação. 
3. **"type": "connector"** - Esta descrição determina para o orquestrador que o o tipo da transformação será "connector". Ou seja, os dados serão transformados da forma que o usuário implementar. 
4.  **"script_import"** - Aqui deverá ser passada a string com o nome do script (.py) dentro da pasta "CONNECTORS_SOURCE".
5. **"class_name"** - Aqui deverá passada a string com o nome da classe escrita no script pelo usuário.
6. **Atributos** - Poderão ser adicionados aqui os atributos para inicialização da classe. 

### Conector para carga
>**Importante:** O seu método para carga receberá um pandas.Dataframe.
>
Para acoplar na pipeline um conector para transformação, o script implementado deve ter o seguinte padrão:
```Python
from abstract_connectors.interfaces import AbstractLoad

class ClassName(AbstractLoad):  
    def __init__(self, data: DataFrame, **kwargs):  
        ""Aqui você deve inicializar os atributos necessários à sua classe. 
        É uma boa prática que os parâmetros sejam passados dentro do 
        dicionário “params” no nó correspondente no arquivo de configuração 
        da pipeline (.json).""  
        
       self.data = data # Variável que guarda o Dataframe recebido da pipeline.
  
    def load(self):
	    ""Aqui o código para carga será escrito.""
```

Uma vez que o seu código esteja na pasta “CONNECTOR_SOURCE” e implemente "AbstractLoad", é necessário que o nó de carga no arquivo de configuração da pipeline (.json) tenha a seguinte configuração:

```JSON
 "(1)connector_node_name": {
	 "(2)class": "load",
	 "(3)type": "connector",
	 "params": {
		 "(4)script_import": "CONNECTORS_SOURCE.script_name", 
		 "(5)class_name": "ClassName", 
		 "(6)atributo_1": "valor_atributo_1",
		 "(6)atributo_1": "valor_atributo_2",
		 "(6)atributo_n": "valor_atributo_n"
	}
 }
```
1. **"connector_node_name"** - A string que dá nome ao nó.
2. **"class": "load"** - Esta descrição determina para o orquestrador que o nó é da classe de carga. 
3. **"type": "connector"** - Esta descrição determina para o orquestrador que o o tipo da carga será "connector". Ou seja, os dados serão carregado na fonte/forma que o usuário implementar. 
4.  **"script_import"** - Aqui deverá ser passada a string com o nome do script (.py) dentro da pasta "CONNECTORS_SOURCE".
5. **"class_name"** - Aqui deverá passada a string com o nome da classe escrita no script pelo usuário.
6. **Atributos** - Poderão ser adicionados aqui os atributos para inicialização da classe (um caminho para um arquivo de credenciais, uma url, entre outros). 
