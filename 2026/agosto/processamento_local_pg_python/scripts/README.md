# 📘 Documentação da Aula

## 📦 Pacotes necessários

Instale as bibliotecas para executar os códigos desta aula:

    pip install pandas pyarrow sqlalchemy psycopg2-binary python-dotenv

---

## ⚙️ Configuração do ambiente

- Crie o arquivo `.env` na raiz do projeto.  
- Adicione o `.env` no arquivo `.gitignore` para evitar versionar informações sensíveis.  

---

## 🗄️ Criação do banco de dados

Instalação:

sudo apt update
sudo apt install postgresql postgresql-contrib

sudo service postgresql start

sudo service postgresql status


Acesse o PostgreSQL:

    sudo -u postgres psql

Criar usuário:

CREATE USER aula_user WITH PASSWORD 'aula123';

CREATE DATABASE aula_dados
    OWNER aula_user;

Crie o banco de dados:

    CREATE DATABASE medical_analysis;

---

## ▶️ Execução do script

Navegue até a pasta do script no terminal:

    C:\Users\geova\OneDrive\Documentos\empregadados\codigo\aulas_empregadados\scripts\aula_1_banco>

⚠️ **Atenção:**  
- O script deve ser executado **dentro da pasta do script**.  
- Se rodar em outro diretório, as variáveis de ambiente não serão carregadas.  
- Alternativamente, insira no código o **caminho absoluto** do arquivo `.env`.

---

## 🔄 Desfazer commits no Git

Para desfazer o último commit (ou mais de um, ajustando o número):

    git reset --hard HEAD~1


Dados: https://synthea.mitre.org/