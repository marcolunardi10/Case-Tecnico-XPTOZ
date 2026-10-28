# XPTOZ: Análise de domínios de e-mail

Pipeline de dados que carrega cadastros de pessoas no PostgreSQL, transforma os dados em camadas com **dbt** e consolida a quantidade de registros por domínio de e-mail. A execução é orquestrada diariamente pelo **Apache Airflow**.

## Arquitetura

```
 generate_data.py          dbt (PostgreSQL)                                         Airflow
 (Python + Faker)
        │
        ▼
 ┌──────────────┐   ┌──────────────────┐   ┌───────────────────────┐   ┌─────────────────────┐
 │ RAW          │──▶│ STAGING          │──▶│ INTERMEDIATE          │──▶│ MARTS               │
 │ public.      │   │ staging.         │   │ intermediate.         │   │ marts.              │
 │ users_info   │   │ stg_users_info   │   │ int_users_enriquecido │   │ mart_dominios_email │
 └──────────────┘   └──────────────────┘   └───────────────────────┘   └─────────────────────┘
  dados brutos       maiúsculas, trim,      primeiro/último nome,        total de registros
                     remove "Sr./Dra.",     domínio do e-mail            por domínio
                     created_at/updated_at
```

Cada camada fica em um **schema próprio** no PostgreSQL (`public`, `staging`, `intermediate`, `marts`), o que deixa a separação visível direto no banco.

### DAG `xptoz_dominios_email`

```
carregar_dados ──▶ testar_raw ──▶ dbt_staging ──▶ dbt_intermediate ──▶ dbt_marts
 (opcional)        dbt test        dbt build       dbt build            dbt build
```

- **Agendamento:** todo dia às 06:00 (`America/Sao_Paulo`), `catchup=False`, uma execução por vez.
- **Uma task por camada:** cada uma roda `dbt build --select <camada>`, ou seja, os modelos **e** os testes da camada. Se um teste falha, as camadas seguintes não rodam, e dado inconsistente não chega ao mart.
- **`carregar_dados`:** por padrão é pulada (*skipped*), porque a carga raw é feita pelo script. Ao disparar a DAG manualmente com `carregar_novos_dados = true`, ela gera e insere novos registros antes das transformações. Isso facilita demonstrar a atualização dos resultados.
- **Retries:** 2 tentativas com intervalo de 2 minutos.

### Qualidade dos dados (testes dbt)

| Camada | Testes |
|---|---|
| raw | `id` único e não nulo, `email` único |
| staging | `id`/`email` únicos e não nulos, formato de e-mail válido (`email_valido`), idade entre 18 e 80 (`entre`) |
| intermediate | `id` único, `primeiro_nome` e `dominio_email` não nulos |
| marts | `dominio_email` único, `total_registros` ≥ 1 e **soma dos totais = total de linhas da raw** (nenhum registro perdido ou duplicado entre camadas) |

`email_valido` e `entre` são testes genéricos próprios, em `dbt/xptoz/macros/tests/`, para o projeto não depender de pacotes externos.

## Estrutura do projeto

```
.
├── dags/
│   └── xptoz_dominios_email.py        # DAG do Airflow
├── dbt/xptoz/
│   ├── dbt_project.yml                # materialização e schema por camada
│   ├── profiles.yml                   # conexão via variáveis de ambiente
│   ├── macros/
│   │   ├── generate_schema_name.sql   # usa staging/intermediate/marts como schema
│   │   ├── normalizar_texto.sql       # trim + colapsa espaços + upper
│   │   └── tests/                     # testes genéricos: email_valido, entre
│   ├── models/
│   │   ├── staging/                   # _sources.yml + stg_users_info
│   │   ├── intermediate/              # int_users_enriquecido
│   │   └── marts/                     # mart_dominios_email
│   └── tests/                         # teste singular de reconciliação raw x mart
├── scripts/
│   └── generate_data.py               # geração e carga dos dados (seção 8 do case)
├── docker/postgres/init.sql           # cria o banco de metadados do Airflow
├── Dockerfile                         # Airflow 3.1 + dbt-postgres (em venv isolado)
├── docker-compose.yml                 # PostgreSQL 16 + Airflow
├── requirements.txt
└── .env.example                       # modelo de variáveis de ambiente
```

## Tecnologias

| Ferramenta | Versão | Uso |
|---|---|---|
| PostgreSQL | 16 | armazenamento (raw + camadas dbt) e metadados do Airflow |
| Python | 3.12 | `psycopg2` + `Faker` para gerar e carregar os dados |
| dbt-core / dbt-postgres | 1.12 / 1.11 | transformações em camadas e testes |
| Apache Airflow | 3.1 | orquestração e agendamento |
| Docker Compose | | ambiente local |

O dbt é instalado em um virtualenv separado dentro da imagem (`/opt/dbt_venv`), para não conflitar com as dependências do Airflow.

## Como executar

### Pré-requisitos

- Docker Desktop (ou Docker Engine + Compose v2)
- Portas livres: `5432` (PostgreSQL) e `8080` (Airflow)

### 1. Configurar as variáveis de ambiente

```bash
cp .env.example .env
```

Edite o `.env` se quiser, principalmente o `AIRFLOW_JWT_SECRET`. **O `.env` está no `.gitignore`**: as credenciais nunca vão para o repositório. O script de carga, o `profiles.yml` do dbt e a DAG leem a conexão dessas variáveis:

| Variável | Descrição |
|---|---|
| `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` | conexão com o banco analítico |
| `DB_SSLMODE` | `disable` no Postgres local, `require` no Render |
| `POSTGRES_USER`, `POSTGRES_PASSWORD` | usuário do container PostgreSQL local |
| `AIRFLOW_JWT_SECRET` | segredo interno do Airflow 3 |

#### Usando PostgreSQL no Render (opcional)

1. Crie a instância seguindo [a documentação do Render](https://render.com/docs/postgresql-creating-connecting).
2. Em **Connections → External Database URL**, copie host, usuário, senha e banco para as variáveis `DB_*` do `.env`.
3. Defina `DB_SSLMODE=require`.

O container `postgres` continua sendo usado, mas só para os metadados do Airflow.

### 2. Subir o ambiente

```bash
docker compose up -d --build
```

A primeira subida leva alguns minutos (build da imagem). A UI do Airflow fica em **http://localhost:8080**. Neste ambiente local o login está desativado (`SIMPLE_AUTH_MANAGER_ALL_ADMINS=True`), o que **não deve ser usado em produção**.

### 3. Carregar os dados (raw)

```bash
docker compose exec airflow python /opt/airflow/scripts/generate_data.py
```

O script cria a `users_info` (se não existir) e insere até 1.000 registros, ignorando e-mails duplicados:

```
INFO 1000 registros processados, 996 inseridos, 4 ignorados (e-mail duplicado). Total na tabela: 996
```

Opções: `--qtd N` define a quantidade e `--dominios-variados` gera domínios realistas (veja a observação abaixo).

> **Observação sobre os domínios:** o script do case usa `fake.email()`, que por padrão usa só os domínios `example.com`, `example.net` e `example.org`. Com isso, o mart fica com 3 linhas. A flag `--dominios-variados` usa `fake.email(safe=False)` e gera domínios como `gmail.com`, `bol.com.br`, `uol.com.br` e domínios de empresas, o que deixa a análise mais interessante. O padrão continua igual ao do case.

### 4. Executar a DAG no Airflow

Pela UI: abra a DAG `xptoz_dominios_email`, ative-a (toggle) e clique em **Trigger**. Para também carregar novos dados na mesma execução, marque `carregar_novos_dados` (e, se quiser, `dominios_variados`) no formulário de parâmetros.

Pela CLI:

```bash
docker compose exec airflow airflow dags unpause xptoz_dominios_email
docker compose exec airflow airflow dags trigger xptoz_dominios_email \
  --conf '{"carregar_novos_dados": true, "dominios_variados": true}'
docker compose exec airflow airflow dags list-runs xptoz_dominios_email
```

Depois de ativada, a DAG roda sozinha todos os dias às 06:00.

### 5. Executar o dbt manualmente (opcional)

```bash
docker compose exec -w /opt/airflow/dbt/xptoz airflow dbt build                 # tudo
docker compose exec -w /opt/airflow/dbt/xptoz airflow dbt build --select marts  # uma camada
docker compose exec -w /opt/airflow/dbt/xptoz airflow dbt docs generate         # documentação
```

> No Git Bash do Windows, prefixe os comandos com `MSYS_NO_PATHCONV=1`. Sem isso, o Git Bash converte `/opt/...` em um caminho do Windows. No PowerShell, os comandos funcionam como estão.

### 6. Consultar os resultados

```bash
docker compose exec postgres psql -U xptoz -d xptoz
```

```sql
-- Resultado final
SELECT dominio_email, total_registros
FROM marts.mart_dominios_email
ORDER BY total_registros DESC;

-- Camadas intermediárias
SELECT * FROM staging.stg_users_info LIMIT 10;
SELECT * FROM intermediate.int_users_enriquecido LIMIT 10;

-- Validação da carga (seção 8.5 do case)
SELECT COUNT(*) AS total_registros FROM public.users_info;
```

## Evidências de execução

`dbt build` completo (3 modelos, 20 testes):

```
Finished running 3 table models, 20 data tests in 0 hours 0 minutes and 1.26 seconds (1.26s).
Completed successfully
Done. PASS=23 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=23
```

Execuções da DAG (uma agendada e uma manual com carga de novos dados), com as 5 tasks em `success`:

```
dag_id                run_id                                             state
xptoz_dominios_email  manual__2026-10-24T14:25:44.860686+00:00_U3pVKnoH  success
xptoz_dominios_email  scheduled__2026-10-24T09:00:00+00:00               success
```

Mart após as duas cargas (1.000 registros com o padrão do case + 1.000 com `--dominios-variados`): 246 domínios, somando 1.996 registros, igual ao total da raw.

```
 dominio_email | total_registros
---------------+-----------------
 EXAMPLE.COM   |             355
 EXAMPLE.NET   |             322
 EXAMPLE.ORG   |             319
 IG.COM.BR     |             100
 UOL.COM.BR    |              84
 GMAIL.COM     |              81
 HOTMAIL.COM   |              80
 BOL.COM.BR    |              79
 YAHOO.COM.BR  |              72
 ...
```

## Decisões de modelagem

- **Textos em maiúsculas, inclusive o e-mail**, como pede o case. A macro `normalizar_texto` também remove espaços nas pontas, colapsa espaços repetidos e converte string vazia em `NULL`.
- **Pronomes de tratamento** (`Sr.`, `Sra.`, `Srta.`, `Dr.`, `Dra.`), que o Faker coloca em parte dos nomes, são removidos no staging. Sem isso, `primeiro_nome` viraria `DRA.`.
- **Último nome** é a última palavra do nome (`MARIA HELENA DUARTE` → `DUARTE`). Nomes com uma palavra só ficam com `ultimo_nome` nulo.
- **Domínio** é tudo após o `@`. O teste `email_valido` garante que existe exatamente um `@`.
- **`created_at` / `updated_at`** usam `now()`, como especificado. No intermediate eles são herdados do staging. No mart, refletem o momento da última consolidação.
- **Materialização `table`** em todas as camadas: o volume é pequeno, e as tabelas físicas deixam cada camada fácil de inspecionar.

## Encerrando o ambiente

```bash
docker compose down        # para os containers, mantém os dados
docker compose down -v     # remove também o volume do PostgreSQL
```
