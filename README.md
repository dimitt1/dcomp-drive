# DComp Drive — Sistema Distribuído de Armazenamento de Arquivos

Sistema distribuído de armazenamento de arquivos em blocos (*chunks*) com replicação tolerante a falhas, verificação de integridade ponta a ponta por hash (`SHA-256`), criptografia autenticada opcional no cliente (`AES-256-GCM`) e recuperação automática (*self-healing*), desenvolvido para a disciplina de **Sistemas Distribuídos (COMP0470)** da **Universidade Federal de Sergipe (UFS)**.

O projeto inspira-se nas arquiteturas do **HDFS**, **Google File System (GFS)**, **Ceph** e **MinIO**, separando estritamente o **Plano de Controle** (gerenciamento da árvore de diretórios, metadados, alocação, *leases* de concorrência e monitoramento de nós) do **Plano de Dados** (transferência paralela de blocos diretamente entre o Cliente e os Nós de Armazenamento, além de cópia direta nó-a-nó na re-replicação).

---

## Sumário

1. [Arquitetura e Papéis](#1-arquitetura-e-papéis)
2. [Decisões de Projeto (Consistência, Concorrência e Criptografia)](#2-decisões-de-projeto-consistência-concorrência-e-criptografia)
3. [Pré-requisitos e Instalação](#3-pré-requisitos-e-instalação)
4. [Subindo o Cluster com Docker Compose (1 Controlador + 5 Nós)](#4-subindo-o-cluster-com-docker-compose-1-controlador--5-nós)
5. [Interface Web e Endpoints de Observabilidade](#5-interface-web-e-endpoints-de-observabilidade)
6. [Guia Completo da CLI (Exemplos de todos os comandos)](#6-guia-completo-da-cli-exemplos-de-todos-os-comandos)
7. [Roteiro Prático de Demonstração (Tolerância a Falhas e Corrupção)](#7-roteiro-prático-de-demonstração-tolerância-a-falhas-e-corrupção)
8. [Configurações do Sistema (`config.yaml` e Variáveis de Ambiente)](#8-configurações-do-sistema-configyaml-e-variáveis-de-ambiente)
9. [Executando a Suíte de Testes Automatizados](#9-executando-a-suíte-de-testes-automatizados)
10. [Estrutura do Repositório](#10-estrutura-do-repositório)

---

## 1. Arquitetura e Papéis

```mermaid
flowchart TB
    subgraph ClientLayer["Cliente (CLI / SDK Assíncrono)"]
        CLI["Cliente CLI (Typer + Rich)\nChunker + SHA-256 + AES-256-GCM"]
    end

    subgraph ControlPlane["Plano de Controle (Porta 8000)"]
        CTRL["Controlador + Nó de Metadados (FastAPI)\nAlocador • Monitor de Heartbeats • Fila de Re-replicação • Leases • Web UI"]
        DB[("PostgreSQL 16\nÁrvore de Diretórios, Blocos, Réplicas e Nós")]
        CTRL <--> DB
    end

    subgraph DataPlane["Plano de Dados — 5 Nós de Armazenamento (Portas 8001..8005)"]
        N1["node-1 (machine-1)\ndisk-a | disk-b"]
        N2["node-2 (machine-2)\ndisk-a | disk-b"]
        N3["node-3 (machine-3)\ndisk-a | disk-b"]
        N4["node-4 (machine-4)\ndisk-a | disk-b"]
        N5["node-5 (machine-5)\ndisk-a | disk-b"]
    end

    CLI -- "1. Metadados: allocate / commit / stat / ls / mkdir / mv / rm (JSON)" --> CTRL
    CLI <== "2. Dados: PUT / GET /blocks/{id} em paralelo (Streaming Binário)" ==> N1 & N2 & N3 & N4 & N5
    N1 & N2 & N3 & N4 & N5 -. "Registro Ativo, Heartbeats (5s) e Reporte de Corrupção" .-> CTRL
    CTRL -. "Ordem de Re-replicação: POST /blocks/replicate" .-> N1 & N2 & N3
    N2 == "Cópia direta nó-a-nó (Streaming)" ==> N4
```

| Papel | Componente | Responsabilidades Principais |
| :--- | :--- | :--- |
| **Controlador + Nó de Metadados** | `src/controller/` | Processos unificados no mesmo serviço FastAPI (`:8000`). Mantém a árvore de diretórios e o mapeamento `arquivo → blocos → réplicas` persistidos no PostgreSQL; recebe registro ativo e reconcilia blocos de nós que retornam; monitora *heartbeats* (`ALIVE` → `SUSPECT` → `DEAD`); seleciona destinos respeitando espaço livre, limite de 90% e regra de não repetição; gerencia *leases* de escrita e coordena a fila priorizada de re-replicação. |
| **Nó de Armazenamento (*Storage Node*)** | `src/storage_node/` | Serviços independentes (`:8001` a `:8005`), cada um gerenciando múltiplos discos locais (`disk-a` e `disk-b`). Recebem e servem blocos em streaming binário; validam `SHA-256` na escrita e antes de cada leitura; persistem atomicamente `<block_id>.blk` e `<block_id>.sha256` com `fsync`; executam *scrubbing* periódico em background; enviam *heartbeats* com telemetria de disco e copiam blocos diretamente para outros nós quando ordenados pelo Controlador. |
| **Cliente (CLI)** | `src/client/` | Interface de linha de comando (`python -m src.client`). Divide arquivos em *chunks* configuráveis (padrão `4 MiB`), cifra opcionalmente cada bloco com `AES-256-GCM` no cliente, calcula hashes `SHA-256` do bloco e do arquivo completo e transfere blocos diretamente para/dos Nós de Armazenamento com *fallback* transparente e reporte automático de réplicas corrompidas. |

---

## 2. Decisões de Projeto (Consistência, Concorrência e Criptografia)

1. **Modelo de Consistência e Quórum de Escrita ($W \le N$):**
   - No fluxo de escrita (`put`), o cliente recebe $N$ destinos por bloco e envia os dados em paralelo. Assim que pelo menos $W$ réplicas (`write_quorum`, padrão `2`) confirmam a gravação e a verificação do `SHA-256`, o cliente executa `POST /files/commit`.
   - Caso todas as $N$ réplicas confirmem, o arquivo nasce `HEALTHY`. Caso apenas $W < N$ confirmem (por exemplo, um nó lento ou indisponível), o arquivo é confirmado imediatamente como `SUB_REPLICATED` e o monitor do Controlador completa as $N - W$ cópias restantes em segundo plano.
   - Em relação ao **Teorema CAP**, o plano de metadados privilegia consistência forte (CP via transações ACID no PostgreSQL e *leases* exclusivos), enquanto o plano de dados adota escrita por quórum ($W$) com convergência assíncrona para o fator $N$, equilibrando baixa latência de confirmação com alta disponibilidade de leitura (tolerando até $N - 1$ falhas simultâneas).
2. **Criptografia no Cliente e Hash sobre o Conteúdo de Rede:**
   - A cifra `AES-256-GCM` ocorre exclusivamente no **Cliente**. A chave simétrica de 256 bits (`ENCRYPTION_KEY`, 64 caracteres hexadecimais) fica restrita ao ambiente do cliente (variável de ambiente ou `.env` local) e **nunca** trafega pela rede nem é armazenada no Controlador ou nos Nós de Armazenamento.
   - Cada bloco cifrado utiliza um `nonce` aleatório de 12 bytes (24 hex chars) armazenado nos metadados do bloco (`encryption_nonce`).
   - O `block_hash` (`SHA-256`) é calculado sobre os **bytes que trafegam na rede** (o texto cifrado + tag de autenticação GCM de 16 bytes quando a criptografia está ativa), permitindo que os Nós de Armazenamento verifiquem a integridade na escrita, na leitura e no *scrubbing* sem conhecer a chave secreta. Já o `file_hash` nos metadados registra o `SHA-256` do arquivo completo em texto claro para validação final após a decifragem no cliente.

---

## 3. Pré-requisitos e Instalação

- **Docker** (24.0+) e **Docker Compose** (v2) — para subir o cluster completo em contêineres.
- **Python 3.11+** — caso deseje executar a CLI ou a suíte de testes diretamente na máquina hospedeira.

### Instalação local do ambiente Python (opcional, para usar a CLI fora do Docker ou rodar testes)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Se desejar utilizar criptografia `AES-256-GCM` localmente, crie um arquivo `.env` a partir do modelo `.env.example` (ou exporte a variável `ENCRYPTION_KEY`):

```bash
cp .env.example .env
# Gere uma chave de 256 bits (64 caracteres hexadecimais):
export ENCRYPTION_KEY=$(openssl rand -hex 32)
```

---

## 4. Subindo o Cluster com Docker Compose (1 Controlador + 5 Nós)

O arquivo [docker-compose.yml](./docker-compose.yml) sobe automaticamente:
- `postgres` (`dcomp-postgres`): PostgreSQL 16 (exposto no host em `localhost:5433` para não conflitar com instalações locais na porta `5432`).
- `controller` (`dcomp-controller`): Controlador + Nó de Metadados + Interface Web em `http://localhost:8000`.
- `node-1` a `node-5` (`dcomp-node-1` .. `dcomp-node-5`): 5 Nós de Armazenamento independentes (`machine-1` a `machine-5`), expostos nas portas `8001` a `8005`, cada um com **2 discos independentes** simulados por volumes Docker (`disk-a` em `/data/disk_a` e `disk-b` em `/data/disk_b`), totalizando **10 discos** no cluster.

### Comandos para iniciar o ambiente

```bash
# Construir as imagens e subir todo o cluster em background
docker compose up -d --build

# Verificar se os 7 contêineres estão em execução
docker compose ps

# Acompanhar os logs estruturados (JSON) do controlador e dos nós
docker compose logs -f controller
```

Para encerrar o ambiente:

```bash
# Parar os contêineres mantendo os dados persistidos nos volumes
docker compose down

# Parar e apagar todos os volumes (reset completo de metadados e discos)
docker compose down -v
```

---

## 5. Interface Web e Endpoints de Observabilidade

Com o cluster em execução, acesse no navegador:

- **Interface Web (DComp Drive UI):** `http://localhost:8000/` ou `http://localhost:8000/drive`
  - Permite criar pastas, fazer upload de arquivos (com seleção de fator de replicação $N$ e criptografia `AES-256-GCM`), baixar arquivos com verificação de integridade, renomear/mover, excluir, filtrar por estado (`HEALTHY`, `SUB_REPLICATED`, `RECOVERING`, `CORRUPTED`, `ENCRYPTED`), inspecionar o mapa de blocos/réplicas de cada arquivo e gerenciar os nós do cluster (incluindo botão de *Drain* e disparo manual do ciclo do monitor).
- **Documentação Interativa da API (Swagger / OpenAPI):** `http://localhost:8000/docs`
- **Estado do Cluster (JSON):** `http://localhost:8000/cluster/status`
- **Métricas de Observabilidade (JSON):** `http://localhost:8000/metrics`
- **Healthcheck do Controlador:** `http://localhost:8000/health`

---

## 6. Guia Completo da CLI (Exemplos de todos os comandos)

A CLI pode ser executada **de dentro do contêiner do controlador** (`docker compose exec controller python -m src.client ...`) ou **diretamente da sua máquina host** (`python -m src.client ...`).

> **Nota sobre execução a partir do Host vs. Docker:**
> Quando os nós de armazenamento rodam no Docker Compose, eles anunciam suas URLs internas da rede Docker (`http://node-1:8001`, ..., `http://node-5:8005`).
> - Se você executar a CLI **via `docker compose exec controller`**, ela já está na mesma rede Docker e acessa `http://controller:8000` e `http://node-X:800X` diretamente sem flags extras.
> - Se você executar a CLI **fora do Docker (no host)**, passe `--controller-url http://localhost:8000` e mapeie as portas locais dos nós com `--node-url`:
>   ```bash
>   alias dcomp-cli="python -m src.client --controller-url http://localhost:8000 \
>     --node-url node-1=http://localhost:8001 \
>     --node-url node-2=http://localhost:8002 \
>     --node-url node-3=http://localhost:8003 \
>     --node-url node-4=http://localhost:8004 \
>     --node-url node-5=http://localhost:8005"
>   ```

Abaixo estão exemplos de uso de todos os comandos (executados via `docker compose exec` para praticidade):

### 6.1. Consultar estado do cluster e dos discos (`cluster-status`)
Exibe todos os nós (`ALIVE`, `SUSPECT`, `DEAD`, `DRAINING`), capacidade total/usada/livre de cada disco, re-replicações ativas e contagem de arquivos por estado:

```bash
docker compose exec controller python -m src.client cluster-status
```

### 6.2. Criar diretórios (`mkdir`)
Cria diretórios na árvore de metadados (cria diretórios pais automaticamente por padrão com `--parents` / `-p`):

```bash
docker compose exec controller python -m src.client mkdir /projetos/2026/relatorios
```

### 6.3. Enviar arquivos (`put`) — Texto claro, replicação customizada e Criptografia AES-256-GCM
```bash
# Enviar um arquivo usando o fator de replicação padrão (N=3)
docker compose exec controller python -m src.client put /app/config.yaml /projetos/2026/config_backup.yaml

# Enviar especificando fator de replicação N=4
docker compose exec controller python -m src.client put /app/requirements.txt /projetos/2026/reqs.txt --replicas 4

# Enviar com criptografia autenticada AES-256-GCM no cliente
docker compose exec -e ENCRYPTION_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  controller python -m src.client put /app/config.yaml /projetos/2026/segredo.yaml --encrypt
```

### 6.4. Listar diretórios (`ls`)
```bash
docker compose exec controller python -m src.client ls /
docker compose exec controller python -m src.client ls /projetos/2026
```

### 6.5. Inspecionar metadados, hashes e localização das réplicas (`stat`)
Mostra o estado do arquivo (`HEALTHY`, `SUB_REPLICATED`, `RECOVERING`, `CORRUPTED`), tamanho original, `SHA-256` completo, status de criptografia e a tabela detalhada de cada bloco com seus nós e discos:

```bash
docker compose exec controller python -m src.client stat /projetos/2026/config_backup.yaml
```

### 6.6. Baixar arquivos com validação `SHA-256` (`get`)
Baixa os blocos em paralelo das réplicas disponíveis, valida o `SHA-256` de cada bloco (fazendo *fallback* automático para outra réplica se detectar falha ou corrupção), decifra se necessário e valida o `SHA-256` do arquivo completo:

```bash
# Baixar arquivo em texto claro
docker compose exec controller python -m src.client get /projetos/2026/config_backup.yaml /tmp/config_recuperado.yaml

# Baixar arquivo cifrado (fornecendo a mesma ENCRYPTION_KEY)
docker compose exec -e ENCRYPTION_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  controller python -m src.client get /projetos/2026/segredo.yaml /tmp/segredo_recuperado.yaml
```

### 6.7. Renomear ou mover arquivos e diretórios (`mv`)
```bash
# Renomear/mover um arquivo
docker compose exec controller python -m src.client mv /projetos/2026/reqs.txt /projetos/2026/relatorios/requirements_final.txt

# Mover uma árvore de diretório inteira
docker compose exec controller python -m src.client mv /projetos/2026 /projetos/2027
```

### 6.8. Remover arquivos e diretórios (`rm`)
Remove os metadados do banco e dispara a exclusão física assíncrona dos arquivos `.blk` e `.sha256` nos Nós de Armazenamento:

```bash
# Remover um arquivo
docker compose exec controller python -m src.client rm /projetos/2027/relatorios/requirements_final.txt

# Remover um diretório recursivamente (-r / --recursive)
docker compose exec controller python -m src.client rm /projetos --recursive
```

### 6.9. Esvaziamento planejado de um nó (`drain`)
Marca um nó como `DRAINING` para que não receba novas alocações e migra todas as suas réplicas verificadas para outros nós vivos:

```bash
docker compose exec controller python -m src.client drain node-5
```

> **Saída em JSON para scripts:** Todos os comandos aceitam a flag global `--json` antes do subcomando (ex: `python -m src.client --json stat /arquivo.txt`).

---

## 7. Roteiro Prático de Demonstração (Tolerância a Falhas e Corrupção)

Abaixo está o passo a passo para demonstrar os 4 cenários principais (gravação com replicação, leitura, queda de nó com recuperação automática e detecção/recuperação de bloco corrompido):

### Passo 1 — Gravação com Replicação ($N = 3$) e Leitura
```bash
# 1. Envia um arquivo com N=3
docker compose exec controller python -m src.client put /app/config.yaml /demo.yaml --replicas 3

# 2. Inspeciona em quais 3 nós distintos o bloco foi gravado (ex: node-1, node-2, node-3)
docker compose exec controller python -m src.client stat /demo.yaml

# 3. Baixa o arquivo e verifica a integridade
docker compose exec controller python -m src.client get /demo.yaml /tmp/demo_baixado.yaml
```

### Passo 2 — Queda de um Nó, Detecção por Timeout e Re-replicação Automática
```bash
# 1. Derruba um dos nós que hospeda o bloco (ex: node-1)
docker compose stop node-1

# 2. Após ~10s, o nó passa para SUSPECT; após ~15s, passa para DEAD e o monitor re-replica o bloco para outro nó vivo (ex: node-4 ou node-5)
sleep 18
docker compose exec controller python -m src.client cluster-status

# 3. Verifica que /demo.yaml voltou ao estado HEALTHY com 3 réplicas VERIFIED em nós ALIVE
docker compose exec controller python -m src.client stat /demo.yaml

# 4. Religando o node-1, ele se re-registra e reconcilia seus blocos automaticamente (apagando a cópia excedente)
docker compose start node-1
```

### Passo 3 — Detecção de Corrupção (*Bit Rot*) e Recuperação Automática
```bash
# 1. Descobre o block_id e um dos nós onde o arquivo está armazenado
docker compose exec controller python -m src.client --json stat /demo.yaml

# 2. Corrompe propositalmente o arquivo .blk dentro de um dos nós (substitua <NODE>, <DISK_PATH> e <BLOCK_ID>)
docker compose exec node-2 sh -c 'echo "dados_corrompidos" > /data/disk_a/<BLOCK_ID>.blk'

# 3. Ao ler o arquivo (ou aguardar o ciclo de scrubbing), a réplica corrompida é detectada,
#    reportada ao controlador (/blocks/report-corruption), o cliente faz fallback transparente
#    para outra réplica íntegra e o controlador re-replica o bloco em um novo nó!
docker compose exec controller python -m src.client get /demo.yaml /tmp/demo_integro.yaml
docker compose exec controller python -m src.client stat /demo.yaml
```

---

## 8. Configurações do Sistema (`config.yaml` e Variáveis de Ambiente)

Todas as configurações ficam centralizadas em [config.yaml](./config.yaml) e podem ser sobrescritas por variáveis de ambiente (em maiúsculas) sem recompilar ou alterar o código:

| Parâmetro (`config.yaml`) | Variável de Ambiente | Padrão | Descrição |
| :--- | :--- | :--- | :--- |
| `encryption_enabled` | `ENCRYPTION_ENABLED` | `false` | Habilita cifra `AES-256-GCM` por padrão no cliente. |
| `encryption_algorithm` | `ENCRYPTION_ALGORITHM` | `"AES-256-GCM"` | Algoritmo de criptografia autenticada. |
| — *(exclusivo de env)* | `ENCRYPTION_KEY` | `None` | Chave simétrica de 256 bits (64 caracteres hexadecimais) restrita ao cliente. |
| `replication_factor` | `REPLICATION_FACTOR` | `3` | Nível de replicação padrão $N$. |
| `chunk_size_mb` | `CHUNK_SIZE_MB` | `4` | Tamanho de cada bloco (*chunk*) em MiB. |
| `write_quorum` | `WRITE_QUORUM` | `2` | Quórum de escrita $W$ exigido antes do `commit`. |
| `allow_under_replicated_writes` | `ALLOW_UNDER_REPLICATED_WRITES` | `true` | Aceita ou rejeita escritas quando há menos máquinas vivas que $N$. |
| `hash_algorithm` | `HASH_ALGORITHM` | `"sha256"` | Algoritmo de hash para integridade de blocos e arquivos. |
| `heartbeat_interval_sec` | `HEARTBEAT_INTERVAL_SEC` | `5` | Intervalo de envio de *heartbeats* pelos Storage Nodes. |
| `suspect_timeout_sec` | `SUSPECT_TIMEOUT_SEC` | `10` | Tempo sem *heartbeat* para marcar um nó como `SUSPECT`. |
| `heartbeat_timeout_sec` | `HEARTBEAT_TIMEOUT_SEC` | `15` | Tempo sem *heartbeat* para declarar um nó `DEAD` e iniciar re-replicação. |
| `scrubbing_interval_sec` | `SCRUBBING_INTERVAL_SEC` | `60` | Intervalo da verificação periódica de integridade em disco. |
| `lease_timeout_sec` | `LEASE_TIMEOUT_SEC` | `300` | Tempo de expiração do *lease* de escrita concorrente. |
| `lease_cleanup_interval_sec` | `LEASE_CLEANUP_INTERVAL_SEC` | `15` | Frequência de limpeza de *leases* expirados. |
| `disk_usage_limit_pct` | `DISK_USAGE_LIMIT_PCT` | `90.0` | Percentual máximo de uso de disco para receber novas réplicas. |
| `placement_policy` | `PLACEMENT_POLICY` | `"free_space"` | Política de posicionamento (`free_space`, `low_utilization`, `random`). |
| `max_concurrent_replications` | `MAX_CONCURRENT_REPLICATIONS` | `4` | Limite global de cópias de re-replicação simultâneas. |
| `monitor_interval_sec` | `MONITOR_INTERVAL_SEC` | `5` | Intervalo do ciclo do monitor de falhas e fila de re-replicação. |
| `database_url` | `DATABASE_URL` | `postgresql+asyncpg://...` | URL de conexão assíncrona com o PostgreSQL. |
| `controller_url` | `CONTROLLER_URL` | `http://controller:8000` | URL base do Controlador. |

---

## 9. Executando a Suíte de Testes Automatizados

O repositório conta com **70 testes automatizados** (`pytest` + `pytest-asyncio`) cobrindo chunking, criptografia, CLI, armazenamento atômico em disco, *scrubbing*, metadados, *leases* de concorrência, alocador, detecção de falhas `SUSPECT`/`DEAD`, fila priorizada de re-replicação, *drain*, reconciliação e integração completa cliente-controlador-nós:

### 9.1. Testes unitários que não dependem do PostgreSQL (Chunker, CLI e Storage Node)
```bash
.venv/bin/pytest tests/test_client_chunker.py tests/test_client_cli.py tests/test_storage_node.py -v
```

### 9.2. Suíte completa (com o container do PostgreSQL rodando na porta `5433`)
```bash
# Suba apenas o banco PostgreSQL (caso o cluster ainda não esteja ativo)
docker compose up -d postgres

# Execute todos os testes de integração e controlador
.venv/bin/pytest -v

# Para incluir também o teste E2E com subprocessos reais via TCP:
TEST_DATABASE_URL="postgresql+asyncpg://dcomp:dcomp@localhost:5433/dcomp_drive" .venv/bin/pytest -v
```

---

## 10. Estrutura do Repositório

```text
dcomp-drive/
├── config.yaml                              # Configurações globais do sistema
├── .env.example                             # Modelo de variáveis de ambiente (ENCRYPTION_KEY)
├── docker-compose.yml                       # Orquestração: PostgreSQL + 1 Controlador + 5 Storage Nodes
├── Dockerfile                               # Imagem Python 3.11-slim compartilhada pelos serviços
├── requirements.txt                         # Dependências Python do projeto
├── pytest.ini                               # Configuração do Pytest
├── docs/                                    # Relatório Técnico da Parte 1 (LaTeX, PDF e seções)
│   ├── relatorio_tecnico_parte1.tex         # Código-fonte LaTeX do Relatório Técnico completo
│   ├── relatorio_tecnico_parte1.pdf         # PDF compilado do Relatório Técnico (7 páginas)
│   ├── relatorio_integrante_2.md            # Rascunho: Posicionamento, Detecção de Falhas e Re-replicação
│   └── relatorio_integrante_3.md            # Rascunho: Arquitetura do Plano de Dados e Storage Node
├── src/
│   ├── common/
│   │   ├── config.py                        # Carregador Pydantic Settings + YAML
│   │   ├── logger.py                        # Logger JSON estruturado
│   │   └── schemas.py                       # Contratos e modelos Pydantic REST
│   ├── controller/
│   │   ├── main.py                          # Aplicação FastAPI do Controlador + Lifespan
│   │   ├── database.py                      # Engine e sessões assíncronas SQLAlchemy 2.0 + PostgreSQL
│   │   ├── models.py                        # Modelagem relacional e constraints de não repetição
│   │   ├── allocator.py                     # Algoritmo de posicionamento de réplicas e gestão de espaço
│   │   ├── monitor.py                       # Loops de detecção SUSPECT/DEAD, re-replicação e leases
│   │   ├── routes_metadata.py               # Rotas de arquivos, diretórios, leases e corrupção
│   │   ├── routes_cluster.py                # Rotas de registro, reconciliação, heartbeat, drain e métricas
│   │   └── web_ui.py                        # Interface Web estilo Google Drive e painel do cluster
│   ├── storage_node/
│   │   ├── main.py                          # API FastAPI do Nó (PUT/GET/DELETE /blocks e /blocks/replicate)
│   │   ├── disk_manager.py                  # Escrita/leitura atômica (.blk + .sha256), locks e fsync
│   │   └── workers.py                       # Registro ativo, reconciliação, heartbeats e scrubbing
│   └── client/
│       ├── __main__.py                      # Entrypoint `python -m src.client`
│       ├── cli.py                           # Comandos da CLI (Typer + tabelas Rich)
│       ├── chunker.py                       # Divisão em blocos, SHA-256 e cifra AES-256-GCM
│       └── transfer.py                      # Cliente assíncrono httpx com quórum W, fallback e reporte
└── tests/
    ├── test_client_chunker.py               # Testes de divisão em blocos, hashes e AES-256-GCM
    ├── test_client_cli.py                   # Testes de interface de linha de comando
    ├── test_client_integration.py           # Testes integrados Cliente + Controlador + 5 Storage Nodes
    ├── test_client_e2e.py                   # Teste ponta a ponta em subprocessos TCP reais
    ├── test_controller_metadata.py          # Testes de árvore de diretórios, leases e corrupção
    ├── test_controller_cluster_monitor.py   # Testes de alocação, limite de 90%, SUSPECT/DEAD, drain e Web UI
    └── test_storage_node.py                 # Testes de atomicidade em disco, scrubbing e replicação nó-a-nó
```
