"""
Passo 7 - Setup do banco Postgres
Cria as tabelas que vão receber os dados processados pelo Spark Streaming.
Rode uma vez só (ou sempre que quiser recriar as tabelas do zero).
"""

import psycopg

CONN_PARAMS = dict(
    host="localhost",
    port=5433,
    dbname="erb_monitoring",
    user="erb_admin",
    password="erb_pass",
)

DDL = """
DROP TABLE IF EXISTS eventos_energia;
CREATE TABLE eventos_energia (
    id_alarme               VARCHAR(36) PRIMARY KEY,
    numero_estacao          VARCHAR(20) NOT NULL,
    prestadora              VARCHAR(200),
    municipio               VARCHAR(200),
    uf                      VARCHAR(2),
    tipo_alarme             VARCHAR(50),
    timestamp_evento        TIMESTAMP NOT NULL,
    timestamp_fim_estimado  TIMESTAMP,
    chuva_mm_no_momento     DOUBLE PRECISION,
    duracao_estimada_minutos INTEGER,
    timestamp_publicacao    TIMESTAMP,
    inserido_em             TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_eventos_municipio ON eventos_energia (municipio, uf);
CREATE INDEX idx_eventos_timestamp ON eventos_energia (timestamp_evento);
CREATE INDEX idx_eventos_estacao ON eventos_energia (numero_estacao);

DROP TABLE IF EXISTS alarmes_agregados_municipio;
CREATE TABLE alarmes_agregados_municipio (
    janela_inicio           TIMESTAMP NOT NULL,
    janela_fim              TIMESTAMP NOT NULL,
    municipio               VARCHAR(200) NOT NULL,
    uf                      VARCHAR(2) NOT NULL,
    erbs_afetadas           BIGINT,
    total_alarmes           BIGINT,
    duracao_media_minutos   DOUBLE PRECISION,
    chuva_maxima_mm         DOUBLE PRECISION,
    atualizado_em           TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (janela_inicio, municipio, uf)
);
CREATE INDEX idx_agregado_janela ON alarmes_agregados_municipio (janela_inicio);
"""

print("Conectando ao Postgres...")
conn = psycopg.connect(**CONN_PARAMS, autocommit=True)
cur = conn.cursor()

print("Criando tabelas...")
cur.execute(DDL)

print("\n=== Tabelas criadas com sucesso ===")
cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public' ORDER BY table_name;
""")
for (nome,) in cur.fetchall():
    print(f"  - {nome}")

cur.close()
conn.close()
