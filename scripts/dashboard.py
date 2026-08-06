"""
Passo 8 - Dashboard de Monitoramento de Impacto de Energia na Rede Móvel
Lê as tabelas populadas pelo pipeline (Kafka -> Spark Streaming -> Postgres)
e exibe uma visão consolidada: KPIs, série temporal de ERBs afetadas e
lista de eventos individuais.
"""

import psycopg
import pandas as pd
import streamlit as st

PG_CONN_PARAMS = dict(
    host="localhost",
    port=5433,
    dbname="erb_monitoring",
    user="erb_admin",
    password="erb_pass",
)

st.set_page_config(
    page_title="Monitoramento de Energia - Rede Móvel",
    page_icon="📡",
    layout="wide",
)


@st.cache_data(ttl=30)
def carregar_agregado():
    with psycopg.connect(**PG_CONN_PARAMS) as conn:
        return pd.read_sql(
            """
            SELECT janela_inicio, janela_fim, municipio, uf,
                   erbs_afetadas, total_alarmes,
                   duracao_media_minutos, chuva_maxima_mm
            FROM alarmes_agregados_municipio
            ORDER BY janela_inicio
            """,
            conn,
        )


@st.cache_data(ttl=30)
def carregar_eventos():
    with psycopg.connect(**PG_CONN_PARAMS) as conn:
        return pd.read_sql(
            """
            SELECT id_alarme, numero_estacao, prestadora, municipio, uf,
                   timestamp_evento, timestamp_fim_estimado,
                   chuva_mm_no_momento, duracao_estimada_minutos
            FROM eventos_energia
            ORDER BY timestamp_evento
            """,
            conn,
        )


st.title("📡 Monitoramento de Impacto de Energia na Rede Móvel")
st.caption(
    "Pipeline: ANATEL + INMET (dados reais) → simulador de eventos → "
    "Kafka → Spark Structured Streaming → PostgreSQL"
)

agregado = carregar_agregado()
eventos = carregar_eventos()

if eventos.empty:
    st.warning("Nenhum dado encontrado. Rode o pipeline (produtor + consumidor) primeiro.")
    st.stop()

# ---------- Filtros ----------
municipios = sorted(eventos["municipio"].unique())
municipio_selecionado = st.sidebar.selectbox("Município", municipios)

eventos_filtrados = eventos[eventos["municipio"] == municipio_selecionado]
agregado_filtrado = agregado[agregado["municipio"] == municipio_selecionado]

# ---------- KPIs ----------
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total de alarmes", len(eventos_filtrados))
col2.metric("ERBs distintas afetadas", eventos_filtrados["numero_estacao"].nunique())
col3.metric(
    "Duração média da queda",
    f'{eventos_filtrados["duracao_estimada_minutos"].mean():.0f} min',
)
pico_erbs = agregado_filtrado["erbs_afetadas"].max()
col4.metric("Pico de ERBs afetadas (1h)", int(pico_erbs) if pd.notna(pico_erbs) else 0)

st.divider()

# ---------- Série temporal: ERBs afetadas por hora ----------
st.subheader("ERBs afetadas por hora")
serie = agregado_filtrado.set_index("janela_inicio")["erbs_afetadas"]
st.bar_chart(serie)

# ---------- Série temporal: chuva máxima por hora ----------
st.subheader("Chuva máxima registrada por hora (mm)")
serie_chuva = agregado_filtrado.set_index("janela_inicio")["chuva_maxima_mm"]
st.line_chart(serie_chuva)

st.divider()

# ---------- Tabela de operadoras mais afetadas ----------
st.subheader("Alarmes por operadora")
por_operadora = (
    eventos_filtrados.groupby("prestadora")
    .agg(
        total_alarmes=("id_alarme", "count"),
        erbs_distintas=("numero_estacao", "nunique"),
        duracao_media_min=("duracao_estimada_minutos", "mean"),
    )
    .sort_values("total_alarmes", ascending=False)
    .round(1)
)
st.dataframe(por_operadora, use_container_width=True)

st.divider()

# ---------- Tabela de eventos individuais ----------
st.subheader("Eventos individuais (mais recentes primeiro)")
st.dataframe(
    eventos_filtrados.sort_values("timestamp_evento", ascending=False).head(200),
    use_container_width=True,
)
