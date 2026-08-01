"""
Passo 5 - Produtor Kafka de alarmes de queda de energia
Lê os eventos simulados (Passo 4) e publica cada um como mensagem JSON
no tópico Kafka, simulando o comportamento de alarmes chegando de uma
rede real de telecom (ex: sistema de monitoramento de ERBs).
"""

import glob
import json
import time
import uuid
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer

TOPIC = "alarmes-energia-erb"
BOOTSTRAP_SERVERS = "localhost:9092"
EVENTOS_DIR = "data/processed/eventos_energia_simulados"

# Spark salva o CSV com um nome gerado automaticamente (part-00000-...).
# Encontramos o arquivo real na pasta em vez de fixar o nome.
csv_path = glob.glob(f"{EVENTOS_DIR}/part-*.csv")[0]
print(f"Lendo eventos de: {csv_path}")

df = pd.read_csv(csv_path)
print(f"Total de eventos a publicar: {len(df)}")

# Ordena por horário do evento, pra publicar na ordem cronológica real
df = df.sort_values("timestamp_queda").reset_index(drop=True)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
)

DELAY_ENTRE_MENSAGENS_SEGUNDOS = 0.05  # ritmo de publicação (ajustável)

enviados = 0
for _, row in df.iterrows():
    alarme = {
        "id_alarme": str(uuid.uuid4()),
        "numero_estacao": str(row["numero_estacao"]),
        "prestadora": row["prestadora"],
        "municipio": row["municipio"],
        "uf": row["uf"],
        "tipo_alarme": "FALTA_ENERGIA",
        "timestamp_evento": row["timestamp_queda"],
        "chuva_mm_no_momento": row["chuva_mm_no_momento"],
        "duracao_estimada_minutos": int(row["duracao_minutos"]),
        "timestamp_publicacao": datetime.utcnow().isoformat(),
    }

    # Usa o número da estação como chave -> garante que alarmes da
    # mesma ERB caem sempre na mesma partição (ordem preservada por estação)
    producer.send(TOPIC, key=alarme["numero_estacao"], value=alarme)
    enviados += 1

    if enviados % 100 == 0:
        print(f"  {enviados}/{len(df)} alarmes publicados...")

    time.sleep(DELAY_ENTRE_MENSAGENS_SEGUNDOS)

producer.flush()
producer.close()

print(f"\n=== Concluído: {enviados} alarmes publicados no tópico '{TOPIC}' ===")
