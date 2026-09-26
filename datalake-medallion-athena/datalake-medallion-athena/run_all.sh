#!/usr/bin/env bash
# Executa o pipeline completo: ingestão (Raw) -> DQ/Quarentena/Silver/Gold -> Athena (DDL + auditoria)
# Uso: BUCKET=meu-bucket AWS_DEFAULT_REGION=us-east-1 ./run_all.sh [YYYY-MM-DD]
set -euo pipefail
: "${BUCKET:?Defina BUCKET, ex.: export BUCKET=meu-bucket}"
DATA="${1:-$(date +%F)}"

python -m src.ingestao_raw       --bucket "$BUCKET" --ingest-date "$DATA"
python -m src.pipeline_medallion --bucket "$BUCKET" --ingest-date "$DATA"
python -m src.athena_runner      --bucket "$BUCKET"
