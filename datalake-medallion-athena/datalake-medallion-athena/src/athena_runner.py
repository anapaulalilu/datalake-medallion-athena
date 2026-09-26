"""Etapa 3 - Executa os scripts SQL (sql/*.sql) no Amazon Athena via boto3.

  * Substitui o placeholder <seu-bucket> pelo nome do bucket.
  * Os resultados/metadados de cada consulta são gravados em s3://<bucket>/athena-results/
  * Imprime o resultado das consultas SELECT no terminal (útil para conferir antes dos prints).

Uso:
    python -m src.athena_runner --bucket meu-bucket                    # roda todos os arquivos sql/*.sql
    python -m src.athena_runner --bucket meu-bucket --arquivo sql/05_conciliacao_integridade.sql

Observação: os PRINTS exigidos na entrega devem ser tirados no CONSOLE do Athena
(veja o README). Este runner serve para automatizar e para gerar o histórico em athena-results/.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path
from typing import List

from src.common import ATHENA_RESULTS

RAIZ = Path(__file__).resolve().parent.parent
SQL_DIR = RAIZ / "sql"
MAX_LINHAS_IMPRESSAS = 30


def dividir_statements(texto_sql: str) -> List[str]:
    """Remove comentários de linha (-- ...) e separa os comandos por ';'."""
    sem_comentarios = "\n".join(l for l in texto_sql.splitlines() if not l.strip().startswith("--"))
    return [s.strip() for s in sem_comentarios.split(";") if s.strip()]


def executar(athena, sql: str, saida: str, workgroup: str, database: str = "default", timeout_s: int = 300) -> str:
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": saida},
        WorkGroup=workgroup,
    )
    qid = resp["QueryExecutionId"]
    inicio = time.time()
    while True:
        exec_ = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        estado = exec_["Status"]["State"]
        if estado in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() - inicio > timeout_s:
            raise TimeoutError(f"Consulta {qid} excedeu {timeout_s}s")
        time.sleep(1)
    if estado != "SUCCEEDED":
        motivo = exec_["Status"].get("StateChangeReason", "sem detalhes")
        raise RuntimeError(f"Athena {estado} (QueryExecutionId={qid}): {motivo}\n--- SQL ---\n{sql}")
    return qid


def imprimir_resultado(athena, qid: str) -> None:
    linhas = athena.get_query_results(QueryExecutionId=qid, MaxResults=MAX_LINHAS_IMPRESSAS + 1)["ResultSet"]["Rows"]
    tabela = [[c.get("VarCharValue", "") for c in l["Data"]] for l in linhas]
    if not tabela:
        return
    larguras = [max(len(str(l[i])) for l in tabela) for i in range(len(tabela[0]))]
    for n, l in enumerate(tabela):
        print("   " + " | ".join(str(v).ljust(larguras[i]) for i, v in enumerate(l)))
        if n == 0:
            print("   " + "-+-".join("-" * w for w in larguras))


def main(argv: list[str] | None = None) -> None:
    import boto3

    parser = argparse.ArgumentParser(description="Executa scripts SQL no Athena")
    parser.add_argument("--bucket", default=os.getenv("BUCKET"), required=os.getenv("BUCKET") is None)
    parser.add_argument("--region", default=os.getenv("AWS_DEFAULT_REGION"))
    parser.add_argument("--workgroup", default="primary")
    parser.add_argument("--arquivo", default=None, help="Executa apenas este arquivo .sql (padrão: todos em sql/)")
    args = parser.parse_args(argv)

    athena = boto3.client("athena", region_name=args.region)
    saida = f"s3://{args.bucket}/{ATHENA_RESULTS}/"
    arquivos = [Path(args.arquivo)] if args.arquivo else sorted(SQL_DIR.glob("*.sql"))

    for arq in arquivos:
        print(f"\n=== {arq.name} ===")
        texto = arq.read_text(encoding="utf-8").replace("<seu-bucket>", args.bucket)
        for stmt in dividir_statements(texto):
            resumo = re.sub(r"\s+", " ", stmt)[:110]
            qid = executar(athena, stmt, saida, args.workgroup)
            print(f"[OK] {resumo}...  (QueryExecutionId={qid})")
            if stmt.lstrip().upper().startswith(("SELECT", "WITH")):
                imprimir_resultado(athena, qid)
    print(f"\nHistórico e resultados em {saida}")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, TimeoutError) as e:
        sys.exit(str(e))
