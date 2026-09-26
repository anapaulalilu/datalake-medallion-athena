"""Etapa 2 - Data Quality, Quarentena, Silver (processed/) e Gold.

Lê a camada RAW de uma data de ingestão e:
  1. Aplica as regras de Data Quality em pedidos:
       R1  quantidade <= 0 (ou não numérica/nula)  -> rejeita
       R2  cliente_id inexistente em clientes      -> rejeita
       R3  product_id inexistente em produtos      -> rejeita
     Um pedido pode violar mais de uma regra: ele é gravado UMA vez na quarentena, com todos os motivos.
  2. Quarentena: registros inválidos + motivo, em JSON (uma linha por registro - JSON Lines, exigido pelo SerDe do Athena):
       s3://<bucket>/quarantine/pedidos_rejeitados/data=YYYY-MM-DD/rejeitados.json
  3. Silver: pedidos válidos + JOIN com clientes + produtos, com valor_total = quantidade * preco, em Parquet/Snappy:
       s3://<bucket>/processed/fato_vendas/ingest_date=YYYY-MM-DD/part-00000.snappy.parquet
  4. Gold: agregação analítica por uf e categoria (Parquet/Snappy):
       s3://<bucket>/gold/vendas_uf_categoria/ingest_date=YYYY-MM-DD/part-00000.snappy.parquet
  5. Confere no próprio Python a equação de integridade: RAW = SILVER + QUARENTENA.

O processamento é idempotente por partição: reexecutar a mesma data sobrescreve as saídas daquela data.

Uso:
    python -m src.pipeline_medallion --bucket meu-bucket --ingest-date 2026-09-19
    python -m src.pipeline_medallion --local-dir local_lake --ingest-date 2026-09-19   # offline
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import sys
from typing import Dict, List, Tuple

import pandas as pd

from src.common import GOLD, QUARANTINE, RAW, SILVER, Storage, adicionar_args_comuns, criar_storage

# A ordem das colunas abaixo é a mesma usada no DDL do Athena (sql/02_tabelas_externas.sql)
COLUNAS_SILVER = [
    "pedido_id", "cliente_id", "product_id", "quantidade", "data_pedido",
    "nome_cliente", "email", "cidade", "uf", "nome_produto", "categoria", "preco", "valor_total",
]
COLUNAS_GOLD = [
    "uf", "categoria", "qtd_pedidos", "qtd_itens", "clientes_unicos",
    "receita_total", "ticket_medio", "participacao_receita_pct",
]

MOTIVO_QTD = "quantidade_menor_ou_igual_a_zero"
MOTIVO_CLI = "cliente_id_inexistente"
MOTIVO_PROD = "product_id_inexistente"


# --------------------------------------------------------------------------- leitura (RAW)
def ler_raw(storage: Storage, tabela: str, ingest_date: str) -> pd.DataFrame:
    """Lê o CSV da partição. Tudo como texto: a camada Raw não impõe tipos (tipagem ocorre na Silver)."""
    key = f"{RAW}/{tabela}/ingest_date={ingest_date}/{tabela}.csv"
    return pd.read_csv(io.BytesIO(storage.get_bytes(key)), dtype=str, keep_default_na=False)


# --------------------------------------------------------------------------- Data Quality
def validar_pedidos(
    pedidos: pd.DataFrame, clientes: pd.DataFrame, produtos: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Retorna (pedidos_validos_tipados, pedidos_rejeitados_com_motivo). Nenhum registro se perde."""
    qtd = pd.to_numeric(pedidos["quantidade"], errors="coerce")
    cli = pd.to_numeric(pedidos["cliente_id"], errors="coerce")
    prod = pd.to_numeric(pedidos["product_id"], errors="coerce")

    ids_clientes = pd.to_numeric(clientes["cliente_id"], errors="coerce").dropna().astype("int64").tolist()
    ids_produtos = pd.to_numeric(produtos["product_id"], errors="coerce").dropna().astype("int64").tolist()

    viola_qtd = qtd.isna() | (qtd <= 0)
    viola_cli = ~cli.isin(ids_clientes)  # nulo/não numérico também não existe na dimensão
    viola_prod = ~prod.isin(ids_produtos)
    invalido = viola_qtd | viola_cli | viola_prod

    def montar_motivo(i: int) -> str:
        motivos: List[str] = []
        if viola_qtd.iloc[i]:
            motivos.append(MOTIVO_QTD)
        if viola_cli.iloc[i]:
            motivos.append(MOTIVO_CLI)
        if viola_prod.iloc[i]:
            motivos.append(MOTIVO_PROD)
        return ";".join(motivos)

    rejeitados = pedidos[invalido.values].copy()
    posicoes = [i for i, flag in enumerate(invalido.values) if flag]
    rejeitados["motivo_rejeicao"] = [montar_motivo(i) for i in posicoes]

    validos = pedidos[~invalido.values].copy()
    validos["pedido_id"] = pd.to_numeric(validos["pedido_id"]).astype("int64")
    validos["cliente_id"] = pd.to_numeric(validos["cliente_id"]).astype("int64")
    validos["product_id"] = pd.to_numeric(validos["product_id"]).astype("int64")
    validos["quantidade"] = pd.to_numeric(validos["quantidade"]).astype("int64")
    validos["data_pedido"] = pd.to_datetime(validos["data_pedido"]).dt.date
    return validos, rejeitados


# --------------------------------------------------------------------------- Silver
def construir_silver(validos: pd.DataFrame, clientes: pd.DataFrame, produtos: pd.DataFrame) -> pd.DataFrame:
    dim_cli = clientes.rename(columns={"nome": "nome_cliente"})[["cliente_id", "nome_cliente", "email", "cidade", "uf"]].copy()
    dim_cli["cliente_id"] = pd.to_numeric(dim_cli["cliente_id"]).astype("int64")
    dim_cli = dim_cli.drop_duplicates(subset="cliente_id")  # garante 1 linha por chave -> JOIN não duplica pedidos

    dim_prod = produtos[["product_id", "nome_produto", "categoria", "preco"]].copy()
    dim_prod["product_id"] = pd.to_numeric(dim_prod["product_id"]).astype("int64")
    dim_prod["preco"] = pd.to_numeric(dim_prod["preco"]).astype("float64")
    dim_prod = dim_prod.drop_duplicates(subset="product_id")

    silver = (
        validos.merge(dim_cli, on="cliente_id", how="inner", validate="many_to_one")
        .merge(dim_prod, on="product_id", how="inner", validate="many_to_one")
    )
    silver["valor_total"] = (silver["quantidade"] * silver["preco"]).round(2)  # campo derivado
    return silver[COLUNAS_SILVER].sort_values("pedido_id").reset_index(drop=True)


# --------------------------------------------------------------------------- Gold
def construir_gold(silver: pd.DataFrame) -> pd.DataFrame:
    """Vendas por UF x categoria com métricas de negócio."""
    if silver.empty:
        return pd.DataFrame({c: pd.Series(dtype="float64" if c not in ("uf", "categoria") else "object") for c in COLUNAS_GOLD})
    gold = (
        silver.groupby(["uf", "categoria"], as_index=False)
        .agg(
            qtd_pedidos=("pedido_id", "nunique"),
            qtd_itens=("quantidade", "sum"),
            clientes_unicos=("cliente_id", "nunique"),
            receita_total=("valor_total", "sum"),
        )
    )
    gold["receita_total"] = gold["receita_total"].round(2)
    gold["ticket_medio"] = (gold["receita_total"] / gold["qtd_pedidos"]).round(2)
    gold["participacao_receita_pct"] = (100 * gold["receita_total"] / gold["receita_total"].sum()).round(2)
    for c in ("qtd_pedidos", "qtd_itens", "clientes_unicos"):
        gold[c] = gold[c].astype("int64")
    return gold[COLUNAS_GOLD].sort_values("receita_total", ascending=False).reset_index(drop=True)


# --------------------------------------------------------------------------- escrita
def gravar_parquet(storage: Storage, prefixo: str, ingest_date: str, df: pd.DataFrame) -> str:
    """Parquet + compressão Snappy, em <prefixo>/ingest_date=<data>/. Limpa a partição antes (idempotência)."""
    particao = f"{prefixo}/ingest_date={ingest_date}/"
    storage.delete_prefix(particao)
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
    key = f"{particao}part-00000.snappy.parquet"
    storage.put_bytes(key, buffer.getvalue())
    return storage.uri(key)


def gravar_quarentena(storage: Storage, ingest_date: str, rejeitados: pd.DataFrame) -> str:
    """JSON Lines (1 objeto por linha), pois o SerDe JSON do Athena exige um registro por linha."""
    particao = f"{QUARANTINE}/data={ingest_date}/"
    storage.delete_prefix(particao)
    rejeitado_em = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    linhas = []
    for reg in rejeitados.to_dict(orient="records"):
        reg["rejeitado_em"] = rejeitado_em
        linhas.append(json.dumps(reg, ensure_ascii=False))
    conteudo = ("\n".join(linhas) + ("\n" if linhas else "")).encode("utf-8")
    key = f"{particao}rejeitados.json"
    storage.put_bytes(key, conteudo)
    return storage.uri(key)


# --------------------------------------------------------------------------- orquestração
def processar(storage: Storage, ingest_date: str) -> Dict[str, int]:
    clientes = ler_raw(storage, "clientes", ingest_date)
    produtos = ler_raw(storage, "produtos", ingest_date)
    pedidos = ler_raw(storage, "pedidos", ingest_date)
    print(f"[RAW] pedidos={len(pedidos)} clientes={len(clientes)} produtos={len(produtos)} (ingest_date={ingest_date})")

    validos, rejeitados = validar_pedidos(pedidos, clientes, produtos)
    silver = construir_silver(validos, clientes, produtos)
    gold = construir_gold(silver)

    print("[DQ ] violações por regra (um pedido pode violar mais de uma):")
    for motivo in (MOTIVO_QTD, MOTIVO_CLI, MOTIVO_PROD):
        n = int(rejeitados["motivo_rejeicao"].str.contains(motivo, regex=False).sum()) if len(rejeitados) else 0
        print(f"        {motivo:<36} = {n}")

    print(f"[QUAR] {len(rejeitados):>6} registros -> {gravar_quarentena(storage, ingest_date, rejeitados)}")
    print(f"[SILVER] {len(silver):>4} registros -> {gravar_parquet(storage, SILVER, ingest_date, silver)}")
    print(f"[GOLD] {len(gold):>6} linhas     -> {gravar_parquet(storage, GOLD, ingest_date, gold)}")

    resultado = {
        "raw": len(pedidos),
        "silver": len(silver),
        "quarentena": len(rejeitados),
        "receita_silver_centavos": int(round(silver["valor_total"].sum() * 100)),
        "receita_gold_centavos": int(round(gold["receita_total"].sum() * 100)) if len(gold) else 0,
    }
    ok = resultado["raw"] == resultado["silver"] + resultado["quarentena"]
    print(
        f"[CONCILIACAO] raw={resultado['raw']} | silver={resultado['silver']} + quarentena={resultado['quarentena']}"
        f" = {resultado['silver'] + resultado['quarentena']} -> {'OK' if ok else 'DIVERGENTE'}"
    )
    if not ok:
        raise SystemExit("Falha de integridade: RAW != SILVER + QUARENTENA")
    return resultado


def main(argv: list[str] | None = None) -> Dict[str, int]:
    parser = argparse.ArgumentParser(description="Pipeline Medallion: Data Quality, Quarentena, Silver e Gold")
    adicionar_args_comuns(parser)
    args = parser.parse_args(argv)
    return processar(criar_storage(args), args.ingest_date)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
