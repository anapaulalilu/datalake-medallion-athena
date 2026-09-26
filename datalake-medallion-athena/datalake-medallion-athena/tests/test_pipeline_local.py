"""Teste ponta a ponta em modo local (sem AWS): ingestão -> DQ/quarentena -> Silver/Gold."""
import json

import pandas as pd

from src import ingestao_raw, pipeline_medallion


def test_pipeline_concilia_raw_silver_quarentena(tmp_path):
    lake = str(tmp_path / "lake")
    data = "2026-09-19"

    esperado = ingestao_raw.main([
        "--local-dir", lake, "--ingest-date", data, "--n-pedidos", "1000",
        "--anom-quantidade", "30", "--anom-cliente", "20", "--anom-produto", "20", "--anom-multiplas", "5",
    ])
    r = pipeline_medallion.main(["--local-dir", lake, "--ingest-date", data])

    # números batem com o que foi injetado
    assert r["quarentena"] == esperado["pedidos_invalidos"] == 75
    assert r["silver"] == esperado["pedidos_validos"] == 925
    # equação de integridade
    assert r["raw"] == r["silver"] + r["quarentena"]
    # Gold preserva a receita da Silver
    assert r["receita_gold_centavos"] == r["receita_silver_centavos"]

    silver = pd.read_parquet(f"{lake}/processed/fato_vendas/ingest_date={data}/part-00000.snappy.parquet")
    assert (silver["quantidade"] > 0).all()
    assert ((silver["quantidade"] * silver["preco"]).round(2) - silver["valor_total"]).abs().max() < 0.005

    linhas = open(f"{lake}/quarantine/pedidos_rejeitados/data={data}/rejeitados.json", encoding="utf-8").read().splitlines()
    registros = [json.loads(l) for l in linhas]
    assert len(registros) == 75 and all(reg["motivo_rejeicao"] for reg in registros)
    assert sum(";" in reg["motivo_rejeicao"] for reg in registros) == 5  # múltiplas violações num único registro
