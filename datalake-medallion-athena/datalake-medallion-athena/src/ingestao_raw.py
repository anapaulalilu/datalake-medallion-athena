"""Etapa 1 - Ingestão (camada RAW).

Gera massa de dados simulada (clientes, produtos, pedidos) com anomalias INTENCIONAIS e controladas,
e grava em CSV (delimitado por vírgula) no S3 com particionamento Hive por data de ingestão:

    s3://<bucket>/raw/clientes/ingest_date=YYYY-MM-DD/clientes.csv
    s3://<bucket>/raw/produtos/ingest_date=YYYY-MM-DD/produtos.csv
    s3://<bucket>/raw/pedidos/ingest_date=YYYY-MM-DD/pedidos.csv

Anomalias injetadas em pedidos (quantidades exatas, definidas por argumento, com seed fixa):
  * quantidade <= 0 (zero ou negativa)
  * cliente_id inexistente na dimensão de clientes
  * product_id inexistente na dimensão de produtos
  * registros com MAIS DE UMA violação (quantidade inválida + cliente inexistente)

Uso:
    python -m src.ingestao_raw --bucket meu-bucket --ingest-date 2026-09-19
    python -m src.ingestao_raw --local-dir local_lake            # teste offline
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from src.common import RAW, Storage, adicionar_args_comuns, criar_storage

NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fábio", "Gabriela", "Hugo", "Isabela", "João",
         "Karen", "Lucas", "Marina", "Nicolas", "Olívia", "Paulo", "Rafaela", "Sérgio", "Tatiane", "Vitor"]
SOBRENOMES = ["Silva", "Souza", "Oliveira", "Santos", "Lima", "Pereira", "Costa", "Ribeiro", "Almeida", "Carvalho"]
CIDADES_UF = [("São Paulo", "SP"), ("Campinas", "SP"), ("Rio de Janeiro", "RJ"), ("Niterói", "RJ"),
              ("Belo Horizonte", "MG"), ("Uberlândia", "MG"), ("Porto Alegre", "RS"), ("Curitiba", "PR"),
              ("Salvador", "BA"), ("Florianópolis", "SC"), ("Recife", "PE"), ("Fortaleza", "CE"), ("Brasília", "DF")]
CATEGORIAS = {
    "Eletrônicos": ["Smartphone", "Fone Bluetooth", "Notebook", "Smart TV", "Carregador Turbo", "Mouse Sem Fio"],
    "Casa e Cozinha": ["Air Fryer", "Liquidificador", "Jogo de Panelas", "Cafeteira", "Aspirador Robô"],
    "Esportes": ["Tênis de Corrida", "Bola de Futebol", "Bicicleta Aro 29", "Halteres 10kg", "Tapete de Yoga"],
    "Livros": ["Livro de Dados", "Livro de Ficção", "Livro Técnico", "Box de Clássicos"],
    "Moda": ["Camiseta Básica", "Calça Jeans", "Jaqueta", "Mochila", "Relógio Casual"],
    "Beleza": ["Perfume", "Kit Skincare", "Secador de Cabelo", "Protetor Solar"],
}
# faixa de preço (R$) por categoria, para métricas de negócio plausíveis na Gold
FAIXA_PRECO = {"Eletrônicos": (149.9, 4999.9), "Casa e Cozinha": (79.9, 1899.9), "Esportes": (39.9, 2499.9),
               "Livros": (19.9, 149.9), "Moda": (29.9, 599.9), "Beleza": (24.9, 699.9)}
ID_INEXISTENTE_MIN, ID_INEXISTENTE_MAX = 9000, 9999  # faixa reservada para chaves "órfãs"


def gerar_clientes(rng: np.random.Generator, n: int, ingest_date: dt.date) -> pd.DataFrame:
    linhas = []
    for i in range(1, n + 1):
        cidade, uf = CIDADES_UF[int(rng.integers(len(CIDADES_UF)))]
        nome = f"{NOMES[int(rng.integers(len(NOMES)))]} {SOBRENOMES[int(rng.integers(len(SOBRENOMES)))]}"
        cadastro = ingest_date - dt.timedelta(days=int(rng.integers(30, 900)))
        linhas.append((i, nome, f"cliente{i}@exemplo.com", cidade, uf, cadastro.isoformat()))
    return pd.DataFrame(linhas, columns=["cliente_id", "nome", "email", "cidade", "uf", "data_cadastro"])


def gerar_produtos(rng: np.random.Generator, n: int) -> pd.DataFrame:
    catalogo = [(cat, nome) for cat, nomes in CATEGORIAS.items() for nome in nomes]
    linhas = []
    for i in range(1, n + 1):
        cat, nome = catalogo[(i - 1) % len(catalogo)]
        sufixo = "" if i <= len(catalogo) else f" v{(i - 1) // len(catalogo) + 1}"
        preco = round(float(rng.uniform(*FAIXA_PRECO[cat])), 2)
        linhas.append((i, f"{nome}{sufixo}", cat, preco))
    return pd.DataFrame(linhas, columns=["product_id", "nome_produto", "categoria", "preco"])


def gerar_pedidos(
    rng: np.random.Generator,
    n_pedidos: int,
    n_clientes: int,
    n_produtos: int,
    ingest_date: dt.date,
    n_qtd_invalida: int,
    n_cliente_inexistente: int,
    n_produto_inexistente: int,
    n_multiplas: int,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """Gera pedidos válidos e injeta anomalias em linhas DISJUNTAS (contagens exatas e auditáveis)."""
    total_anomalias = n_qtd_invalida + n_cliente_inexistente + n_produto_inexistente + n_multiplas
    if total_anomalias > n_pedidos:
        raise SystemExit("A soma das anomalias não pode exceder o total de pedidos.")

    pedidos = pd.DataFrame(
        {
            "pedido_id": np.arange(1, n_pedidos + 1),
            "cliente_id": rng.integers(1, n_clientes + 1, n_pedidos),
            "product_id": rng.integers(1, n_produtos + 1, n_pedidos),
            "quantidade": rng.integers(1, 8, n_pedidos),
            "data_pedido": [
                (ingest_date - dt.timedelta(days=int(d))).isoformat() for d in rng.integers(0, 90, n_pedidos)
            ],
        }
    )

    embaralhado = rng.permutation(n_pedidos)  # posições sorteadas, sem repetição
    pos = 0

    def fatia(k: int) -> np.ndarray:
        nonlocal pos
        idx = embaralhado[pos : pos + k]
        pos += k
        return idx

    idx_qtd = fatia(n_qtd_invalida)
    idx_cli = fatia(n_cliente_inexistente)
    idx_prod = fatia(n_produto_inexistente)
    idx_multi = fatia(n_multiplas)

    pedidos.loc[idx_qtd, "quantidade"] = rng.choice([0, -1, -2, -5, -10], len(idx_qtd))
    pedidos.loc[idx_cli, "cliente_id"] = rng.integers(ID_INEXISTENTE_MIN, ID_INEXISTENTE_MAX + 1, len(idx_cli))
    pedidos.loc[idx_prod, "product_id"] = rng.integers(ID_INEXISTENTE_MIN, ID_INEXISTENTE_MAX + 1, len(idx_prod))
    pedidos.loc[idx_multi, "quantidade"] = -3
    pedidos.loc[idx_multi, "cliente_id"] = rng.integers(ID_INEXISTENTE_MIN, ID_INEXISTENTE_MAX + 1, len(idx_multi))

    esperado = {
        "pedidos_total": n_pedidos,
        "pedidos_invalidos": total_anomalias,
        "pedidos_validos": n_pedidos - total_anomalias,
        "violacoes_quantidade": n_qtd_invalida + n_multiplas,
        "violacoes_cliente_id": n_cliente_inexistente + n_multiplas,
        "violacoes_product_id": n_produto_inexistente,
    }
    return pedidos, esperado


def gravar_csv(storage: Storage, tabela: str, ingest_date: str, df: pd.DataFrame) -> str:
    """Grava <tabela>.csv em raw/<tabela>/ingest_date=<data>/ (estilo Hive). Idempotente: sobrescreve a chave."""
    key = f"{RAW}/{tabela}/ingest_date={ingest_date}/{tabela}.csv"
    conteudo = df.to_csv(index=False, sep=",", lineterminator="\n").encode("utf-8")
    storage.put_bytes(key, conteudo)
    return storage.uri(key)


def main(argv: list[str] | None = None) -> Dict[str, int]:
    parser = argparse.ArgumentParser(description="Ingestão RAW: gera CSVs com anomalias e grava particionado no S3")
    adicionar_args_comuns(parser)
    parser.add_argument("--seed", type=int, default=42, help="Semente aleatória (reprodutibilidade)")
    parser.add_argument("--n-clientes", type=int, default=200)
    parser.add_argument("--n-produtos", type=int, default=60)
    parser.add_argument("--n-pedidos", type=int, default=5000)
    parser.add_argument("--anom-quantidade", type=int, default=150, help="Pedidos com quantidade <= 0")
    parser.add_argument("--anom-cliente", type=int, default=100, help="Pedidos com cliente_id inexistente")
    parser.add_argument("--anom-produto", type=int, default=100, help="Pedidos com product_id inexistente")
    parser.add_argument("--anom-multiplas", type=int, default=20, help="Pedidos com quantidade<=0 E cliente inexistente")
    args = parser.parse_args(argv)

    storage = criar_storage(args)
    ingest_date = dt.date.fromisoformat(args.ingest_date)
    rng = np.random.default_rng(args.seed)

    clientes = gerar_clientes(rng, args.n_clientes, ingest_date)
    produtos = gerar_produtos(rng, args.n_produtos)
    pedidos, esperado = gerar_pedidos(
        rng, args.n_pedidos, args.n_clientes, args.n_produtos, ingest_date,
        args.anom_quantidade, args.anom_cliente, args.anom_produto, args.anom_multiplas,
    )

    print(f"[RAW] ingest_date={args.ingest_date} seed={args.seed}")
    for nome, df in (("clientes", clientes), ("produtos", produtos), ("pedidos", pedidos)):
        print(f"[RAW] {len(df):>6} linhas -> {gravar_csv(storage, nome, args.ingest_date, df)}")

    print("[RAW] Anomalias injetadas (valores ESPERADOS na etapa de Data Quality):")
    for chave, valor in esperado.items():
        print(f"        {chave:<22} = {valor}")
    return esperado


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
