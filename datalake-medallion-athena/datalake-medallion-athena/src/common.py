"""Utilitários compartilhados: abstração de armazenamento (S3 ou pasta local) e argumentos comuns.

O modo local (--local-dir) espelha exatamente a estrutura de chaves do S3 e serve para
testar o pipeline sem custo/credenciais antes de rodar na AWS.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
from pathlib import Path
from typing import List

# Nome do database no Athena (usado também pelos scripts SQL)
DATABASE = "datalake_atividade2"

# Prefixos (camadas) do data lake
RAW = "raw"
QUARANTINE = "quarantine/pedidos_rejeitados"
SILVER = "processed/fato_vendas"
GOLD = "gold/vendas_uf_categoria"
ATHENA_RESULTS = "athena-results"


class Storage:
    """Interface mínima de armazenamento orientada a chaves (estilo S3)."""

    def put_bytes(self, key: str, data: bytes) -> None:
        raise NotImplementedError

    def get_bytes(self, key: str) -> bytes:
        raise NotImplementedError

    def list_keys(self, prefix: str) -> List[str]:
        raise NotImplementedError

    def delete_prefix(self, prefix: str) -> int:
        raise NotImplementedError

    def uri(self, key: str) -> str:
        raise NotImplementedError


class S3Storage(Storage):
    def __init__(self, bucket: str, region: str | None = None):
        import boto3  # import tardio: o modo local não exige boto3 configurado

        self.bucket = bucket
        self.s3 = boto3.client("s3", region_name=region)

    def put_bytes(self, key: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    def get_bytes(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def list_keys(self, prefix: str) -> List[str]:
        keys: List[str] = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys

    def delete_prefix(self, prefix: str) -> int:
        keys = self.list_keys(prefix)
        for i in range(0, len(keys), 1000):  # limite da API: 1000 chaves por chamada
            lote = [{"Key": k} for k in keys[i : i + 1000]]
            self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": lote})
        return len(keys)

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"


class LocalStorage(Storage):
    def __init__(self, root: str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / key

    def put_bytes(self, key: str, data: bytes) -> None:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def get_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def list_keys(self, prefix: str) -> List[str]:
        base = self._path(prefix)
        if not base.exists():
            return []
        if base.is_file():
            return [prefix]
        return sorted(str(p.relative_to(self.root)).replace(os.sep, "/") for p in base.rglob("*") if p.is_file())

    def delete_prefix(self, prefix: str) -> int:
        base = self._path(prefix)
        n = len(self.list_keys(prefix))
        if base.exists():
            shutil.rmtree(base) if base.is_dir() else base.unlink()
        return n

    def uri(self, key: str) -> str:
        return str(self._path(key))


def data_iso(valor: str) -> str:
    """Valida o formato YYYY-MM-DD (usado nas partições Hive)."""
    try:
        return dt.date.fromisoformat(valor).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"data inválida '{valor}': use YYYY-MM-DD") from exc


def adicionar_args_comuns(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bucket", default=os.getenv("BUCKET"), help="Nome do bucket S3 (ou env BUCKET)")
    parser.add_argument("--region", default=os.getenv("AWS_DEFAULT_REGION"), help="Região AWS (ou env AWS_DEFAULT_REGION)")
    parser.add_argument(
        "--ingest-date",
        type=data_iso,
        default=os.getenv("INGEST_DATE") or dt.date.today().isoformat(),
        help="Data de ingestão YYYY-MM-DD (padrão: hoje)",
    )
    parser.add_argument("--local-dir", default=None, help="Se informado, grava em pasta local em vez do S3 (teste offline)")


def criar_storage(args: argparse.Namespace) -> Storage:
    if args.local_dir:
        return LocalStorage(args.local_dir)
    if not args.bucket:
        raise SystemExit("Informe --bucket (ou a variável de ambiente BUCKET), ou use --local-dir para teste offline.")
    return S3Storage(args.bucket, args.region)
