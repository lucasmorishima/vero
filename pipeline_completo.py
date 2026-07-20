from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from urllib3.util.retry import Retry


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline.yaml"
DEFAULT_CNPJ_TEST_FILE = PROJECT_ROOT / "output" / "cnpj_test_results.csv"


@dataclass
class PipelineConfig:
    source_url: str
    raw_filename: str
    processed_filename: str
    train_filename: str
    test_filename: str
    id_column: str | None
    target_column: str | None
    required_columns: list[str]
    test_size: float
    random_state: int


@dataclass
class PipelinePaths:
    raw_dir: Path
    processed_dir: Path
    output_dir: Path
    logs_dir: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Executa pipeline completo de dados")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Caminho para arquivo de configuracao YAML",
    )
    parser.add_argument(
        "--test-cnpjs",
        action="store_true",
        help="Executa teste para CNPJs e salva resultado em CSV",
    )
    parser.add_argument(
        "--cnpjs",
        nargs="+",
        default=None,
        help="Lista de CNPJs para teste. Se omitido, usa 3 CNPJs gerados para validacao.",
    )
    return parser.parse_args()


def setup_logging(logs_dir: Path) -> None:
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "pipeline.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def load_config(config_path: Path) -> PipelineConfig:
    if not config_path.exists():
        raise FileNotFoundError(f"Arquivo de config nao encontrado: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file)

    required_keys = {
        "source_url",
        "raw_filename",
        "processed_filename",
        "train_filename",
        "test_filename",
        "required_columns",
        "test_size",
        "random_state",
    }
    missing = required_keys - set(data.keys())
    if missing:
        raise ValueError(f"Chaves ausentes no config: {sorted(missing)}")

    return PipelineConfig(
        source_url=data["source_url"],
        raw_filename=data["raw_filename"],
        processed_filename=data["processed_filename"],
        train_filename=data["train_filename"],
        test_filename=data["test_filename"],
        id_column=data.get("id_column"),
        target_column=data.get("target_column"),
        required_columns=data["required_columns"],
        test_size=float(data["test_size"]),
        random_state=int(data["random_state"]),
    )


def ensure_project_paths(base_dir: Path) -> PipelinePaths:
    paths = PipelinePaths(
        raw_dir=base_dir / "data" / "raw",
        processed_dir=base_dir / "data" / "processed",
        output_dir=base_dir / "output",
        logs_dir=base_dir / "logs",
    )

    for folder in [paths.raw_dir, paths.processed_dir, paths.output_dir, paths.logs_dir]:
        folder.mkdir(parents=True, exist_ok=True)

    return paths


def download_source_csv(url: str, destination_file: Path) -> Path:
    logging.info("Baixando dados de %s", url)
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    destination_file.write_bytes(response.content)
    logging.info("Arquivo bruto salvo em %s", destination_file)
    return destination_file


def load_dataframe(csv_file: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_file)
    logging.info("Dados carregados: %s linhas e %s colunas", len(df), len(df.columns))
    return df


def normalize_column_name(column_name: str) -> str:
    name = column_name.strip().lower()
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def validate_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Colunas obrigatorias ausentes no dataset: {missing}")


def clean_and_transform_dataframe(
    df: pd.DataFrame,
    id_column: str | None,
    target_column: str | None,
) -> pd.DataFrame:
    transformed = df.copy()

    transformed.columns = [normalize_column_name(col) for col in transformed.columns]
    if id_column:
        id_column = normalize_column_name(id_column)
    if target_column:
        target_column = normalize_column_name(target_column)

    transformed = transformed.drop_duplicates().reset_index(drop=True)

    numeric_cols = transformed.select_dtypes(include=["number"]).columns.tolist()
    categorical_cols = transformed.select_dtypes(exclude=["number"]).columns.tolist()

    for col in numeric_cols:
        transformed[col] = transformed[col].fillna(transformed[col].median())

    for col in categorical_cols:
        mode_value = transformed[col].mode(dropna=True)
        if not mode_value.empty:
            transformed[col] = transformed[col].fillna(mode_value.iloc[0])

    if id_column and id_column in transformed.columns:
        transformed = transformed.drop(columns=[id_column])

    # Padroniza atributos numericos para melhorar consumo por modelos.
    scale_cols = [col for col in numeric_cols if col != target_column and col in transformed.columns]
    if scale_cols:
        scaler = StandardScaler()
        transformed[scale_cols] = scaler.fit_transform(transformed[scale_cols])

    return transformed


def split_train_test(
    df: pd.DataFrame,
    target_column: str | None,
    test_size: float,
    random_state: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if target_column and target_column in df.columns:
        train_df, test_df = train_test_split(
            df,
            test_size=test_size,
            random_state=random_state,
            stratify=df[target_column],
        )
    else:
        train_df, test_df = train_test_split(
            df,
            test_size=test_size,
            random_state=random_state,
        )

    return train_df.reset_index(drop=True), test_df.reset_index(drop=True)


def save_dataframe(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)
    logging.info("Arquivo salvo: %s", path)


def sanitize_cnpj(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj)


def calculate_cnpj_digit(base_numbers: str, weights: list[int]) -> str:
    total = sum(int(num) * weight for num, weight in zip(base_numbers, weights))
    remainder = total % 11
    digit = 0 if remainder < 2 else 11 - remainder
    return str(digit)


def build_valid_cnpj(base_twelve_digits: str) -> str:
    if not re.fullmatch(r"\d{12}", base_twelve_digits):
        raise ValueError("Base do CNPJ deve conter exatamente 12 digitos")

    first_weights = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    second_weights = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    first_digit = calculate_cnpj_digit(base_twelve_digits, first_weights)
    second_digit = calculate_cnpj_digit(base_twelve_digits + first_digit, second_weights)
    return base_twelve_digits + first_digit + second_digit


def is_valid_cnpj(cnpj: str) -> bool:
    normalized = sanitize_cnpj(cnpj)
    if len(normalized) != 14:
        return False
    if normalized == normalized[0] * 14:
        return False

    base = normalized[:12]
    expected = build_valid_cnpj(base)
    return normalized == expected


def fetch_cnpj_info(cnpj: str) -> dict[str, Any]:
    normalized = sanitize_cnpj(cnpj)
    url = f"https://brasilapi.com.br/api/cnpj/v1/{quote(normalized)}"
    retry = Retry(
        total=4,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)

    headers = {"User-Agent": "VeroPipelineCNPJ/1.0"}

    try:
        response = session.get(url, timeout=20, headers=headers)

        # Em 429 persistente, aguarda rapidamente e tenta uma ultima vez.
        if response.status_code == 429:
            time.sleep(2)
            response = session.get(url, timeout=20, headers=headers)

        if response.status_code == 200:
            payload = response.json()
            return {
                "api_status": "ok",
                "razao_social": payload.get("razao_social"),
                "nome_fantasia": payload.get("nome_fantasia"),
                "descricao_situacao_cadastral": payload.get("descricao_situacao_cadastral"),
                "uf": payload.get("uf"),
                "municipio": payload.get("municipio"),
            }

        if response.status_code == 429:
            return {
                "api_status": "erro_http_429_limite_tentativas",
                "razao_social": None,
                "nome_fantasia": None,
                "descricao_situacao_cadastral": None,
                "uf": None,
                "municipio": None,
            }

        return {
            "api_status": f"erro_http_{response.status_code}",
            "razao_social": None,
            "nome_fantasia": None,
            "descricao_situacao_cadastral": None,
            "uf": None,
            "municipio": None,
        }
    except requests.RequestException:
        return {
            "api_status": "erro_rede",
            "razao_social": None,
            "nome_fantasia": None,
            "descricao_situacao_cadastral": None,
            "uf": None,
            "municipio": None,
        }
    finally:
        session.close()


def default_test_cnpjs() -> list[str]:
    # CNPJs sintaticamente validos para teste local.
    bases = ["123456780001", "112223330001", "987654320001"]
    return [build_valid_cnpj(base) for base in bases]


def run_cnpj_test(cnpjs: list[str]) -> dict[str, Any]:
    paths = ensure_project_paths(PROJECT_ROOT)
    setup_logging(paths.logs_dir)

    records: list[dict[str, Any]] = []
    for raw_cnpj in cnpjs:
        normalized = sanitize_cnpj(raw_cnpj)
        valid = is_valid_cnpj(normalized)
        api_info = fetch_cnpj_info(normalized) if valid else {
            "api_status": "nao_consultado_cnpj_invalido",
            "razao_social": None,
            "nome_fantasia": None,
            "descricao_situacao_cadastral": None,
            "uf": None,
            "municipio": None,
        }

        records.append(
            {
                "cnpj_informado": raw_cnpj,
                "cnpj_normalizado": normalized,
                "cnpj_valido": valid,
                **api_info,
            }
        )

    result_df = pd.DataFrame(records)
    save_dataframe(result_df, DEFAULT_CNPJ_TEST_FILE)

    summary = {
        "total_cnpjs": len(result_df),
        "validos": int(result_df["cnpj_valido"].sum()),
        "invalidos": int((~result_df["cnpj_valido"]).sum()),
        "arquivo_saida": str(DEFAULT_CNPJ_TEST_FILE),
    }
    logging.info("Teste de CNPJ finalizado: %s", summary)
    return summary


def run_pipeline(config_path: Path) -> dict[str, Any]:
    load_dotenv()
    paths = ensure_project_paths(PROJECT_ROOT)
    setup_logging(paths.logs_dir)

    config = load_config(config_path)

    raw_file = paths.raw_dir / config.raw_filename
    download_source_csv(config.source_url, raw_file)

    raw_df = load_dataframe(raw_file)

    normalized_required = [normalize_column_name(col) for col in config.required_columns]
    raw_df.columns = [normalize_column_name(col) for col in raw_df.columns]
    validate_columns(raw_df, normalized_required)

    transformed_df = clean_and_transform_dataframe(
        raw_df,
        id_column=config.id_column,
        target_column=config.target_column,
    )

    processed_file = paths.processed_dir / config.processed_filename
    save_dataframe(transformed_df, processed_file)

    normalized_target = normalize_column_name(config.target_column) if config.target_column else None
    train_df, test_df = split_train_test(
        transformed_df,
        target_column=normalized_target,
        test_size=config.test_size,
        random_state=config.random_state,
    )

    train_file = paths.output_dir / config.train_filename
    test_file = paths.output_dir / config.test_filename
    save_dataframe(train_df, train_file)
    save_dataframe(test_df, test_file)

    summary = {
        "raw_file": str(raw_file),
        "processed_file": str(processed_file),
        "train_file": str(train_file),
        "test_file": str(test_file),
        "total_rows": len(transformed_df),
        "train_rows": len(train_df),
        "test_rows": len(test_df),
    }

    logging.info("Pipeline finalizado com sucesso: %s", summary)
    return summary


def main() -> None:
    args = parse_args()
    if args.test_cnpjs:
        cnpjs = args.cnpjs or default_test_cnpjs()
        result = run_cnpj_test(cnpjs)
    else:
        result = run_pipeline(args.config)
    print(result)


if __name__ == "__main__":
    main()
