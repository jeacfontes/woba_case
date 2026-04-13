"""
Pipeline de identificação de empresas em risco de churn.

Consome o output do modelo dbt `mart_credit_utilization_l3m` (taxa de utilização
de créditos por empresa nos últimos 3 meses) e identifica candidatas a churn
com base no threshold de 30% de utilização.

Em produção, a leitura seria feita via AWS Athena (awswrangler.athena.read_sql_query).
Aqui, os dados são mockados em JSON para demonstração.

Uso:
    python src/churn_analysis.py
    python src/churn_analysis.py --threshold 25 --output csv
"""

import json
import csv
import argparse
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Dados mockados: simulam o output do mart_credit_utilization_l3m no Athena
# ---------------------------------------------------------------------------
MOCK_MART_OUTPUT: list[dict[str, Any]] = [
    {
        "company_id": 101,
        "company_name": "Acme Corp",
        "company_size_tier": "Enterprise",
        "plan_name": "Enterprise Woba",
        "plan_type": "enterprise",
        "cost_per_credit": 2.50,
        "creditos_consumidos_trimestre": 450,
        "teto_creditos_trimestre": 500,
        "taxa_utilizacao_perc": 90.0,
        "media_geral_rede_perc": 65.5,
        "delta_vs_media": 24.5,
        "status_de_engajamento": "Acima da Média",
    },
    {
        "company_id": 202,
        "company_name": "Tech Inactive Inc",
        "company_size_tier": "Medium",
        "plan_name": "Premium Woba",
        "plan_type": "premium",
        "cost_per_credit": 3.00,
        "creditos_consumidos_trimestre": 50,
        "teto_creditos_trimestre": 300,
        "taxa_utilizacao_perc": 16.6,
        "media_geral_rede_perc": 65.5,
        "delta_vs_media": -48.9,
        "status_de_engajamento": "Crítico (Risco Churn)",
    },
    {
        "company_id": 303,
        "company_name": "Startup Lenta",
        "company_size_tier": "Small",
        "plan_name": "Starter Pack",
        "plan_type": "starter",
        "cost_per_credit": 4.50,
        "creditos_consumidos_trimestre": 25,
        "teto_creditos_trimestre": 100,
        "taxa_utilizacao_perc": 25.0,
        "media_geral_rede_perc": 65.5,
        "delta_vs_media": -40.5,
        "status_de_engajamento": "Crítico (Risco Churn)",
    },
    {
        "company_id": 404,
        "company_name": "Cowork Lovers",
        "company_size_tier": "Large",
        "plan_name": "Premium Woba",
        "plan_type": "premium",
        "cost_per_credit": 3.00,
        "creditos_consumidos_trimestre": 280,
        "teto_creditos_trimestre": 300,
        "taxa_utilizacao_perc": 93.3,
        "media_geral_rede_perc": 65.5,
        "delta_vs_media": 27.8,
        "status_de_engajamento": "Acima da Média",
    },
    {
        "company_id": 505,
        "company_name": "Low Engage SA",
        "company_size_tier": "Medium",
        "plan_name": "Enterprise Woba",
        "plan_type": "enterprise",
        "cost_per_credit": 2.50,
        "creditos_consumidos_trimestre": 120,
        "teto_creditos_trimestre": 500,
        "taxa_utilizacao_perc": 24.0,
        "media_geral_rede_perc": 65.5,
        "delta_vs_media": -41.5,
        "status_de_engajamento": "Crítico (Risco Churn)",
    },
]


def load_credit_utilization(source_path: Path | None = None) -> list[dict[str, Any]]:
    """Carrega dados de utilização de créditos.

    Em produção, usaríamos awswrangler para consultar o Athena diretamente:
        import awswrangler as wr
        df = wr.athena.read_sql_query("SELECT * FROM mart_credit_utilization_l3m", database="woba_dw")
        return df.to_dict(orient='records')

    Para o desafio, usamos dados mockados ou lemos de um JSON local.

    Args:
        source_path: Caminho para um JSON com os dados. Se None, usa mock.

    Returns:
        Lista de dicts com os dados de utilização por empresa.
    """
    if source_path and source_path.exists():
        with open(source_path, encoding="utf-8") as f:
            return json.load(f)

    return MOCK_MART_OUTPUT


def identify_churn_candidates(
    data: list[dict[str, Any]],
    threshold: float = 30.0,
) -> list[dict[str, Any]]:
    """Filtra empresas com taxa de utilização abaixo do threshold.

    Empresas com baixa utilização de créditos são candidatas a churn:
    estão pagando por um plano que não usam, o que aumenta a probabilidade
    de cancelamento.

    Args:
        data: Lista de registros do mart de utilização.
        threshold: Percentual mínimo de utilização. Default: 30%.

    Returns:
        Lista filtrada e enriquecida com campos de ação para CS.
    """
    candidates: list[dict[str, Any]] = []

    for row in data:
        utilizacao = row.get("taxa_utilizacao_perc", 100.0)

        if utilizacao < threshold:
            enriched = {
                **row,
                "churn_risk_level": "CRITICAL" if utilizacao < 15.0 else "HIGH",
                "suggested_action": (
                    "Agendar reunião urgente de retenção com CS"
                    if utilizacao < 15.0
                    else "Acionar CS para reunião de engajamento"
                ),
            }
            candidates.append(enriched)

    return candidates


def export_json(candidates: list[dict[str, Any]], output_path: Path) -> None:
    """Exporta candidatos a churn em formato JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(candidates, f, indent=2, ensure_ascii=False)

    print(f"[JSON] Exportados {len(candidates)} empresas → {output_path}")


def export_csv(candidates: list[dict[str, Any]], output_path: Path) -> None:
    """Exporta candidatos a churn em formato CSV."""
    if not candidates:
        print("[CSV] Nenhuma empresa em risco para exportar.")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = candidates[0].keys()

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(candidates)

    print(f"[CSV] Exportados {len(candidates)} empresas → {output_path}")


def run_pipeline(threshold: float = 30.0, output_format: str = "json") -> None:
    """Executa o pipeline completo de identificação de churn.

    Args:
        threshold: Percentual mínimo de utilização para considerar risco.
        output_format: Formato de saída ('json' ou 'csv').
    """
    print(f"{'=' * 60}")
    print(f"Pipeline de Churn — Threshold: {threshold}%")
    print(f"{'=' * 60}")

    # 1. Carga
    dataset = load_credit_utilization()
    print(f"[LOAD] {len(dataset)} empresas carregadas do mart")

    # 2. Filtragem
    candidates = identify_churn_candidates(dataset, threshold=threshold)
    print(f"[FILTER] {len(candidates)} empresas abaixo de {threshold}% de utilização")

    if not candidates:
        print("[DONE] Nenhuma empresa em risco. Pipeline finalizado.")
        return

    # 3. Exportação
    output_dir = Path(__file__).parent / "data"
    if output_format == "csv":
        export_csv(candidates, output_dir / "churn_candidates.csv")
    else:
        export_json(candidates, output_dir / "churn_candidates.json")

    # 4. Resumo
    print(f"\n{'=' * 60}")
    print("Resumo das empresas em risco:")
    print(f"{'=' * 60}")
    for c in candidates:
        print(
            f"  • {c['company_name']} ({c['company_size_tier']}) "
            f"— {c['taxa_utilizacao_perc']}% utilização "
            f"— Risco: {c['churn_risk_level']}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Identifica empresas em risco de churn com base na utilização de créditos."
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=30.0,
        help="Percentual mínimo de utilização (default: 30.0)",
    )
    parser.add_argument(
        "--output",
        choices=["json", "csv"],
        default="json",
        help="Formato de saída (default: json)",
    )
    args = parser.parse_args()
    run_pipeline(threshold=args.threshold, output_format=args.output)
