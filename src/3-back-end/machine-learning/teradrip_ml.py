"""TeraDrip hybrid Market Basket Analysis engine.

This script:
1) Extracts transactions from Supabase.
2) Builds a basket matrix.
3) Dynamically picks Apriori or FP-Growth.
4) Auto-tunes support/confidence/lift.
5) Simulates 3 learning phases.
6) Logs governance + rule deltas.
7) Stores frontend-ready recommendation payloads.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd

try:
    import psycopg
except ImportError:  # pragma: no cover - runtime fallback
    psycopg = None

from mlxtend.frequent_patterns import apriori, association_rules, fpgrowth


ML_DIR = Path(__file__).resolve().parent
if str(ML_DIR) not in sys.path:
    sys.path.insert(0, str(ML_DIR))

from database import get_supabase_rest_config, select_table


# Accept both product/item IDs (i00001) and service IDs (s00001).
ITEM_COL_PATTERN = re.compile(r"^[is]\d{4,}$", re.IGNORECASE)
DEFAULT_SOURCE_TABLE = "teradrip_datasets_hairstylist_datasets"
DEFAULT_OUTPUT_TABLE = "ml_recommendations"


@dataclass
class Thresholds:
    min_support: float
    min_confidence: float
    min_lift: float


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def to_bool_series(series: pd.Series) -> pd.Series:
    lowered = series.fillna(0).astype(str).str.strip().str.lower()
    return lowered.isin({"1", "1.0", "true", "t", "yes", "y"})


def is_boolean_like_series(series: pd.Series) -> bool:
    non_null = series.dropna()
    if non_null.empty:
        return False
    lowered = non_null.astype(str).str.strip().str.lower()
    allowed = {"1", "1.0", "0", "0.0", "true", "false", "t", "f", "yes", "no", "y", "n"}
    return lowered.isin(allowed).all()


def normalize_item_name(item: str) -> str:
    value = str(item).strip()
    if not value:
        return "unknown_item"
    return value.lower().replace(" ", "_")


def json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_ready(v) for v in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


class TeraDripMBAEngine:
    def __init__(
        self,
        source_table: str = DEFAULT_SOURCE_TABLE,
        output_table: str = DEFAULT_OUTPUT_TABLE,
        max_loops: int = 5,
    ):
        self.source_table = source_table
        self.output_table = output_table
        self.max_loops = max_loops
        self.run_id = str(uuid.uuid4())
        self.generated_at = utc_now_iso()

    def extract_transactions(self, limit: int | None = None) -> pd.DataFrame:
        rows = select_table(self.source_table, limit=limit)
        if not rows:
            raise ValueError(f"No rows found in source table: {self.source_table}")
        return pd.DataFrame(rows)

    def _detect_item_columns(self, df: pd.DataFrame) -> List[str]:
        candidates = [col for col in df.columns if ITEM_COL_PATTERN.match(str(col))]
        return sorted(candidates)

    def _detect_boolean_indicator_columns(self, df: pd.DataFrame) -> List[str]:
        excluded_tokens = {
            "id",
            "date",
            "time",
            "total",
            "cost",
            "amount",
            "price",
            "transaction",
            "availed",
            "services_availed",
            "items_used",
        }

        indicators: List[str] = []
        for col in df.columns:
            name = str(col).strip().lower()
            if any(token in name for token in excluded_tokens):
                continue

            series = df[col]
            if str(series.dtype) == "bool" or is_boolean_like_series(series):
                # Require at least one positive flag so non-informative columns are ignored.
                if to_bool_series(series).any():
                    indicators.append(col)

        return sorted(indicators)

    def transform_to_basket_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
        item_columns = self._detect_item_columns(df)

        if item_columns:
            matrix = pd.DataFrame({col.lower(): to_bool_series(df[col]) for col in item_columns})
            matrix = matrix.astype(bool)
            matrix = matrix.loc[:, matrix.any(axis=0)]
            if matrix.empty:
                raise ValueError("No positive item indicators found after transform")
            return matrix

        indicator_columns = self._detect_boolean_indicator_columns(df)
        if indicator_columns:
            matrix = pd.DataFrame({str(col).lower(): to_bool_series(df[col]) for col in indicator_columns})
            matrix = matrix.astype(bool)
            matrix = matrix.loc[:, matrix.any(axis=0)]
            if matrix.empty:
                raise ValueError("No positive indicator values found in boolean item/service columns")
            return matrix

        token_columns = ["items_used", "services_availed"]
        token_source = next((col for col in token_columns if col in df.columns), None)
        if token_source is not None:
            encoded_rows: List[List[str]] = []
            all_items: set[str] = set()
            for raw in df[token_source].fillna("").astype(str):
                tokens = [normalize_item_name(tok) for tok in raw.split(",") if tok.strip()]
                encoded_rows.append(tokens)
                all_items.update(tokens)

            if all_items:
                ordered_items = sorted(all_items)
                mat = []
                for tokens in encoded_rows:
                    token_set = set(tokens)
                    mat.append([item in token_set for item in ordered_items])

                matrix = pd.DataFrame(mat, columns=ordered_items).astype(bool)
                matrix = matrix.loc[:, matrix.any(axis=0)]
                if not matrix.empty:
                    return matrix

        raise ValueError("No item/service indicator columns detected (expected i####/s####, boolean flags, or items_used/services_availed)")

    def basket_density(self, basket: pd.DataFrame) -> float:
        if basket.empty:
            return 0.0
        return float(basket.to_numpy(dtype="int8").mean())

    def basket_stats(self, basket: pd.DataFrame) -> Dict[str, float]:
        sizes = basket.sum(axis=1)
        return {
            "transactions": float(len(basket)),
            "distinct_items": float(basket.shape[1]),
            "avg_basket_size": float(sizes.mean()),
            "basket_size_variance": float(sizes.var(ddof=0)),
            "density": self.basket_density(basket),
        }

    def choose_algorithm(self, basket: pd.DataFrame) -> str:
        density = self.basket_density(basket)
        n_rows, n_cols = basket.shape
        if density < 0.12 and n_rows <= 5000 and n_cols <= 300:
            return "apriori"
        return "fpgrowth"

    def auto_thresholds(self, basket: pd.DataFrame) -> Thresholds:
        stats = self.basket_stats(basket)
        n_rows = max(1.0, stats["transactions"])
        density = stats["density"]
        var = stats["basket_size_variance"]
        avg = max(1.0, stats["avg_basket_size"])

        volume_penalty = min(0.08, 2500.0 / (n_rows + 2500.0) * 0.08)
        variance_boost = min(0.05, (var / avg) * 0.015)

        min_support = clamp(0.02 + volume_penalty + (density * 0.08) + variance_boost, 0.005, 0.22)
        min_confidence = clamp(0.40 + (density * 0.22) - (n_rows / 50000.0) + variance_boost, 0.25, 0.9)
        min_lift = clamp(1.02 + (density * 0.30), 1.01, 2.2)

        return Thresholds(
            min_support=round(min_support, 4),
            min_confidence=round(min_confidence, 4),
            min_lift=round(min_lift, 4),
        )

    def _mine_rules_once(self, basket: pd.DataFrame, thresholds: Thresholds) -> tuple[str, pd.DataFrame, pd.DataFrame]:
        algorithm = self.choose_algorithm(basket)
        if algorithm == "apriori":
            itemsets = apriori(basket, min_support=thresholds.min_support, use_colnames=True)
        else:
            itemsets = fpgrowth(basket, min_support=thresholds.min_support, use_colnames=True)

        if itemsets.empty:
            return algorithm, itemsets, pd.DataFrame()

        rules = association_rules(itemsets, metric="confidence", min_threshold=thresholds.min_confidence)
        if rules.empty:
            return algorithm, itemsets, rules

        rules = rules[rules["lift"] >= thresholds.min_lift].copy()
        if rules.empty:
            return algorithm, itemsets, rules

        rules["antecedent_items"] = rules["antecedents"].apply(lambda s: sorted(map(str, s)))
        rules["consequent_items"] = rules["consequents"].apply(lambda s: sorted(map(str, s)))
        rules["rule_key"] = rules.apply(
            lambda r: f"{'|'.join(r['antecedent_items'])}=>{'|'.join(r['consequent_items'])}", axis=1
        )
        rules = rules.sort_values(["confidence", "lift", "support"], ascending=False).reset_index(drop=True)
        return algorithm, itemsets, rules

    def _target_rule_count(self, basket: pd.DataFrame) -> int:
        stats = self.basket_stats(basket)
        n_rows = stats["transactions"]
        n_items = stats["distinct_items"]
        density = stats["density"]
        est = (n_rows ** 0.45) * (n_items ** 0.35) * (1.0 + (density * 2.2))
        return int(clamp(est, 12, 220))

    def _score_rules(self, rules: pd.DataFrame, basket: pd.DataFrame, target_rules: int) -> float:
        if rules.empty:
            return 0.0
        distinct_items = set()
        for row in rules.itertuples(index=False):
            distinct_items.update(row.antecedent_items)
            distinct_items.update(row.consequent_items)

        coverage = len(distinct_items) / max(1, basket.shape[1])
        mean_conf = float(rules["confidence"].mean())
        mean_lift = float(rules["lift"].mean())
        normalized_lift = clamp(mean_lift / 4.0, 0.0, 1.0)

        # Reward quality + item coverage, while discouraging too-few/too-many rules.
        quality = (mean_conf * 0.50) + (normalized_lift * 0.30) + (coverage * 0.20)
        count_penalty = abs(len(rules) - target_rules) / max(float(target_rules), 1.0)
        low_conf_penalty = max(0.0, 0.45 - mean_conf) * 0.55
        return round(quality - (count_penalty * 0.22) - low_conf_penalty, 6)

    def _stability_report(self, new_rules: pd.DataFrame, historical_rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        old_map = {row["rule_key"]: row for row in historical_rules if "rule_key" in row}
        new_map = {
            row["rule_key"]: {
                "confidence": float(row["confidence"]),
                "lift": float(row["lift"]),
                "support": float(row["support"]),
            }
            for row in new_rules[["rule_key", "confidence", "lift", "support"]].to_dict(orient="records")
        }

        added = sorted(set(new_map.keys()) - set(old_map.keys()))
        removed = sorted(set(old_map.keys()) - set(new_map.keys()))

        changed = []
        anomalies = []
        for key in sorted(set(new_map.keys()) & set(old_map.keys())):
            old_conf = float(old_map[key].get("confidence", 0.0))
            new_conf = float(new_map[key].get("confidence", 0.0))
            delta = round(new_conf - old_conf, 4)
            if abs(delta) >= 0.05:
                changed.append({"rule_key": key, "delta_confidence": delta})
            if delta < -0.30:
                anomalies.append({"rule_key": key, "reason": "confidence_drop_gt_0.30", "delta": delta})

        for row in new_rules.itertuples(index=False):
            if row.confidence >= 0.95 and row.support < 0.01:
                anomalies.append({"rule_key": row.rule_key, "reason": "high_conf_low_support"})

        strong_old = {
            key
            for key, value in old_map.items()
            if float(value.get("confidence", 0)) >= 0.65 and float(value.get("support", 0)) >= 0.02
        }
        dropped_strong = sorted(strong_old.intersection(removed))

        return {
            "added_count": len(added),
            "removed_count": len(removed),
            "changed_count": len(changed),
            "dropped_strong_count": len(dropped_strong),
            "added": added[:20],
            "removed": removed[:20],
            "changed": changed[:20],
            "anomalies": anomalies[:30],
            "manual_review_required": len(anomalies) > 0,
        }

    def _optimization_loop(
        self,
        basket: pd.DataFrame,
        historical_rules: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        best: Dict[str, Any] | None = None
        plateau_hits = 0
        previous_score = None
        loop_trace: List[Dict[str, Any]] = []
        target_rules = self._target_rule_count(basket)
        min_acceptable_rules = max(8, int(target_rules * 0.35))

        base = self.auto_thresholds(basket)
        for loop_id in range(1, self.max_loops + 1):
            min_support = clamp(base.min_support * (1.0 - (loop_id - 1) * 0.08), 0.003, 0.25)
            min_conf = clamp(base.min_confidence * (1.0 - (loop_id - 1) * 0.04), 0.20, 0.95)
            min_lift = clamp(base.min_lift * (1.0 - (loop_id - 1) * 0.03), 1.01, 3.0)
            candidate_thresholds = [
                Thresholds(
                    round(clamp(min_support * 1.12, 0.003, 0.25), 4),
                    round(clamp(min_conf * 1.06, 0.20, 0.95), 4),
                    round(clamp(min_lift * 1.05, 1.01, 3.0), 4),
                ),  # stricter
                Thresholds(round(min_support, 4), round(min_conf, 4), round(min_lift, 4)),  # baseline
                Thresholds(
                    round(clamp(min_support * 0.82, 0.003, 0.25), 4),
                    round(clamp(min_conf * 0.92, 0.20, 0.95), 4),
                    round(clamp(min_lift * 0.95, 1.01, 3.0), 4),
                ),  # relaxed
            ]

            loop_best: Dict[str, Any] | None = None
            candidate_scores: List[Dict[str, Any]] = []
            for idx, thresholds in enumerate(candidate_thresholds, start=1):
                algo, itemsets, rules = self._mine_rules_once(basket, thresholds)
                score = self._score_rules(rules, basket, target_rules=target_rules)
                candidate_scores.append(
                    {
                        "candidate": idx,
                        "thresholds": thresholds.__dict__,
                        "algorithm": algo,
                        "rules": int(len(rules)),
                        "itemsets": int(len(itemsets)),
                        "score": score,
                    }
                )
                if loop_best is None or score > loop_best["score"]:
                    loop_best = {
                        "algorithm": algo,
                        "thresholds": thresholds,
                        "itemsets": itemsets,
                        "rules": rules,
                        "score": score,
                    }

            if loop_best is None:
                continue

            loop_trace.append(
                {
                    "loop": loop_id,
                    "target_rules": int(target_rules),
                    "selected_algorithm": loop_best["algorithm"],
                    "selected_thresholds": loop_best["thresholds"].__dict__,
                    "selected_rules": int(len(loop_best["rules"])),
                    "selected_score": loop_best["score"],
                    "candidates": candidate_scores,
                }
            )

            if best is None or loop_best["score"] > best["score"]:
                best = loop_best

            if previous_score is not None and abs(loop_best["score"] - previous_score) < 0.0015:
                plateau_hits += 1
            else:
                plateau_hits = 0

            previous_score = loop_best["score"]

            # Stop early only after achieving useful rule volume.
            if plateau_hits >= 2 and int(len(best["rules"])) >= min_acceptable_rules:
                break

        # Rescue pass: if rule volume is still too low, relax constraints further.
        if best is not None and int(len(best["rules"])) < min_acceptable_rules:
            rescue_base = best["thresholds"]
            for rescue_idx in range(1, 4):
                rescue_thresholds = Thresholds(
                    round(clamp(rescue_base.min_support * (0.78 ** rescue_idx), 0.002, 0.25), 4),
                    round(clamp(rescue_base.min_confidence * (0.90 ** rescue_idx), 0.18, 0.95), 4),
                    round(clamp(rescue_base.min_lift * (0.94 ** rescue_idx), 1.01, 3.0), 4),
                )
                algo, itemsets, rules = self._mine_rules_once(basket, rescue_thresholds)
                score = self._score_rules(rules, basket, target_rules=target_rules)
                loop_trace.append(
                    {
                        "loop": f"rescue_{rescue_idx}",
                        "target_rules": int(target_rules),
                        "selected_algorithm": algo,
                        "selected_thresholds": rescue_thresholds.__dict__,
                        "selected_rules": int(len(rules)),
                        "selected_score": score,
                    }
                )
                if score > best["score"]:
                    best = {
                        "algorithm": algo,
                        "thresholds": rescue_thresholds,
                        "itemsets": itemsets,
                        "rules": rules,
                        "score": score,
                    }
                if int(len(best["rules"])) >= min_acceptable_rules:
                    break

        if best is None:
            raise ValueError("Optimization failed: no model output")

        best["loop_trace"] = loop_trace
        best["governance"] = self._stability_report(best["rules"], historical_rules)
        return best

    def _sample_phase_dataframe(self, raw: pd.DataFrame, phase: int) -> pd.DataFrame:
        n = len(raw)
        if phase == 1:
            return raw.head(max(100, int(n * 0.50))).copy()
        if phase == 2:
            return raw.head(max(150, int(n * 0.75))).copy()
        return raw.copy()

    def _rules_to_records(self, rules: pd.DataFrame, limit: int = 200) -> List[Dict[str, Any]]:
        if rules.empty:
            return []
        out = []
        for row in rules.head(limit).itertuples(index=False):
            out.append(
                {
                    "rule_key": row.rule_key,
                    "antecedent_items": row.antecedent_items,
                    "consequent_items": row.consequent_items,
                    "support": round(float(row.support), 6),
                    "confidence": round(float(row.confidence), 6),
                    "lift": round(float(row.lift), 6),
                    "leverage": round(float(row.leverage), 6),
                    "conviction": (
                        round(float(row.conviction), 6)
                        if pd.notna(row.conviction) and math.isfinite(float(row.conviction))
                        else None
                    ),
                }
            )
        return out

    def _itemsets_to_bundles(self, itemsets: pd.DataFrame, limit: int = 10) -> List[Dict[str, Any]]:
        if itemsets.empty:
            return []
        # A bundle should contain at least two items; filter singleton itemsets out.
        multi_itemsets = itemsets[itemsets["itemsets"].apply(lambda s: len(s) >= 2)]
        if multi_itemsets.empty:
            return []

        sorted_sets = multi_itemsets.sort_values("support", ascending=False).head(limit)
        bundles = []
        for row in sorted_sets.itertuples(index=False):
            items = sorted(map(str, row.itemsets))
            bundles.append(
                {
                    "items": items,
                    "support": round(float(row.support), 6),
                    "explanation": (
                        f"These services/products co-occur in {row.support:.1%} of baskets, "
                        "suggesting a premium add-on pathway customers repeatedly choose."
                    ),
                }
            )
        return bundles

    def _fbt_widget(self, rules: pd.DataFrame, limit: int = 12) -> List[Dict[str, Any]]:
        rows = []
        for row in rules.head(limit).itertuples(index=False):
            rows.append(
                {
                    "if_items": row.antecedent_items,
                    "then_items": row.consequent_items,
                    "confidence": round(float(row.confidence), 4),
                    "lift": round(float(row.lift), 4),
                    "support": round(float(row.support), 4),
                }
            )
        return rows

    def _cross_sell_map(self, rules: pd.DataFrame, max_per_item: int = 5) -> Dict[str, List[str]]:
        mapping: Dict[str, List[Tuple[str, float]]] = {}
        singleton = rules[rules["antecedent_items"].apply(lambda x: len(x) == 1)]
        for row in singleton.itertuples(index=False):
            base = row.antecedent_items[0]
            suggestions = row.consequent_items[:]
            for s in suggestions:
                mapping.setdefault(base, []).append((s, float(row.confidence) * float(row.lift)))

        compressed: Dict[str, List[str]] = {}
        for base, values in mapping.items():
            sorted_values = sorted(values, key=lambda x: x[1], reverse=True)
            dedup = []
            seen = set()
            for item, _score in sorted_values:
                if item in seen:
                    continue
                dedup.append(item)
                seen.add(item)
                if len(dedup) >= max_per_item:
                    break
            compressed[base] = dedup
        return compressed

    def _promo_recommendations(self, rules: pd.DataFrame, limit: int = 8) -> List[Dict[str, Any]]:
        promos = []
        for row in rules.head(limit * 3).itertuples(index=False):
            antecedent = row.antecedent_items
            consequent = row.consequent_items
            combined = antecedent + consequent
            if len(combined) >= 3 and row.confidence >= 0.65:
                offer_type = "buy_2_get_1"
                text = f"Buy any 2 from {combined[:3]} and get 1 add-on discounted"
            else:
                offer_type = "bundle_discount"
                text = f"Bundle {combined[:2]} at 10-15% off to increase average ticket"

            promos.append(
                {
                    "offer_type": offer_type,
                    "trigger_items": antecedent,
                    "target_items": consequent,
                    "message": text,
                    "confidence": round(float(row.confidence), 4),
                    "lift": round(float(row.lift), 4),
                }
            )
            if len(promos) >= limit:
                break
        return promos

    def _business_insights(self, phase_metrics: List[Dict[str, Any]], final_rules: pd.DataFrame) -> List[str]:
        insights: List[str] = []
        if not phase_metrics:
            return insights

        start = phase_metrics[0]
        end = phase_metrics[-1]
        score_delta = round(end.get("score", 0) - start.get("score", 0), 4)
        insights.append(
            f"Model quality score moved from {start.get('score', 0):.4f} to {end.get('score', 0):.4f} across learning phases (delta {score_delta:+.4f})."
        )

        if not final_rules.empty:
            top = final_rules.iloc[0]
            insights.append(
                "Top affinity rule: "
                f"{', '.join(top['antecedent_items'])} -> {', '.join(top['consequent_items'])} "
                f"(confidence {top['confidence']:.2%}, lift {top['lift']:.2f})."
            )

        algorithms = [m.get("algorithm") for m in phase_metrics if m.get("algorithm")]
        if algorithms:
            insights.append(f"Hybrid selector favored: {', '.join(algorithms)} across phases based on basket density shifts.")

        return insights

    def _rule_quality_metrics(self, rules: pd.DataFrame, basket: pd.DataFrame) -> Dict[str, float]:
        if rules.empty:
            return {
                "avg_confidence": 0.0,
                "avg_lift": 0.0,
                "item_coverage": 0.0,
            }
        items = set()
        for row in rules.itertuples(index=False):
            items.update(row.antecedent_items)
            items.update(row.consequent_items)
        coverage = len(items) / max(1, basket.shape[1])
        return {
            "avg_confidence": round(float(rules["confidence"].mean()), 6),
            "avg_lift": round(float(rules["lift"].mean()), 6),
            "item_coverage": round(float(coverage), 6),
        }

    def _latest_historical_rules(self) -> List[Dict[str, Any]]:
        try:
            rows = select_table(self.output_table)
        except Exception:
            return []
        if not rows:
            return []

        # Keep historical comparison scoped to the same source dataset table.
        filtered = [r for r in rows if str(r.get("source_table", "")) == self.source_table]
        if not filtered:
            return []

        rows = sorted(filtered, key=lambda r: str(r.get("generated_at", "")), reverse=True)
        payload = rows[0].get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                return []
        if not isinstance(payload, dict):
            return []
        rules = payload.get("rules", [])
        return rules if isinstance(rules, list) else []

    def _ensure_output_table(self) -> None:
        db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL")
        if not db_url or psycopg is None:
            return
        create_sql = f'''
        CREATE TABLE IF NOT EXISTS public."{self.output_table}" (
            id BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            phase INTEGER NOT NULL,
            source_table TEXT NOT NULL,
            generated_at TIMESTAMPTZ NOT NULL,
            algorithm_used TEXT NOT NULL,
            payload JSONB NOT NULL,
            metrics JSONB NOT NULL,
            governance JSONB NOT NULL
        );
        '''
        with psycopg.connect(db_url, autocommit=True, prepare_threshold=None) as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)

    def _store_with_postgres(self, record: Dict[str, Any]) -> bool:
        db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL")
        if not db_url or psycopg is None:
            return False

        sql = f'''
        INSERT INTO public."{self.output_table}"
        (run_id, phase, source_table, generated_at, algorithm_used, payload, metrics, governance)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb);
        '''
        with psycopg.connect(db_url, autocommit=True, prepare_threshold=None) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        record["run_id"],
                        record["phase"],
                        record["source_table"],
                        record["generated_at"],
                        record["algorithm_used"],
                        json.dumps(record["payload"], ensure_ascii=False),
                        json.dumps(record["metrics"], ensure_ascii=False),
                        json.dumps(record["governance"], ensure_ascii=False),
                    ),
                )
        return True

    def _store_with_rest(self, record: Dict[str, Any]) -> bool:
        base_url, key = get_supabase_rest_config()
        url = f"{base_url}/rest/v1/{self.output_table}"
        payload = json.dumps([json_ready(record)], ensure_ascii=False).encode("utf-8")
        headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }
        req = Request(url, headers=headers, data=payload, method="POST")
        try:
            with urlopen(req, timeout=30):
                return True
        except HTTPError:
            return False
        except URLError:
            return False

    def _store_record(self, record: Dict[str, Any], local_fallback_path: Path) -> None:
        self._ensure_output_table()
        if self._store_with_postgres(record):
            return
        if self._store_with_rest(record):
            return

        local_fallback_path.parent.mkdir(parents=True, exist_ok=True)
        local_fallback_path.write_text(json.dumps(json_ready(record), indent=2, ensure_ascii=False), encoding="utf-8")

    def run(self, limit: int | None = None, persist: bool = True) -> Dict[str, Any]:
        raw = self.extract_transactions(limit=limit)
        historical_rules = self._latest_historical_rules()

        phase_records = []
        current_history = historical_rules

        for phase in (1, 2, 3):
            phase_df = self._sample_phase_dataframe(raw, phase)
            basket = self.transform_to_basket_matrix(phase_df)
            optimization = self._optimization_loop(basket, current_history)

            algorithm = optimization["algorithm"]
            rules = optimization["rules"]
            itemsets = optimization["itemsets"]
            thresholds: Thresholds = optimization["thresholds"]
            governance = optimization["governance"]

            payload = {
                "top_bundles": self._itemsets_to_bundles(itemsets, limit=12),
                "frequently_bought_together": self._fbt_widget(rules, limit=16),
                "cross_sell_suggestions": self._cross_sell_map(rules, max_per_item=6),
                "promo_recommendations": self._promo_recommendations(rules, limit=10),
                "rules": self._rules_to_records(rules, limit=250),
            }

            metrics = {
                "phase": phase,
                "score": optimization["score"],
                "algorithm": algorithm,
                "thresholds": thresholds.__dict__,
                "basket_stats": self.basket_stats(basket),
                "rule_count": int(len(rules)),
                "itemset_count": int(len(itemsets)),
                "quality": self._rule_quality_metrics(rules, basket),
                "target_rule_count": int(self._target_rule_count(basket)),
                "loop_trace": optimization["loop_trace"],
            }

            record = {
                "run_id": self.run_id,
                "phase": phase,
                "source_table": self.source_table,
                "generated_at": self.generated_at,
                "algorithm_used": algorithm,
                "payload": payload,
                "metrics": metrics,
                "governance": governance,
            }
            phase_records.append(record)

            if persist:
                local_path = ML_DIR / "outputs" / f"{self.run_id}_phase_{phase}.json"
                self._store_record(record, local_fallback_path=local_path)

            current_history = payload["rules"]

        phase_metrics = [r["metrics"] for r in phase_records]
        final_rules_df = pd.DataFrame(phase_records[-1]["payload"]["rules"]) if phase_records else pd.DataFrame()
        insights = self._business_insights(phase_metrics, final_rules_df)
        phase_records[-1]["payload"]["business_insights"] = insights

        return {
            "run_id": self.run_id,
            "source_table": self.source_table,
            "generated_at": self.generated_at,
            "phases": phase_records,
            "final": phase_records[-1] if phase_records else {},
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TeraDrip Hybrid Market Basket Analysis Engine")
    parser.add_argument("--source-table", default=DEFAULT_SOURCE_TABLE, help="Supabase source transaction table")
    parser.add_argument("--output-table", default=DEFAULT_OUTPUT_TABLE, help="Supabase output table for ML payloads")
    parser.add_argument("--max-loops", type=int, default=5, help="Max optimization loops before stopping")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit for source rows")
    parser.add_argument("--no-persist", action="store_true", help="Run pipeline without writing output to storage")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    engine = TeraDripMBAEngine(
        source_table=args.source_table,
        output_table=args.output_table,
        max_loops=max(1, int(args.max_loops)),
    )
    result = engine.run(limit=args.limit, persist=not args.no_persist)
    final = result.get("final", {})
    final_metrics = final.get("metrics", {})
    print("[OK] TeraDrip MBA completed")
    print(f"run_id={result.get('run_id')}")
    print(f"algorithm={final_metrics.get('algorithm')}")
    print(f"rule_count={final_metrics.get('rule_count')}")
    print(f"output_table={engine.output_table}")
