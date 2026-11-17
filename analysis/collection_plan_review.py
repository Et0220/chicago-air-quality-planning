"""Reproduce coverage gaps, close pairs, and explicit fixed-cohort backup rules."""

import json
from pathlib import Path

import yaml

from analysis.collection_review import distance


def build(root):
    def read(name):
        return json.loads((root / (name + ".json")).read_text())

    sensors, selected, backups, refs = (
        read(name)
        for name in (
            "sensor_quality",
            "selected_sensors",
            "backup_sensors",
            "airnow_reference_sites",
        )
    )
    active = {s["sensor_id"] for s in selected}
    covered = {s["nearest_airnow_site"] for s in selected}
    gaps = []
    for ref in refs:
        if not ref["in_study_counties"] or ref["site_id"] in covered:
            continue
        ranked = sorted(
            [
                {
                    "sensor_id": s["sensor_id"],
                    "distance_km": round(
                        distance(
                            (s["latitude"], s["longitude"]),
                            (ref["latitude"], ref["longitude"]),
                        ),
                        3,
                    ),
                    "historical_quality_eligible": s["eligible"],
                    "trial_qc_pass_fraction": s["qc_pass_fraction"],
                    "already_selected": s["sensor_id"] in active,
                    "primary_reference": s["nearest_airnow_name"],
                    "health_check_pass_at_review": s["current_health_pass"],
                }
                for s in sensors
            ],
            key=lambda s: s["distance_km"],
        )
        candidates = [
            s
            for s in ranked
            if s["historical_quality_eligible"]
            and s["distance_km"] <= 10
            and s["health_check_pass_at_review"] is not False
        ]
        gaps.append(
            {
                "reference_site_id": ref["site_id"],
                "reference_name": ref["site_name"],
                "valid_pm25_hours": ref["valid_pm25_hours"],
                "reference_eligible": ref["valid_pm25_hours"] >= 65,
                "nearest_sensors": ranked[:3],
                "nearest_historically_qualified_sensor": next(
                    (s for s in ranked if s["historical_quality_eligible"]), None
                ),
                "qualified_candidates_within_10km": candidates,
                "unselected_candidates_within_10km": [
                    s for s in candidates if not s["already_selected"]
                ],
            }
        )
    plans = []
    for sensor in selected:
        matching = [
            b
            for b in backups
            if b["nearest_airnow_site"] == sensor["nearest_airnow_site"]
            and b["current_health_pass"]
        ]
        primary = sensor["reference_distance_km"] <= 5
        permitted = [
            b["sensor_id"]
            for b in matching
            if not primary or b["reference_distance_km"] <= 5
        ]
        widening = [
            b["sensor_id"]
            for b in matching
            if primary and b["reference_distance_km"] > 5
        ]
        plans.append(
            {
                "active_sensor_id": sensor["sensor_id"],
                "reference_site_id": sensor["nearest_airnow_site"],
                "reference_name": sensor["nearest_airnow_name"],
                "analysis_stratum": "primary_le5km"
                if primary
                else "exploratory_5to10km",
                "backup_order_same_stratum": permitted,
                "exploratory_only_candidates_require_reclassification": widening,
                "requires_current_health_recheck": True,
                "on_failure": (
                    "Use first healthy, unassigned same-stratum backup; otherwise "
                    "hold this slot and document reduced coverage."
                ),
            }
        )
    pairs = []
    for i, sensor in enumerate(selected):
        for other in selected[i + 1 :]:
            if sensor["nearest_airnow_site"] == other["nearest_airnow_site"]:
                pairs.append(
                    {
                        "reference_name": sensor["nearest_airnow_name"],
                        "sensor_ids": [sensor["sensor_id"], other["sensor_id"]],
                        "sensor_separation_km": round(
                            distance(
                                (sensor["latitude"], sensor["longitude"]),
                                (other["latitude"], other["longitude"]),
                            ),
                            3,
                        ),
                        "interpretation": (
                            "Within-reference redundancy; not independent "
                            "geographic replicates."
                        ),
                    }
                )
    report = {
        "input_scope": (
            "Saved three-day trial and November 16 Chicago-time health checks; "
            "not a 23-day reliability forecast."
        ),
        "eligible_references": sum(
            r["in_study_counties"] and r["valid_pm25_hours"] >= 65 for r in refs
        ),
        "covered_primary_references": len(covered),
        "uncovered_eligible_references": [g for g in gaps if g["reference_eligible"]],
        "other_ineligible_references": [g for g in gaps if not g["reference_eligible"]],
        "same_reference_pairs": pairs,
        "decision": (
            "Retain the 20-ID nearest-reference cohort; report gaps and distance "
            "strata. A secondary comparison does not create an independent "
            "primary location."
        ),
    }
    for name, value in [("coverage_gaps", report), ("replacement_plan", plans)]:
        (root / (name + ".json")).write_text(json.dumps(value, indent=2) + "\n")
    strategy = yaml.safe_load(Path("config/collection_strategy.yaml").read_text())
    pa = strategy["purpleair"]
    if pa["sensor_ids"] != [s["sensor_id"] for s in selected]:
        raise ValueError("Strategy roster differs from reviewed selected sensors")
    if set(strategy["study_scope"]["exploratory_sensor_ids"]) != {
        s["sensor_id"] for s in selected if s["reference_distance_km"] > 5
    }:
        raise ValueError("Strategy distance strata differ from reviewed locations")
    overlay = {
        "purpleair_poll_enabled": False,
        "purpleair_poll_schedule": pa["cron"],
        "purpleair_poll_sensor_ids": pa["sensor_ids"],
        "purpleair_poll_max_age_seconds": pa["max_age_seconds"],
        "purpleair_poll_stale_after_seconds": pa["client_max_response_age_seconds"],
        "purpleair_poll_stop_after_utc": pa["stop_after_utc"],
        "purpleair_poll_not_before_utc": None,
    }
    (root / "campaign.tfvars.json").write_text(json.dumps(overlay, indent=2) + "\n")
    print(
        json.dumps(
            {
                "eligible_references": report["eligible_references"],
                "covered_primary_references": len(covered),
                "replacement_rules": len(plans),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    build(Path("output/collection_review"))
