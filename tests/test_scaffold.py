"""Guard the shared handoff against accidental configuration drift."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_contract_configuration_has_all_planned_hourly_groups() -> None:
    settings = yaml.safe_load((ROOT / "config/settings.yaml").read_text())
    assert settings["project"]["timezone"] == "America/Chicago"
    assert settings["project"]["history_start_year"] == 1990
    assert settings["epa"]["hourly_groups"] == {
        "ozone": "44201",
        "sulfur_dioxide": "42401",
        "carbon_monoxide": "42101",
        "nitrogen_dioxide": "42602",
        "pm25_frm_fem": "88101",
        "pm25_non_frm_fem": "88502",
        "pm10": "81102",
        "lead": "LEAD",
    }
    assert "sensor_index" not in settings["purpleair"]["fields"]
    assert "location_type" in settings["purpleair"]["fields"]


def test_example_environment_contains_no_credentials() -> None:
    content = (ROOT / ".env.example").read_text()
    for line in content.splitlines():
        if line.startswith("AIR_QUALITY_"):
            assert line.endswith("="), f"Example variable has a value: {line}"
