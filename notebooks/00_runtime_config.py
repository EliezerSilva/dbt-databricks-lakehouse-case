# Databricks notebook source
# Read-only runtime configuration for the DEV target.
# Values must come from the calling Job's notebook parameters.

import re as _runtime_re

EXPECTED_CONFIG = {
    "catalog": "workspace",
    "source_root": "/Volumes/workspace/lakehouse_case/sources",
    "bronze_schema": "bronze_dev",
    "silver_schema": "silver_dev",
    "gold_schema": "gold_dev",
    "control_schema": "control_dev",
}

RUNTIME_CONFIG = {}

for key, expected in EXPECTED_CONFIG.items():
    try:
        value = dbutils.widgets.get(key)
    except Exception as exc:
        raise ValueError(
            f"Missing required DAB parameter: {key}"
        ) from exc

    if value != expected:
        raise ValueError(
            f"DEV isolation violation: {key}={value!r}"
        )

    RUNTIME_CONFIG[key] = value

CATALOG = RUNTIME_CONFIG["catalog"]
SOURCE_ROOT = RUNTIME_CONFIG["source_root"]

SCHEMAS = {
    layer: RUNTIME_CONFIG[f"{layer}_schema"]
    for layer in ("bronze", "silver", "gold", "control")
}

BRONZE_SCHEMA = SCHEMAS["bronze"]
SILVER_SCHEMA = SCHEMAS["silver"]
GOLD_SCHEMA = SCHEMAS["gold"]
CONTROL_SCHEMA = SCHEMAS["control"]

for name in (CATALOG, *SCHEMAS.values()):
    if not _runtime_re.fullmatch(r"[a-z][a-z0-9_]*", name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")

print("DAB_RUNTIME_CONFIG_VALIDATED")
print(f"Catalog: {CATALOG}")
print(f"Schemas: {SCHEMAS}")
