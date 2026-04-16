"""
scripts/datavault_generator.py
AI-driven Data Vault 2.0 object generator for ProcureSpendIQ (req 5, 7).

For each table detected in INFORMATION_MART that is not yet represented in
schema_metadata.yaml, this script:

  1. Reads the column metadata from the Fabric Lakehouse.
  2. Calls Azure OpenAI to classify each column as a business key,
     descriptive attribute, or foreign key.
  3. Generates T-SQL DDL for Hub, Satellite, and Link tables.
  4. Updates schema_metadata.yaml with Data Vault metadata and sample questions.

Run as a one-off migration or wire it into a Fabric pipeline / CI job.

Usage:
    python scripts/datavault_generator.py --schema INFORMATION_MART --dry-run
    python scripts/datavault_generator.py --schema INFORMATION_MART
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DDL templates
# ---------------------------------------------------------------------------

_HUB_DDL = """
-- Hub: {hub_name}
-- Source table: {source_table}
IF OBJECT_ID('{raw_vault_schema}.{hub_name}', 'U') IS NULL
BEGIN
    CREATE TABLE {raw_vault_schema}.{hub_name} (
        {hub_name}_HK          CHAR(32)        NOT NULL,   -- Hash key (MD5/SHA1)
        LOAD_DATE              DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        RECORD_SOURCE          VARCHAR(100)    NOT NULL,
{bk_columns}
        CONSTRAINT PK_{hub_name} PRIMARY KEY ({hub_name}_HK)
    );
END;
"""

_SAT_DDL = """
-- Satellite: {sat_name}
IF OBJECT_ID('{raw_vault_schema}.{sat_name}', 'U') IS NULL
BEGIN
    CREATE TABLE {raw_vault_schema}.{sat_name} (
        {hub_name}_HK          CHAR(32)        NOT NULL,
        LOAD_DATE              DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        LOAD_END_DATE          DATETIME2       NULL,
        HASH_DIFF              CHAR(32)        NOT NULL,
        RECORD_SOURCE          VARCHAR(100)    NOT NULL,
{attr_columns}
        CONSTRAINT PK_{sat_name} PRIMARY KEY ({hub_name}_HK, LOAD_DATE)
    );
END;
"""

_LINK_DDL = """
-- Link: {link_name}
IF OBJECT_ID('{raw_vault_schema}.{link_name}', 'U') IS NULL
BEGIN
    CREATE TABLE {raw_vault_schema}.{link_name} (
        {link_name}_HK         CHAR(32)        NOT NULL,
        LOAD_DATE              DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        RECORD_SOURCE          VARCHAR(100)    NOT NULL,
{fk_columns}
        CONSTRAINT PK_{link_name} PRIMARY KEY ({link_name}_HK)
    );
END;
"""


def _sql_type_for(fabric_type: str) -> str:
    """Map Fabric/SQL Server data types to DDL-friendly equivalents."""
    t = fabric_type.lower()
    if t in ("int", "bigint", "smallint", "tinyint"):
        return t.upper()
    if t in ("float", "real"):
        return "FLOAT"
    if t in ("decimal", "numeric", "money", "smallmoney"):
        return "DECIMAL(18, 4)"
    if t == "date":
        return "DATE"
    if t in ("datetime", "datetime2", "smalldatetime"):
        return "DATETIME2"
    if t == "bit":
        return "BIT"
    return "NVARCHAR(500)"


def generate_hub_ddl(
    hub_name: str,
    source_table: str,
    business_keys: List[str],
    raw_vault_schema: str,
    column_type_map: dict,
) -> str:
    bk_cols = "\n".join(
        f"        {bk:<35} {_sql_type_for(column_type_map.get(bk, 'varchar'))},"
        for bk in business_keys
    )
    return _HUB_DDL.format(
        hub_name=hub_name,
        source_table=source_table,
        raw_vault_schema=raw_vault_schema,
        bk_columns=bk_cols,
    )


def generate_sat_ddl(
    sat_name: str,
    hub_name: str,
    attributes: List[str],
    raw_vault_schema: str,
    column_type_map: dict,
) -> str:
    attr_cols = "\n".join(
        f"        {a:<35} {_sql_type_for(column_type_map.get(a, 'varchar'))},"
        for a in attributes
    )
    return _SAT_DDL.format(
        sat_name=sat_name,
        hub_name=hub_name,
        raw_vault_schema=raw_vault_schema,
        attr_columns=attr_cols,
    )


def generate_link_ddl(
    link_name: str,
    foreign_keys: List[str],
    raw_vault_schema: str,
) -> str:
    fk_cols = "\n".join(
        f"        {fk}_HK{'':20} CHAR(32)   NOT NULL,"
        for fk in foreign_keys
    )
    return _LINK_DDL.format(
        link_name=link_name,
        raw_vault_schema=raw_vault_schema,
        fk_columns=fk_cols,
    )


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run(schema: str, dry_run: bool, yaml_path: str | None = None) -> None:
    from config import Config
    from db_service import get_table_columns, get_primary_keys, list_tables_in_schema
    from llm_service_full import _infer_data_vault_objects, enrich_yaml_for_table

    raw_vault_schema = Config._settings.get("data_vault", {}).get(
        "raw_vault_schema", "RAW_VAULT"
    ) if hasattr(Config, "_settings") else "RAW_VAULT"

    tables_df = list_tables_in_schema(schema)
    if tables_df.empty:
        logger.warning("No tables found in schema: %s", schema)
        return

    for _, row in tables_df.iterrows():
        table_name = str(row.get("TABLE_NAME", "")).strip()
        if not table_name:
            continue

        logger.info("Processing table: %s.%s", schema, table_name)

        cols_df = get_table_columns(table_name, schema)
        if cols_df.empty:
            continue

        pks             = get_primary_keys(table_name, schema)
        columns         = cols_df.to_dict("records")
        col_type_map    = {c["COLUMN_NAME"]: c["DATA_TYPE"] for c in columns}

        # AI inference
        enrichment = _infer_data_vault_objects(table_name, columns, pks)
        dv         = enrichment.get("data_vault", {})

        hub_def  = dv.get("hub",       {})
        sat_def  = dv.get("satellite", {})
        link_def = dv.get("links",     [])

        hub_name = hub_def.get("name", f"{Config.DATA_VAULT_HUB_PREFIX}{table_name}")
        sat_name = sat_def.get("name", f"{Config.DATA_VAULT_SAT_PREFIX}{table_name}")
        bk_cols  = hub_def.get("business_keys",         pks or [columns[0]["COLUMN_NAME"]])
        attr_cols = sat_def.get("descriptive_attributes",
                                [c["COLUMN_NAME"] for c in columns if c["COLUMN_NAME"] not in bk_cols])

        hub_ddl = generate_hub_ddl(hub_name, f"{schema}.{table_name}", bk_cols,
                                   raw_vault_schema, col_type_map)
        sat_ddl = generate_sat_ddl(sat_name, hub_name, attr_cols,
                                   raw_vault_schema, col_type_map)

        link_ddls = []
        for lnk in link_def:
            lnk_name = lnk.get("name", f"{Config.DATA_VAULT_LINK_PREFIX}{table_name}")
            lnk_hubs = lnk.get("hubs", [hub_name])
            link_ddls.append(generate_link_ddl(lnk_name, lnk_hubs, raw_vault_schema))

        all_ddl = "\n".join([hub_ddl, sat_ddl] + link_ddls)

        if dry_run:
            print(f"\n-- ===== DDL for {table_name} =====")
            print(all_ddl)
        else:
            # Write DDL file
            ddl_dir = Path("scripts/ddl")
            ddl_dir.mkdir(parents=True, exist_ok=True)
            ddl_file = ddl_dir / f"{table_name.lower()}_data_vault.sql"
            ddl_file.write_text(all_ddl, encoding="utf-8")
            logger.info("DDL written to: %s", ddl_file)

            # Enrich YAML
            enrich_yaml_for_table(table_name, schema, yaml_path)

    logger.info("Data Vault generation complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="Generate Data Vault 2.0 DDL from Fabric schema")
    parser.add_argument("--schema",   default="INFORMATION_MART", help="Source schema name")
    parser.add_argument("--yaml",     default=None,               help="Path to schema_metadata.yaml")
    parser.add_argument("--dry-run",  action="store_true",        help="Print DDL instead of writing files")
    args = parser.parse_args()

    run(schema=args.schema, dry_run=args.dry_run, yaml_path=args.yaml)
