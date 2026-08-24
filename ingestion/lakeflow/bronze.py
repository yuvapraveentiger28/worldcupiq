# Lakeflow Declarative Pipeline source file
#
# Bronze layer via Auto Loader (cloudFiles) — covers ONLY the one remaining
# source that isn't a git repo: Onside Arena predictions.
#
# FIFA official (fifa.com pages) was dropped entirely — there's no clean
# CSV/JSON feed behind those pages, just tournament article pages that would
# need manual scraping/transcription before landing. `fifa_matches` and
# `venues` are covered by the relational dataset's own `matches`/`venues`
# tables instead (see bronze_github_parse.py), so no scraper was built.
#
# The other three bronze sources (FIFA Training Centre-derived, relational,
# historical) are git repos, forked into yuvapraveentiger's GitHub account, and
# now ingested via Lakeflow Connect's GitHub connector instead (see
# github_ingestion_pipeline in databricks.yml) — that lands raw file content,
# which a separate downstream parsing step turns into the real bronze tables.
# They were previously built here too via Auto Loader; removed when the
# GitHub Connect path replaced them, to avoid two competing ingestion paths
# for the same tables.
#
# Schema convention: `mrlc-catalog` is a shared catalog (other projects live in it
# too), so every schema this project owns is namespaced fifaworldcup_<...>.
#
# IMPORTANT: a single Lakeflow pipeline publishes to ONE catalog.schema, set at
# the pipeline level — there is no supported per-table schema override (an
# earlier version of this file tried @dlt.table(schema=...), which actually
# means "this table's column structure", not a UC namespace, and produced a
# malformed CREATE TABLE). So this one file is deployed as separate pipeline
# resources in databricks.yml — one per datasource — each pointing its own
# catalog/schema pipeline setting at the matching fifaworldcup_bronze_* schema
# and passing `datasource_filter` via pipeline `configuration` so each instance
# only builds the tables for its own datasource.
#
# The landing volume itself stays flat and source-agnostic (one folder per table
# name, no source subfolder), mirroring silver's flat table naming — the schema
# already carries the source distinction, so the physical path doesn't need to
# repeat it.
#
# Land files at:
#   /Volumes/mrlc-catalog/fifaworldcup_landing/raw_files/<table_name>/...
#
# Assumes conf/00_catalog.sql has already created the fifaworldcup_landing schema
# + raw_files volume and the fifaworldcup_bronze_predictions schema in the
# mrlc-catalog catalog (Phase 0) — the catalog itself is shared/pre-existing.
#
# File formats below are placeholders — confirm the real extension per source
# when the actual dataset files are wired in during Phase 1.

import dlt
from pyspark.sql.functions import col, current_timestamp

# catalog/schema_prefix are passed via pipeline `configuration` in
# databricks.yml (${var.catalog} / ${var.schema_prefix}) — defaults here
# match the original mrlc-catalog (dev, historically) convention so this
# file still works if run without that configuration set.
CATALOG = spark.conf.get("catalog", "mrlc-catalog")
SCHEMA_PREFIX = spark.conf.get("schema_prefix", "fifaworldcup_")
LANDING_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA_PREFIX}landing/raw_files"

# table_name -> (datasource name, file format)
# datasource name selects which pipeline instance builds this table (see
# datasource_filter below) — the destination schema comes from that pipeline's
# own catalog/schema setting in databricks.yml, not from anything in this file.
BRONZE_SOURCES = {
    # Pre-match predictions (Onside Arena, onsidearena.com/data/predictions.csv)
    "pre_match_prediction": ("predictions", "csv"),
}


def _make_bronze_table(table_name: str, datasource: str, fmt: str):
    # Factory so each loop iteration closes over its own table_name/datasource/fmt
    # instead of all dlt.table functions sharing the loop variable's final value.

    @dlt.table(
        name=table_name,
        comment=f"Raw 1:1 landing of {table_name} from {datasource} via Auto Loader.",
        table_properties={"quality": "bronze"},
    )
    def _bronze():
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", fmt)
            .option("cloudFiles.schemaLocation", f"{LANDING_VOLUME}/_schemas/{table_name}")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
        )
        if fmt == "csv":
            reader = reader.option("header", "true")

        return (
            reader.load(f"{LANDING_VOLUME}/{table_name}")
            .withColumn("_ingested_at", current_timestamp())
            # input_file_name() is blocked under Unity Catalog governed compute
            # (UC_COMMAND_NOT_SUPPORTED) — _metadata.file_path is the supported
            # equivalent, available on every cloudFiles-sourced DataFrame.
            .withColumn("_source_file", col("_metadata.file_path"))
        )

    return _bronze


# Set per-pipeline via `configuration: {datasource_filter: <datasource>}` in
# databricks.yml. Falls back to building every table (useful for a single
# combined pipeline / local testing) when unset.
_datasource_filter = spark.conf.get("datasource_filter", None)

for _table_name, (_datasource, _fmt) in BRONZE_SOURCES.items():
    if _datasource_filter and _datasource != _datasource_filter:
        continue
    _make_bronze_table(_table_name, _datasource, _fmt)
