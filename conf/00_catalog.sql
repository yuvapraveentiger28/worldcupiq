-- Phase 0 — Foundation
-- Schemas and the raw-landing volume for WorldCupIQ, inside the project's own
-- dedicated `worldcupiq_catalog` catalog. Unlike the earlier `mrlc-catalog`
-- setup (shared with other projects, so schemas were namespaced
-- fifaworldcup_<...>), this catalog belongs solely to WorldCupIQ, so schema
-- names below drop that prefix — the catalog itself already scopes
-- everything to this project. See databricks.yml's `schema_prefix` bundle
-- variable, which every pipeline reads via spark.conf.get("schema_prefix", ...)
-- to construct these same schema names at runtime.
--
-- The catalog's external location / storage credential are assumed to already
-- be configured — this script only adds schemas and a volume, it does not
-- touch catalog-level storage config.

-- Landing zone: raw source files land here via Auto Loader, one folder per
-- bronze table name (see ingestion/lakeflow/bronze.py). Kept separate from the
-- bronze_* schemas below, which hold the resulting Delta tables, not files.
CREATE SCHEMA IF NOT EXISTS worldcupiq_catalog.landing
  COMMENT 'WorldCupIQ — raw file landing zone — Volumes only, no tables';

CREATE VOLUME IF NOT EXISTS worldcupiq_catalog.landing.raw_files
  COMMENT 'Auto Loader source: /Volumes/worldcupiq_catalog/landing/raw_files/<table_name>/...';

-- Raw landing for the GitHub Connect ingestion pipeline (github_ingestion_pipeline
-- in databricks.yml) — holds the repo_contents streaming table (one row per file,
-- unparsed) for the three sources that come from git repos (FIFA Training
-- Centre-derived, relational, historical). A downstream parsing step turns this
-- into the real bronze_fifa_training_centre / bronze_relational / bronze_historical
-- tables below.
CREATE SCHEMA IF NOT EXISTS worldcupiq_catalog.github_raw
  COMMENT 'WorldCupIQ — raw repo_contents from Lakeflow Connect GitHub ingestion, unparsed';

-- Bronze — one schema per datasource (bronze_<datasource_name>), matching the
-- BRONZE_SOURCES registry in ingestion/lakeflow/bronze.py. Raw, 1:1 with
-- source. These must exist before the bronze pipeline's first run/update —
-- UC pipelines require the target schema to exist.
--
-- No bronze_fifa_official schema: FIFA official (fifa.com pages) was dropped
-- as a source — no clean CSV/JSON feed exists behind those pages.
-- matches/venues are covered by bronze_relational instead.
CREATE SCHEMA IF NOT EXISTS worldcupiq_catalog.bronze_fifa_training_centre
  COMMENT 'WorldCupIQ bronze — FIFA Training Centre-derived match/event/tracking export (21 tables)';

CREATE SCHEMA IF NOT EXISTS worldcupiq_catalog.bronze_relational
  COMMENT 'WorldCupIQ bronze — relational dataset, complementary xG & VAR + squads';

CREATE SCHEMA IF NOT EXISTS worldcupiq_catalog.bronze_historical
  COMMENT 'WorldCupIQ bronze — historical World Cups (2014-2026), context';

CREATE SCHEMA IF NOT EXISTS worldcupiq_catalog.bronze_predictions
  COMMENT 'WorldCupIQ bronze — pre-match predictions, context';

-- Silver, gold, and the governed layers — normalized/business tables, not raw landing.
CREATE SCHEMA IF NOT EXISTS worldcupiq_catalog.silver
  COMMENT 'WorldCupIQ silver — normalized, entity-resolved, UC PK/FK constraints';

CREATE SCHEMA IF NOT EXISTS worldcupiq_catalog.gold
  COMMENT 'WorldCupIQ gold — star schema: facts, dimensions, fact_team_tournament';

CREATE SCHEMA IF NOT EXISTS worldcupiq_catalog.semantic
  COMMENT 'WorldCupIQ semantic layer — Unity Catalog Metric Views';

CREATE SCHEMA IF NOT EXISTS worldcupiq_catalog.ontology
  COMMENT 'WorldCupIQ ontology — entity/relationship tables, GraphFrames source, UC functions';

CREATE SCHEMA IF NOT EXISTS worldcupiq_catalog.context
  COMMENT 'WorldCupIQ context layer — concept store, policies, Vector Search index';
