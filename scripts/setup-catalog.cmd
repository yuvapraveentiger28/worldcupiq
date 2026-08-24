@echo off
REM Creates the schemas + volume from conf/00_catalog.sql directly via the
REM Databricks CLI's Unity Catalog commands (schemas/volumes REST API) —
REM no SQL warehouse or cluster needed, unlike running the .sql file itself.
REM
REM Dedicated worldcupiq_catalog catalog — no fifaworldcup_ prefix on schema
REM names (that was only needed on the earlier shared mrlc-catalog setup).
REM
REM Usage: scripts\load-env.cmd && scripts\setup-catalog.cmd

set "CATALOG=worldcupiq_catalog"

echo Creating schemas in %CATALOG%...

databricks schemas create landing %CATALOG% --comment "WorldCupIQ - raw file landing zone - Volumes only, no tables"
databricks schemas create github_raw %CATALOG% --comment "WorldCupIQ - raw repo_contents from Lakeflow Connect GitHub ingestion, unparsed"
databricks schemas create bronze_fifa_training_centre %CATALOG% --comment "WorldCupIQ bronze - FIFA Training Centre-derived match/event/tracking export (21 tables)"
databricks schemas create bronze_relational %CATALOG% --comment "WorldCupIQ bronze - relational dataset, complementary xG and VAR plus squads"
databricks schemas create bronze_historical %CATALOG% --comment "WorldCupIQ bronze - historical World Cups (2014-2026), context"
databricks schemas create bronze_predictions %CATALOG% --comment "WorldCupIQ bronze - pre-match predictions, context"
databricks schemas create silver %CATALOG% --comment "WorldCupIQ silver - normalized, entity-resolved, UC PK/FK constraints"
databricks schemas create gold %CATALOG% --comment "WorldCupIQ gold - star schema: facts, dimensions, fact_team_tournament"
databricks schemas create semantic %CATALOG% --comment "WorldCupIQ semantic layer - Unity Catalog Metric Views"
databricks schemas create ontology %CATALOG% --comment "WorldCupIQ ontology - entity/relationship tables, GraphFrames source, UC functions"
databricks schemas create context %CATALOG% --comment "WorldCupIQ context layer - concept store, policies, Vector Search index"

echo Creating volume landing.raw_files...
databricks volumes create %CATALOG% landing raw_files MANAGED --comment "Auto Loader source: /Volumes/worldcupiq_catalog/landing/raw_files/<table_name>/..."

echo Done.
