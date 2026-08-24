# Lakeflow Declarative Pipeline source file
#
# Parses raw GitHub file content (mrlc-catalog.fifaworldcup_github_raw.repo_contents,
# landed by the github_ingestion_pipeline / Lakeflow Connect) into real structured
# bronze tables. repo_contents holds one row per file with the WHOLE FILE as a
# single text blob in `content` — this file turns each target (repository_id, path)
# pair into a proper table with real columns.
#
# Method: read the specific file's `content`, split into lines, infer a CSV schema
# from the header line via Spark's schema_of_csv() (passed as a parameterized
# DataFrame value, not string-interpolated — header text is untrusted external
# data), then parse the remaining lines with from_csv() against that schema.
#
# KNOWN LIMITATION: this is a per-line split, so a CSV field containing an
# embedded newline inside quotes would break. None of the sources here are known
# to have such fields, but this isn't a fully RFC 4180-compliant multi-line parser.
#
# Same one-pipeline-one-schema constraint as bronze.py applies here too — each
# repo's tables land in a different fifaworldcup_bronze_* schema, so this file is
# deployed as three pipeline resources (one per repo/datasource) in databricks.yml,
# filtered via `datasource_filter` in pipeline `configuration`, same pattern as
# bronze.py's BRONZE_SOURCES.
#
# Table -> (repository_id, path in repo, datasource) registry below. repository_id
# values and exact paths were read directly off the landed repo_contents table
# (2026-08-22) — see docs/demo-design-notes.md §2 for repo provenance.

import dlt
from pyspark.sql import functions as F

# catalog/schema_prefix passed via pipeline `configuration` in databricks.yml
# (${var.catalog} / ${var.schema_prefix}) — defaults match the original
# mrlc-catalog convention so this file still works without that config set.
_CATALOG = spark.conf.get("catalog", "mrlc-catalog")
_SCHEMA_PREFIX = spark.conf.get("schema_prefix", "fifaworldcup_")
RAW_TABLE = f"`{_CATALOG}`.`{_SCHEMA_PREFIX}github_raw`.`repo_contents`"

# datasource -> { output_table_name -> (repository_id, path) }
# Nested per-datasource (not one flat dict) because table names like "matches" and
# "teams" legitimately repeat across datasources — they land in different schemas
# at runtime, but a single flat dict would silently collide on those keys (this
# actually happened on the first pass: fifa_training_centre's "matches"/"teams"
# entries got overwritten by relational's, dropping 2 tables with no error).
GITHUB_SOURCES = {
    # yuvapraveentiger28/Worldcup26 (repository_id 1343166099) — 21 CSVs under
    # data/csv/, the analytical foundation dataset.
    "fifa_training_centre": {
        "attempts_at_goal": (1343166099, "data/csv/attempts_at_goal.csv"),
        "match_appearances": (1343166099, "data/csv/match_appearances.csv"),
        "match_teams": (1343166099, "data/csv/match_teams.csv"),
        "matches": (1343166099, "data/csv/matches.csv"),
        "passing_network_edges": (1343166099, "data/csv/passing_network_edges.csv"),
        "player_crosses_open_play": (1343166099, "data/csv/player_crosses_open_play.csv"),
        "player_events": (1343166099, "data/csv/player_events.csv"),
        "player_in_possession_distributions": (1343166099, "data/csv/player_in_possession_distributions.csv"),
        "player_line_breaks": (1343166099, "data/csv/player_line_breaks.csv"),
        "player_offers_receptions": (1343166099, "data/csv/player_offers_receptions.csv"),
        "player_out_of_possession": (1343166099, "data/csv/player_out_of_possession.csv"),
        "player_physical_data": (1343166099, "data/csv/player_physical_data.csv"),
        "players": (1343166099, "data/csv/players.csv"),
        "team_aerial_control": (1343166099, "data/csv/team_aerial_control.csv"),
        "team_defensive_pressure": (1343166099, "data/csv/team_defensive_pressure.csv"),
        "team_goal_prevention": (1343166099, "data/csv/team_goal_prevention.csv"),
        "team_goalkeeping_distribution": (1343166099, "data/csv/team_goalkeeping_distribution.csv"),
        "team_key_stats": (1343166099, "data/csv/team_key_stats.csv"),
        "team_phases": (1343166099, "data/csv/team_phases.csv"),
        "team_set_plays": (1343166099, "data/csv/team_set_plays.csv"),
        "teams": (1343166099, "data/csv/teams.csv"),
    },

    # yuvapraveentiger28/FIFA-World-Cup-2026-Dataset (repository_id 1343166340) —
    # root-level CSVs only, skipping the docs/ duplicates, binary assets, and
    # generate_*.py/validate_dataset.py/*_X.csv/*_y.csv ML-pipeline scaffolding.
    "relational": {
        "match_events": (1343166340, "match_events.csv"),
        "match_lineups": (1343166340, "match_lineups.csv"),
        "match_prediction_features": (1343166340, "match_prediction_features.csv"),
        "match_team_stats": (1343166340, "match_team_stats.csv"),
        "matches": (1343166340, "matches.csv"),
        "player_stats": (1343166340, "player_stats.csv"),
        "referees": (1343166340, "referees.csv"),
        "squads_and_players": (1343166340, "squads_and_players.csv"),
        "teams": (1343166340, "teams.csv"),
        "tournament_stages": (1343166340, "tournament_stages.csv"),
        "venues": (1343166340, "venues.csv"),
    },

    # yuvapraveentiger28/worldcup (repository_id 1343166547) — 27 CSVs under
    # data-csv/, the R package's clean exported data (not the raw Wikipedia HTML
    # scrape in data-raw/ or the RData binaries in data/). hist_ prefix kept for
    # consistency with README §6's original naming.
    "historical": {
        "hist_award_winners": (1343166547, "data-csv/award_winners.csv"),
        "hist_awards": (1343166547, "data-csv/awards.csv"),
        "hist_bookings": (1343166547, "data-csv/bookings.csv"),
        "hist_confederations": (1343166547, "data-csv/confederations.csv"),
        "hist_goals": (1343166547, "data-csv/goals.csv"),
        "hist_group_standings": (1343166547, "data-csv/group_standings.csv"),
        "hist_groups": (1343166547, "data-csv/groups.csv"),
        "hist_host_countries": (1343166547, "data-csv/host_countries.csv"),
        "hist_manager_appearances": (1343166547, "data-csv/manager_appearances.csv"),
        "hist_manager_appointments": (1343166547, "data-csv/manager_appointments.csv"),
        "hist_managers": (1343166547, "data-csv/managers.csv"),
        "hist_matches": (1343166547, "data-csv/matches.csv"),
        "hist_penalty_kicks": (1343166547, "data-csv/penalty_kicks.csv"),
        "hist_player_appearances": (1343166547, "data-csv/player_appearances.csv"),
        "hist_players": (1343166547, "data-csv/players.csv"),
        "hist_qualified_teams": (1343166547, "data-csv/qualified_teams.csv"),
        "hist_referee_appearances": (1343166547, "data-csv/referee_appearances.csv"),
        "hist_referee_appointments": (1343166547, "data-csv/referee_appointments.csv"),
        "hist_referees": (1343166547, "data-csv/referees.csv"),
        "hist_squads": (1343166547, "data-csv/squads.csv"),
        "hist_stadiums": (1343166547, "data-csv/stadiums.csv"),
        "hist_substitutions": (1343166547, "data-csv/substitutions.csv"),
        "hist_team_appearances": (1343166547, "data-csv/team_appearances.csv"),
        "hist_teams": (1343166547, "data-csv/teams.csv"),
        "hist_tournament_stages": (1343166547, "data-csv/tournament_stages.csv"),
        "hist_tournament_standings": (1343166547, "data-csv/tournament_standings.csv"),
        "hist_tournaments": (1343166547, "data-csv/tournaments.csv"),
    },
}


def _make_parsed_table(output_name: str, repository_id: int, path: str):
    # Factory so each loop iteration closes over its own output_name/repository_id/path.

    @dlt.table(
        name=output_name,
        comment=f"Parsed from repo_contents: repository_id={repository_id}, path={path}",
        table_properties={"quality": "bronze"},
    )
    def _parsed():
        raw = (
            spark.table(RAW_TABLE)
            .filter(
                (F.col("repository_id") == repository_id)
                & (F.col("path") == path)
                & (F.col("type") == "blob")
                # is_deleted is NULL (not false) for ordinary files — "NOT NULL" is
                # NULL in SQL and a WHERE clause silently drops the row, so use
                # IS NOT TRUE rather than negating the column directly.
                & F.expr("is_deleted IS NOT TRUE")
            )
            .select("content")
            .limit(1)
        )
        row = raw.collect()

        if not row or row[0]["content"] is None:
            # File not landed yet (or landed binary/empty) — empty table rather than
            # failing the pipeline. Re-run after github_ingestion_pipeline has run.
            return spark.createDataFrame([], "_pending_source STRING")

        content = row[0]["content"]
        lines = [line for line in content.replace("\r\n", "\n").split("\n") if line != ""]
        if len(lines) < 2:
            return spark.createDataFrame([], "_pending_source STRING")

        header_line, data_lines = lines[0], lines[1:]

        # schema_of_csv() requires a foldable (compile-time-constant) argument —
        # it can't take a per-row DataFrame column, so header_line (already a
        # plain Python string from the .collect() above) goes through F.lit(),
        # a literal baked into the query plan directly via the PySpark API —
        # not string-interpolated into raw SQL text, so no injection risk even
        # though header_line is untrusted external content.
        schema_str = (
            spark.range(1)
            .select(F.schema_of_csv(F.lit(header_line)).alias("s"))
            .collect()[0]["s"]
        )

        # schema_of_csv() infers TYPES from the sample row — it has no concept of
        # a header row, so from_csv() below would otherwise produce generic _c0,
        # _c1, ... names. Rename positionally from the real header instead. Naive
        # comma split (not quote-aware) — fine for these sources' plain headers,
        # but would mis-split a header containing an embedded comma inside quotes.
        header_cols = [c.strip().strip('"') for c in header_line.split(",")]

        parsed = (
            spark.createDataFrame([(line,) for line in data_lines], ["line"])
            .select(F.from_csv(F.col("line"), schema_str).alias("r"))
            .select("r.*")
        )
        if len(header_cols) == len(parsed.columns):
            parsed = parsed.toDF(*header_cols)

        return (
            parsed
            .withColumn("_source_repository_id", F.lit(repository_id))
            .withColumn("_source_path", F.lit(path))
            .withColumn("_ingested_at", F.current_timestamp())
        )

    return _parsed


# Required (unlike bronze.py's optional filter) — this file has no meaningful
# "build everything" mode since table names collide across datasources by design
# (see the comment on GITHUB_SOURCES above).
_datasource_filter = spark.conf.get("datasource_filter")
if not _datasource_filter or _datasource_filter not in GITHUB_SOURCES:
    raise ValueError(
        f"pipeline configuration must set datasource_filter to one of "
        f"{list(GITHUB_SOURCES.keys())}, got {_datasource_filter!r}"
    )

for _output_name, (_repository_id, _path) in GITHUB_SOURCES[_datasource_filter].items():
    _make_parsed_table(_output_name, _repository_id, _path)
