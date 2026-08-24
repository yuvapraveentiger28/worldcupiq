# Lakeflow Declarative Pipeline source file
#
# Silver layer — normalizes and entity-resolves the 4 bronze sources into the
# 28-table model reviewed in architecture.html's Silver Model tab (all bronze
# tables are STRING-typed from Auto Loader/parse inference, so every numeric/
# date column here is explicitly cast).
#
# Column names below are the REAL, verified bronze schemas (checked via
# `databricks tables get` against the actually-landed data on 2026-08-23) —
# not guesses. A few real shapes differ from the original review:
#   - relational.match_prediction_features is MATCH-grain with ~60 named
#     home_*/away_* columns, not the "65 anonymous features, match+team grain"
#     placeholder the review used — modeled here as the real shape instead.
#   - historical.hist_host_countries is actually "how the host team performed"
#     (team_id + performance), not a geography table — folded into
#     tournament_standing instead of a separate host_country concept; the
#     host nation name itself comes from hist_tournaments.host_country.
#   - historical.hist_referee_appointments is tournament-grain (a referee is
#     on a tournament's panel), not match-grain — referee_assignment reflects
#     that; only relational.matches carries a real match-level referee_id
#     (2026 only).
#   - team_key_stats, team_phases, team_set_plays, team_goalkeeping_distribution
#     (fifa_training_centre) are EAV-shaped (metric/value rows, not wide
#     columns) and their exact metric label strings haven't been sampled yet.
#     match_team below uses the wide-shaped stat tables only
#     (team_defensive_pressure, team_aerial_control, team_goal_prevention,
#     relational.match_team_stats) — the EAV tables are a follow-up once their
#     metric labels are confirmed, not built into this pass.
#
# Entity resolution (team/player/venue/referee/manager appearing in multiple
# bronze sources) is by exact upper+trim name match via a deterministic hash
# surrogate key (`_sk`) — not fuzzy matching. Same name across sources ⇒ same
# silver row; a real name variant across sources (e.g. different transliteration)
# will NOT be resolved to one row in this first pass.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# catalog/schema_prefix passed via pipeline `configuration` in databricks.yml
# (${var.catalog} / ${var.schema_prefix}) — defaults match the original
# mrlc-catalog convention so this file still works without that config set.
CATALOG = spark.conf.get("catalog", "mrlc-catalog")
_SCHEMA_PREFIX = spark.conf.get("schema_prefix", "fifaworldcup_")
FTC = f"{_SCHEMA_PREFIX}bronze_fifa_training_centre"
REL = f"{_SCHEMA_PREFIX}bronze_relational"
HIST = f"{_SCHEMA_PREFIX}bronze_historical"
PRED = f"{_SCHEMA_PREFIX}bronze_predictions"


def _b(schema, table):
    return f"`{CATALOG}`.`{schema}`.`{table}`"


def _sk(*cols):
    # Deterministic hash surrogate key from natural-key columns — stable across
    # incremental runs (unlike row_number()/monotonically_increasing_id()), and
    # the mechanism by which the same real-world entity named identically
    # across two bronze sources resolves to one silver row instead of two.
    parts = [F.upper(F.trim(c.cast("string"))) for c in cols]
    return F.xxhash64(F.concat_ws("||", *parts))


# ================================================================
# Cluster A — Tournament & Reference
# ================================================================

@dlt.table(name="confederation", comment="Confederation lookup.", table_properties={"quality": "silver"})
def confederation():
    hist = spark.table(_b(HIST, "hist_confederations")).select(
        F.col("confederation_name").alias("name"),
        F.col("confederation_code").alias("code"),
    )
    rel = (
        spark.table(_b(REL, "teams"))
        .select(F.col("confederation").alias("code"))
        .where(F.col("confederation").isNotNull())
        .withColumn("name", F.col("code"))
        .select("name", "code")
    )
    return (
        hist.unionByName(rel)
        .dropDuplicates(["code"])
        .withColumn("confederation_id", _sk(F.col("code")))
        .select("confederation_id", "name", "code")
    )


@dlt.table(name="tournament", comment="One row per World Cup edition, 1930-2026.", table_properties={"quality": "silver"})
def tournament():
    hist = spark.table(_b(HIST, "hist_tournaments")).select(
        F.col("tournament_name").alias("name"),
        F.col("year").cast("int").alias("year"),
        F.col("host_country").alias("host_country_name"),
        F.col("count_teams").cast("int").alias("count_teams"),
    )
    seed_2026 = spark.createDataFrame(
        [("FIFA World Cup 2026", 2026, "United States / Mexico / Canada", 48)],
        schema="name string, year int, host_country_name string, count_teams int",
    )
    return (
        hist.unionByName(seed_2026)
        .withColumn("tournament_id", _sk(F.col("year")))
        .select("tournament_id", "name", "year", "host_country_name", "count_teams")
    )


@dlt.table(name="stage", comment="Stage per tournament (Group Stage, Round of 16, ...).", table_properties={"quality": "silver"})
def stage():
    rel = (
        spark.table(_b(REL, "tournament_stages"))
        .select(F.col("stage_name").alias("name"))
        .withColumn("tournament_id", _sk(F.lit(2026)))
    )
    hist = (
        spark.table(_b(HIST, "hist_tournament_stages"))
        .select(
            F.col("tournament_id").alias("_src_tournament_id"),
            F.col("stage_name").alias("name"),
        )
        .join(
            spark.table(_b(HIST, "hist_tournaments")).select(
                F.col("tournament_id").alias("_src_tournament_id"),
                F.col("year").cast("int").alias("year"),
            ),
            "_src_tournament_id",
        )
        .withColumn("tournament_id", _sk(F.col("year")))
        .select("tournament_id", "name")
    )
    return (
        rel.unionByName(hist)
        .dropDuplicates(["tournament_id", "name"])
        .withColumn("stage_id", _sk(F.col("tournament_id"), F.col("name")))
        .select("stage_id", "tournament_id", "name")
    )


@dlt.table(name="tournament_group", comment="Group A/B/C/... per tournament stage.", table_properties={"quality": "silver"})
def tournament_group():
    tid_2026 = _sk(F.lit(2026))
    rel = (
        spark.table(_b(REL, "teams"))
        .select(F.col("group_letter").alias("name"))
        .where(F.col("group_letter").isNotNull())
        .dropDuplicates(["name"])
        .withColumn("tournament_id", tid_2026)
    )
    hist = (
        spark.table(_b(HIST, "hist_groups"))
        .select(
            F.col("tournament_id").alias("_src_tournament_id"),
            F.col("group_name").alias("name"),
        )
        .where(F.col("group_name").isNotNull())
        .join(
            spark.table(_b(HIST, "hist_tournaments")).select(
                F.col("tournament_id").alias("_src_tournament_id"),
                F.col("year").cast("int").alias("year"),
            ),
            "_src_tournament_id",
        )
        .withColumn("tournament_id", _sk(F.col("year")))
        .select("tournament_id", "name")
    )
    return (
        rel.unionByName(hist)
        .dropDuplicates(["tournament_id", "name"])
        .withColumn("group_id", _sk(F.col("tournament_id"), F.col("name")))
        .withColumn(
            "stage_id",
            _sk(F.col("tournament_id"), F.lit("Group Stage")),
        )
        .select("group_id", "tournament_id", "stage_id", "name")
    )


@dlt.table(
    name="qualified_team",
    comment="Team's qualification to one tournament. Historical only — no 2026 qualification-route bronze data yet.",
    table_properties={"quality": "silver"},
)
def qualified_team():
    hist = spark.table(_b(HIST, "hist_qualified_teams")).select(
        F.col("tournament_id").alias("_src_tournament_id"),
        F.col("team_name").alias("_team_name"),
        F.col("performance").alias("qualification_route"),
    )
    tourn = spark.table(_b(HIST, "hist_tournaments")).select(
        F.col("tournament_id").alias("_src_tournament_id"),
        F.col("year").cast("int").alias("year"),
    )
    return (
        hist.join(tourn, "_src_tournament_id")
        .withColumn("tournament_id", _sk(F.col("year")))
        .withColumn("team_id", _sk(F.col("_team_name")))
        .select("tournament_id", "team_id", "qualification_route")
    )


# ================================================================
# Cluster B — Team & Player
# ================================================================

@dlt.table(
    name="team",
    comment="One row per national team, entity-resolved by name across all 3 team/player sources.",
    table_properties={"quality": "silver"},
)
def team():
    rel = spark.table(_b(REL, "teams")).select(
        F.col("team_name").alias("name"),
        F.col("fifa_code").alias("code"),
        F.col("confederation").alias("_confed_code"),
        F.col("manager_name").alias("manager_name"),
    )
    ftc = (
        spark.table(_b(FTC, "teams"))
        .select(F.col("team_name").alias("name"))
        .withColumn("code", F.lit(None).cast("string"))
        .withColumn("_confed_code", F.lit(None).cast("string"))
        .withColumn("manager_name", F.lit(None).cast("string"))
    )
    hist = spark.table(_b(HIST, "hist_teams")).select(
        F.col("team_name").alias("name"),
        F.col("team_code").alias("code"),
        F.col("confederation_code").alias("_confed_code"),
        F.lit(None).cast("string").alias("manager_name"),
    )
    confed = (
        dlt.read("confederation")
        .select(F.col("confederation_id"), F.col("code").alias("_confed_lookup_code"))
    )

    combined = (
        rel.unionByName(ftc)
        .unionByName(hist)
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy(F.upper(F.trim(F.col("name"))))
            .orderBy(F.col("code").isNull(), F.col("manager_name").isNull())
        ))
        .where(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("team_id", _sk(F.col("name")))
    )
    return (
        combined.join(confed, combined["_confed_code"] == confed["_confed_lookup_code"], "left")
        .select("team_id", "name", "code", "confederation_id", "manager_name")
    )


@dlt.table(
    name="team_tournament",
    comment="Team's participation in one tournament: group, final rank. Merges hist_team_appearances-equivalent standing data.",
    table_properties={"quality": "silver"},
)
def team_tournament():
    tid_2026 = _sk(F.lit(2026))
    rel = (
        spark.table(_b(REL, "teams"))
        .select(
            F.col("team_name").alias("_team_name"),
            F.col("group_letter").alias("_group_name"),
        )
        .withColumn("tournament_id", tid_2026)
    )
    hist = (
        spark.table(_b(HIST, "hist_tournament_standings"))
        .select(
            F.col("tournament_id").alias("_src_tournament_id"),
            F.col("team_name").alias("_team_name"),
            F.col("position").cast("int").alias("final_rank"),
        )
        .join(
            spark.table(_b(HIST, "hist_tournaments")).select(
                F.col("tournament_id").alias("_src_tournament_id"),
                F.col("year").cast("int").alias("year"),
            ),
            "_src_tournament_id",
        )
        .withColumn("tournament_id", _sk(F.col("year")))
        .withColumn("_group_name", F.lit(None).cast("string"))
    )
    combined = rel.unionByName(hist, allowMissingColumns=True)
    groups = dlt.read("tournament_group").select(
        F.col("tournament_id").alias("_g_tid"), F.col("group_id"), F.col("name").alias("_g_name")
    )
    return (
        combined.withColumn("team_id", _sk(F.col("_team_name")))
        .join(
            groups,
            (combined["tournament_id"] == groups["_g_tid"]) & (combined["_group_name"] == groups["_g_name"]),
            "left",
        )
        .withColumn("team_tournament_id", _sk(F.col("tournament_id"), F.col("team_id")))
        .select(
            "team_tournament_id", "tournament_id", "team_id", "group_id",
            F.col("final_rank") if "final_rank" in combined.columns else F.lit(None).cast("int").alias("final_rank"),
        )
    )


@dlt.table(
    name="player",
    comment="One row per player, entity-resolved by name across fifa_training_centre, relational, and historical.",
    table_properties={"quality": "silver"},
)
def player():
    ftc = spark.table(_b(FTC, "players")).select(
        F.col("display_name").alias("name"),
        F.lit(None).cast("date").alias("date_of_birth"),
        F.lit(None).cast("string").alias("position"),
    )
    rel = spark.table(_b(REL, "squads_and_players")).select(
        F.col("player_name").alias("name"),
        F.to_date(F.col("date_of_birth")).alias("date_of_birth"),
        F.col("position").alias("position"),
    )
    hist = spark.table(_b(HIST, "hist_players")).select(
        F.concat_ws(" ", F.col("given_name"), F.col("family_name")).alias("name"),
        F.to_date(F.col("birth_date")).alias("date_of_birth"),
        F.lit(None).cast("string").alias("position"),
    )
    return (
        ftc.unionByName(rel)
        .unionByName(hist)
        .where(F.col("name").isNotNull() & (F.col("name") != ""))
        .withColumn("player_id", _sk(F.col("name")))
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("player_id")
                .orderBy(F.col("date_of_birth").isNull(), F.col("position").isNull())
            ),
        )
        .where(F.col("_rn") == 1)
        .select("player_id", "name", "date_of_birth", "position")
    )


@dlt.table(
    name="squad_member",
    comment="Player's roster slot for one tournament (shirt number). FK: tournament, team, player.",
    table_properties={"quality": "silver"},
)
def squad_member():
    tid_2026 = _sk(F.lit(2026))
    rel = spark.table(_b(REL, "squads_and_players")).select(
        F.col("player_name").alias("_player_name"),
        F.col("team_id").alias("_src_team_id"),
    ).withColumn("tournament_id", tid_2026)
    rel_team_map = spark.table(_b(REL, "teams")).select(
        F.col("team_id").alias("_src_team_id"), F.col("team_name").alias("_team_name")
    )
    rel = rel.join(rel_team_map, "_src_team_id").drop("_src_team_id")

    hist = (
        spark.table(_b(HIST, "hist_squads"))
        .select(
            F.col("tournament_id").alias("_src_tournament_id"),
            F.concat_ws(" ", F.col("given_name"), F.col("family_name")).alias("_player_name"),
            F.col("team_name").alias("_team_name"),
            F.col("shirt_number").cast("int").alias("shirt_number"),
        )
        .join(
            spark.table(_b(HIST, "hist_tournaments")).select(
                F.col("tournament_id").alias("_src_tournament_id"),
                F.col("year").cast("int").alias("year"),
            ),
            "_src_tournament_id",
        )
        .withColumn("tournament_id", _sk(F.col("year")))
        .drop("_src_tournament_id", "year")
    )
    combined = rel.unionByName(hist, allowMissingColumns=True)
    return (
        combined
        .withColumn("team_id", _sk(F.col("_team_name")))
        .withColumn("player_id", _sk(F.col("_player_name")))
        .withColumn("squad_member_id", _sk(F.col("tournament_id"), F.col("team_id"), F.col("player_id")))
        .select("squad_member_id", "tournament_id", "team_id", "player_id", "shirt_number")
    )


# ================================================================
# Cluster C — Officials
# ================================================================

@dlt.table(name="referee", comment="One row per referee.", table_properties={"quality": "silver"})
def referee():
    rel = spark.table(_b(REL, "referees")).select(
        F.col("name").alias("name"), F.col("country").alias("country_code")
    )
    hist = spark.table(_b(HIST, "hist_referees")).select(
        F.concat_ws(" ", F.col("given_name"), F.col("family_name")).alias("name"),
        F.col("country_name").alias("country_code"),
    )
    return (
        rel.unionByName(hist)
        .where(F.col("name").isNotNull() & (F.col("name") != ""))
        .dropDuplicates(["name"])
        .withColumn("referee_id", _sk(F.col("name")))
        .select("referee_id", "name", "country_code")
    )


@dlt.table(
    name="referee_assignment",
    comment="Referee's assignment to a tournament panel — tournament-grain (historical bronze has no match-level referee data; only relational.matches has a real match.referee_id, kept on match itself). FK: tournament, referee.",
    table_properties={"quality": "silver"},
)
def referee_assignment():
    hist = (
        spark.table(_b(HIST, "hist_referee_appointments"))
        .select(
            F.col("tournament_id").alias("_src_tournament_id"),
            F.concat_ws(" ", F.col("given_name"), F.col("family_name")).alias("_referee_name"),
        )
        .join(
            spark.table(_b(HIST, "hist_tournaments")).select(
                F.col("tournament_id").alias("_src_tournament_id"),
                F.col("year").cast("int").alias("year"),
            ),
            "_src_tournament_id",
        )
        .withColumn("tournament_id", _sk(F.col("year")))
    )
    return (
        hist.withColumn("referee_id", _sk(F.col("_referee_name")))
        .withColumn("assignment_id", _sk(F.col("tournament_id"), F.col("referee_id")))
        .select("assignment_id", "tournament_id", "referee_id")
    )


@dlt.table(name="manager", comment="One row per manager/coach.", table_properties={"quality": "silver"})
def manager():
    rel = (
        spark.table(_b(REL, "teams"))
        .select(F.col("manager_name").alias("name"))
        .where(F.col("manager_name").isNotNull())
        .withColumn("country_code", F.lit(None).cast("string"))
    )
    hist = spark.table(_b(HIST, "hist_managers")).select(
        F.concat_ws(" ", F.col("given_name"), F.col("family_name")).alias("name"),
        F.col("country_name").alias("country_code"),
    )
    return (
        rel.unionByName(hist)
        .where(F.col("name").isNotNull() & (F.col("name") != ""))
        .dropDuplicates(["name"])
        .withColumn("manager_id", _sk(F.col("name")))
        .select("manager_id", "name", "country_code")
    )


@dlt.table(
    name="manager_appointment",
    comment="Which manager led which team in which tournament. Merges hist_manager_appointments + the 2026 manager_name on relational.teams. FK: tournament, team, manager.",
    table_properties={"quality": "silver"},
)
def manager_appointment():
    tid_2026 = _sk(F.lit(2026))
    rel = (
        spark.table(_b(REL, "teams"))
        .select(F.col("team_name").alias("_team_name"), F.col("manager_name").alias("_manager_name"))
        .where(F.col("manager_name").isNotNull())
        .withColumn("tournament_id", tid_2026)
    )
    hist = (
        spark.table(_b(HIST, "hist_manager_appointments"))
        .select(
            F.col("tournament_id").alias("_src_tournament_id"),
            F.col("team_name").alias("_team_name"),
            F.concat_ws(" ", F.col("given_name"), F.col("family_name")).alias("_manager_name"),
        )
        .join(
            spark.table(_b(HIST, "hist_tournaments")).select(
                F.col("tournament_id").alias("_src_tournament_id"),
                F.col("year").cast("int").alias("year"),
            ),
            "_src_tournament_id",
        )
        .withColumn("tournament_id", _sk(F.col("year")))
        .drop("_src_tournament_id", "year")
    )
    combined = rel.unionByName(hist)
    return (
        combined
        .withColumn("team_id", _sk(F.col("_team_name")))
        .withColumn("manager_id", _sk(F.col("_manager_name")))
        .withColumn("appointment_id", _sk(F.col("tournament_id"), F.col("team_id"), F.col("manager_id")))
        .select("appointment_id", "tournament_id", "team_id", "manager_id")
    )


# ================================================================
# Cluster D — Match Spine (focal)
# ================================================================

@dlt.table(name="venue", comment="One row per stadium, 2026 + historical.", table_properties={"quality": "silver"})
def venue():
    rel = spark.table(_b(REL, "venues")).select(
        F.col("stadium_name").alias("name"),
        F.col("city").alias("city"),
        F.col("country").alias("country_code"),
        F.col("capacity").cast("int").alias("capacity"),
    )
    hist = (
        spark.table(_b(HIST, "hist_stadiums"))
        .select(
            F.col("stadium_name").alias("name"),
            F.col("city_name").alias("city"),
            F.col("country_name").alias("country_code"),
            F.col("stadium_capacity").cast("int").alias("capacity"),
        )
    )
    return (
        rel.unionByName(hist)
        .where(F.col("name").isNotNull())
        .dropDuplicates(["name"])
        .withColumn("venue_id", _sk(F.col("name")))
        .select("venue_id", "name", "city", "country_code", "capacity")
    )


@dlt.table(
    name="match",
    comment="Fact — one row per match, every tournament 1930-2026. FK: tournament, stage, group, venue, home/away/winner team, referee.",
    table_properties={"quality": "silver"},
)
def match():
    # team_id/venue_id/stage_id/referee_id below are computed with the same
    # deterministic _sk() hash the team/venue/stage/referee tables use for
    # their own surrogate keys — same name in, same id out — so no join back
    # to those tables is needed here for the keys to line up.
    tid_2026 = _sk(F.lit(2026))

    rel = (
        spark.table(_b(REL, "matches"))
        .select(
            F.col("match_id").alias("_src_match_id"),
            F.to_date(F.col("date")).alias("match_date"),
            F.col("stage_id").alias("_src_stage_id"),
            F.col("venue_id").alias("_src_venue_id"),
            F.col("home_team_id").alias("_src_home_id"),
            F.col("away_team_id").alias("_src_away_id"),
            F.col("home_score").cast("int").alias("home_score"),
            F.col("away_score").cast("int").alias("away_score"),
            F.col("home_penalty_score").cast("int").alias("home_penalties"),
            F.col("away_penalty_score").cast("int").alias("away_penalties"),
            F.col("home_xg").cast("double").alias("home_xg"),
            F.col("away_xg").cast("double").alias("away_xg"),
            F.col("referee_id").alias("_src_referee_id"),
        )
        .withColumn("tournament_id", tid_2026)
    )
    # Resolve relational's own team_id/venue_id/stage_id -> canonical names, then to silver surrogate keys.
    rel_team_map = spark.table(_b(REL, "teams")).select(
        F.col("team_id").alias("_src_id"), F.col("team_name").alias("_name")
    )
    rel_venue_map = spark.table(_b(REL, "venues")).select(
        F.col("venue_id").alias("_src_id"), F.col("stadium_name").alias("_name")
    )
    rel_stage_map = spark.table(_b(REL, "tournament_stages")).select(
        F.col("stage_id").alias("_src_id"), F.col("stage_name").alias("_name")
    )
    rel_referee_map = spark.table(_b(REL, "referees")).select(
        F.col("referee_id").alias("_src_id"), F.col("name").alias("_name")
    )

    rel = (
        rel.join(rel_team_map.withColumnRenamed("_src_id", "_src_home_id").withColumnRenamed("_name", "_home_name"), "_src_home_id", "left")
        .join(rel_team_map.withColumnRenamed("_src_id", "_src_away_id").withColumnRenamed("_name", "_away_name"), "_src_away_id", "left")
        .join(rel_venue_map.withColumnRenamed("_src_id", "_src_venue_id").withColumnRenamed("_name", "_venue_name"), "_src_venue_id", "left")
        .join(rel_stage_map.withColumnRenamed("_src_id", "_src_stage_id").withColumnRenamed("_name", "_stage_name"), "_src_stage_id", "left")
        .join(rel_referee_map.withColumnRenamed("_src_id", "_src_referee_id").withColumnRenamed("_name", "_referee_name"), "_src_referee_id", "left")
    )

    hist = (
        spark.table(_b(HIST, "hist_matches"))
        .select(
            F.col("tournament_id").alias("_src_tournament_id"),
            F.to_date(F.col("match_date")).alias("match_date"),
            F.col("stage_name").alias("_stage_name"),
            F.col("home_team_name").alias("_home_name"),
            F.col("away_team_name").alias("_away_name"),
            F.col("stadium_name").alias("_venue_name"),
            F.col("home_team_score").cast("int").alias("home_score"),
            F.col("away_team_score").cast("int").alias("away_score"),
            F.col("home_team_score_penalties").cast("int").alias("home_penalties"),
            F.col("away_team_score_penalties").cast("int").alias("away_penalties"),
            F.lit(None).cast("double").alias("home_xg"),
            F.lit(None).cast("double").alias("away_xg"),
            F.lit(None).cast("string").alias("_referee_name"),
        )
        .join(
            spark.table(_b(HIST, "hist_tournaments")).select(
                F.col("tournament_id").alias("_src_tournament_id"),
                F.col("year").cast("int").alias("year"),
            ),
            "_src_tournament_id",
        )
        .withColumn("tournament_id", _sk(F.col("year")))
        .drop("_src_tournament_id", "year")
    )

    common_cols = [
        "tournament_id", "match_date", "_stage_name", "_home_name", "_away_name",
        "_venue_name", "home_score", "away_score", "home_penalties", "away_penalties",
        "home_xg", "away_xg", "_referee_name",
    ]
    combined = rel.select(*common_cols).unionByName(hist.select(*common_cols))

    return (
        combined
        .withColumn("match_id", _sk(F.col("tournament_id"), F.col("_home_name"), F.col("_away_name"), F.col("match_date")))
        .withColumn("home_team_id", _sk(F.col("_home_name")))
        .withColumn("away_team_id", _sk(F.col("_away_name")))
        .withColumn(
            "winner_team_id",
            F.when(F.col("home_score") > F.col("away_score"), F.col("home_team_id"))
            .when(F.col("away_score") > F.col("home_score"), F.col("away_team_id"))
            .otherwise(F.lit(None).cast("long")),
        )
        .withColumn("venue_id", _sk(F.col("_venue_name")))
        .withColumn("stage_id", _sk(F.col("tournament_id"), F.col("_stage_name")))
        .withColumn("referee_id", _sk(F.col("_referee_name")))
        .withColumn("referee_id", F.when(F.col("_referee_name").isNull(), F.lit(None).cast("long")).otherwise(F.col("referee_id")))
        .select(
            "match_id", "tournament_id", "stage_id", "venue_id",
            "home_team_id", "away_team_id", "winner_team_id", "referee_id",
            "match_date", "home_score", "away_score", "home_penalties", "away_penalties",
            "home_xg", "away_xg",
        )
    )


@dlt.table(
    name="group_standing",
    comment="Group-stage table row per team. Historical only — no 2026 bronze source publishes a standings table yet (it's derivable from match/match_team once those are populated, in a later refinement). FK: group, team.",
    table_properties={"quality": "silver"},
)
def group_standing():
    hist = (
        spark.table(_b(HIST, "hist_group_standings"))
        .select(
            F.col("tournament_id").alias("_src_tournament_id"),
            F.col("group_name").alias("_group_name"),
            F.col("team_name").alias("_team_name"),
            F.col("played").cast("int").alias("played"),
            F.col("wins").cast("int").alias("won"),
            F.col("draws").cast("int").alias("drawn"),
            F.col("losses").cast("int").alias("lost"),
            F.col("goals_for").cast("int").alias("goals_for"),
            F.col("goals_against").cast("int").alias("goals_against"),
            F.col("points").cast("int").alias("points"),
            F.col("position").cast("int").alias("rank"),
        )
        .join(
            spark.table(_b(HIST, "hist_tournaments")).select(
                F.col("tournament_id").alias("_src_tournament_id"),
                F.col("year").cast("int").alias("year"),
            ),
            "_src_tournament_id",
        )
        .withColumn("tournament_id", _sk(F.col("year")))
    )
    groups = dlt.read("tournament_group").select(
        F.col("tournament_id").alias("_g_tid"), F.col("group_id"), F.col("name").alias("_g_name")
    )
    return (
        hist.join(groups, (hist["tournament_id"] == groups["_g_tid"]) & (hist["_group_name"] == groups["_g_name"]), "left")
        .withColumn("team_id", _sk(F.col("_team_name")))
        .withColumn("group_standing_id", _sk(F.col("group_id"), F.col("team_id")))
        .select(
            "group_standing_id", "group_id", "team_id", "played", "won", "drawn",
            "lost", "goals_for", "goals_against", "points", "rank",
        )
    )


@dlt.table(
    name="tournament_standing",
    comment="Final tournament placement per team. Historical from hist_tournament_standings; also folds in hist_host_countries (host team + hosting performance) as extra context on the same grain. FK: tournament, team.",
    table_properties={"quality": "silver"},
)
def tournament_standing():
    hist = (
        spark.table(_b(HIST, "hist_tournament_standings"))
        .select(
            F.col("tournament_id").alias("_src_tournament_id"),
            F.col("team_name").alias("_team_name"),
            F.col("position").cast("int").alias("final_rank"),
        )
        .join(
            spark.table(_b(HIST, "hist_tournaments")).select(
                F.col("tournament_id").alias("_src_tournament_id"),
                F.col("year").cast("int").alias("year"),
            ),
            "_src_tournament_id",
        )
        .withColumn("tournament_id", _sk(F.col("year")))
        .drop("_src_tournament_id", "year")
    )
    host_perf = (
        spark.table(_b(HIST, "hist_host_countries"))
        .select(
            F.col("tournament_id").alias("_src_tournament_id"),
            F.col("team_name").alias("_host_team_name"),
            F.col("performance").alias("host_performance"),
        )
        .join(
            spark.table(_b(HIST, "hist_tournaments")).select(
                F.col("tournament_id").alias("_src_tournament_id"),
                F.col("year").cast("int").alias("year"),
            ),
            "_src_tournament_id",
        )
        .withColumn("tournament_id", _sk(F.col("year")))
        .withColumn("team_id", _sk(F.col("_host_team_name")))
        .select("tournament_id", "team_id", "host_performance")
    )
    return (
        hist.withColumn("team_id", _sk(F.col("_team_name")))
        .withColumn("tournament_standing_id", _sk(F.col("tournament_id"), F.col("team_id")))
        .join(host_perf, ["tournament_id", "team_id"], "left")
        .select("tournament_standing_id", "tournament_id", "team_id", "final_rank", "host_performance")
    )


# ================================================================
# Cluster E — Match Performance (2026 only)
# ================================================================

@dlt.table(
    name="match_team",
    comment="Team stats per match. Combines relational.match_team_stats (possession/shots/corners) with fifa_training_centre's wide stat tables (defensive pressure, aerial control, goal prevention). The EAV-shaped stat tables (team_key_stats, team_phases, team_set_plays) are NOT included yet — see file header. FK: match, team.",
    table_properties={"quality": "silver"},
)
def match_team():
    ftc_mt = spark.table(_b(FTC, "match_teams")).select(
        F.col("match_id").alias("_src_match_id"),
        F.col("match_team_id").alias("_src_match_team_id"),
        F.col("team_name").alias("_team_name"),
        F.col("home_away").alias("home_away"),
    )
    ftc_matches = spark.table(_b(FTC, "matches")).select(
        F.col("match_id").alias("_src_match_id"),
        F.col("home_team").alias("_home_name"),
        F.col("away_team").alias("_away_name"),
        F.to_date(F.col("date")).alias("match_date"),
    )
    rel_stats = spark.table(_b(REL, "match_team_stats")).select(
        F.col("match_id").alias("_src_match_id"),
        F.col("team_id").alias("_src_team_id"),
        F.col("possession_pct").cast("double").alias("possession_pct"),
        F.col("total_shots").cast("int").alias("shots"),
        F.col("shots_on_target").cast("int").alias("shots_on_target"),
        F.col("corners").cast("int").alias("corners"),
        F.col("fouls").cast("int").alias("fouls"),
        F.col("offsides").cast("int").alias("offsides"),
    )
    rel_team_map = spark.table(_b(REL, "teams")).select(
        F.col("team_id").alias("_src_team_id"), F.col("team_name").alias("_team_name")
    )
    rel_stats = rel_stats.join(rel_team_map, "_src_team_id").drop("_src_team_id")

    ftc_press = spark.table(_b(FTC, "team_defensive_pressure")).select(
        F.col("match_team_id").alias("_src_match_team_id"),
        F.col("total_pressures").cast("int").alias("pressures"),
        F.col("forced_turnovers").cast("int").alias("recoveries"),
    )
    ftc_aerial = spark.table(_b(FTC, "team_aerial_control")).select(
        F.col("match_team_id").alias("_src_match_team_id"),
        F.col("delivery_total").cast("int").alias("aerial_duels"),
    )

    ftc_base = (
        ftc_mt.join(ftc_matches, "_src_match_id")
        .withColumn("match_id", _sk(F.col("_home_name"), F.col("_away_name"), F.col("match_date")))
        .withColumn("team_id", _sk(F.col("_team_name")))
        .join(ftc_press, "_src_match_team_id", "left")
        .join(ftc_aerial, "_src_match_team_id", "left")
        .select("match_id", "team_id", "home_away", "pressures", "recoveries", "aerial_duels")
    )

    rel_base = (
        rel_stats
        .join(
            spark.table(_b(REL, "matches")).select(
                F.col("match_id").alias("_src_match_id"),
                F.col("home_team_id").alias("_src_home_id"),
                F.col("away_team_id").alias("_src_away_id"),
                F.to_date(F.col("date")).alias("match_date"),
            ),
            "_src_match_id",
        )
        .join(rel_team_map.withColumnRenamed("_src_team_id", "_src_home_id").withColumnRenamed("_team_name", "_home_name"), "_src_home_id")
        .join(rel_team_map.withColumnRenamed("_src_team_id", "_src_away_id").withColumnRenamed("_team_name", "_away_name"), "_src_away_id")
        .withColumn("match_id", _sk(F.col("_home_name"), F.col("_away_name"), F.col("match_date")))
        .withColumn("team_id", _sk(F.col("_team_name")))
        .select(
            "match_id", "team_id", "possession_pct", "shots", "shots_on_target",
            "corners", "fouls", "offsides",
        )
    )

    return (
        ftc_base.join(rel_base, ["match_id", "team_id"], "outer")
        .withColumn("match_team_id", _sk(F.col("match_id"), F.col("team_id")))
        .select(
            "match_team_id", "match_id", "team_id", "home_away", "possession_pct",
            "shots", "shots_on_target", "corners", "fouls", "offsides",
            "pressures", "recoveries", "aerial_duels",
        )
    )


@dlt.table(
    name="match_player",
    comment="Player stats per match. Base identity from fifa_training_centre.match_appearances + relational.match_lineups; measures from player_in_possession_distributions and relational.player_stats (career/tournament aggregate, not strictly match-grain — see column comment). FK: match, team, player.",
    table_properties={"quality": "silver"},
)
def match_player():
    ftc_app = spark.table(_b(FTC, "match_appearances")).select(
        F.col("appearance_id").alias("_appearance_id"),
        F.col("match_id").alias("_src_match_id"),
        F.col("player_name").alias("_player_name"),
        F.col("team").alias("_team_name"),
        F.col("is_starter").cast("boolean").alias("started"),
        F.col("position").alias("position_played"),
    )
    ftc_matches = spark.table(_b(FTC, "matches")).select(
        F.col("match_id").alias("_src_match_id"),
        F.col("home_team").alias("_home_name"),
        F.col("away_team").alias("_away_name"),
        F.to_date(F.col("date")).alias("match_date"),
    )
    ftc_poss = spark.table(_b(FTC, "player_in_possession_distributions")).select(
        F.col("appearance_id").alias("_appearance_id"),
        F.col("goals").cast("int").alias("goals"),
        F.col("attempts_at_goal").cast("int").alias("shots"),
        F.col("passes_attempted").cast("int").alias("passes"),
        F.col("passes_completed").cast("int").alias("passes_completed"),
        F.col("line_breaks_completed").cast("int").alias("progressive_passes"),
    )
    ftc_defense = spark.table(_b(FTC, "player_out_of_possession")).select(
        F.col("appearance_id").alias("_appearance_id"),
        F.col("interceptions").cast("int").alias("recoveries"),
        F.col("tackles_made_won").cast("int").alias("duels"),
    )

    base = (
        ftc_app.join(ftc_matches, "_src_match_id")
        .withColumn("match_id", _sk(F.col("_home_name"), F.col("_away_name"), F.col("match_date")))
        .withColumn("team_id", _sk(F.col("_team_name")))
        .withColumn("player_id", _sk(F.col("_player_name")))
        .join(ftc_poss, "_appearance_id", "left")
        .join(ftc_defense, "_appearance_id", "left")
    )

    return (
        base
        .withColumn("match_player_id", _sk(F.col("match_id"), F.col("player_id")))
        .select(
            "match_player_id", "match_id", "team_id", "player_id", "started",
            "position_played", "goals", "shots", "passes", "passes_completed",
            "progressive_passes", "recoveries", "duels",
        )
    )


@dlt.table(
    name="match_player_physical",
    comment="Split from match_player — GPS/physical tracking, distinct grain/availability from on-ball stats. FK: match_player.",
    table_properties={"quality": "silver"},
)
def match_player_physical():
    phys = spark.table(_b(FTC, "player_physical_data")).select(
        F.col("appearance_id").alias("_appearance_id"),
        F.col("match_id").alias("_src_match_id"),
        F.col("player_name").alias("_player_name"),
        (F.col("total_distance_m").cast("double") / 1000.0).alias("distance_covered_km"),
        F.col("top_speed_kmh").cast("double").alias("top_speed_kmh"),
    )
    ftc_matches = spark.table(_b(FTC, "matches")).select(
        F.col("match_id").alias("_src_match_id"),
        F.col("home_team").alias("_home_name"),
        F.col("away_team").alias("_away_name"),
        F.to_date(F.col("date")).alias("match_date"),
    )
    return (
        phys.join(ftc_matches, "_src_match_id")
        .withColumn("match_id", _sk(F.col("_home_name"), F.col("_away_name"), F.col("match_date")))
        .withColumn("player_id", _sk(F.col("_player_name")))
        .withColumn("match_player_id", _sk(F.col("match_id"), F.col("player_id")))
        .select("match_player_id", "distance_covered_km", "top_speed_kmh")
    )


@dlt.table(
    name="match_goalkeeper",
    comment="Goalkeeper-specific measures per match, split out of match_team (team_goal_prevention/team_goalkeeping_distribution are keeper-grain, not team-grain, despite the bronze table names). FK: match, team, player — player resolved via a 1:1 join to match_appearances' starting GK for the same match_team_id (team_goal_prevention itself has no player column).",
    table_properties={"quality": "silver"},
)
def match_goalkeeper():
    ftc_gp = spark.table(_b(FTC, "team_goal_prevention")).select(
        F.col("match_id").alias("_src_match_id"),
        F.col("match_team_id").alias("_src_match_team_id"),
        F.col("team").alias("_team_name"),
        F.col("total_attempts_faced").cast("int").alias("shots_faced"),
        F.col("save_attempt").cast("int").alias("saves"),
        F.col("save_pct").cast("double").alias("save_pct"),
    )
    ftc_matches = spark.table(_b(FTC, "matches")).select(
        F.col("match_id").alias("_src_match_id"),
        F.col("home_team").alias("_home_name"),
        F.col("away_team").alias("_away_name"),
        F.to_date(F.col("date")).alias("match_date"),
    )
    # The starting keeper for each team_goal_prevention row (match_team_id is
    # 1:1 with exactly one is_starter='True' GK appearance in this bronze data).
    starting_gk = spark.table(_b(FTC, "match_appearances")).select(
        F.col("match_team_id").alias("_src_match_team_id"),
        F.col("player_name").alias("_player_name"),
    ).where((F.col("position") == "GK") & (F.col("is_starter") == "True"))

    return (
        ftc_gp.join(ftc_matches, "_src_match_id")
        .join(starting_gk, "_src_match_team_id", "left")
        .withColumn("match_id", _sk(F.col("_home_name"), F.col("_away_name"), F.col("match_date")))
        .withColumn("team_id", _sk(F.col("_team_name")))
        .withColumn(
            "player_id",
            F.when(F.col("_player_name").isNull(), F.lit(None).cast("long")).otherwise(_sk(F.col("_player_name"))),
        )
        .withColumn("match_goalkeeper_id", _sk(F.col("match_id"), F.col("team_id")))
        .select("match_goalkeeper_id", "match_id", "team_id", "player_id", "shots_faced", "saves", "save_pct")
    )


@dlt.table(
    name="passing_edge",
    comment="Directed passer->receiver edges per match (GraphFrames source). Composite key. FK: match, team, player x2.",
    table_properties={"quality": "silver"},
)
def passing_edge():
    edges = spark.table(_b(FTC, "passing_network_edges")).select(
        F.col("match_id").alias("_src_match_id"),
        F.col("team").alias("_team_name"),
        F.col("from_player").alias("_passer_name"),
        F.col("to_player").alias("_receiver_name"),
        F.col("pass_count").cast("int").alias("passes"),
    )
    ftc_matches = spark.table(_b(FTC, "matches")).select(
        F.col("match_id").alias("_src_match_id"),
        F.col("home_team").alias("_home_name"),
        F.col("away_team").alias("_away_name"),
        F.to_date(F.col("date")).alias("match_date"),
    )
    return (
        edges.join(ftc_matches, "_src_match_id")
        .withColumn("match_id", _sk(F.col("_home_name"), F.col("_away_name"), F.col("match_date")))
        .withColumn("team_id", _sk(F.col("_team_name")))
        .withColumn("passer_id", _sk(F.col("_passer_name")))
        .withColumn("receiver_id", _sk(F.col("_receiver_name")))
        .select("match_id", "team_id", "passer_id", "receiver_id", "passes")
    )


# ================================================================
# Cluster F — Events & Awards
# ================================================================

@dlt.table(
    name="match_event",
    comment="One row per event, every tournament. Merges fifa_training_centre.attempts_at_goal (2026 shots/goals) + relational.match_events (2026 goals/cards/VAR) + hist_goals/hist_bookings/hist_substitutions/hist_penalty_kicks (historical). FK: match, team, player x2.",
    table_properties={"quality": "silver"},
)
def match_event():
    ftc_shots = spark.table(_b(FTC, "attempts_at_goal")).select(
        F.col("match_id").alias("_src_match_id"),
        F.col("team").alias("_team_name"),
        F.col("player_name").alias("_player_name"),
        F.lit(None).cast("string").alias("_secondary_name"),
        F.lit("SHOT").alias("event_type"),
        F.col("minute").cast("int").alias("minute"),
        F.col("outcome").alias("detail"),
    )
    rel_events = spark.table(_b(REL, "match_events")).select(
        F.col("match_id").alias("_src_match_id2"),
        F.col("team_id").alias("_src_team_id"),
        F.col("player_id").alias("_src_player_id"),
        F.col("event_type").alias("event_type"),
        F.col("minute").cast("int").alias("minute"),
    )
    rel_team_map = spark.table(_b(REL, "teams")).select(F.col("team_id").alias("_src_team_id"), F.col("team_name").alias("_team_name"))
    rel_player_map = spark.table(_b(REL, "squads_and_players")).select(F.col("player_id").alias("_src_player_id"), F.col("player_name").alias("_player_name"))
    rel_events = (
        rel_events.join(rel_team_map, "_src_team_id", "left")
        .join(rel_player_map, "_src_player_id", "left")
        .withColumn("_secondary_name", F.lit(None).cast("string"))
        .withColumn("detail", F.lit(None).cast("string"))
    )

    hist_goals = spark.table(_b(HIST, "hist_goals")).select(
        F.col("tournament_id").alias("_src_tid"),
        F.col("match_date").alias("_match_date"),
        F.col("home_team").alias("_home_name"),
        F.col("away_team").alias("_away_name"),
        F.col("team_name").alias("_team_name"),
        F.concat_ws(" ", F.col("given_name"), F.col("family_name")).alias("_player_name"),
        F.lit("GOAL").alias("event_type"),
        F.col("minute_regulation").cast("int").alias("minute"),
        F.when(F.col("own_goal") == "true", "OWN_GOAL").when(F.col("penalty") == "true", "PENALTY").alias("detail"),
    )
    hist_bookings = spark.table(_b(HIST, "hist_bookings")).select(
        F.col("tournament_id").alias("_src_tid"),
        F.col("match_date").alias("_match_date"),
        F.col("home_team").alias("_home_name"),
        F.col("away_team").alias("_away_name"),
        F.col("team_name").alias("_team_name"),
        F.concat_ws(" ", F.col("given_name"), F.col("family_name")).alias("_player_name"),
        F.lit("CARD").alias("event_type"),
        F.col("minute_regulation").cast("int").alias("minute"),
        F.when(F.col("red_card") == "true", "RED").when(F.col("second_yellow_card") == "true", "SECOND_YELLOW").otherwise("YELLOW").alias("detail"),
    )
    hist_subs = spark.table(_b(HIST, "hist_substitutions")).select(
        F.col("tournament_id").alias("_src_tid"),
        F.col("match_date").alias("_match_date"),
        F.col("home_team").alias("_home_name"),
        F.col("away_team").alias("_away_name"),
        F.col("team_name").alias("_team_name"),
        F.concat_ws(" ", F.col("given_name"), F.col("family_name")).alias("_player_name"),
        F.lit("SUBSTITUTION").alias("event_type"),
        F.col("minute_regulation").cast("int").alias("minute"),
        F.when(F.col("coming_on") == "true", "IN").otherwise("OUT").alias("detail"),
    )

    hist_tourn = spark.table(_b(HIST, "hist_tournaments")).select(
        F.col("tournament_id").alias("_src_tid"), F.col("year").cast("int").alias("year")
    )
    hist_common = ["_src_tid", "_match_date", "_home_name", "_away_name", "_team_name", "_player_name", "event_type", "minute", "detail"]
    hist_combined = (
        hist_goals.select(*hist_common)
        .unionByName(hist_bookings.select(*hist_common))
        .unionByName(hist_subs.select(*hist_common))
        .join(hist_tourn, "_src_tid")
        .withColumn("tournament_id", _sk(F.col("year")))
        .withColumn("match_id", _sk(F.col("tournament_id"), F.col("_home_name"), F.col("_away_name"), F.to_date(F.col("_match_date"))))
        .withColumn("_secondary_name", F.lit(None).cast("string"))
    )

    ftc_common = (
        ftc_shots.join(
            spark.table(_b(FTC, "matches")).select(
                F.col("match_id").alias("_src_match_id"),
                F.col("home_team").alias("_home_name"),
                F.col("away_team").alias("_away_name"),
                F.to_date(F.col("date")).alias("match_date"),
            ),
            "_src_match_id",
        )
        .withColumn("match_id", _sk(F.col("_home_name"), F.col("_away_name"), F.col("match_date")))
        .select("match_id", "_team_name", "_player_name", "_secondary_name", "event_type", "minute", "detail")
    )
    rel_common = (
        rel_events.join(
            spark.table(_b(REL, "matches")).select(
                F.col("match_id").alias("_src_match_id2"), F.to_date(F.col("date")).alias("match_date")
            ),
            "_src_match_id2",
        )
        .join(
            spark.table(_b(REL, "matches")).select(
                F.col("match_id").alias("_mm"),
                F.col("home_team_id").alias("_src_home_id"),
                F.col("away_team_id").alias("_src_away_id"),
            ),
            F.col("_src_match_id2") == F.col("_mm"),
        )
        .join(rel_team_map.withColumnRenamed("_src_team_id", "_src_home_id").withColumnRenamed("_team_name", "_home_name"), "_src_home_id")
        .join(rel_team_map.withColumnRenamed("_src_team_id", "_src_away_id").withColumnRenamed("_team_name", "_away_name"), "_src_away_id")
        .withColumn("match_id", _sk(F.col("_home_name"), F.col("_away_name"), F.col("match_date")))
        .select("match_id", "_team_name", "_player_name", "_secondary_name", "event_type", "minute", "detail")
    )
    hist_final = hist_combined.select("match_id", "_team_name", "_player_name", "_secondary_name", "event_type", "minute", "detail")

    combined = ftc_common.unionByName(rel_common).unionByName(hist_final)
    return (
        combined
        .withColumn("team_id", _sk(F.col("_team_name")))
        .withColumn("player_id", _sk(F.col("_player_name")))
        .withColumn(
            "secondary_player_id",
            F.when(F.col("_secondary_name").isNull(), F.lit(None).cast("long")).otherwise(_sk(F.col("_secondary_name"))),
        )
        .withColumn("event_id", F.xxhash64(F.col("match_id"), F.col("team_id"), F.col("player_id"), F.col("event_type"), F.col("minute"), F.monotonically_increasing_id()))
        .select("event_id", "match_id", "team_id", "player_id", "secondary_player_id", "event_type", "minute", "detail")
    )


@dlt.table(name="award", comment="Award per tournament (Golden Boot, etc). Historical only.", table_properties={"quality": "silver"})
def award():
    return (
        spark.table(_b(HIST, "hist_awards"))
        .select(F.col("award_name").alias("name"))
        .dropDuplicates(["name"])
        .withColumn("award_id", _sk(F.col("name")))
        # award is tournament-agnostic (a lookup of award TYPES); award_winner
        # carries the tournament_id for a specific year's winner.
        .select("award_id", "name")
    )


@dlt.table(name="award_winner", comment="Award winner per tournament. FK: award, tournament, player, team.", table_properties={"quality": "silver"})
def award_winner():
    hist = spark.table(_b(HIST, "hist_award_winners")).select(
        F.col("tournament_id").alias("_src_tid"),
        F.col("award_name").alias("_award_name"),
        F.concat_ws(" ", F.col("given_name"), F.col("family_name")).alias("_player_name"),
        F.col("team_name").alias("_team_name"),
    )
    tourn = spark.table(_b(HIST, "hist_tournaments")).select(
        F.col("tournament_id").alias("_src_tid"), F.col("year").cast("int").alias("year")
    )
    return (
        hist.join(tourn, "_src_tid")
        .withColumn("tournament_id", _sk(F.col("year")))
        .withColumn("award_id", _sk(F.col("_award_name")))
        .withColumn("player_id", F.when(F.col("_player_name").isNull() | (F.col("_player_name") == " "), F.lit(None).cast("long")).otherwise(_sk(F.col("_player_name"))))
        .withColumn("team_id", F.when(F.col("_team_name").isNull(), F.lit(None).cast("long")).otherwise(_sk(F.col("_team_name"))))
        .withColumn("award_winner_id", _sk(F.col("tournament_id"), F.col("award_id"), F.coalesce(F.col("player_id"), F.col("team_id"))))
        .select("award_winner_id", "tournament_id", "award_id", "player_id", "team_id")
    )


# ================================================================
# Cluster G — Predictions
# ================================================================

@dlt.table(
    name="match_prediction_feature",
    comment="ML input features per match (mominullptr's feature set) — match-grain with real named home_*/away_* columns, not the anonymous feature_01..65 placeholder used in the design review. NOT the same as prediction below (inputs vs. output). FK: match.",
    table_properties={"quality": "silver"},
)
def match_prediction_feature():
    feats = spark.table(_b(REL, "match_prediction_features"))
    rel_team_map = spark.table(_b(REL, "teams")).select(F.col("team_id").alias("_src_id"), F.col("team_name").alias("_name"))
    base = (
        feats
        .join(rel_team_map.withColumnRenamed("_src_id", "home_team_id").withColumnRenamed("_name", "_home_name"), "home_team_id", "left")
        .join(rel_team_map.withColumnRenamed("_src_id", "away_team_id").withColumnRenamed("_name", "_away_name"), "away_team_id", "left")
        .withColumn("match_date", F.to_date(F.col("date")))
    )
    return (
        base
        .withColumn("match_id", _sk(F.col("_home_name"), F.col("_away_name"), F.col("match_date")))
        .select(
            "match_id",
            F.col("home_fifa_rank").cast("int").alias("home_fifa_rank"),
            F.col("away_fifa_rank").cast("int").alias("away_fifa_rank"),
            F.col("home_elo").cast("double").alias("home_elo"),
            F.col("away_elo").cast("double").alias("away_elo"),
            F.col("home_squad_avg_value_eur").cast("double").alias("home_squad_avg_value_eur"),
            F.col("away_squad_avg_value_eur").cast("double").alias("away_squad_avg_value_eur"),
            F.col("home_prev_avg_xg_scored").cast("double").alias("home_prev_avg_xg_scored"),
            F.col("away_prev_avg_xg_scored").cast("double").alias("away_prev_avg_xg_scored"),
            F.col("home_rest_days").cast("int").alias("home_rest_days"),
            F.col("away_rest_days").cast("int").alias("away_rest_days"),
        )
    )


@dlt.table(
    name="prediction",
    comment="Onside Arena's actual pre-match probabilities. Joined to match by team CODE (predictions has no shared match id with the other 3 sources) + kickoff date. FK: match, team.",
    table_properties={"quality": "silver"},
)
def prediction():
    preds = spark.table(_b(PRED, "pre_match_prediction")).select(
        F.col("home_code").alias("_home_code"),
        F.col("away_code").alias("_away_code"),
        F.to_date(F.col("kickoff_utc")).alias("match_date"),
        F.col("model_home_pct").cast("double").alias("home_win_probability"),
        F.col("model_draw_pct").cast("double").alias("draw_probability"),
        F.col("model_away_pct").cast("double").alias("away_win_probability"),
    )
    team_map = spark.table(_b(REL, "teams")).select(F.col("fifa_code").alias("_code"), F.col("team_name").alias("_name"))
    base = (
        preds
        .join(team_map.withColumnRenamed("_code", "_home_code").withColumnRenamed("_name", "_home_name"), "_home_code", "left")
        .join(team_map.withColumnRenamed("_code", "_away_code").withColumnRenamed("_name", "_away_name"), "_away_code", "left")
    )
    return (
        base
        .withColumn("match_id", _sk(F.col("_home_name"), F.col("_away_name"), F.col("match_date")))
        .withColumn("home_team_id", _sk(F.col("_home_name")))
        .withColumn("away_team_id", _sk(F.col("_away_name")))
        .select(
            "match_id", "home_team_id", "away_team_id",
            "home_win_probability", "draw_probability", "away_win_probability",
        )
    )
