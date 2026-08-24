# Lakeflow Declarative Pipeline source file
#
# Gold layer — star schema (6 dims + 10 facts) built from the 27-table Silver
# model, reviewed in architecture.html's Gold Model tab. 9 of Silver's 27
# tables aren't carried forward here (qualified_team, squad_member, referee,
# referee_assignment, manager, manager_appointment, award, award_winner,
# match_prediction_feature) — none needed by the 5 demo questions at BI grain;
# they stay queryable directly against Silver. See the Gold Model tab's
# Silver -> Gold coverage table for the full per-table mapping/reasoning.
#
# fact_team_match's W/D/L/points/goals_for/goals_against/xg_for/xg_against are
# derived here (not stored on silver.match_team) by splitting silver.match into
# a home-perspective and away-perspective row per match — the same logic
# validated by hand as ad-hoc SQL before this pipeline was built. Win/loss on a
# level score falls back to the penalty shootout score, which
# silver.match.winner_team_id does NOT do (a known Silver limitation noted when
# that table was built).
#
# fact_team_tournament aggregates fact_team_match (built earlier in this same
# pipeline) rather than re-deriving from silver.match, so the two facts can't
# drift apart. fact_team_dominance normalizes fact_team_tournament's raw
# aggregates into README Section 7's governed Performance Dominance Score —
# min-max normalized per tournament, weights 0.30/0.25/0.15/0.15/0.15, same
# formula validated as ad-hoc SQL (Query 3) before this pipeline existed.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# catalog/schema_prefix passed via pipeline `configuration` in databricks.yml
# (${var.catalog} / ${var.schema_prefix}) — defaults match the original
# mrlc-catalog convention so this file still works without that config set.
CATALOG = spark.conf.get("catalog", "mrlc-catalog")
_SCHEMA_PREFIX = spark.conf.get("schema_prefix", "fifaworldcup_")
SILVER = f"{_SCHEMA_PREFIX}silver"


def _s(table):
    return f"`{CATALOG}`.`{SILVER}`.`{table}`"


def _minmax_norm(raw_col, part_col, invert=False):
    # Min-max normalize raw_col to [0,1] within each partition (tournament).
    # invert=True flips the scale for "lower raw value is better" metrics
    # (defensive leakiness: fewer goals conceded per unit of xGA is better).
    w = Window.partitionBy(part_col)
    mn, mx = F.min(raw_col).over(w), F.max(raw_col).over(w)
    span = mx - mn
    norm = F.when(span == 0, F.lit(0.0)).otherwise((F.col(raw_col) - mn) / span)
    return (F.lit(1.0) - norm) if invert else norm


def _c(col, comment):
    # Source-level documentation only — confirmed by actually deploying that
    # PySpark's alias(metadata={"comment": ...}) does NOT propagate to a
    # Unity Catalog column comment for a Lakeflow Declarative Pipeline
    # materialized view (tested with both an incremental run and a
    # --full-refresh-all; column comments stayed null both times, even
    # though the table-level @dlt.table(comment=...) does persist). The
    # real UC column comments for these tables are applied post-deploy via
    # COMMENT ON COLUMN in ingestion/lakeflow/gold_comments.sql instead,
    # same pattern as the semantic layer's comments.sql. Kept here so the
    # description lives next to the column it documents, for humans reading
    # this file — but re-run gold_comments.sql after any --full-refresh-all
    # of this pipeline, since that recreates the tables and wipes the
    # UC-side comments (confirmed; a plain incremental run does not).
    return F.col(col).alias(col, metadata={"comment": comment})


# ================================================================
# Dimensions (6)
# ================================================================

@dlt.table(name="dim_tournament", comment="One row per tournament.", table_properties={"quality": "gold"})
def dim_tournament():
    return spark.table(_s("tournament")).select(
        "tournament_id",
        _c("name", "Tournament's display name (e.g. '2026 FIFA World Cup')."),
        _c("year", "Year the tournament was held."),
        _c("host_country_name", "Name of the hosting country/countries."),
        _c("count_teams", "Number of teams that competed in this tournament (e.g. 48 for 2026)."),
    )


@dlt.table(name="dim_stage", comment="Stage per tournament. FK: tournament.", table_properties={"quality": "gold"})
def dim_stage():
    return spark.table(_s("stage")).select(
        "stage_id", "tournament_id",
        _c("name", "Stage name (e.g. 'Group Stage', 'Round of 32', 'Final'). Determines stakes and how a level score is resolved."),
    )


@dlt.table(name="dim_group", comment="Group per tournament stage. FK: tournament, stage.", table_properties={"quality": "gold"})
def dim_group():
    return spark.table(_s("tournament_group")).select(
        "group_id", "tournament_id", "stage_id",
        _c("name", "Group label (e.g. 'Group A'). Meaningful only for group-stage matches."),
    )


@dlt.table(
    name="dim_team",
    comment="One row per team — silver.team + silver.confederation denormalized in (no separate dim_confederation).",
    table_properties={"quality": "gold"},
)
def dim_team():
    team = spark.table(_s("team"))
    confed = spark.table(_s("confederation")).select(
        F.col("confederation_id").alias("_cid"),
        F.col("name").alias("confederation_name"),
    )
    # No country_code here — silver.team never actually carries one (no clean
    # source column on relational.teams; only fifa_code/confederation exist).
    return (
        team.join(confed, team["confederation_id"] == confed["_cid"], "left")
        .select(
            team["team_id"],
            _c("name", "Team's display name."),
            _c("code", "Team's short code (e.g. FIFA 3-letter abbreviation)."),
            _c("confederation_name", "The team's continental confederation (e.g. UEFA, CONMEBOL). Use for opponent-strength or region-level rollups."),
            _c("manager_name", "Name of the team's manager/head coach at the time of the tournament."),
        )
    )


@dlt.table(name="dim_player", comment="One row per player.", table_properties={"quality": "gold"})
def dim_player():
    return spark.table(_s("player")).select(
        "player_id",
        _c("name", "Player's display name."),
        _c("date_of_birth", "Player's date of birth (YYYY-MM-DD). Use to compute age; contextual for aerial ability / experience profiling."),
        _c("position", "Player's nominal position (GK, defender, midfielder, forward). Compare within position; cross-position comparisons need role-aware metrics."),
    )


@dlt.table(name="dim_venue", comment="One row per stadium.", table_properties={"quality": "gold"})
def dim_venue():
    return spark.table(_s("venue")).select(
        "venue_id",
        _c("name", "Stadium's display name."),
        _c("city", "City the stadium is located in."),
        _c("country_code", "ISO country code the stadium is located in."),
        _c("capacity", "Stadium seating capacity. Atmosphere/scale context."),
    )


# ================================================================
# Match-grain facts (7)
# ================================================================

@dlt.table(
    name="fact_match",
    comment="One row per match. group_id is derived (joined via the home team's tournament group — null for knockout matches). FK: tournament, stage, group, venue, home/away/winner team.",
    table_properties={"quality": "gold"},
)
def fact_match():
    m = spark.table(_s("match"))
    tt = spark.table(_s("team_tournament")).select(
        F.col("team_id").alias("_tt_team"),
        F.col("tournament_id").alias("_tt_tournament"),
        F.col("group_id"),
    )
    return (
        m.join(
            tt,
            (m["home_team_id"] == tt["_tt_team"]) & (m["tournament_id"] == tt["_tt_tournament"]),
            "left",
        )
        .select(
            "match_id", "tournament_id", "stage_id", "group_id", "venue_id",
            "home_team_id", "away_team_id",
            _c("winner_team_id", "Team that progressed/won. Null on a group-stage draw. For matches decided by a shootout, reflects who advanced, not the 90-minute (open-play) result — don't use this alone to judge which team played better."),
            _c("match_date", "Calendar date the match was played."),
            _c("home_score", "Home team's goals at full time (after extra time for knockouts)."),
            _c("away_score", "Away team's goals at full time (after extra time for knockouts)."),
            _c("home_xg", "Home team's Expected Goals in this match — sum of scoring probabilities of its chances. Measures chance quality created, not goals actually scored."),
            _c("away_xg", "Away team's Expected Goals in this match."),
        )
    )


@dlt.table(
    name="fact_team_match",
    comment="Team-per-match — one row per team per match, home and away perspective both included. result/points/goals/xg are derived by splitting silver.match; the rest comes from silver.match_team. FK: match, team.",
    table_properties={"quality": "gold"},
)
def fact_team_match():
    m = spark.table(_s("match"))

    def _result_expr(for_col, against_col, for_pen, against_pen):
        return (
            F.when(F.col(for_col) > F.col(against_col), F.lit("W"))
            .when(F.col(for_col) < F.col(against_col), F.lit("L"))
            .when(
                F.col(for_pen).isNotNull() & F.col(against_pen).isNotNull(),
                F.when(F.col(for_pen) > F.col(against_pen), F.lit("W")).otherwise(F.lit("L")),
            )
            .otherwise(F.lit("D"))
        )

    home = m.select(
        F.col("match_id"),
        F.col("home_team_id").alias("team_id"),
        F.lit("HOME").alias("home_away"),
        F.col("home_score").alias("goals_for"),
        F.col("away_score").alias("goals_against"),
        F.col("home_xg").alias("xg_for"),
        F.col("away_xg").alias("xg_against"),
        _result_expr("home_score", "away_score", "home_penalties", "away_penalties").alias("result"),
    )
    away = m.select(
        F.col("match_id"),
        F.col("away_team_id").alias("team_id"),
        F.lit("AWAY").alias("home_away"),
        F.col("away_score").alias("goals_for"),
        F.col("home_score").alias("goals_against"),
        F.col("away_xg").alias("xg_for"),
        F.col("home_xg").alias("xg_against"),
        _result_expr("away_score", "home_score", "away_penalties", "home_penalties").alias("result"),
    )
    team_match = (
        home.unionByName(away)
        .withColumn(
            "points",
            F.when(F.col("result") == "W", 3).when(F.col("result") == "D", 1).otherwise(0),
        )
    )

    # Drop match_team's own home_away — team_match already derived one from
    # silver.match directly, and both sides having a column of that name past
    # the ["match_id","team_id"] join-key list is an ambiguous reference.
    mt = spark.table(_s("match_team")).drop("home_away")
    return (
        team_match.join(mt, ["match_id", "team_id"], "left")
        .withColumn("match_team_id", F.xxhash64(F.col("match_id"), F.col("team_id")))
        .select(
            "match_team_id", "match_id", "team_id",
            _c("home_away", "Whether this team played at home or away in this match."),
            _c("result", "Match outcome from this team's perspective: W, D, or L. For knockout matches decided by penalties, reflects who advanced, not the 90-minute scoreline."),
            _c("points", "Points earned from this match (3 for a win, 1 for a draw, 0 for a loss)."),
            _c("goals_for", "Goals scored by this team in this match."),
            _c("goals_against", "Goals conceded by this team in this match."),
            _c("xg_for", "This team's Expected Goals generated in this match."),
            _c("xg_against", "This team's Expected Goals conceded in this match."),
            _c("possession_pct", "Share of the ball this team held in this match."),
            _c("shots", "Shot attempts by this team in this match."),
            _c("shots_on_target", "Attempts on the goal frame by this team in this match."),
            _c("corners", "Corner kicks won by this team in this match."),
            _c("fouls", "Fouls committed by this team in this match."),
            _c("offsides", "Offsides called against this team in this match."),
            _c("pressures", "Pressing actions applied by this team in this match."),
            _c("recoveries", "Ball recoveries by this team in this match."),
            _c("aerial_duels", "Aerial contests won by this team in this match."),
        )
    )


@dlt.table(
    name="fact_player_match",
    comment="Player-per-match — silver.match_player + silver.match_player_physical merged in. FK: match, team, player.",
    table_properties={"quality": "gold"},
)
def fact_player_match():
    mp = spark.table(_s("match_player"))
    phys = spark.table(_s("match_player_physical"))
    return (
        mp.join(phys, "match_player_id", "left")
        .select(
            "match_player_id", "match_id", "team_id", "player_id",
            _c("started", "Whether the player was in the starting lineup for this match."),
            _c("position_played", "The position this player actually played in this specific match — may differ from their nominal position in dim_player."),
            _c("goals", "Goals scored by this player in this match."),
            _c("shots", "Shot attempts by this player in this match."),
            _c("passes", "Passes attempted by this player in this match."),
            _c("passes_completed", "Passes completed by this player in this match."),
            _c("progressive_passes", "Passes that moved the ball meaningfully toward the opponent's goal. Better proxy for attacking intent than raw pass count."),
            _c("recoveries", "Ball recoveries by this player in this match."),
            _c("duels", "Total duels contested by this player in this match (not split by won/lost)."),
            _c("distance_covered_km", "Distance covered by this player in the match, in kilometers."),
            _c("top_speed_kmh", "Player's peak recorded speed in the match, in km/h."),
        )
    )


@dlt.table(
    name="fact_goalkeeper_match",
    comment="Goalkeeper-per-match. FK: match, team, player.",
    table_properties={"quality": "gold"},
)
def fact_goalkeeper_match():
    return spark.table(_s("match_goalkeeper")).select(
        "match_goalkeeper_id", "match_id", "team_id", "player_id",
        _c("shots_faced", "Shots faced by this goalkeeper in this match."),
        _c("saves", "Saves made by this goalkeeper in this match."),
        _c("save_pct", "saves divided by shots_faced for this match. A high number of saves can also just mean a busy match, not necessarily a strong defence in front of the keeper."),
    )


@dlt.table(
    name="fact_pass",
    comment="Directed passer->receiver edge per match (GraphFrames source). Composite key. FK: match, team, player x2. No xt_added — silver.passing_edge never actually computed it (expected-threat modeling was never built, just documented as an aspiration during the Silver review).",
    table_properties={"quality": "gold"},
)
def fact_pass():
    return spark.table(_s("passing_edge")).select(
        "match_id", "team_id", "passer_id", "receiver_id",
        _c("passes", "Number of passes from passer_id to receiver_id in this match. Directed edge weight for GraphFrames passing-network analysis."),
    )


@dlt.table(
    name="fact_match_event",
    comment="One row per event, every tournament — merges the original blueprint's fact_goal/fact_shot/fact_card/fact_substitution. FK: match, team, player x2.",
    table_properties={"quality": "gold"},
)
def fact_match_event():
    return spark.table(_s("match_event")).select(
        "event_id", "match_id", "team_id", "player_id", "secondary_player_id",
        _c("event_type", "Category of the event: goal, assist, card (yellow/red), substitution, or VAR review."),
        _c("minute", "Match minute the event occurred."),
        _c("detail", "Free-text detail about the event (e.g. card reason, substitution note)."),
    )


@dlt.table(
    name="fact_prediction",
    comment="Onside Arena's pre-match probabilities — feeds the prediction/surprise-analysis demo beat. FK: match, home/away team.",
    table_properties={"quality": "gold"},
)
def fact_prediction():
    return spark.table(_s("prediction")).select(
        "match_id", "home_team_id", "away_team_id",
        _c("home_win_probability", "Onside Arena's pre-match probability the home team wins."),
        _c("draw_probability", "Onside Arena's pre-match probability of a draw."),
        _c("away_win_probability", "Onside Arena's pre-match probability the away team wins."),
    )


# ================================================================
# Tournament & group-grain facts (3)
# ================================================================

@dlt.table(
    name="fact_team_tournament",
    comment="The semantic-layer foundation — aggregates fact_team_match (built earlier in this pipeline) + silver.team_tournament (group) + silver.tournament_standing (final rank, host performance). FK: tournament, team, group.",
    table_properties={"quality": "gold"},
)
def fact_team_tournament():
    tm = dlt.read("fact_team_match")
    match_tid = spark.table(_s("match")).select(
        F.col("match_id").alias("_m_id"), F.col("tournament_id")
    )
    tm = tm.join(match_tid, tm["match_id"] == match_tid["_m_id"]).drop("_m_id")

    agg = tm.groupBy("tournament_id", "team_id").agg(
        F.count("*").alias("matches"),
        F.sum(F.when(F.col("result") == "W", 1).otherwise(0)).alias("wins"),
        F.sum(F.when(F.col("result") == "D", 1).otherwise(0)).alias("draws"),
        F.sum(F.when(F.col("result") == "L", 1).otherwise(0)).alias("losses"),
        F.sum("goals_for").alias("goals_for"),
        F.sum("goals_against").alias("goals_against"),
        F.sum("points").alias("points"),
        F.sum("xg_for").alias("xg"),
        F.sum("xg_against").alias("xga"),
        F.avg("possession_pct").alias("possession"),
        F.sum("shots").alias("shots"),
        F.sum("shots_on_target").alias("shots_on_target"),
        F.sum("pressures").alias("pressures"),
    )

    tt = spark.table(_s("team_tournament")).select("tournament_id", "team_id", "group_id")
    ts = spark.table(_s("tournament_standing")).select("tournament_id", "team_id", "final_rank", "host_performance")

    return (
        agg.join(tt, ["tournament_id", "team_id"], "left")
        .join(ts, ["tournament_id", "team_id"], "left")
        .withColumn("team_tournament_id", F.xxhash64(F.col("tournament_id"), F.col("team_id")))
        .select(
            "team_tournament_id", "tournament_id", "team_id", "group_id",
            _c("matches", "Total matches played in the tournament."),
            _c("wins", "Matches won."),
            _c("draws", "Matches drawn."),
            _c("losses", "Matches lost."),
            _c("goals_for", "Total goals scored."),
            _c("goals_against", "Total goals conceded."),
            _c("points", "Total points earned across the tournament (3 per win, 1 per draw)."),
            _c("xg", "Total Expected Goals generated across the tournament."),
            _c("xga", "Total Expected Goals conceded across the tournament."),
            _c("possession", "Average share of the ball held per match."),
            _c("shots", "Total shot attempts."),
            _c("shots_on_target", "Total attempts on the goal frame."),
            _c("pressures", "Total pressing actions applied."),
            _c("final_rank", "Final placement in the tournament (1 = champion). An outcome measure, not a performance measure — a team can rank high on luck (penalties, favorable draw) as much as on xG dominance."),
            _c("host_performance", "Host-nation performance context, populated only for the tournament host team."),
        )
    )


@dlt.table(
    name="fact_team_dominance",
    comment="README Section 7's governed Performance Dominance Score — min-max normalized components of fact_team_tournament, per tournament. Weights (0.30/0.25/0.15/0.15/0.15) live only here. 1:1 with fact_team_tournament.",
    table_properties={"quality": "gold"},
)
def fact_team_dominance():
    ftt = dlt.read("fact_team_tournament")

    comp = ftt.select(
        "tournament_id", "team_id",
        F.col("points").alias("results_raw"),
        (F.col("xg") - F.col("xga")).alias("xg_diff_raw"),
        F.when(F.col("shots") > 0, F.col("xg") / F.col("shots")).otherwise(F.lit(0.0)).alias("shot_quality_raw"),
        F.when(F.col("matches") > 0, F.col("xg") / F.col("matches")).otherwise(F.lit(0.0)).alias("chance_creation_raw"),
        F.when(F.col("xga") > 0, F.col("goals_against") / F.col("xga")).otherwise(F.lit(0.0)).alias("defensive_leak_raw"),
    )

    normed = (
        comp
        .withColumn("results_score", _minmax_norm("results_raw", "tournament_id"))
        .withColumn("xg_differential_score", _minmax_norm("xg_diff_raw", "tournament_id"))
        .withColumn("shot_quality_score", _minmax_norm("shot_quality_raw", "tournament_id"))
        .withColumn("chance_creation_score", _minmax_norm("chance_creation_raw", "tournament_id"))
        .withColumn("defensive_efficiency_score", _minmax_norm("defensive_leak_raw", "tournament_id", invert=True))
    )

    return (
        normed
        .withColumn(
            "performance_dominance_score",
            0.30 * F.col("results_score")
            + 0.25 * F.col("xg_differential_score")
            + 0.15 * F.col("shot_quality_score")
            + 0.15 * F.col("chance_creation_score")
            + 0.15 * F.col("defensive_efficiency_score"),
        )
        .select(
            "tournament_id", "team_id",
            _c("results_score", "0-1 min-max normalized points total, normalized within the tournament (1.0 = most points of any team that year). Component of performance_dominance_score at weight 0.30."),
            _c("xg_differential_score", "0-1 min-max normalized (xG for minus xG against), normalized within the tournament. Component of performance_dominance_score at weight 0.25."),
            _c("shot_quality_score", "0-1 min-max normalized (xG per shot), normalized within the tournament. Component of performance_dominance_score at weight 0.15."),
            _c("chance_creation_score", "0-1 min-max normalized (xG per match), normalized within the tournament. Component of performance_dominance_score at weight 0.15."),
            _c("defensive_efficiency_score", "0-1 min-max normalized AND INVERTED (goals against per xG against), normalized within the tournament — conceding fewer goals than xG against predicts scores HIGH here, not low. Component of performance_dominance_score at weight 0.15."),
            _c("performance_dominance_score", "The governed WorldCupIQ headline metric: 0.30*results + 0.25*xG differential + 0.15*shot quality + 0.15*chance creation + 0.15*defensive efficiency. Weights are fixed in this pipeline, not editable downstream. The `best_team` context policy (README Section 9) resolves to the highest value of this measure, ties broken by final_rank."),
        )
    )


@dlt.table(
    name="fact_group_standing",
    comment="Group-stage table row per team. FK: group, team.",
    table_properties={"quality": "gold"},
)
def fact_group_standing():
    return spark.table(_s("group_standing")).select(
        "group_standing_id", "group_id", "team_id",
        _c("played", "Matches played in the group stage."),
        _c("won", "Matches won in the group stage."),
        _c("drawn", "Matches drawn in the group stage."),
        _c("lost", "Matches lost in the group stage."),
        _c("goals_for", "Goals scored in the group stage."),
        _c("goals_against", "Goals conceded in the group stage."),
        _c("points", "Points earned in the group stage."),
        _c("rank", "Final standing within the group (1 = group winner)."),
    )
