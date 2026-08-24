-- Column-level business descriptions for the 16 Gold tables (selective —
-- skips plain surrogate PK/FK columns, covers metrics/statuses/business fields).
--
-- WHY THIS FILE EXISTS: confirmed by actually deploying that PySpark's
-- .alias(col, metadata={'comment': ...}) in gold.py's _c() helper does NOT
-- propagate to a Unity Catalog column comment for a Lakeflow Declarative
-- Pipeline materialized view (tested with an incremental run AND a
-- --full-refresh-all; comments stayed null both times, even though the
-- table-level @dlt.table(comment=...) persists fine). COMMENT ON COLUMN is
-- the mechanism that actually works, same as semantic/comments.sql.
--
-- DURABILITY: a normal `databricks bundle run gold_pipeline` (incremental)
-- preserves these comments (confirmed). A `--full-refresh-all` recreates the
-- tables and wipes them (confirmed) — re-run this script after any full
-- refresh of the gold_pipeline.

-- ============================================================
-- dim_tournament
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_tournament.name IS 'Tournament''s display name (e.g. ''2026 FIFA World Cup'').';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_tournament.year IS 'Year the tournament was held.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_tournament.host_country_name IS 'Name of the hosting country/countries.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_tournament.count_teams IS 'Number of teams that competed in this tournament (e.g. 48 for 2026).';

-- ============================================================
-- dim_stage
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_stage.name IS 'Stage name (e.g. ''Group Stage'', ''Round of 32'', ''Final''). Determines stakes and how a level score is resolved.';

-- ============================================================
-- dim_group
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_group.name IS 'Group label (e.g. ''Group A''). Meaningful only for group-stage matches.';

-- ============================================================
-- dim_team
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_team.name IS 'Team''s display name.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_team.code IS 'Team''s short code (e.g. FIFA 3-letter abbreviation).';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_team.confederation_name IS 'The team''s continental confederation (e.g. UEFA, CONMEBOL). Use for opponent-strength or region-level rollups.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_team.manager_name IS 'Name of the team''s manager/head coach at the time of the tournament.';

-- ============================================================
-- dim_player
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_player.name IS 'Player''s display name.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_player.date_of_birth IS 'Player''s date of birth (YYYY-MM-DD). Use to compute age; contextual for aerial ability / experience profiling.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_player.position IS 'Player''s nominal position (GK, defender, midfielder, forward). Compare within position; cross-position comparisons need role-aware metrics.';

-- ============================================================
-- dim_venue
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_venue.name IS 'Stadium''s display name.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_venue.city IS 'City the stadium is located in.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_venue.country_code IS 'ISO country code the stadium is located in.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_venue.capacity IS 'Stadium seating capacity. Atmosphere/scale context.';

-- ============================================================
-- fact_match
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match.winner_team_id IS 'Team that progressed/won. Null on a group-stage draw. For matches decided by a shootout, reflects who advanced, not the 90-minute (open-play) result — don''t use this alone to judge which team played better.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match.match_date IS 'Calendar date the match was played.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match.home_score IS 'Home team''s goals at full time (after extra time for knockouts).';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match.away_score IS 'Away team''s goals at full time (after extra time for knockouts).';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match.home_xg IS 'Home team''s Expected Goals in this match — sum of scoring probabilities of its chances. Measures chance quality created, not goals actually scored.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match.away_xg IS 'Away team''s Expected Goals in this match.';

-- ============================================================
-- fact_team_match
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.home_away IS 'Whether this team played at home or away in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.result IS 'Match outcome from this team''s perspective: W, D, or L. For knockout matches decided by penalties, reflects who advanced, not the 90-minute scoreline.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.points IS 'Points earned from this match (3 for a win, 1 for a draw, 0 for a loss).';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.goals_for IS 'Goals scored by this team in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.goals_against IS 'Goals conceded by this team in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.xg_for IS 'This team''s Expected Goals generated in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.xg_against IS 'This team''s Expected Goals conceded in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.possession_pct IS 'Share of the ball this team held in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.shots IS 'Shot attempts by this team in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.shots_on_target IS 'Attempts on the goal frame by this team in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.corners IS 'Corner kicks won by this team in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.fouls IS 'Fouls committed by this team in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.offsides IS 'Offsides called against this team in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.pressures IS 'Pressing actions applied by this team in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.recoveries IS 'Ball recoveries by this team in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.aerial_duels IS 'Aerial contests won by this team in this match.';

-- ============================================================
-- fact_player_match
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.started IS 'Whether the player was in the starting lineup for this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.position_played IS 'The position this player actually played in this specific match — may differ from their nominal position in dim_player.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.goals IS 'Goals scored by this player in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.shots IS 'Shot attempts by this player in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.passes IS 'Passes attempted by this player in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.passes_completed IS 'Passes completed by this player in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.progressive_passes IS 'Passes that moved the ball meaningfully toward the opponent''s goal. Better proxy for attacking intent than raw pass count.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.recoveries IS 'Ball recoveries by this player in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.duels IS 'Total duels contested by this player in this match (not split by won/lost).';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.distance_covered_km IS 'Distance covered by this player in the match, in kilometers.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.top_speed_kmh IS 'Player''s peak recorded speed in the match, in km/h.';

-- ============================================================
-- fact_goalkeeper_match
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_goalkeeper_match.shots_faced IS 'Shots faced by this goalkeeper in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_goalkeeper_match.saves IS 'Saves made by this goalkeeper in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_goalkeeper_match.save_pct IS 'saves divided by shots_faced for this match. A high number of saves can also just mean a busy match, not necessarily a strong defence in front of the keeper.';

-- ============================================================
-- fact_pass
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_pass.passes IS 'Number of passes from passer_id to receiver_id in this match. Directed edge weight for GraphFrames passing-network analysis.';

-- ============================================================
-- fact_match_event
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match_event.event_type IS 'Category of the event: goal, assist, card (yellow/red), substitution, or VAR review.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match_event.minute IS 'Match minute the event occurred.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match_event.detail IS 'Free-text detail about the event (e.g. card reason, substitution note).';

-- ============================================================
-- fact_prediction
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_prediction.home_win_probability IS 'Onside Arena''s pre-match probability the home team wins.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_prediction.draw_probability IS 'Onside Arena''s pre-match probability of a draw.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_prediction.away_win_probability IS 'Onside Arena''s pre-match probability the away team wins.';

-- ============================================================
-- fact_team_tournament
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.matches IS 'Total matches played in the tournament.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.wins IS 'Matches won.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.draws IS 'Matches drawn.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.losses IS 'Matches lost.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.goals_for IS 'Total goals scored.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.goals_against IS 'Total goals conceded.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.points IS 'Total points earned across the tournament (3 per win, 1 per draw).';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.xg IS 'Total Expected Goals generated across the tournament.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.xga IS 'Total Expected Goals conceded across the tournament.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.possession IS 'Average share of the ball held per match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.shots IS 'Total shot attempts.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.shots_on_target IS 'Total attempts on the goal frame.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.pressures IS 'Total pressing actions applied.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.final_rank IS 'Final placement in the tournament (1 = champion). An outcome measure, not a performance measure — a team can rank high on luck (penalties, favorable draw) as much as on xG dominance.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.host_performance IS 'Host-nation performance context, populated only for the tournament host team.';

-- ============================================================
-- fact_team_dominance
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_dominance.results_score IS '0-1 min-max normalized points total, normalized within the tournament (1.0 = most points of any team that year). Component of performance_dominance_score at weight 0.30.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_dominance.xg_differential_score IS '0-1 min-max normalized (xG for minus xG against), normalized within the tournament. Component of performance_dominance_score at weight 0.25.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_dominance.shot_quality_score IS '0-1 min-max normalized (xG per shot), normalized within the tournament. Component of performance_dominance_score at weight 0.15.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_dominance.chance_creation_score IS '0-1 min-max normalized (xG per match), normalized within the tournament. Component of performance_dominance_score at weight 0.15.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_dominance.defensive_efficiency_score IS '0-1 min-max normalized AND INVERTED (goals against per xG against), normalized within the tournament — conceding fewer goals than xG against predicts scores HIGH here, not low. Component of performance_dominance_score at weight 0.15.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_dominance.performance_dominance_score IS 'The governed WorldCupIQ headline metric: 0.30*results + 0.25*xG differential + 0.15*shot quality + 0.15*chance creation + 0.15*defensive efficiency. Weights are fixed in this pipeline, not editable downstream. The `best_team` context policy (README Section 9) resolves to the highest value of this measure, ties broken by final_rank.';

-- ============================================================
-- fact_group_standing
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_group_standing.played IS 'Matches played in the group stage.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_group_standing.won IS 'Matches won in the group stage.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_group_standing.drawn IS 'Matches drawn in the group stage.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_group_standing.lost IS 'Matches lost in the group stage.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_group_standing.goals_for IS 'Goals scored in the group stage.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_group_standing.goals_against IS 'Goals conceded in the group stage.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_group_standing.points IS 'Points earned in the group stage.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_group_standing.rank IS 'Final standing within the group (1 = group winner).';

