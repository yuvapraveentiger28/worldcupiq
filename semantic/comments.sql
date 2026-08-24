-- Column-level business descriptions for the 5 deployed Metric Views.
--
-- WHY THIS FILE EXISTS: Unity Catalog Metric View YAML (com.databricks.sql.serde.v10)
-- only accepts {name, expr, window} per dimension/measure — a `comment:` key is
-- rejected with METRIC_VIEW_INVALID_VIEW_DEFINITION (confirmed by actually
-- deploying one). COMMENT ON COLUMN is the only mechanism this platform version
-- supports for field-level descriptions on a Metric View.
--
-- IMPORTANT — RE-RUN THIS AFTER EVERY REDEPLOY: CREATE OR REPLACE VIEW wipes
-- column comments (confirmed: redeployed team_performance unchanged, the
-- `points` comment came back NULL). Any time you run
-- CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML on one of these 5
-- views, re-run this script's block for that view immediately after.
--
-- Style follows docs/WorldCupIQ_Context_4_Field_and_Metric_Glossary sample:
-- definition + an interpretation note where the field is easy to misread.

-- ================================================================
-- Table-level descriptions (also wiped by CREATE OR REPLACE VIEW,
-- re-apply alongside the column comments below)
-- ================================================================
COMMENT ON TABLE `worldcupiq_catalog`.semantic.team_performance IS 'Governed team-tournament vocabulary. Grain: team x tournament (one row per team per World Cup).';
COMMENT ON TABLE `worldcupiq_catalog`.semantic.team_dominance IS 'The governed Performance Dominance Score. Grain: team x tournament, 1:1 with gold.fact_team_dominance. The best_team context policy resolves through this view, not the raw gold table, so the agent''s answer stays traceable through UC lineage.';
COMMENT ON TABLE `worldcupiq_catalog`.semantic.match_performance IS 'Governed team-per-match vocabulary. Grain: team x match (two rows per match, home + away perspective).';
COMMENT ON TABLE `worldcupiq_catalog`.semantic.player_match_performance IS 'Governed outfield-player-per-match vocabulary. Grain: player x match.';
COMMENT ON TABLE `worldcupiq_catalog`.semantic.goalkeeper_match_performance IS 'Governed goalkeeper-per-match vocabulary. Grain: goalkeeper (player) x match.';

-- ================================================================
-- team_performance — team x tournament
-- ================================================================
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.team_id IS 'Surrogate key for the team, hashed from the team name in gold.dim_team. Join key, not a display value.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.team_name IS "Team's display name.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.team_code IS "Team's short code (e.g. FIFA 3-letter abbreviation).";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.confederation_name IS "The team's continental confederation (e.g. UEFA, CONMEBOL). Use for opponent-strength or region-level rollups.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.tournament_id IS 'Surrogate key for the tournament (one World Cup edition).';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.tournament_name IS "Tournament's display name.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.tournament_year IS 'Year the tournament was held.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.group_id IS "Surrogate key for the team's group in that tournament's group stage.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.final_rank IS 'Final placement in the tournament (1 = champion). An outcome measure, not a performance measure — a team can rank high on luck (penalties, favorable draw) as much as on xG dominance.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.host_performance IS 'Host-nation performance context, populated only for the tournament host team.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.matches IS 'Total matches played in the tournament.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.wins IS 'Matches won.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.draws IS 'Matches drawn.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.losses IS 'Matches lost.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.points IS 'Total points earned across the tournament (3 per win, 1 per draw).';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.goals_for IS 'Total goals scored.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.goals_against IS 'Total goals conceded.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.goal_difference IS 'goals_for minus goals_against. Standard tie-break metric.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.xg_for IS "Total Expected Goals generated — sum of scoring probabilities of the team's own chances. Measures chance quality created, not goals; a team can win while out-xG'd.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.xg_against IS 'Total Expected Goals conceded — quality of chances given up. Low values indicate good chance prevention, separate from actual goals conceded.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.xg_differential IS 'xg_for minus xg_against. Persistently above 0 signals a team controlling games even when results are close.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.shot_conversion IS 'goals_for divided by shots. Finishing efficiency — noisy over one tournament; read alongside shot_quality.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.shot_quality IS 'xg_for divided by shots. High values mean the team creates fewer, better chances rather than shooting from anywhere.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.defensive_efficiency IS 'goals_against divided by xg_against. Below 1.0 means conceding fewer goals than the chances allowed would predict (strong keeping); above 1.0 means the opposite.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.chance_creation IS 'xg_for divided by matches. Attacking output rate, independent of how many matches the team played — fairer across teams eliminated early vs. deep runs.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.possession_pct IS 'Average share of the ball held per match. High possession does not by itself mean dominance — read alongside xg_differential and shot_quality.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.shots IS 'Total shot attempts.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.shots_on_target IS 'Total attempts on the goal frame.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.shot_accuracy IS 'shots_on_target divided by shots.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_performance.pressures IS 'Total pressing actions applied. Proxy for defensive workrate — interpret relative to possession_pct, since a low-possession team naturally presses more.';

-- ================================================================
-- team_dominance — team x tournament (governed Performance Dominance Score)
-- ================================================================
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_dominance.team_id IS 'Surrogate key for the team, hashed from the team name in gold.dim_team. Join key, not a display value.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_dominance.team_name IS "Team's display name.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_dominance.tournament_id IS 'Surrogate key for the tournament. All scores on this row are min-max normalized within this tournament only — never compare raw component scores across tournament_id values without checking tournament_year.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_dominance.tournament_name IS "Tournament's display name.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_dominance.tournament_year IS "Year the tournament was held. A 0.7 in 2022 and a 0.7 in 2026 both mean 'top of that year's field', not the same absolute performance level.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_dominance.results_score IS '0-1 min-max normalized points total, normalized within the tournament (1.0 = most points of any team that year). Component of performance_dominance_score at weight 0.30.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_dominance.xg_differential_score IS '0-1 min-max normalized (xG for minus xG against), normalized within the tournament. Measures sustained chance-quality control, not just results. Component of performance_dominance_score at weight 0.25.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_dominance.shot_quality_score IS '0-1 min-max normalized (xG per shot), normalized within the tournament. High values mean fewer, better chances rather than shooting from anywhere. Component of performance_dominance_score at weight 0.15.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_dominance.chance_creation_score IS '0-1 min-max normalized (xG per match), normalized within the tournament. Attacking output rate, independent of matches played. Component of performance_dominance_score at weight 0.15.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_dominance.defensive_efficiency_score IS '0-1 min-max normalized AND INVERTED (goals against per xG against), normalized within the tournament — a team conceding fewer goals than its xG against predicts scores HIGH here, not low. Component of performance_dominance_score at weight 0.15.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.team_dominance.performance_dominance_score IS "The governed WorldCupIQ headline metric: 0.30*results + 0.25*xG differential + 0.15*shot quality + 0.15*chance creation + 0.15*defensive efficiency. Weights are fixed in gold.py, not editable here. The `best_team` context policy (README Section 9) resolves to the highest value of this measure, ties broken by final_rank.";

-- ================================================================
-- match_performance — team x match
-- ================================================================
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.match_id IS 'Surrogate key for the match, hashed from home team + away team + match date.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.match_date IS 'Calendar date the match was played.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.team_id IS "Surrogate key for the team this row's stats belong to. Two rows per match — one per team.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.team_name IS "Team's display name.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.home_away IS "Whether this team played at home or away in this match.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.result IS "Match outcome from this team's perspective: W, D, or L. For knockout matches decided by penalties, reflects who advanced, not the 90-minute scoreline.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.tournament_id IS 'Raw surrogate-key ID for the tournament — NOT resolved to a display name in this view. UC Metric View joins are single-hop from source only, and tournament_id lives on gold.fact_match, one hop removed from this view''s source table. Resolve via gold.dim_tournament downstream.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.stage_id IS 'Raw surrogate-key ID for the tournament stage — not resolved to a display name in this view, same single-hop-join reason as tournament_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.venue_id IS 'Raw surrogate-key ID for the venue — not resolved to a display name in this view, same single-hop-join reason as tournament_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.points IS 'Points earned from this single match (3/1/0).';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.goals_for IS 'Goals scored in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.goals_against IS 'Goals conceded in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.goal_difference IS 'goals_for minus goals_against for this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.xg_for IS "This match's Expected Goals generated. A single match's xG is a signal, not a verdict — small-sample noise is much higher here than at tournament grain.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.xg_against IS "This match's Expected Goals conceded.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.xg_differential IS 'xg_for minus xg_against for this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.shot_conversion IS 'goals_for divided by shots, this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.shot_quality IS 'xg_for divided by shots, this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.shot_accuracy IS 'shots_on_target divided by shots, this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.possession_pct IS 'Share of the ball held in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.shots IS 'Shot attempts in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.shots_on_target IS 'Attempts on the goal frame in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.corners IS 'Corner kicks won in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.fouls IS 'Fouls committed in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.offsides IS 'Offsides called in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.pressures IS 'Pressing actions applied in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.recoveries IS 'Ball recoveries in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.match_performance.aerial_duels_won IS 'Aerial contests won in this match. Relevant to set-piece and long-ball styles.';

-- ================================================================
-- player_match_performance — player x match
-- ================================================================
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.match_id IS 'Surrogate key for the match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.match_date IS 'Calendar date the match was played.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.player_id IS "Surrogate key for the player, hashed from the player's name in gold.dim_player.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.player_name IS "Player's display name.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.position_played IS "The position this player actually played in this specific match — may differ from their nominal position in gold.dim_player.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.started IS 'Whether the player was in the starting lineup. Exclude low-minutes non-starters from rate-based comparisons (shot_conversion, pass_completion_pct) to avoid small-sample noise.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.team_id IS 'Surrogate key for the team the player represented in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.team_name IS "Team's display name.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.tournament_id IS 'Raw surrogate-key ID for the tournament — not resolved to a display name in this view (single-hop join constraint, same as match_performance). Resolve via gold.dim_tournament downstream.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.goals IS 'Goals scored in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.shots IS 'Shot attempts in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.shot_conversion IS 'goals divided by shots, this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.passes IS 'Passes attempted in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.passes_completed IS 'Passes completed in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.pass_completion_pct IS 'passes_completed divided by passes. Style indicator — very high completion can mean safe, sideways passing rather than incisive play; read alongside progressive_passes.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.progressive_passes IS "Passes that moved the ball meaningfully toward the opponent's goal. Better proxy for attacking intent than raw pass count.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.recoveries IS 'Ball recoveries in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.duels IS 'Total duels contested in this match (not split by won/lost).';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.distance_covered_km IS 'Distance covered in the match, in kilometers.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.player_match_performance.top_speed_kmh IS "Player's peak recorded speed in the match, in km/h.";

-- ================================================================
-- goalkeeper_match_performance — goalkeeper (player) x match
-- ================================================================
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.goalkeeper_match_performance.match_id IS 'Surrogate key for the match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.goalkeeper_match_performance.match_date IS 'Calendar date the match was played.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.goalkeeper_match_performance.player_id IS 'Surrogate key for the goalkeeper. Resolved via a join to match_appearances'' starting GK per match_team_id in silver.match_goalkeeper — was null on every row until that join was fixed; see the Semantic Model tab review notes.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.goalkeeper_match_performance.player_name IS "Goalkeeper's display name.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.goalkeeper_match_performance.team_id IS 'Surrogate key for the team the goalkeeper represented in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.goalkeeper_match_performance.team_name IS "Team's display name.";
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.goalkeeper_match_performance.tournament_id IS 'Raw surrogate-key ID for the tournament — not resolved to a display name in this view (single-hop join constraint, same as match_performance). Resolve via gold.dim_tournament downstream.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.goalkeeper_match_performance.shots_faced IS 'Shots faced in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.goalkeeper_match_performance.saves IS 'Saves made in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.semantic.goalkeeper_match_performance.save_percentage IS "saves divided by shots_faced. A high number of saves can also just mean a busy match, not necessarily a strong defence in front of the keeper — read alongside the team's defensive_efficiency in team_performance.";
