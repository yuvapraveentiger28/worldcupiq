-- Column-level business descriptions for the 27 Silver tables (selective —
-- skips plain surrogate PK/FK columns, covers names/dates/stats/business fields).
--
-- WHY THIS FILE EXISTS: same mechanism as gold_comments.sql and
-- semantic/comments.sql — PySpark alias(metadata={'comment': ...}) does not
-- propagate to a Unity Catalog column comment for a Lakeflow Declarative
-- Pipeline table; COMMENT ON COLUMN is the mechanism that actually works.
-- Table-level comments (via @dlt.table(comment=...) in silver.py) DO persist
-- and are not duplicated here.
--
-- DURABILITY: a normal `databricks bundle run silver_pipeline` (incremental)
-- preserves these comments. A `--full-refresh-all` recreates the tables and
-- wipes them — re-run this script after any full refresh of silver_pipeline.

-- ============================================================
-- confederation
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.confederation.name IS 'Confederation display name (e.g. UEFA, CONMEBOL).';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.confederation.code IS 'Confederation short code.';

-- ============================================================
-- tournament
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.tournament.name IS 'Tournament''s display name (e.g. ''2026 FIFA World Cup'').';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.tournament.year IS 'Year the tournament was held. Used to build the deterministic tournament_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.tournament.host_country_name IS 'Name of the hosting country/countries (2026 is a tri-nation hosting: United States / Mexico / Canada).';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.tournament.count_teams IS 'Number of teams that competed in this tournament (e.g. 48 for 2026, up from 32 in prior editions).';

-- ============================================================
-- stage
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.stage.name IS 'Stage name (e.g. ''Group Stage'', ''Round of 32'', ''Final'').';

-- ============================================================
-- tournament_group
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.tournament_group.name IS 'Group label (e.g. ''Group A''). Meaningful only for the group-stage phase of a tournament.';

-- ============================================================
-- qualified_team
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.qualified_team.qualification_route IS 'How/via what performance the team qualified for this tournament. Historical only — no 2026 qualification-route source is in bronze yet.';

-- ============================================================
-- team
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.team.name IS 'Team''s display name. Entity-resolution key — same uppercased+trimmed name across relational/fifa_training_centre/historical bronze sources collapses to one silver row.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.team.code IS 'Team''s short code (e.g. FIFA 3-letter abbreviation). Only populated from the relational and historical sources — fifa_training_centre carries no code.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.team.manager_name IS 'Name of the team''s manager/head coach at the time of the tournament. Only populated from the relational (2026) source at this table''s grain — see manager_appointment for a fuller, tournament-scoped history.';

-- ============================================================
-- team_tournament
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.team_tournament.final_rank IS 'Final placement of the team in this tournament. Historical only (from hist_tournament_standings) — null for the 2026 row until the tournament concludes.';

-- ============================================================
-- player
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.player.name IS 'Player''s display name. Entity-resolution key across fifa_training_centre, relational, and historical bronze sources.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.player.date_of_birth IS 'Player''s date of birth. Only populated from relational (2026) and historical sources — fifa_training_centre carries no birth date.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.player.position IS 'Player''s nominal position (e.g. GK, DEF, MID, FWD). Only populated from the relational (2026) source — may differ from the position actually played in a given match (see match_player.position_played).';

-- ============================================================
-- squad_member
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.squad_member.shirt_number IS 'Player''s squad shirt number for this tournament.';

-- ============================================================
-- referee
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.referee.name IS 'Referee''s display name.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.referee.country_code IS 'Referee''s country/federation of origin.';

-- ============================================================
-- manager
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.manager.name IS 'Manager/head coach''s display name.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.manager.country_code IS 'Manager''s nationality. Only populated from the historical source.';

-- ============================================================
-- venue
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.venue.name IS 'Stadium''s display name.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.venue.city IS 'City the stadium is located in.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.venue.country_code IS 'Country the stadium is located in.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.venue.capacity IS 'Stadium seating capacity.';

-- ============================================================
-- match
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match.winner_team_id IS 'Team that progressed/won. Null on a group-stage draw. For matches decided by a shootout, reflects who advanced, not the 90-minute (open-play) result.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match.referee_id IS 'Officiating referee. Only populated for 2026 matches (relational.matches has a real match-level referee_id) — null for all historical matches, which only carry tournament-panel-grain referee data (see referee_assignment).';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match.match_date IS 'Calendar date the match was played.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match.home_score IS 'Home team''s goals at full time (after extra time for knockouts).';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match.away_score IS 'Away team''s goals at full time (after extra time for knockouts).';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match.home_penalties IS 'Home team''s penalty-shootout score, if the match was decided by penalties. Null otherwise.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match.away_penalties IS 'Away team''s penalty-shootout score, if the match was decided by penalties. Null otherwise.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match.home_xg IS 'Home team''s Expected Goals in this match. Only populated for 2026 matches — null for historical matches (no xG source pre-2026).';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match.away_xg IS 'Away team''s Expected Goals in this match. Only populated for 2026 matches — null for historical matches.';

-- ============================================================
-- group_standing
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.group_standing.played IS 'Matches played in the group stage.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.group_standing.won IS 'Matches won in the group stage.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.group_standing.drawn IS 'Matches drawn in the group stage.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.group_standing.lost IS 'Matches lost in the group stage.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.group_standing.goals_for IS 'Goals scored in the group stage.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.group_standing.goals_against IS 'Goals conceded in the group stage.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.group_standing.points IS 'Points earned in the group stage.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.group_standing.rank IS 'Final standing within the group (1 = group winner). Historical only — no 2026 bronze source publishes a standings table yet.';

-- ============================================================
-- tournament_standing
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.tournament_standing.final_rank IS 'Final placement in the tournament (1 = champion). Historical only.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.tournament_standing.host_performance IS 'Host-nation performance context, populated only for the tournament host team (folded in from hist_host_countries).';

-- ============================================================
-- match_team
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_team.home_away IS 'Whether this team played at home or away in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_team.possession_pct IS 'Share of the ball this team held in this match. Only populated from the relational (2026) source.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_team.shots IS 'Shot attempts by this team in this match. Only populated from the relational (2026) source.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_team.shots_on_target IS 'Attempts on the goal frame by this team in this match. Only populated from the relational (2026) source.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_team.corners IS 'Corner kicks won by this team in this match. Only populated from the relational (2026) source.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_team.fouls IS 'Fouls committed by this team in this match. Only populated from the relational (2026) source.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_team.offsides IS 'Offsides called against this team in this match. Only populated from the relational (2026) source.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_team.pressures IS 'Pressing actions applied by this team in this match. From fifa_training_centre.team_defensive_pressure.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_team.recoveries IS 'Forced turnovers won by this team in this match. From fifa_training_centre.team_defensive_pressure.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_team.aerial_duels IS 'Aerial contests (total deliveries) involving this team in this match. From fifa_training_centre.team_aerial_control.';

-- ============================================================
-- match_player
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_player.started IS 'Whether the player was in the starting lineup for this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_player.position_played IS 'The position this player actually played in this specific match — may differ from their nominal position in the player table.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_player.goals IS 'Goals scored by this player in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_player.shots IS 'Shot attempts by this player in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_player.passes IS 'Passes attempted by this player in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_player.passes_completed IS 'Passes completed by this player in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_player.progressive_passes IS 'Passes that moved the ball meaningfully toward the opponent''s goal (line breaks completed). Better proxy for attacking intent than raw pass count.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_player.recoveries IS 'Interceptions made by this player in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_player.duels IS 'Tackles made and won by this player in this match (not total duels contested).';

-- ============================================================
-- match_player_physical
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_player_physical.distance_covered_km IS 'Distance covered by this player in the match, in kilometers (converted from meters at source).';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_player_physical.top_speed_kmh IS 'Player''s peak recorded speed in the match, in km/h.';

-- ============================================================
-- match_goalkeeper
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_goalkeeper.shots_faced IS 'Shots faced by this goalkeeper in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_goalkeeper.saves IS 'Saves made by this goalkeeper in this match.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_goalkeeper.save_pct IS 'saves divided by shots_faced for this match, as published at source (not recomputed).';

-- ============================================================
-- passing_edge
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.passing_edge.passes IS 'Number of passes from passer_id to receiver_id in this match. Directed edge weight for GraphFrames passing-network analysis.';

-- ============================================================
-- match_event
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_event.event_type IS 'Category of the event: SHOT, GOAL, CARD, SUBSTITUTION, or a relational-source event_type string. Vocabulary is not fully unified across the 3 merged bronze sources — treat as source-dependent when filtering across tournaments.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_event.minute IS 'Match minute the event occurred (regulation time for historical sources).';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_event.detail IS 'Free-text or coded detail about the event — e.g. shot outcome, OWN_GOAL/PENALTY for goals, RED/SECOND_YELLOW/YELLOW for cards, IN/OUT for substitutions. Meaning depends on event_type.';

-- ============================================================
-- award
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.award.name IS 'Award type name (e.g. Golden Boot, Golden Ball). A lookup of award types, not tied to a specific tournament — see award_winner for the per-tournament winner.';

-- ============================================================
-- match_prediction_feature
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_prediction_feature.home_fifa_rank IS 'Home team''s FIFA world ranking at kickoff. Model input feature, not an outcome.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_prediction_feature.away_fifa_rank IS 'Away team''s FIFA world ranking at kickoff. Model input feature, not an outcome.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_prediction_feature.home_elo IS 'Home team''s Elo rating at kickoff. Model input feature.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_prediction_feature.away_elo IS 'Away team''s Elo rating at kickoff. Model input feature.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_prediction_feature.home_squad_avg_value_eur IS 'Home squad''s average player market value in EUR. Model input feature, a proxy for squad quality/depth.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_prediction_feature.away_squad_avg_value_eur IS 'Away squad''s average player market value in EUR. Model input feature.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_prediction_feature.home_prev_avg_xg_scored IS 'Home team''s average Expected Goals scored in prior matches, going into this match. Model input feature — a form indicator, not this match''s xG (see match.home_xg for that).';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_prediction_feature.away_prev_avg_xg_scored IS 'Away team''s average Expected Goals scored in prior matches, going into this match. Model input feature.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_prediction_feature.home_rest_days IS 'Days of rest the home team had before this match. Model input feature — fatigue/schedule-congestion proxy.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.match_prediction_feature.away_rest_days IS 'Days of rest the away team had before this match. Model input feature.';

-- ============================================================
-- prediction
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.silver.prediction.home_win_probability IS 'Onside Arena''s pre-match model probability the home team wins.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.prediction.draw_probability IS 'Onside Arena''s pre-match model probability of a draw.';
COMMENT ON COLUMN `worldcupiq_catalog`.silver.prediction.away_win_probability IS 'Onside Arena''s pre-match model probability the away team wins.';
