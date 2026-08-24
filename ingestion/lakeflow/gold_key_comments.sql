-- Column-level comments for the remaining surrogate PK/FK columns on the 16
-- Gold tables — the ones gold_comments.sql deliberately skipped (business
-- columns only, by design). These ARE useful for OntoBricks: since gold/
-- silver materialize as Unity Catalog MATERIALIZED_VIEWs, not physical Delta
-- tables, ALTER TABLE ADD CONSTRAINT PRIMARY KEY/FOREIGN KEY isn't supported
-- on them (confirmed by actually trying it — EXPECT_TABLE_NOT_VIEW error on
-- every one of the 16 tables). Explicit "FK -> table.column" comments on
-- every key column are the best available substitute signal for OntoBricks'
-- Ontology Wizard to infer the relationship graph without formal constraints.
--
-- DURABILITY: same as gold_comments.sql — a `--full-refresh-all` of
-- gold_pipeline wipes these; re-run after any full refresh.

-- ============================================================
-- dim_tournament
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_tournament.tournament_id IS 'Surrogate key for the tournament, hashed from year. Primary key of this table.';

-- ============================================================
-- dim_stage
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_stage.stage_id IS 'Surrogate key for the stage. Primary key of this table.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_stage.tournament_id IS 'The tournament this stage belongs to. FK -> dim_tournament.tournament_id.';

-- ============================================================
-- dim_group
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_group.group_id IS 'Surrogate key for the group. Primary key of this table.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_group.tournament_id IS 'The tournament this group belongs to. FK -> dim_tournament.tournament_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_group.stage_id IS 'The group stage this group belongs to. FK -> dim_stage.stage_id.';

-- ============================================================
-- dim_team
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_team.team_id IS 'Surrogate key for the team, hashed from the team name. Primary key of this table.';

-- ============================================================
-- dim_player
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_player.player_id IS "Surrogate key for the player, hashed from the player's name. Primary key of this table.";

-- ============================================================
-- dim_venue
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.dim_venue.venue_id IS 'Surrogate key for the venue/stadium. Primary key of this table.';

-- ============================================================
-- fact_match
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match.match_id IS 'Surrogate key for the match. Primary key of this table.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match.tournament_id IS 'FK -> dim_tournament.tournament_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match.stage_id IS 'FK -> dim_stage.stage_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match.group_id IS "FK -> dim_group.group_id. Derived via the home team's tournament group; null for knockout-stage matches.";
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match.venue_id IS 'FK -> dim_venue.venue_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match.home_team_id IS 'The home team. FK -> dim_team.team_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match.away_team_id IS 'The away team. FK -> dim_team.team_id.';

-- ============================================================
-- fact_team_match
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.match_team_id IS 'Surrogate key, hashed from match_id + team_id. Primary key of this table.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.match_id IS 'FK -> fact_match.match_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_match.team_id IS "The team this row's stats belong to (either the home or away side). FK -> dim_team.team_id.";

-- ============================================================
-- fact_player_match
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.match_player_id IS 'Surrogate key. Primary key of this table.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.match_id IS 'FK -> fact_match.match_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.team_id IS 'The team this player represented in this match. FK -> dim_team.team_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_player_match.player_id IS 'FK -> dim_player.player_id.';

-- ============================================================
-- fact_goalkeeper_match
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_goalkeeper_match.match_goalkeeper_id IS 'Surrogate key. Primary key of this table.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_goalkeeper_match.match_id IS 'FK -> fact_match.match_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_goalkeeper_match.team_id IS 'The team this goalkeeper represented in this match. FK -> dim_team.team_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_goalkeeper_match.player_id IS 'The goalkeeper. FK -> dim_player.player_id. Resolved via a join to the starting GK appearance — can be null if that join finds no match.';

-- ============================================================
-- fact_pass
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_pass.match_id IS 'FK -> fact_match.match_id. Part of the composite primary key.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_pass.team_id IS 'The team both passer and receiver played for. FK -> dim_team.team_id. Part of the composite primary key.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_pass.passer_id IS 'The player who made the pass. FK -> dim_player.player_id. Part of the composite primary key.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_pass.receiver_id IS 'The player who received the pass. FK -> dim_player.player_id. Part of the composite primary key.';

-- ============================================================
-- fact_match_event
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match_event.event_id IS 'Surrogate key, hashed from match/team/player/event_type/minute plus a row-uniqueness tiebreaker. Primary key of this table.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match_event.match_id IS 'FK -> fact_match.match_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match_event.team_id IS 'The team involved in this event. FK -> dim_team.team_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match_event.player_id IS 'The primary player involved (scorer, carded player, player subbed off, etc). FK -> dim_player.player_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_match_event.secondary_player_id IS 'The secondary player involved where relevant (assist provider, player subbed on). FK -> dim_player.player_id. Null when the event has no secondary player.';

-- ============================================================
-- fact_prediction
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_prediction.match_id IS 'FK -> fact_match.match_id. Primary key of this table — one prediction row per match.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_prediction.home_team_id IS 'FK -> dim_team.team_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_prediction.away_team_id IS 'FK -> dim_team.team_id.';

-- ============================================================
-- fact_team_tournament
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.team_tournament_id IS 'Surrogate key, hashed from tournament_id + team_id. Primary key of this table.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.tournament_id IS 'FK -> dim_tournament.tournament_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.team_id IS 'FK -> dim_team.team_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_tournament.group_id IS 'FK -> dim_group.group_id.';

-- ============================================================
-- fact_team_dominance
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_dominance.tournament_id IS "FK -> dim_tournament.tournament_id. Part of the composite primary key (1:1 with fact_team_tournament's tournament_id + team_id).";
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_team_dominance.team_id IS 'FK -> dim_team.team_id. Part of the composite primary key.';

-- ============================================================
-- fact_group_standing
-- ============================================================
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_group_standing.group_standing_id IS 'Surrogate key, hashed from group_id + team_id. Primary key of this table.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_group_standing.group_id IS 'FK -> dim_group.group_id.';
COMMENT ON COLUMN `worldcupiq_catalog`.gold.fact_group_standing.team_id IS 'FK -> dim_team.team_id.';
