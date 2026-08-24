-- Unity Catalog PK/FK constraints on the 16 Gold tables.
--
-- WHY THIS FILE EXISTS: Lakeflow Declarative Pipelines don't declare UC
-- constraints from the pipeline source (dlt.table has no constraint= param);
-- these are applied post-deploy via ALTER TABLE, same pattern as
-- gold_comments.sql / silver_comments.sql. Databricks UC PK/FK constraints
-- are informational by default (RELY, not physically enforced against
-- existing or future data) — they exist so query optimizers and metadata
-- tools (e.g. OntoBricks' Ontology Wizard) can read the real relational
-- spine, not to police data quality. NOT NULL on PK columns IS enforced.
--
-- Composite keys: fact_pass and fact_team_dominance have no surrogate id
-- column in gold.py, so their PK is the natural composite key instead.
--
-- DURABILITY: a `--full-refresh-all` of gold_pipeline recreates these tables
-- and drops all constraints — re-run this script after any full refresh.

-- ============================================================
-- 1. NOT NULL on every PK column (required before ADD CONSTRAINT PRIMARY KEY)
-- ============================================================
ALTER TABLE `worldcupiq_catalog`.gold.dim_tournament ALTER COLUMN tournament_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.dim_stage ALTER COLUMN stage_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.dim_group ALTER COLUMN group_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.dim_team ALTER COLUMN team_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.dim_player ALTER COLUMN player_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.dim_venue ALTER COLUMN venue_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.fact_match ALTER COLUMN match_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.fact_team_match ALTER COLUMN match_team_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.fact_player_match ALTER COLUMN match_player_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.fact_goalkeeper_match ALTER COLUMN match_goalkeeper_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.fact_pass ALTER COLUMN match_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.fact_pass ALTER COLUMN team_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.fact_pass ALTER COLUMN passer_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.fact_pass ALTER COLUMN receiver_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.fact_match_event ALTER COLUMN event_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.fact_prediction ALTER COLUMN match_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.fact_team_tournament ALTER COLUMN team_tournament_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.fact_team_dominance ALTER COLUMN tournament_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.fact_team_dominance ALTER COLUMN team_id SET NOT NULL;
ALTER TABLE `worldcupiq_catalog`.gold.fact_group_standing ALTER COLUMN group_standing_id SET NOT NULL;

-- ============================================================
-- 2. PRIMARY KEY constraints
-- ============================================================
ALTER TABLE `worldcupiq_catalog`.gold.dim_tournament ADD CONSTRAINT pk_dim_tournament PRIMARY KEY (tournament_id);
ALTER TABLE `worldcupiq_catalog`.gold.dim_stage ADD CONSTRAINT pk_dim_stage PRIMARY KEY (stage_id);
ALTER TABLE `worldcupiq_catalog`.gold.dim_group ADD CONSTRAINT pk_dim_group PRIMARY KEY (group_id);
ALTER TABLE `worldcupiq_catalog`.gold.dim_team ADD CONSTRAINT pk_dim_team PRIMARY KEY (team_id);
ALTER TABLE `worldcupiq_catalog`.gold.dim_player ADD CONSTRAINT pk_dim_player PRIMARY KEY (player_id);
ALTER TABLE `worldcupiq_catalog`.gold.dim_venue ADD CONSTRAINT pk_dim_venue PRIMARY KEY (venue_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_match ADD CONSTRAINT pk_fact_match PRIMARY KEY (match_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_team_match ADD CONSTRAINT pk_fact_team_match PRIMARY KEY (match_team_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_player_match ADD CONSTRAINT pk_fact_player_match PRIMARY KEY (match_player_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_goalkeeper_match ADD CONSTRAINT pk_fact_goalkeeper_match PRIMARY KEY (match_goalkeeper_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_pass ADD CONSTRAINT pk_fact_pass PRIMARY KEY (match_id, team_id, passer_id, receiver_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_match_event ADD CONSTRAINT pk_fact_match_event PRIMARY KEY (event_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_prediction ADD CONSTRAINT pk_fact_prediction PRIMARY KEY (match_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_team_tournament ADD CONSTRAINT pk_fact_team_tournament PRIMARY KEY (team_tournament_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_team_dominance ADD CONSTRAINT pk_fact_team_dominance PRIMARY KEY (tournament_id, team_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_group_standing ADD CONSTRAINT pk_fact_group_standing PRIMARY KEY (group_standing_id);

-- ============================================================
-- 3. FOREIGN KEY constraints (informational — nullable FK columns allowed,
--    e.g. fact_match.group_id is null for knockout matches,
--    fact_goalkeeper_match.player_id can be null per silver's known gap)
-- ============================================================

-- dim_stage, dim_group -> dim_tournament
ALTER TABLE `worldcupiq_catalog`.gold.dim_stage ADD CONSTRAINT fk_dim_stage_tournament FOREIGN KEY (tournament_id) REFERENCES `worldcupiq_catalog`.gold.dim_tournament (tournament_id);
ALTER TABLE `worldcupiq_catalog`.gold.dim_group ADD CONSTRAINT fk_dim_group_tournament FOREIGN KEY (tournament_id) REFERENCES `worldcupiq_catalog`.gold.dim_tournament (tournament_id);
ALTER TABLE `worldcupiq_catalog`.gold.dim_group ADD CONSTRAINT fk_dim_group_stage FOREIGN KEY (stage_id) REFERENCES `worldcupiq_catalog`.gold.dim_stage (stage_id);

-- fact_match -> tournament, stage, group, venue, team x3
ALTER TABLE `worldcupiq_catalog`.gold.fact_match ADD CONSTRAINT fk_fact_match_tournament FOREIGN KEY (tournament_id) REFERENCES `worldcupiq_catalog`.gold.dim_tournament (tournament_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_match ADD CONSTRAINT fk_fact_match_stage FOREIGN KEY (stage_id) REFERENCES `worldcupiq_catalog`.gold.dim_stage (stage_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_match ADD CONSTRAINT fk_fact_match_group FOREIGN KEY (group_id) REFERENCES `worldcupiq_catalog`.gold.dim_group (group_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_match ADD CONSTRAINT fk_fact_match_venue FOREIGN KEY (venue_id) REFERENCES `worldcupiq_catalog`.gold.dim_venue (venue_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_match ADD CONSTRAINT fk_fact_match_home_team FOREIGN KEY (home_team_id) REFERENCES `worldcupiq_catalog`.gold.dim_team (team_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_match ADD CONSTRAINT fk_fact_match_away_team FOREIGN KEY (away_team_id) REFERENCES `worldcupiq_catalog`.gold.dim_team (team_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_match ADD CONSTRAINT fk_fact_match_winner_team FOREIGN KEY (winner_team_id) REFERENCES `worldcupiq_catalog`.gold.dim_team (team_id);

-- fact_team_match -> match, team
ALTER TABLE `worldcupiq_catalog`.gold.fact_team_match ADD CONSTRAINT fk_fact_team_match_match FOREIGN KEY (match_id) REFERENCES `worldcupiq_catalog`.gold.fact_match (match_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_team_match ADD CONSTRAINT fk_fact_team_match_team FOREIGN KEY (team_id) REFERENCES `worldcupiq_catalog`.gold.dim_team (team_id);

-- fact_player_match -> match, team, player
ALTER TABLE `worldcupiq_catalog`.gold.fact_player_match ADD CONSTRAINT fk_fact_player_match_match FOREIGN KEY (match_id) REFERENCES `worldcupiq_catalog`.gold.fact_match (match_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_player_match ADD CONSTRAINT fk_fact_player_match_team FOREIGN KEY (team_id) REFERENCES `worldcupiq_catalog`.gold.dim_team (team_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_player_match ADD CONSTRAINT fk_fact_player_match_player FOREIGN KEY (player_id) REFERENCES `worldcupiq_catalog`.gold.dim_player (player_id);

-- fact_goalkeeper_match -> match, team, player (player nullable)
ALTER TABLE `worldcupiq_catalog`.gold.fact_goalkeeper_match ADD CONSTRAINT fk_fact_gk_match_match FOREIGN KEY (match_id) REFERENCES `worldcupiq_catalog`.gold.fact_match (match_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_goalkeeper_match ADD CONSTRAINT fk_fact_gk_match_team FOREIGN KEY (team_id) REFERENCES `worldcupiq_catalog`.gold.dim_team (team_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_goalkeeper_match ADD CONSTRAINT fk_fact_gk_match_player FOREIGN KEY (player_id) REFERENCES `worldcupiq_catalog`.gold.dim_player (player_id);

-- fact_pass -> match, team, player x2 (passer/receiver)
ALTER TABLE `worldcupiq_catalog`.gold.fact_pass ADD CONSTRAINT fk_fact_pass_match FOREIGN KEY (match_id) REFERENCES `worldcupiq_catalog`.gold.fact_match (match_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_pass ADD CONSTRAINT fk_fact_pass_team FOREIGN KEY (team_id) REFERENCES `worldcupiq_catalog`.gold.dim_team (team_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_pass ADD CONSTRAINT fk_fact_pass_passer FOREIGN KEY (passer_id) REFERENCES `worldcupiq_catalog`.gold.dim_player (player_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_pass ADD CONSTRAINT fk_fact_pass_receiver FOREIGN KEY (receiver_id) REFERENCES `worldcupiq_catalog`.gold.dim_player (player_id);

-- fact_match_event -> match, team, player x2 (secondary_player nullable)
ALTER TABLE `worldcupiq_catalog`.gold.fact_match_event ADD CONSTRAINT fk_fact_match_event_match FOREIGN KEY (match_id) REFERENCES `worldcupiq_catalog`.gold.fact_match (match_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_match_event ADD CONSTRAINT fk_fact_match_event_team FOREIGN KEY (team_id) REFERENCES `worldcupiq_catalog`.gold.dim_team (team_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_match_event ADD CONSTRAINT fk_fact_match_event_player FOREIGN KEY (player_id) REFERENCES `worldcupiq_catalog`.gold.dim_player (player_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_match_event ADD CONSTRAINT fk_fact_match_event_secondary_player FOREIGN KEY (secondary_player_id) REFERENCES `worldcupiq_catalog`.gold.dim_player (player_id);

-- fact_prediction -> match, team x2 (home/away)
ALTER TABLE `worldcupiq_catalog`.gold.fact_prediction ADD CONSTRAINT fk_fact_prediction_match FOREIGN KEY (match_id) REFERENCES `worldcupiq_catalog`.gold.fact_match (match_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_prediction ADD CONSTRAINT fk_fact_prediction_home_team FOREIGN KEY (home_team_id) REFERENCES `worldcupiq_catalog`.gold.dim_team (team_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_prediction ADD CONSTRAINT fk_fact_prediction_away_team FOREIGN KEY (away_team_id) REFERENCES `worldcupiq_catalog`.gold.dim_team (team_id);

-- fact_team_tournament -> tournament, team, group (group nullable — see silver.team_tournament coverage note)
ALTER TABLE `worldcupiq_catalog`.gold.fact_team_tournament ADD CONSTRAINT fk_fact_team_tournament_tournament FOREIGN KEY (tournament_id) REFERENCES `worldcupiq_catalog`.gold.dim_tournament (tournament_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_team_tournament ADD CONSTRAINT fk_fact_team_tournament_team FOREIGN KEY (team_id) REFERENCES `worldcupiq_catalog`.gold.dim_team (team_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_team_tournament ADD CONSTRAINT fk_fact_team_tournament_group FOREIGN KEY (group_id) REFERENCES `worldcupiq_catalog`.gold.dim_group (group_id);

-- fact_team_dominance -> tournament, team (1:1 with fact_team_tournament, but no surrogate FK to it — same natural key)
ALTER TABLE `worldcupiq_catalog`.gold.fact_team_dominance ADD CONSTRAINT fk_fact_team_dominance_tournament FOREIGN KEY (tournament_id) REFERENCES `worldcupiq_catalog`.gold.dim_tournament (tournament_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_team_dominance ADD CONSTRAINT fk_fact_team_dominance_team FOREIGN KEY (team_id) REFERENCES `worldcupiq_catalog`.gold.dim_team (team_id);

-- fact_group_standing -> group, team
ALTER TABLE `worldcupiq_catalog`.gold.fact_group_standing ADD CONSTRAINT fk_fact_group_standing_group FOREIGN KEY (group_id) REFERENCES `worldcupiq_catalog`.gold.dim_group (group_id);
ALTER TABLE `worldcupiq_catalog`.gold.fact_group_standing ADD CONSTRAINT fk_fact_group_standing_team FOREIGN KEY (team_id) REFERENCES `worldcupiq_catalog`.gold.dim_team (team_id);
