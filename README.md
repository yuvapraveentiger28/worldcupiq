# WorldCupIQ — FIFA World Cup 2026 AI-Ready Data Platform on Databricks

> From match data to football intelligence. A governed knowledge system — **semantic layer + ontology + context layer** — that Genie and a Mosaic AI agent reason over, surfaced through a Streamlit Databricks App.

The platform answers deliberately hard questions like *"Was Spain actually the best team in the 2026 World Cup, or simply the Champion?"* — not by summarizing a CSV, but by resolving intent against governed business definitions, traversing a football knowledge graph, computing governed metrics, and returning an answer **with source evidence and lineage**.

---

## 1. Concept

A dashboard can show scores, possession, goals, shots, xG and player stats. WorldCupIQ makes those facts **reasonable over** by adding three governed layers on top of a Databricks medallion lakehouse:

| Layer | Answers | Databricks realization |
|---|---|---|
| **Semantic layer** | "What does this metric *mean*?" | Unity Catalog **Metric Views** |
| **Ontology** | "What is this entity and how does it *relate*?" | **OntoBricks** (Databricks Labs) — LLM-assisted entity/relationship graph over Gold/Semantic, Lakebase-backed registry, Delta triple materialization |
| **Context layer** | "What should the AI *know, use and trust*?" | Concept/policy Delta store + **Mosaic AI Vector Search** + UC lineage |

An agent sits on top and reasons across all three.

---

## 2. Architecture

Data flows **bottom → top**; questions and reasoning flow **top → bottom**. Unity Catalog governs every layer.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ VISUALIZATION & CONSUMPTION   Streamlit App · AI/BI + Genie chat · Alerting   │
├─────────────────────────────────────────────────────────────────────────────┤
│ API & SERVICE                 Databricks Apps backend · Model Serving endpoints│
├─────────────────────────────────────────────────────────────────────────────┤
│ ORCHESTRATION                 Databricks Workflows / Lakeflow Jobs             │
├─────────────────────────────────────────────────────────────────────────────┤
│ MULTI-AGENT (Mosaic AI)       Intent · Semantic · Ontology · Context · Recommend│
├─────────────────────────────────────────────────────────────────────────────┤
│ SEMANTIC & FEATURES           Metric Views · Context store                   │
├─────────────────────────────────────────────────────────────────────────────┤
│ KNOWLEDGE GRAPH               OntoBricks — entities/relationships over Gold  │
├─────────────────────────────────────────────────────────────────────────────┤
│ DATA INGESTION                Sources → ADLS/Volume → Bronze → Silver → Gold   │
└─────────────────────────────────────────────────────────────────────────────┘
        GOVERNANCE — Unity Catalog: permissions · glossary · lineage · certified assets
```

See `architecture.html` for the rendered layered diagram.

---

## 3. Tech stack (all Databricks)

- **Storage/format:** Delta Lake on ADLS Gen2, exposed via Unity Catalog External Location + Volumes
- **Ingestion:** Auto Loader
- **Pipelines:** Lakeflow Declarative Pipelines (Delta Live Tables)
- **Governance:** Unity Catalog (constraints, tags, certified assets, lineage)
- **Semantic layer:** Unity Catalog Metric Views (YAML)
- **Graph/Ontology:** [OntoBricks](https://github.com/databrickslabs/ontobricks) (Databricks Labs) — LLM-assisted entity/relationship graph, Lakebase-backed registry, Delta triples in Unity Catalog + UC Python functions
- **Retrieval:** Mosaic AI Vector Search
- **Agent:** Mosaic AI Agent Framework on Model Serving; MLflow for tracing + evaluation
- **NL analytics:** AI/BI Genie (Conversation API)
- **App:** Databricks Apps hosting Streamlit
- **Orchestration:** Databricks Workflows / Lakeflow Jobs
- **Language:** Python + SQL

---

## 4. Proposed repository structure

```
worldcupiq/
├── README.md
├── architecture.html               # rendered build-status diagram + per-layer detail tabs (open in a browser)
├── databricks.yml                  # Asset Bundle — bronze_pipeline_predictions, github_ingestion_pipeline,
│                                    # 3 parse_pipeline_<datasource>, silver_pipeline, gold_pipeline, bronze_refresh job
├── .gitattributes                  # line-ending normalization (LF in repo, native in working tree)
├── .env.example                    # DATABRICKS_HOST / DATABRICKS_TOKEN template
├── conf/
│   ├── 00_catalog.sql              # 11 schemas (bronze_<datasource>, silver, gold, semantic, ontology, context, landing) in worldcupiq_catalog
│   └── 01_grants.sql               # groups, service principal, grants — not yet written (Phase 0 follow-up)
├── scripts/
│   ├── load-env.cmd                # loads .env into a cmd.exe session
│   └── setup-catalog.cmd           # CLI-based alternative to 00_catalog.sql
├── ingestion/
│   └── lakeflow/
│       ├── bronze.py               # Auto Loader → bronze_predictions.pre_match_prediction (the one Auto-Loader source)
│       ├── bronze_github_parse.py  # parses github_raw.repo_contents → the 3 GitHub-sourced bronze schemas
│       ├── silver.py               # normalize + entity-resolve → worldcupiq_catalog.silver.* (27 tables)
│       ├── silver_comments.sql     # COMMENT ON COLUMN for silver's business columns — re-run after any --full-refresh-all
│       ├── gold.py                 # star schema → worldcupiq_catalog.gold.* (6 dims + 10 facts)
│       ├── gold_comments.sql       # COMMENT ON COLUMN for Gold's business columns
│       ├── gold_key_comments.sql   # COMMENT ON COLUMN for Gold's surrogate PK/FK columns — see §6 constraint note
│       └── gold_constraints.sql    # NOT CURRENTLY APPLICABLE — see §6, kept for if/when gold ever becomes a physical table
├── semantic/                       # UC Metric View YAML — deployed to semantic (Phase 5, done)
│   ├── team_performance.metricview.yaml
│   ├── team_dominance.metricview.yaml
│   ├── match_performance.metricview.yaml
│   ├── player_match_performance.metricview.yaml
│   ├── goalkeeper_match_performance.metricview.yaml
│   └── comments.sql                # COMMENT ON COLUMN for all 99 Metric View columns — re-run after any CREATE OR REPLACE VIEW (always wipes them)
├── docs/
│   ├── demo-design-notes.md        # original design-doc notes + real candidate dataset URLs
│   └── assets/ontology/            # OntoBricks screenshots embedded in architecture.html's Knowledge Layer tab
├── context/                        # Phase 7 — not yet built, see §9
│   ├── concept_store.sql
│   ├── policies.json
│   └── vector_index.py
├── agent/
│   ├── tools.py                    # Genie tool, ontology fns, retriever, lineage
│   ├── agent.py                    # Mosaic AI Agent Framework
│   └── eval.py                     # MLflow evaluation set
├── genie/
│   └── space_config.md             # datasets, instructions, certified queries
├── app/
│   ├── app.py                      # Streamlit
│   ├── app.yaml                    # Databricks Apps entrypoint
│   └── requirements.txt
└── workflows/
    └── worldcupiq_job.yaml         # end-to-end orchestration
```

> **No `ontology/` directory.** README §8 originally scoped a hand-rolled `entities.sql` /
> `relationships.sql` / `graph_build.py` (GraphFrames) design — none of that was built. Phase 6
> pivoted to **OntoBricks** (Databricks Labs), a separate local application, not a folder in this
> repo — see §8 for what's actually there.

---

## 5. Data sources

| Source | Contents | Role |
|---|---|---|
| **FIFA Training Centre-derived** | 21 tables · 104 matches · 48 teams · 1,277 players · lineups, events, passing networks, team/player stats, pressure, set plays | primary analytical foundation |
| **Relational dataset** | matches, venues, squads, players, xG, events, team stats, VAR | matches & venues authority · xG & VAR |
| **Historical World Cups** *(context)* | tournaments, matches, teams, players, goals, standings (2014–2026) | champion comparison over time |
| **Pre-match predictions** *(context)* | win/draw/loss probabilities, model, timestamp | decision-intelligence / surprise analysis |

> **Dropped: FIFA official (fifa.com pages).** There's no clean CSV/JSON feed behind those pages —
> just tournament article pages, which would need manual scraping/transcription before they could
> land anywhere. Rather than build and maintain a scraper for one source, `matches` and `venues`
> are sourced from the **relational dataset** (`mominullptr/FIFA-World-Cup-2026-Dataset`), which
> already has both as real files. `bronze_fifa_official` and
> `bronze_pipeline_fifa_official` are removed accordingly.

**Ingestion mechanism: two paths, by source shape — both actually built.**

- **`predictions` only** lands as a flat CSV in `worldcupiq_catalog.landing.raw_files/pre_match_prediction/`
  and is read by Auto Loader (`cloudFiles`) — `bronze_pipeline_predictions` in `bronze.py`.
- **The 3 GitHub-hosted sources** (FIFA Training Centre-derived, relational, historical) land via
  **Lakeflow Connect's GitHub connector** instead — not Auto Loader. Each source repo was forked
  into the `yuvapraveentiger28` GitHub organization (the connector requires an org, not a personal
  account — it calls `/orgs/{name}/repos`, which 404s on a personal account) and connected via a
  Unity Catalog connection (`github-fifaworldcupiq`). The connector lands **every file in every repo
  it's connected to** as one row per file in `github_raw.repo_contents`, unparsed; three
  `parse_pipeline_<datasource>` pipelines (`bronze_github_parse.py`) then parse each repo's CSVs into
  real bronze tables.
- **On this workspace specifically:** the `github-fifaworldcupiq` connection doesn't exist yet — it
  needs an admin to grant access to create it, still pending. The 3 GitHub-sourced bronze schemas
  were populated instead via a **one-time migration** from a prior workspace where the connection
  already existed (59 tables, 185,959 rows, verified row-for-row). `github_ingestion_pipeline` and
  the 3 parse pipelines are deployed and ready; they'll run for real once the connection lands. See
  `architecture.html`'s Data Sources / Ingestion tabs for the live status.

---

## 6. Data model

### Catalog layout
`worldcupiq_catalog` — dedicated to this project (not shared) — with 11 schemas, **left unprefixed**:
the catalog itself is the namespace boundary, not a `fifaworldcup_`-style schema prefix. `landing`
(Volume only, no tables) · `github_raw` (Lakeflow Connect raw landing) · four bronze schemas, one per
datasource (`bronze_fifa_training_centre` · `bronze_relational` · `bronze_historical` ·
`bronze_predictions`) · `silver` · `gold` · `semantic` · `ontology` · `context`. See `conf/00_catalog.sql`.

### Bronze — raw, 1:1 with source, split by datasource schema (60 tables, all populated)
| Schema | Tables |
|---|---|
| `worldcupiq_catalog.bronze_fifa_training_centre` (21) | `attempts_at_goal, match_appearances, match_teams, matches, passing_network_edges, player_crosses_open_play, player_events, player_in_possession_distributions, player_line_breaks, player_offers_receptions, player_out_of_possession, player_physical_data, players, team_aerial_control, team_defensive_pressure, team_goal_prevention, team_goalkeeping_distribution, team_key_stats, team_phases, team_set_plays, teams` |
| `worldcupiq_catalog.bronze_relational` (11) | `match_events, match_lineups, match_prediction_features, match_team_stats, matches, player_stats, referees, squads_and_players, teams, tournament_stages, venues` |
| `worldcupiq_catalog.bronze_historical` (27) | `hist_award_winners, hist_awards, hist_bookings, hist_confederations, hist_goals, hist_group_standings, hist_groups, hist_host_countries, hist_manager_appearances, hist_manager_appointments, hist_managers, hist_matches, hist_penalty_kicks, hist_player_appearances, hist_players, hist_qualified_teams, hist_referee_appearances, hist_referee_appointments, hist_referees, hist_squads, hist_stadiums, hist_substitutions, hist_team_appearances, hist_teams, hist_tournament_stages, hist_tournament_standings, hist_tournaments` |
| `worldcupiq_catalog.bronze_predictions` (1) | `pre_match_prediction` |

See `ingestion/lakeflow/bronze.py`'s `BRONZE_SOURCES` registry for the authoritative mapping.

> **Why four pipelines, not one:** a Lakeflow pipeline publishes to exactly one `catalog.schema`,
> set at the pipeline level — there's no supported per-table schema override. `databricks.yml`
> deploys `bronze.py` and `bronze_github_parse.py` as four separate pipeline resources
> (`bronze_pipeline_predictions` + 3 `parse_pipeline_<datasource>`), each with its own
> `catalog`/`schema` and a `datasource_filter` in its `configuration` block that tells the shared
> source file which subset of its registry to build. One scheduled job (`bronze_refresh`) triggers
> all four, plus the GitHub Connect raw landing they depend on, daily — currently only
> `bronze_pipeline_predictions` can actually run; the other three are blocked pending the GitHub
> connection (see §5).

### Silver (`worldcupiq_catalog.silver`) — normalized, entity-resolved (27 tables)
| Cluster | Tables |
|---|---|
| Tournament & reference | `confederation, tournament, stage, tournament_group, qualified_team` |
| Team & player | `team, team_tournament, player, squad_member` |
| Officials | `referee, referee_assignment, manager, manager_appointment` |
| Match spine | `venue, match, group_standing, tournament_standing` |
| Match performance (2026 only) | `match_team, match_player, match_player_physical, match_goalkeeper, passing_edge` |
| Events & awards | `match_event, award, award_winner` |
| Predictions | `match_prediction_feature, prediction` |

Entity resolution (team/player/venue/referee/manager appearing in multiple bronze sources) is by exact
upper+trim name match via a deterministic hash surrogate key (`_sk`), not fuzzy matching.

**UC primary/foreign-key constraints are not currently possible on silver or gold.** The original plan
here was to declare them as the ontology's enforced spine — but every silver and gold table
materializes as a Unity Catalog `MATERIALIZED_VIEW` (confirmed via `information_schema.tables`), and
`ALTER TABLE ... ADD CONSTRAINT` only works on physical Delta tables, not views. Confirmed by actually
trying it — `EXPECT_TABLE_NOT_VIEW` on all 16 Gold tables. Getting real constraints would mean
rearchitecting both pipelines around streaming tables/`apply_changes`, not a DDL tweak. The practical
substitute: every FK-shaped column has an explicit `COMMENT` stating what it references
(`gold_key_comments.sql`) — the best available relationship signal for downstream tools (including
OntoBricks' Ontology Wizard, see §8) without formal constraints.

Key column definitions (real, verified against landed data — not guessed):

```sql
-- silver.match
match_id PK, tournament_id FK, stage_id FK, venue_id FK,
home_team_id FK, away_team_id FK, winner_team_id FK, referee_id FK,
match_date, home_score, away_score, home_penalties, away_penalties, home_xg, away_xg

-- silver.match_team
match_team_id PK, match_id FK, team_id FK, home_away,
possession_pct, shots, shots_on_target, corners, fouls, offsides,
pressures, recoveries, aerial_duels

-- silver.match_player
match_player_id PK, match_id FK, team_id FK, player_id FK, started, position_played,
goals, shots, passes, passes_completed, progressive_passes, recoveries, duels

-- silver.passing_edge
match_id FK, team_id FK, passer_id FK, receiver_id FK, passes
```

### Gold (`worldcupiq_catalog.gold`) — star schema (16 tables: 6 dims + 10 facts)
**Dimensions:** `dim_tournament, dim_stage, dim_group, dim_team, dim_player, dim_venue`
**Match-grain facts:** `fact_match, fact_team_match, fact_player_match, fact_goalkeeper_match, fact_pass, fact_match_event, fact_prediction`
**Tournament/group-grain facts:** `fact_team_tournament, fact_team_dominance, fact_group_standing`

`fact_match_event` merges the original blueprint's separate `fact_goal`/`fact_shot`/`fact_card`/
`fact_substitution` into one polymorphic table (`event_type` distinguishes them) — those four
never existed as separate tables. There's no `dim_country` either — country context lives inline as
`country_code` on `dim_team`/`dim_venue`.

```sql
-- gold.fact_team_tournament (one row per team per edition)
team_tournament_id PK, tournament_id FK, team_id FK, group_id FK,
matches, wins, draws, losses, goals_for, goals_against, points,
xg, xga, possession, shots, shots_on_target, pressures,
final_rank, host_performance
```

Historical facts conform to the **same dims and Metric Views**, so era comparisons are apples-to-apples.

---

## 7. Semantic layer — Metric Views

Define the business vocabulary once, governed. Never expose raw `goals_for / shots / xg` as the vocabulary.

`semantic.team_performance` measures: **xG Differential** (`SUM(xg)-SUM(xga)`), **Shot Conversion** (`goals/shots`), **Shot Quality** (`xg/shots`), **Defensive Efficiency** (`goals_against/xga`), **Chance Creation** (`xg/matches`), plus Points, Goals, Possession, Pass Completion.

### Performance Dominance Score (governed, versioned)
```
Performance Dominance Score =
  0.30 × Results
+ 0.25 × xG Differential
+ 0.15 × Shot Quality
+ 0.15 × Chance Creation
+ 0.15 × Defensive Efficiency
```
Component scores are min-max normalized across the 48 teams in `gold.fact_team_dominance`. As actually built, the weighted composite is computed there too — `gold.py`'s `fact_team_dominance()` owns the 0.30/0.25/0.15/0.15/0.15 formula, not the Metric View YAML. `semantic.team_dominance` re-exposes that governed, already-weighted score (plus its five components) under stable measure names, so the governance guarantee still holds: nothing downstream of Gold can redefine the weights. The context policy `best_team` points at this measure.

### Match-grain Metric Views

Tournament-grain measures above answer "how good is this team across the whole World Cup." These three answer "what happened in this match" — sourced straight off the match-grain Gold facts, no new aggregation tables needed.

**`semantic.match_performance`** — grain: team × match, source `gold.fact_team_match` joined to `gold.fact_match` for date/venue/stage context.
- Result, Points (per-match: 3/1/0)
- Goals For, Goals Against, Goal Difference
- xG For, xG Against, **xG Differential** (`xg_for - xg_against`)
- Shot Conversion (`goals_for/shots`), Shot Quality (`xg_for/shots`), Shot Accuracy (`shots_on_target/shots`)
- Possession %, Corners, Fouls, Offsides, Pressures, Recoveries, Aerial Duels Won

**`semantic.player_match_performance`** — grain: player × match, source `gold.fact_player_match`.
- Goals, Shots, Shot Conversion (`goals/shots`)
- Pass Completion % (`passes_completed/passes`), Progressive Passes
- Recoveries, Duels
- Distance Covered (km), Top Speed (km/h)

**`semantic.goalkeeper_match_performance`** — grain: goalkeeper × match, source `gold.fact_goalkeeper_match`.
- Shots Faced, Saves, Save Percentage

None of these carry weighted/composite scores like Performance Dominance Score — they're governed vocabulary (consistent naming/formulas), not judgment calls, so there's nothing to keep out of the agent's hands the way the dominance weights are. `fact_pass` (passer→receiver edges) and `fact_match_event` (goals/cards/subs) stay as raw Gold facts, not Metric Views — they feed ontology traversal (Phase 6, via OntoBricks — see §8) and the event timeline rather than aggregate measures.

**Real constraint found deploying these three:** UC Metric View joins are single-hop from `source` only — a join's `on:` clause can't reference another join's alias (confirmed by `UNRESOLVED_COLUMN` errors chaining `dim_tournament`/`dim_stage`/`dim_venue` off the already-joined `fact_match` alias). Since `tournament_id`/`stage_id`/`venue_id` live only on `gold.fact_match`, not on `fact_team_match`/`fact_player_match`/`fact_goalkeeper_match` directly, these three views expose those as raw IDs rather than resolved names (`tournament_year`, `stage_name`, `venue_name` were dropped from the deployed YAML). Resolve those in the BI tool or a downstream query instead.

---

## 8. Ontology build — via OntoBricks (Databricks Labs), in progress

**Status:** in progress. The hand-rolled design originally scoped here — `ontology/entities.sql` +
`relationships.sql` + GraphFrames traversal + UC functions — was never built. Once formal UC PK/FK
constraints turned out to be unavailable on gold/silver (§6), that design's "anchor to an enforced
spine" premise no longer held cleanly, and [OntoBricks](https://github.com/databrickslabs/ontobricks)
(Databricks Labs) covered the same job — LLM-assisted entity/relationship generation from real UC
metadata, backed by a proper registry — with far less to hand-build.

**What's actually running:**
- OntoBricks runs locally (`scripts/start.sh`, not deployed as a Databricks App) against the `dev`
  workspace, backed by a **Lakebase** (Postgres) project (`ontobricks-app`/`production`) provisioned
  on this workspace for its domain registry, plus a `worldcupiq_catalog.ontology`-schema UC Volume
  for binary artifacts.
- A domain (`Worldcupiq`) was created, with metadata imported from `worldcupiq_catalog.gold` (16
  tables) and `.semantic` (5 Metric Views) — 21 objects total.
- The **Ontology Wizard** (LLM agent, Ontology → Wizard) generated 16 entities from that metadata —
  one per gold table/view, e.g. `dim_team` → `Team`, `fact_match` → `Match`.
- **Real finding:** the Wizard did not wire relationships for every entity — 7 of 16 (`Player`,
  `Stage`, and the 5 performance/measurement facts) came out completely disconnected. Fixed by
  manually adding 25 relationships, each name kept globally unique per entity pair since OntoBricks
  doesn't allow reusing a relationship name across different pairs (e.g. `teamMatchStatsForTeam` /
  `teamMatchStatsForMatch`, not a generic `forTeam`/`forMatch`). All 16 entities are now connected.
- **Real finding:** node/attribute mapping (Visual Mapping Designer — binding each entity/relationship
  to its real `catalog.schema.table.column`) has to be done one at a time; there's no working bulk-map
  completion despite an advertised "Auto-Map" step.
- Along the way, found and fixed three real bugs in OntoBricks' own code (not our data): two
  `databricks-sql-connector` paramstyle bugs (`%s` used where the driver needs `?`, breaking table
  comment lookups and a schema probe), and a Unity Catalog Metric View incompatibility (`SELECT *`
  fails on a Metric View's unwrapped measure columns — `SELECT 1` fixed it) in the SELECT-permission
  check used by both the data-source screen and the mapping validator.

**Current step:** mapping every entity/relationship to its backing gold/semantic column. Next:
materializing the Knowledge Graph itself (OntoBricks writes it into `worldcupiq_catalog.ontology` as
Delta triple tables, per the workspace's own choice of Lakehouse over Lakebase/Neo4j as the graph
backend, to keep the ontology data itself inside Unity Catalog rather than a separate store).

See `architecture.html`'s **Knowledge Layer** tab for the full before/after graph screenshots, the
complete 25-relationship table, and the mapping-designer state.

---

## 9. Context layer

- `context.concept` (Delta): one row per business concept `{concept, definition, grain, approved, source, related_entities}`.
- `context/policies.json`: governed definitions, e.g. `best_team = highest Performance Dominance Score` with its weight policy.
- **Vector Search** index over concepts, policies, metric descriptions and ontology docs → semantic retrieval at reasoning time.
- **Provenance:** UC lineage gives the Source → Bronze → Silver → Gold → Metric View → answer trail; expose it so the agent can "show the evidence."

---

## 10. Genie space

- **Datasets:** the 5 Metric Views + dims (never bronze/silver event tables).
- **Instructions:** paste the glossary + governing policy ("'best team' = highest Performance Dominance Score; prefer semantic measures over raw columns").
- **Certified queries (trusted assets):** "Rank teams by Performance Dominance Score", "Spain vs Argentina dominance".
- **Access:** grant the app service principal `CAN RUN` on the space and `SELECT` on `semantic`.

---

## 11. Agent (Mosaic AI Agent Framework)

Tools:
- **Genie space** — governed NL→SQL over Metric Views.
- **UC functions** — `tournament_path`, `key_players`, dominance calc.
- **Vector Search retriever** — concept/policy/ontology context.
- **Lineage/evidence** — returns provenance.

Reasoning flow: *resolve intent → pull governed definition → traverse ontology → compute via semantic metrics → answer with evidence.* Trace + evaluate with **MLflow**; register the agent in Unity Catalog.

---

## 12. Streamlit app (Databricks Apps)

```yaml
# app/app.yaml
command: ["streamlit", "run", "app.py"]
env:
  - name: GENIE_SPACE_ID
    value: "01ef...worldcupiq"
```
```
# app/requirements.txt
streamlit
databricks-sdk
databricks-sql-connector
```

`app.py` calls the **Genie Conversation API** via `databricks-sdk`, and for each question shows side by side: resolved definition (Vector Search), generated semantic SQL + result, ontology traversal path, and source lineage. Questions 1–4 route to Genie; Question 5 ("coach Spain") routes to the **agent endpoint**. Phase 9, not yet built — the ontology traversal path itself depends on how Phase 6 finishes exposing OntoBricks' graph (its own GraphQL/SPARQL surface, or UC functions bound to ontology classes via its Class Actions feature — both are real OntoBricks capabilities, which one this app calls is still open).

Deploy: `databricks apps deploy worldcupiq`. Grant the app service principal the Genie space, `SELECT` on `worldcupiq_catalog.semantic` / `worldcupiq_catalog.gold`, whatever ontology query surface Phase 6 lands on, and query on the Vector Search index.

---

## 13. Build order (suggested milestones)

- [x] **Phase 0 — Foundation:** Premium workspace, UC metastore, dedicated catalog `worldcupiq_catalog` (11 unprefixed schemas). `01_grants.sql` (groups, app service principal) not yet written. → `conf/`
- [x] **Phase 1 — Land sources:** `predictions.csv` landed via Auto Loader. The 3 GitHub-sourced datasets landed via a one-time migration from a prior workspace where the GitHub connection already existed — live Lakeflow Connect sync is still blocked pending admin access to create `github-fifaworldcupiq` on this workspace (`fifa_official` was dropped regardless, see §5).
- [x] **Phase 2 — Bronze:** Auto Loader + Lakeflow Connect, 1:1 raw tables. 60 tables across 4 schemas, all populated. → `ingestion/lakeflow/bronze.py`, `bronze_github_parse.py`
- [x] **Phase 3 — Silver:** normalize, entity-resolve. 27 tables, deployed and run against real bronze data. PK/FK constraints turned out to be unavailable (§6 — gold/silver materialize as UC materialized views); EAV-shaped fifa_training_centre stat tables (`team_key_stats`, `team_phases`, `team_set_plays`, `team_goalkeeping_distribution`) not yet pivoted into `match_team`, a follow-up once their metric label strings are sampled. → `silver.py`, `silver_comments.sql`
- [x] **Phase 4 — Gold:** star schema (6 dims + 10 facts) + `fact_team_tournament` + `fact_team_dominance` (governed Performance Dominance Score). 16 tables, deployed and run against real Silver data, full column comment coverage (business columns in `gold_comments.sql`, key/FK columns in `gold_key_comments.sql`). → `gold.py`
- [x] **Phase 5 — Semantic:** 5 Metric Views deployed to `worldcupiq_catalog.semantic` — `team_performance`, `team_dominance` (tournament grain), `match_performance`, `player_match_performance`, `goalkeeper_match_performance` (match grain). Verified live with `MEASURE()` queries against real Gold data. → `semantic/`
- [~] **Phase 6 — Ontology:** in progress via OntoBricks, not the hand-rolled GraphFrames design originally scoped here. 16 entities generated, manually completed to a fully-connected 25-relationship graph, mapping to backing columns underway. See §8.
- [ ] **Phase 7 — Context:** concept store, policies, Vector Search index. Not yet built — no `context.concept` table, no `policies.json`, nothing in `worldcupiq_catalog.context` beyond the empty schema. → `context/`
- [ ] **Phase 8 — Genie + Agent:** Genie space, agent tools + framework, MLflow eval. → `genie/`, `agent/`
- [ ] **Phase 9 — App + Orchestration:** Streamlit app, Databricks Workflow. → `app/`, `workflows/`

---

## 14. The five demo questions

1. **"Rank the 48 teams by performance."** → semantic layer (Performance Dominance Score)
2. **"Why did Spain perform so well?"** → ontology traversal (Team → Match → Player → Event)
3. **"Was Spain the best team, or just the champion?"** → context layer (governed `best_team` policy + evidence)
4. **"Compare Spain's route to the final with Argentina's, adjusted for opponent strength."** → OntoBricks multi-hop ontology traversal + semantic opponent-strength metrics
5. **"If I were coaching Spain, what should I preserve and change?"** → agent combining semantic + ontology + context + evidence + reasoning

---

## 15. Prerequisites

- Databricks **Premium** workspace with Unity Catalog enabled (AI/BI Genie, Model Serving, Vector Search, Databricks Apps).
- ADLS Gen2 storage account + Access Connector for Databricks.
- Permissions to create catalogs, external locations, serving endpoints, and apps.
- Lakeflow Connect GitHub connection (`github-fifaworldcupiq`) — needs a GitHub **organization**
  (not personal account) and admin access to create the UC connection. Blocking Phase 2's live sync
  on this workspace; see §5.
- **For Phase 6 (Ontology) specifically:** [OntoBricks](https://github.com/databrickslabs/ontobricks)
  run locally (Python 3.12+, `uv`) against this workspace, plus a **Lakebase** (Postgres Autoscaling)
  project provisioned on the workspace for its domain registry (`databricks postgres create-project`)
  and a Databricks Model Serving / Foundation Model API endpoint for the Wizard's LLM calls. No
  `graphframes` — that was only needed by the abandoned hand-rolled ontology design.

---

## Notes for the build

- Keep source names generic; wire the exact public dataset URLs when you land Phase 1.
- The code fragments in `semantic/` and `app/` are illustrative blueprints — validate the Metric View YAML and the Genie SDK signatures against your workspace's runtime/SDK version. There is no `ontology/` directory (§8) — ontology is built in OntoBricks, a separate local application, not source files in this repo.
- Governance is the differentiator: every answer must be traceable to a governed Metric View and a source table via Unity Catalog lineage.
