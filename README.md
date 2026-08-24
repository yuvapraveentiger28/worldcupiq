# WorldCupIQ — FIFA World Cup 2026 AI-Ready Data Platform on Databricks

> From match data to football intelligence. A governed knowledge system — **semantic layer + ontology + context layer** — that Genie and a Mosaic AI agent reason over, surfaced through a Streamlit Databricks App.

The platform answers deliberately hard questions like *"Was Spain actually the best team in the 2026 World Cup, or simply the Champion?"* — not by summarizing a CSV, but by resolving intent against governed business definitions, traversing a football knowledge graph, computing governed metrics, and returning an answer **with source evidence and lineage**.

---

## 1. Concept

A dashboard can show scores, possession, goals, shots, xG and player stats. WorldCupIQ makes those facts **reasonable over** by adding three governed layers on top of a Databricks medallion lakehouse:

| Layer | Answers | Databricks realization |
|---|---|---|
| **Semantic layer** | "What does this metric *mean*?" | Unity Catalog **Metric Views** |
| **Ontology** | "What is this entity and how does it *relate*?" | Ontology Delta tables + UC PK/FK + **GraphFrames** |
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
│ SEMANTIC & FEATURES           Metric Views · GraphFrames features · Context store│
├─────────────────────────────────────────────────────────────────────────────┤
│ KNOWLEDGE GRAPH               ontology tables + UC PK/FK spine + GraphFrames   │
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
- **Graph:** GraphFrames + UC PK/FK constraints + UC Python functions
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
├── databricks.yml                  # Asset Bundle — deploys 4 bronze pipelines (one per datasource)
├── conf/
│   ├── 00_catalog.sql              # schemas (incl. fifaworldcup_bronze_<datasource>) + volume in mrlc-catalog
│   └── 01_grants.sql               # groups, service principal, grants
├── ingestion/
│   └── lakeflow/
│       ├── bronze.py               # Auto Loader, shared by 5 pipelines (1 per datasource) → fifaworldcup_bronze_<datasource>.* (1:1 raw)
│       ├── silver.py               # normalize → mrlc-catalog.fifaworldcup_silver.* + constraints
│       ├── gold.py                 # star schema → mrlc-catalog.fifaworldcup_gold.* (column descriptions live in gold.py's _c() calls for source-level docs, but only reach UC via gold_comments.sql — see below)
│       └── gold_comments.sql       # COMMENT ON COLUMN for 89 Gold columns — re-run after any gold_pipeline --full-refresh-all (wipes them; a normal run doesn't)
├── semantic/                       # UC Metric View YAML — deployed to fifaworldcup_semantic (Phase 5)
│   ├── team_performance.metricview.yaml
│   ├── team_dominance.metricview.yaml
│   ├── match_performance.metricview.yaml
│   ├── player_match_performance.metricview.yaml
│   ├── goalkeeper_match_performance.metricview.yaml
│   └── comments.sql                # COMMENT ON COLUMN for 99 Metric View columns — re-run after any CREATE OR REPLACE VIEW on these (always wipes them)
├── ontology/
│   ├── entities.sql                # mrlc-catalog.fifaworldcup_ontology.entity
│   ├── relationships.sql           # mrlc-catalog.fifaworldcup_ontology.relationship
│   ├── graph_build.py              # vertices + edges → GraphFrames
│   └── functions/
│       ├── tournament_path.sql     # UC function
│       └── key_players.sql         # UC function
├── context/
│   ├── concept_store.sql           # mrlc-catalog.fifaworldcup_context.concept
│   ├── policies.json               # governed definitions (best_team, weights)
│   └── vector_index.py             # Vector Search index over concepts/policies
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
> already has both as real files. `fifaworldcup_bronze_fifa_official` and
> `bronze_pipeline_fifa_official` are removed accordingly.

Land raw CSV/JSON in `mrlc-catalog.fifaworldcup_landing.raw_files` (a UC Volume), one folder per bronze table
name — flat, no source subfolder. The source distinction lives at the schema level instead
(see Catalog layout below), not in the file path.

> **Ingestion mechanism:** Auto Loader (`cloudFiles`), not Lakeflow Connect. Lakeflow Connect's
> GitHub pipeline connector syncs a GitHub org's own metadata (`repositories`, `pull_requests`,
> `issues`, ...) into UC tables — it has no mechanism for landing arbitrary CSV/JSON files that
> happen to be committed inside a repo, which is what several of these sources are. Auto Loader
> reading from the landing volume is the correct fit here.

---

## 6. Data model

### Catalog layout
`mrlc-catalog` — a shared catalog (other projects live in it too) — with every WorldCupIQ schema
namespaced `fifaworldcup_<...>` to keep this project's objects grouped: `fifaworldcup_landing`
(Volume only, no tables) · four bronze schemas, one per datasource
(`fifaworldcup_bronze_fifa_training_centre` ·
`fifaworldcup_bronze_relational` · `fifaworldcup_bronze_historical` ·
`fifaworldcup_bronze_predictions`) · `fifaworldcup_silver` · `fifaworldcup_gold` ·
`fifaworldcup_semantic` · `fifaworldcup_ontology` · `fifaworldcup_context`. See `conf/00_catalog.sql`.

### Bronze — raw, 1:1 with source, split by datasource schema
| Schema | Tables |
|---|---|
| `mrlc-catalog.fifaworldcup_bronze_fifa_training_centre` | `fifa_teams, fifa_players, match_events, match_team_stats, match_player_stats, lineups, substitutions, goals, cards, attempts, passing_network, pressure, set_plays, aerial_control, goal_prevention, gk_distribution, player_physical` |
| `mrlc-catalog.fifaworldcup_bronze_relational` | `matches, venues, squads, var_events` |
| `mrlc-catalog.fifaworldcup_bronze_historical` | `hist_tournaments, hist_matches, hist_teams, hist_players, hist_goals, hist_standings` |
| `mrlc-catalog.fifaworldcup_bronze_predictions` | `pre_match_prediction` |

See `ingestion/lakeflow/bronze.py`'s `BRONZE_SOURCES` registry for the authoritative mapping.

> **Why four pipelines, not one:** a Lakeflow pipeline publishes to exactly one `catalog.schema`,
> set at the pipeline level — there's no supported per-table schema override. `databricks.yml`
> deploys `bronze.py` and `bronze_github_parse.py` as four separate pipeline resources
> (`bronze_pipeline_predictions` + 3 `parse_pipeline_<datasource>`), each with its own
> `catalog`/`schema` and a `datasource_filter` in its `configuration` block that tells the shared
> source file which subset of its registry to build. One scheduled job (`bronze_refresh`) triggers
> all four, plus the GitHub Connect raw landing they depend on, daily.

### Silver (`mrlc-catalog.fifaworldcup_silver`) — normalized, entity-resolved
| Table | Grain |
|---|---|
| `match` | one row per match (104) |
| `team` | one row per team (48) |
| `player` | one row per player (1,277) |
| `venue` | one row per stadium |
| `match_team` | team per match (208) |
| `match_player` | player per match |
| `match_event` | one row per event |
| `player_event` | event attributed to a player |
| `team_match_statistics` | team tactical stats per match |
| `player_match_statistics` | player tactical stats per match |
| `passing_edge` | directed passer→receiver edges (GraphFrames source) |

Declare **UC primary/foreign-key constraints** on silver — they double as the ontology's enforced spine.

Key column definitions:

```sql
-- silver.match
match_id PK, match_number, match_date, stage, group_name,
venue_id FK, home_team_id FK, away_team_id FK,
home_score, away_score, home_xg, away_xg, winner_team_id FK

-- silver.match_team
match_team_id PK, match_id FK, team_id FK, home_away,
goals, xg, possession_pct, shots, shots_on_target, passes,
pass_completion_pct, recoveries, pressures, tackles, aerial_duels, corners

-- silver.match_player
match_player_id PK, match_id FK, team_id FK, player_id FK,
minutes, goals, assists, shots, xg, passes, progressive_passes,
pressures, recoveries, duels, cards

-- silver.passing_edge
match_id FK, team_id FK, passer_id FK, receiver_id FK, passes, progressive, xt_added
```

### Gold (`mrlc-catalog.fifaworldcup_gold`) — star schema
**Facts:** `fact_match, fact_team_match, fact_player_match, fact_goal, fact_shot, fact_card, fact_substitution, fact_pass`
**Dimensions:** `dim_tournament, dim_stage, dim_group, dim_team, dim_player, dim_venue, dim_country`
**Aggregate (semantic foundation):** `fact_team_tournament`

```sql
-- gold.fact_team_tournament (one row per team per edition)
tournament_id FK, team_id FK, matches, wins, draws, losses,
goals_for, goals_against, goal_difference, points,
xg, xga, possession, shots, shots_on_target, pass_completion,
pressures, progressive_actions, set_piece_goals, open_play_goals, cards
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

None of these carry weighted/composite scores like Performance Dominance Score — they're governed vocabulary (consistent naming/formulas), not judgment calls, so there's nothing to keep out of the agent's hands the way the dominance weights are. `fact_pass` (passer→receiver edges) and `fact_match_event` (goals/cards/subs) stay as raw Gold facts, not Metric Views — they feed GraphFrames traversal (Phase 6) and the event timeline rather than aggregate measures.

**Real constraint found deploying these three:** UC Metric View joins are single-hop from `source` only — a join's `on:` clause can't reference another join's alias (confirmed by `UNRESOLVED_COLUMN` errors chaining `dim_tournament`/`dim_stage`/`dim_venue` off the already-joined `fact_match` alias). Since `tournament_id`/`stage_id`/`venue_id` live only on `gold.fact_match`, not on `fact_team_match`/`fact_player_match`/`fact_goalkeeper_match` directly, these three views expose those as raw IDs rather than resolved names (`tournament_year`, `stage_name`, `venue_name` were dropped from the deployed YAML). Resolve those in the BI tool or a downstream query instead.

---

## 8. Ontology build (Databricks-native, no external graph DB)

1. **Declare** entity/relationship tables in `mrlc-catalog.fifaworldcup_ontology` (the machine-readable spec).
2. **Anchor** each relationship to a UC PK/FK constraint on silver/gold (enforced spine).
3. **Materialize** vertices + typed edges from silver → wrap in **GraphFrames** for multi-hop traversal.
4. **Expose** useful walks as UC functions: `ontology.tournament_path(team)`, `ontology.key_players(team)`.
5. **Publish** the ontology spec (YAML) to a Volume and index it in Vector Search.

Entities: `Tournament, Stage, Group, Match, Team, Player, Venue, City, MatchEvent (Goal/Shot/Card/Substitution/VAREvent)`
Relationships: `HAS_STAGE, CONTAINS_GROUP, CONTAINS_TEAM, CONTAINS_MATCH, INVOLVES_TEAM, HAS_PLAYER, PARTICIPATES_IN, PLAYED_AT, LOCATED_IN, CONTAINS_EVENT, IS_A`

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

`app.py` calls the **Genie Conversation API** via `databricks-sdk`, and for each question shows side by side: resolved definition (Vector Search), generated semantic SQL + result, ontology traversal path (UC function), and source lineage. Questions 1–4 route to Genie; Question 5 ("coach Spain") routes to the **agent endpoint**.

Deploy: `databricks apps deploy worldcupiq`. Grant the app service principal the Genie space, `SELECT` on `mrlc-catalog.fifaworldcup_semantic` / `mrlc-catalog.fifaworldcup_gold`, `EXECUTE` on ontology functions, and query on the Vector Search index.

---

## 13. Build order (suggested milestones)

- [x] **Phase 0 — Foundation:** workspace (Premium), UC metastore, shared catalog `mrlc-catalog` (created, owned by other projects too). Schemas + `fifaworldcup_landing` volume, groups/grants → `conf/`
- [x] **Phase 1 — Land sources:** `predictions.csv` landed via Auto Loader; the 3 GitHub-sourced datasets sync automatically via Lakeflow Connect (no manual landing needed for those — `fifa_official` was dropped, see §5).
- [x] **Phase 2 — Bronze:** Lakeflow pipeline, Auto Loader, 1:1 raw tables. → `ingestion/lakeflow/bronze.py`
- [x] **Phase 3 — Silver:** normalize, entity-resolve, declare PK/FK, DQ expectations. → `silver.py` (28 tables, deployed and run against real bronze data; EAV-shaped fifa_training_centre stat tables — `team_key_stats`, `team_phases`, `team_set_plays`, `team_goalkeeping_distribution` — not yet pivoted into `match_team`, a follow-up once their metric label strings are sampled)
- [x] **Phase 4 — Gold:** star schema (6 dims + 10 facts) + `fact_team_tournament` + `fact_team_dominance` (governed Performance Dominance Score). → `gold.py` (16 flows deployed and run against real Silver data)
- [x] **Phase 5 — Semantic:** 5 Metric Views deployed to `mrlc-catalog.fifaworldcup_semantic` — `team_performance`, `team_dominance` (tournament grain), `match_performance`, `player_match_performance`, `goalkeeper_match_performance` (match grain). Verified live with `MEASURE()` queries against real Gold data. → `semantic/`
- [ ] **Phase 6 — Ontology:** entity/relationship tables, GraphFrames, UC functions. → `ontology/`
- [ ] **Phase 7 — Context:** concept store, policies, Vector Search index. → `context/`
- [ ] **Phase 8 — Genie + Agent:** Genie space, agent tools + framework, MLflow eval. → `genie/`, `agent/`
- [ ] **Phase 9 — App + Orchestration:** Streamlit app, Databricks Workflow. → `app/`, `workflows/`

---

## 14. The five demo questions

1. **"Rank the 48 teams by performance."** → semantic layer (Performance Dominance Score)
2. **"Why did Spain perform so well?"** → ontology traversal (Team → Match → Player → Event)
3. **"Was Spain the best team, or just the champion?"** → context layer (governed `best_team` policy + evidence)
4. **"Compare Spain's route to the final with Argentina's, adjusted for opponent strength."** → GraphFrames multi-hop + semantic opponent-strength metrics
5. **"If I were coaching Spain, what should I preserve and change?"** → agent combining semantic + ontology + context + evidence + reasoning

---

## 15. Prerequisites

- Databricks **Premium** workspace with Unity Catalog enabled (AI/BI Genie, Model Serving, Vector Search, Databricks Apps).
- ADLS Gen2 storage account + Access Connector for Databricks.
- `graphframes` library available on the cluster/pipeline.
- Permissions to create catalogs, external locations, serving endpoints, and apps.

---

## Notes for the build

- Keep source names generic; wire the exact public dataset URLs when you land Phase 1.
- The code fragments in `semantic/`, `ontology/`, and `app/` are illustrative blueprints — validate the Metric View YAML, the GraphFrames traversals, and the Genie SDK signatures against your workspace's runtime/SDK version.
- Governance is the differentiator: every answer must be traceable to a governed Metric View and a source table via Unity Catalog lineage.
