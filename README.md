# WorldCupIQ — FIFA World Cup 2026 AI-Ready Data Platform on Databricks

> From match data to football intelligence. A governed knowledge system — **semantic layer + ontology + context layer** — that Genie and a Mosaic AI agent reason over, surfaced through a Streamlit Databricks App.

The platform answers deliberately hard questions like *"Was Spain actually the best team in the 2026 World Cup, or simply the champion?"* — not by summarizing a CSV, but by resolving intent against governed business definitions, traversing a football knowledge graph, computing governed metrics, and returning an answer **with source evidence and lineage**.

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
├── conf/
│   ├── 00_catalog.sql              # catalog, schemas, external location, volume
│   └── 01_grants.sql               # groups, service principal, grants
├── ingestion/
│   └── lakeflow/
│       ├── bronze.py               # Auto Loader → worldcup.bronze.* (1:1 raw)
│       ├── silver.py               # normalize → worldcup.silver.* + constraints
│       └── gold.py                 # star schema → worldcup.gold.*
├── semantic/
│   ├── dominance_components.sql    # gold.fact_team_dominance (normalized scores)
│   ├── team_performance.metricview.yaml
│   └── team_dominance.metricview.yaml
├── ontology/
│   ├── entities.sql                # worldcup.ontology.entity
│   ├── relationships.sql           # worldcup.ontology.relationship
│   ├── graph_build.py              # vertices + edges → GraphFrames
│   └── functions/
│       ├── tournament_path.sql     # UC function
│       └── key_players.sql         # UC function
├── context/
│   ├── concept_store.sql           # worldcup.context.concept
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
| **FIFA official** | schedule, fixtures, results, groups, knockout bracket, venues (104 matches, 16 cities) | authoritative tournament metadata & keys |
| **FIFA Training Centre-derived** | 21 tables · 104 matches · 48 teams · 1,277 players · lineups, events, passing networks, team/player stats, pressure, set plays | primary analytical foundation |
| **Relational dataset** | matches, squads, players, xG, events, team stats, VAR | complementary / xG & VAR |
| **Historical World Cups** *(context)* | tournaments, matches, teams, players, goals, standings (2014–2026) | champion comparison over time |
| **Pre-match predictions** *(context)* | win/draw/loss probabilities, model, timestamp | decision-intelligence / surprise analysis |

Land raw CSV/JSON in `worldcup.bronze.landing` (a UC Volume), organized by source.

---

## 6. Data model

### Catalog layout
`worldcup` catalog → schemas: `bronze` · `silver` · `gold` · `semantic` · `ontology` · `context`.

### Bronze (`worldcup.bronze`) — raw, 1:1 with source
`fifa_matches, fifa_teams, fifa_players, venues, match_events, match_team_stats, match_player_stats, lineups, substitutions, goals, cards, attempts, passing_network, pressure, set_plays, aerial_control, goal_prevention, gk_distribution, player_physical, squads, var_events, hist_tournaments, hist_matches, hist_teams, hist_players, hist_goals, hist_standings, pre_match_prediction`

### Silver (`worldcup.silver`) — normalized, entity-resolved
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

### Gold (`worldcup.gold`) — star schema
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
Component scores are min-max normalized across the 48 teams in `gold.fact_team_dominance`; the **weights live only in `semantic.team_dominance`** so the agent can never invent them. The context policy `best_team` points at this measure.

---

## 8. Ontology build (Databricks-native, no external graph DB)

1. **Declare** entity/relationship tables in `worldcup.ontology` (the machine-readable spec).
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

- **Datasets:** the Metric Views + `gold.fact_player_match` + dims (never bronze/silver event tables).
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

Deploy: `databricks apps deploy worldcupiq`. Grant the app service principal the Genie space, `SELECT` on `worldcup.semantic`/`gold`, `EXECUTE` on ontology functions, and query on the Vector Search index.

---

## 13. Build order (suggested milestones)

- [ ] **Phase 0 — Foundation:** workspace (Premium), UC metastore, ADLS + Access Connector, External Location, catalog `worldcup` + schemas + `landing` volume, groups/grants. → `conf/`
- [ ] **Phase 1 — Land sources:** drop all source CSV/JSON into the volume by source folder.
- [ ] **Phase 2 — Bronze:** Lakeflow pipeline, Auto Loader, 1:1 raw tables. → `ingestion/lakeflow/bronze.py`
- [ ] **Phase 3 — Silver:** normalize, entity-resolve, declare PK/FK, DQ expectations. → `silver.py`
- [ ] **Phase 4 — Gold:** star schema + `fact_team_tournament`. → `gold.py`
- [ ] **Phase 5 — Semantic:** `fact_team_dominance` + two Metric Views. → `semantic/`
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
