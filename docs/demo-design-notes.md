# FIFA World Cup 2026 — AI-Ready Data Platform Demo (original design notes)

> **Provenance note:** this is the original demo design document (GCP / BigQuery / Looker /
> Gemini), preserved verbatim in structure for reference. The actual `worldcupiq` repo adapts
> this to an all-Databricks stack (Delta Lake, Lakeflow, Unity Catalog Metric Views, GraphFrames,
> Mosaic AI Agent Framework, AI/BI Genie, Databricks Apps) — see the top-level `README.md` for the
> as-built architecture. This file keeps the original narrative, demo script, and — critically —
> the **real candidate dataset links**, which the README currently keeps generic pending Phase 1.
>
> Source: `20260820 - FIFA World Cup 2026 Agent Demo.docx.pdf` (in Downloads). Hyperlink URLs
> below were extracted directly from the PDF's link annotations, not retyped from anchor text.

---

## 1. The demo concept

**WorldCupIQ — From Match Data to Football Intelligence**

Central question:

> "Why did Spain win the 2026 World Cup, and what evidence shows that their performance was
> sustainable rather than simply the result of a favorable knockout path?"

A dashboard can show scores, possession, goals, shots, xG, passing, player statistics — but an
AI-ready platform should **reason across all of them**. The agent should understand:

```
Spain → team → matches → opponents → venues → stages → players → tactical statistics →
goals → xG → historical context → tournament progression
```

That requires three layers: semantic, ontology, context.

---

## 2. The data sources

Use FIFA as the authoritative tournament source, supplemented by structured public datasets.

### Primary source — FIFA (official)

FIFA provides the official 2026 tournament schedule, fixtures, results, groups, knockout bracket,
venues, and match information — all 104 matches across 16 host cities. 103 of 104 matches provide
at least three days of rest (regionalisation / player-recovery design).

Candidate links (extracted from the PDF's link annotations — verify before use, `utm_source`
tracking params stripped):

- https://www.fifa.com/en/tournaments/mens/worldcup/canadamexicousa2026/articles/updated-fifa-world-cup-2026-match-schedule-now-available
- https://www.fifa.com/en/tournaments/mens/worldcup/canadamexicousa2026/articles/match-schedule-fixtures-results-teams-stadiums
- https://www.fifa.com/en/tournaments/mens/worldcup/canadamexicousa2026/articles/fifa-world-cup-26-match-schedule-revealed

### Structured match/player dataset — FIFA Training Centre-derived — **CONFIRMED**

The **analytical foundation** for the demo. Confirmed 2026-08-21 by opening the repo directly.

**`https://github.com/Alamyy/Worldcup26`** — 21 CSV tables, 104 matches, 48 teams, 1,277 players,
95,739 rows, 1.47M cells (exact match to the description below). Contains: `matches.csv`,
`teams.csv`, `players.csv`, `match_teams.csv`, `match_appearances.csv` (5,392 rows), lineups,
starters, substitutes, cards, goals, attempts/event timelines, passing-network edges (52,072
records), team stats (phases, set plays, pressure, aerial control, goal prevention, goalkeeper
distribution), and player physical/possession metrics. Includes a Data Dictionary, Schema Guide,
and Table Inventory.

> **Correction:** an earlier pass here tentatively guessed `mominullptr/FIFA-World-Cup-2026-Dataset`
> for this role by name-matching alone — that was wrong. It's actually `Alamyy/Worldcup26`; see below.

### Relational dataset (xG & VAR, complementary) — **CONFIRMED**

**`https://github.com/mominullptr/FIFA-World-Cup-2026-Dataset`** — 12 CSV files + a SQLite DB:
`teams.csv`, `venues.csv`, `tournament_stages.csv`, `referees.csv`, `matches.csv` (with xG),
`matches_detailed.csv`, `squads_and_players.csv` (1,248 players, market values/caps/clubs),
`match_events.csv` (goals, assists, cards, **VAR reviews** by minute), `match_team_stats.csv`,
`match_lineups.csv`, `player_stats.csv`, `match_prediction_features.csv` (65 ML features), plus
`real_match_details.json` and `sqlite_fifa_world_cup_2026.db`. Does **not** contain the advanced
tactical tables (passing networks, pressure, set plays, aerial control, goalkeeper distribution) —
confirming it's the complementary source, not the analytical foundation.

**Not a fit — drop from consideration:** `https://github.com/zvizdo/fifa-wc-2026-simulation` is a
Streamlit **simulation/forecasting dashboard** (Poisson/Dixon-Coles model, 100,000 simulated
tournaments), not a real match-statistics dataset. It cites external historical data (1930–2022)
rather than being a source itself. This was the third GitHub URL found in the PDF's link
annotations — it doesn't correspond to either the Training Centre or relational dataset citations;
likely just a tangential reference in the original doc.

**Recommendation for a production-quality demo:** `Alamyy/Worldcup26` as the analytical foundation;
FIFA's official pages as authoritative tournament metadata; `mominullptr/...` for its unique VAR
event data and xG values.

### Historical World Cups (context) — **CONFIRMED**

**`https://github.com/jfjelstul/worldcup`** — the Fjelstul World Cup Database. 22 men's tournaments
(1930–2022), 9 women's tournaments (1991–2019), 27+ datasets, 1.5M data points. Relevant tables:
`tournaments`, `teams`, `players` (10,401), `matches` (1,248), `goals` (3,637),
`tournament_standings`, `groups`/`group_standings`, `squads`. Available as `.csv`, `.json`,
`.RData`, and SQLite. Note: covers through 2022 only (predates 2026) — exactly right for a
"champion comparison over time" context source, since the 2026 tournament itself comes from the
primary FIFA/Training Centre sources above.

### Pre-match predictions (context) — **CONFIRMED**

Onside Arena — https://onsidearena.com/data — CC-BY-4.0 licensed (free for commercial/non-commercial
use, requires attribution link to onsidearena.com). Three datasets, direct CSV/JSON downloads:
- `/data/predictions.csv` — 72 rows × 15 cols: pre-match win/draw/away probability split + actual
  scoreline + verdict, updates live during the tournament
- `/data/champions.csv` — 48 rows × 10 cols: reach-round probabilities (R16/QF/SF/Final) per team,
  5,000-run Monte Carlo, refreshes hourly
- `/data/fixtures.csv` — 104 rows × 9 cols: full fixture list, venues, UTC kickoff times

Only `predictions.csv` maps to the README's `pre_match_prediction` bronze table
(`match_id, team, win_probability, draw_probability, loss_probability, prediction_timestamp,
model`); `champions.csv` and `fixtures.csv` are extra — fixtures overlaps with the FIFA official
source and isn't needed as a separate bronze table.

---

## 3. The architecture (original, GCP-flavored)

```
┌─────────────────────────────┐
│         BUSINESS USER        │
│      "Why did Spain win?"    │
└──────────────┬───────────────┘
               ▼
┌─────────────────────────────┐
│        GEMINI / AGENT        │
│   Reasoning + tools + SQL    │
└──────────────┬───────────────┘
               │
┌──────────────▼───────────────────────────────┐
│                CONTEXT LAYER                  │
│ Business definitions · Source provenance      │
│ Metric definitions · Tournament rules         │
│ Verified queries · Entity resolution          │
│ Historical context · Evidence                 │
└──────────────┬─────────────────────────────────┘
               ▼
┌────────────────────────────────────────────────┐
│                 SEMANTIC LAYER                  │
│ Team / Match / Player / Tactical / Tournament   │
│ Performance                                     │
│ xG | xGA | Possession | PPDA | Shot Conversion  │
│ Pressing | Progression                          │
└──────────────┬─────────────────────────────────┘
               ▼
┌────────────────────────────────────────────────┐
│                   ONTOLOGY                      │
│ Tournament → Stage / Group / Match              │
│              Match → Team → Player              │
│              Match → Venue, Events              │
└──────────────┬─────────────────────────────────┘
               ▼
┌────────────────────────────────────────────────┐
│                   BIGQUERY                      │
│           Bronze → Silver → Gold                │
│ matches, teams, players, events, lineups,       │
│ team_match_stats, player_match_stats,           │
│ passing_network                                 │
└──────────────┬─────────────────────────────────┘
               │
     ┌─────────┴─────────┐
     ▼                    ▼
┌───────────┐      ┌──────────────┐
│  Cloud    │      │  External    │
│  Storage  │      │  sources     │
│ CSV/JSON  │      │ FIFA Datasets│
└───────────┘      └──────────────┘
```

---

## 4. The BigQuery data model (original)

Deliberately relational physical model.

**Bronze** (`worldcup_raw`) — preserve source fidelity, not business-friendly:
`fifa_matches, fifa_teams, fifa_players, match_events, match_team_stats, match_player_stats,
lineups, substitutions, passing_network, venues`

**Silver** (`worldcup_silver`) — normalized:
`match, team, player, venue, match_event, match_team, match_player, player_event,
team_match_statistics, player_match_statistics, passing_edge`

Example `match`: `match_id, match_number, date, stage, group, venue_id, home_team_id,
away_team_id, home_score, away_score, home_xg, away_xg, winner_team_id`

Example `match_team`: `match_team_id, match_id, team_id, home_away, goals, xg, possession_pct,
shots, shots_on_target, passes, pass_completion_pct, recoveries, pressures, tackles,
aerial_duels, corners`

Example `match_player`: `match_player_id, match_id, team_id, player_id, minutes, goals, assists,
shots, xg, passes, progressive_passes, pressures, recoveries, duels, cards`

**Gold** (`worldcup_gold`) — business facts:
- Facts: `fact_match, fact_team_match, fact_player_match, fact_goal, fact_shot, fact_card,
  fact_substitution, fact_pass`
- Dims: `dim_tournament, dim_stage, dim_group, dim_team, dim_player, dim_venue, dim_country`
- Key table: `fact_team_tournament` — `team, matches, wins, draws, losses, goals_for,
  goals_against, goal_difference, points, xg, xga, possession, shots, shots_on_target,
  pass_completion, pressures, progressive_actions, set_piece_goals, open_play_goals, cards`

---

## 5. The semantic layer (original)

Don't expose `goals_for, goals_against, shots, passes, pressures, xg` as the primary business
vocabulary. Instead define:

| Metric | Formula |
|---|---|
| Team attacking efficiency | Goals / Expected Goals |
| Shot conversion | Goals / Shots |
| Shot quality | xG / Shots |
| Defensive efficiency | Goals Conceded / Opponent xG |
| Possession efficiency | Goals / Possession % |
| Chance creation | xG generated per 90 minutes |
| Pressing intensity | source-defined pressure metric — don't invent one |
| Tournament performance | weighted: results, goal difference, xG differential, opponent strength, knockout progression |

### Performance Dominance Score (original weights)

```
Performance Dominance Score =
    30% × Result Score
  + 25% × xG Differential Score
  + 15% × Shot Quality Score
  + 15% × Chance Creation Score
  + 15% × Defensive Efficiency Score
```

> Note: the actual `worldcupiq` README uses slightly different weights (30/25/15/15/15 split
> across Results/xG Differential/Shot Quality/Chance Creation/Defensive Efficiency — same shape,
> confirm exact figures against `README.md` §7, which is the governing source now).

Weights are explicitly governed — the agent must never invent them; the semantic layer owns them.

### BigQuery semantic view example

```sql
CREATE OR REPLACE VIEW `worldcup_semantic.team_performance` AS
SELECT
    team_id,
    team_name,
    COUNT(DISTINCT match_id) AS matches,
    SUM(goals_for) AS goals_for,
    SUM(goals_against) AS goals_against,
    SUM(xg_for) AS xg_for,
    SUM(xg_against) AS xg_against,
    SUM(xg_for) - SUM(xg_against) AS xg_differential,
    SUM(shots) AS shots,
    SAFE_DIVIDE(SUM(goals_for), SUM(shots)) AS shot_conversion,
    SAFE_DIVIDE(SUM(xg_for), SUM(shots)) AS shot_quality,
    AVG(possession_pct) AS average_possession,
    AVG(pass_completion_pct) AS pass_completion
FROM `worldcup_gold.fact_team_match`
GROUP BY team_id, team_name;
```

This view — not the raw event tables — is what the AI agent should query.

### Full semantic model

**Team Performance:** Matches, Wins, Draws, Losses, Goals, Goals Against, Goal Difference,
Expected Goals, Expected Goals Against, xG Differential, Shots, Shots on Target, Shot Conversion,
Shot Quality, Possession, Pass Completion, Progressive Actions, Pressing, Recoveries, Defensive
Actions, Performance Dominance Score

**Player Performance:** Appearances, Minutes, Goals, Assists, xG, Shots, Shot Conversion, Passes,
Progressive Passes, Pressures, Recoveries, Duels, Cards

---

## 6. The ontology (original)

```
Tournament
 ├── has_stage → Group Stage
 │                └── has_group → Group A
 ├── has_stage → Knockout Stage
 └── contains → Match

Match
 ├── played_at → Venue → located_in → City
 ├── involves → Team → has → Player
 │              └── represents → Country
 └── contains → MatchEvent
                 ├── Goal
                 ├── Shot
                 ├── Card
                 ├── Substitution
                 └── VAREvent

Player
 ├── plays_for → Team
 ├── participates_in → Match
 ├── produces → Event
 └── has_position → Position
```

### YAML representation

```yaml
entities:
  Tournament: { key: tournament_id }
  Stage: { key: stage_id }
  Group: { key: group_id }
  Team: { key: team_id }
  Player: { key: player_id }
  Match: { key: match_id }
  Venue: { key: venue_id }
  City: { key: city_id }
  MatchEvent: { key: event_id }
  Goal: { subtype_of: MatchEvent }
  Shot: { subtype_of: MatchEvent }
  Card: { subtype_of: MatchEvent }
  Substitution: { subtype_of: MatchEvent }

relationships:
  - Tournament HAS_STAGE Stage
  - Stage CONTAINS_GROUP Group
  - Group CONTAINS_TEAM Team
  - Tournament CONTAINS_MATCH Match
  - Match INVOLVES_TEAM Team
  - Match PLAYED_AT Venue
  - Venue LOCATED_IN City
  - Team HAS_PLAYER Player
  - Player PARTICIPATES_IN Match
  - Match CONTAINS_EVENT MatchEvent
  - Goal IS_A MatchEvent
  - Shot IS_A MatchEvent
  - Card IS_A MatchEvent
  - Substitution IS_A MatchEvent
```

This matches the entity/relationship list already in `README.md` §8 closely — the as-built
ontology is a direct descendant of this.

---

## 7. Semantic layer vs. ontology — the explicit distinction

- **Semantic layer** answers *"How do I measure team performance?"* — e.g. `Team xG differential
  = Team xG − Opponent xG`.
- **Ontology** answers *"What is a team and what does it participate in?"* — e.g. `Team →
  participates in → Match → played at → Venue → located in → City`.

> The ontology provides the conceptual graph. The semantic layer provides governed calculations
> over that graph.

---

## 8. The context layer

Resolves ambiguity in a question like *"Was Spain the best team at the 2026 World Cup?"* — "best"
could mean champion, most goals, highest xG, best defense, highest xG differential, strongest
opponents, most dominant performances, best player-level performance, or highest tournament
score.

### Context resolution trace (shown on screen during the demo)

```
USER QUESTION: "Was Spain the best team?"

Intent
 └── Evaluate tournament superiority

Candidate concepts
 ├── Champion
 ├── Results
 ├── Goal difference
 ├── xG differential
 ├── Opponent strength
 └── Match dominance

Semantic metrics
 ├── Points
 ├── xG differential
 ├── Shot quality
 ├── Defensive efficiency
 └── Performance Dominance Score

Ontology
 ├── Spain
 ├── Matches played
 ├── Opponents
 ├── Venues
 └── Tournament stages

Evidence
 ├── FIFA results
 ├── FIFA match data
 └── structured performance dataset
```

### Context objects (governed JSON)

```json
{
  "concept": "xg_differential",
  "definition": "Expected goals created by a team minus expected goals conceded",
  "grain": "team_match",
  "approved": true,
  "source": "World Cup match performance dataset",
  "related_entities": ["Team", "Match", "Shot"]
}
```

```json
{
  "concept": "best_team",
  "definition": "Team with highest overall tournament performance under the approved Performance Dominance Score",
  "policy": {
    "results": 0.30,
    "xg_differential": 0.25,
    "shot_quality": 0.15,
    "chance_creation": 0.15,
    "defensive_efficiency": 0.15
  }
}
```

The second object is the important one — it means the agent cannot casually redefine "best."
This maps directly to `context/policies.json` in the as-built repo.

---

## 9. The demo script — five killer questions

The recommended "wow" sequence for an executive/architecture demo, progressively increasing in
sophistication. (This matches `README.md` §14 almost exactly — that section is the governing
version; kept here for the full narrative framing.)

**Q1 — Semantic layer:** *"Rank the 48 teams by performance."* → demonstrates semantic metrics.

**Q2 — Ontology:** *"Why did Spain perform so well?"* → demonstrates Team → Match → Player →
Event relationships.

**Q3 — Context:** *"Was Spain actually the best team, or simply the champion?"* → demonstrates
business definition + governed metric + evidence.

**Q4 — Knowledge graph:** *"Compare Spain's route to the final with Argentina's and adjust for
opponent strength."* → demonstrates multi-hop graph reasoning.

**Q5 — Agent:** *"If I were coaching Spain, what should I preserve and what should I change based
on their 2026 tournament performance?"* → combines semantic metrics + ontology + context +
evidence + reasoning. This is **the AI-agent moment**.

### Extended demo beats (from the full script — useful detail beyond the five headline questions)

1. **Opening framing:** "The semantic layer tells the AI what our metrics mean. The ontology
   tells the AI what our football entities are and how they relate. The context layer tells the
   AI what information, definitions, evidence and policies it should use for a particular
   question."
2. **Scene 1 — Raw data:** show `match, team, player, score, shots, possession, passes, xG,
   events` and note there is no field called "dominance" or "best team" — those are business
   concepts that must be modeled.
3. **Scene 6 — Answer format** for the dominance question:
   ```
   Spain's Performance Dominance Score: XX
   Rank among 48 teams: #X
   Key contributors:
     xG differential: XX
     Shot quality: XX
     Chance creation: XX
     Defensive efficiency: XX
     Results: XX
   ```
   Every number must be traceable to a semantic metric — never hard-coded into the demo.
4. **Head-to-head comparison table** (Q4-style, Spain vs. Argentina):
   ```
                         Spain    Argentina
   Results Score          XX         XX
   Goal Difference         XX         XX
   xG Differential         XX         XX
   Shot Quality            XX         XX
   Chance Creation         XX         XX
   Defensive Efficiency    XX         XX
   Opponent Strength       XX         XX
   ---------------------------------------
   Dominance Score         XX         XX
   ```
5. **Passing-network demonstration** — visually spectacular: model player-to-player passing
   edges as a graph (`Player A ── Player B`, `Player A ── Player C`, `Player B ── Player C`),
   traversed via `Player → plays_for → Team → participates_in → Match`. Semantic layer adds
   Passes, Pass Completion, Progressive Passes, Network Centrality, Possession Chains. Context:
   *"How did Spain build attacks?"*
6. **Scene 10 — Trust / evidence:** ask *"Show me the evidence behind your conclusion"* and
   display source citations plus the full lineage chain:
   ```
   Source → Cloud Storage → BigQuery Bronze → BigQuery Silver → BigQuery Gold →
   Semantic Layer → Context Layer → AI Agent
   ```
   (In the Databricks build this becomes: Source → Volume → `bronze` → `silver` → `gold` →
   Metric View → Vector Search/Context → Mosaic AI Agent — see README §9 "Provenance".)
7. **Historical comparison caveat:** *"metrics from different eras may not be directly comparable
   if underlying event statistics aren't available at the same granularity"* — the context layer
   should carry this caveat explicitly when answering cross-era questions.
8. **Prediction/surprise analysis:** with `pre_match_prediction` loaded, ask *"Which matches
   produced the biggest prediction surprises?"* — the agent computes
   `Prediction → Actual Result → Surprise → Team → Tournament Impact`. This turns the platform
   from descriptive into **decision intelligence**.

### Closing framing (verbatim, still applicable to the Databricks build)

> "A semantic layer makes data understandable. An ontology makes the business world
> understandable. A context layer makes the knowledge usable by an AI agent at the moment of
> reasoning."

### Recommended strongest implementation

Make the demo **live, not slideware**: load the real datasets, build the semantic model and
ontology, then let the presenter ask the five questions while the UI simultaneously exposes the
generated semantic query, ontology traversal, context retrieved, source lineage, and final
answer — this is exactly what `README.md` §12 describes the Streamlit app doing (side-by-side:
resolved definition, generated semantic SQL + result, ontology traversal path, source lineage).

---

## 10. Mapping notes: original (GCP) → as-built (Databricks)

| Original (this doc) | As-built (`README.md`) |
|---|---|
| BigQuery Bronze/Silver/Gold | Delta Lake on ADLS via Unity Catalog, `worldcup.bronze/silver/gold` |
| Cloud Storage (CSV/JSON landing) | UC Volume `worldcup.bronze.landing`, Auto Loader ingestion |
| Looker Semantic Layer | Unity Catalog Metric Views (YAML) |
| Knowledge Catalog (glossary, assets, relationships) | Unity Catalog (glossary, lineage, certified assets) |
| Gemini / Agent | Mosaic AI Agent Framework on Model Serving + AI/BI Genie |
| (implicit graph reasoning) | GraphFrames + UC PK/FK constraints |
| — | Databricks Apps hosting Streamlit (same app concept as originally described) |

Dataset framing is unchanged: FIFA official (authoritative metadata) + FIFA Training
Centre-derived (analytical foundation) + relational dataset (xG/VAR complement) + historical
World Cups (context) + pre-match predictions (context, Onside Arena, CC-BY-4.0).
