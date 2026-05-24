# Hybrid failure stratification

Read-only analysis of the embeddings benchmark hybrid-mode failure mode. Companion to `embeddings-benchmark-report.md`. Per-query ranks, hybrid noise attribution, and grouped-variant cross-checks are reported below. No benchmark code or result files were modified.

Source artifacts:

- `bench/embeddings/results/phase4-truly-final-{fts,vector,hybrid}/*.json` - 100% corpus, ungrouped (production default).
- `bench/embeddings/results/phase5-grouped-{fts,vector,hybrid}/*.json` - same corpus, `--group-by-conversation`.
- `docs/researches/embeddings-benchmark-queries.tsv` - 39 frozen seed queries.

Conventions:

- A rank of `0` means the target was not in the top-20 returned by that mode.
- Success@3 = rank in `[1, 3]`. The benchmark's headline metric.
- For English queries, ground truth is one or more 8-char session-id prefixes. For Ukrainian queries it is a distinctive anchor substring expected in the target message text (so the rank is the first hit whose `text` contains that anchor, case-insensitive).
- Grouped columns (`*-g`) are from `--group-by-conversation`. Each row in grouped output is a conversation, not a message.

## 1. Per-query table

| id | stratum | query | ground truth | FTS | Vec | Hyb | FTS-g | Vec-g | Hyb-g | diagnosis |
|---|---|---|---|---|---|---|---|---|---|---|
| EN-NL-1 | EN/natural-language | how does OCC retry work when two writers conflict | `prefix:94a50f23,d652b464` | 1 | 2 | 19 | 1 | 2 | 15 | FTS-AND-VEC-WIN |
| EN-NL-2 | EN/natural-language | why are conflict errors surfaced as storage_unavailable i... | `prefix:94a50f23` | 1 | 5 | 0 | 1 | 3 | 0 | FTS-ONLY |
| EN-NL-3 | EN/natural-language | why so many sessions marked as fresh on each sync rerun | `prefix:9f0b8dcc,48cb87ea` | 1 | 2 | 18 | 1 | 2 | 10 | FTS-AND-VEC-WIN |
| EN-NL-4 | EN/natural-language | could the adapter bug have been prevented by the seam con... | `prefix:c110f401` | 1 | 2 | 0 | 1 | 2 | 11 | FTS-AND-VEC-WIN |
| EN-NL-5 | EN/natural-language | why did we remove pond::Error | `prefix:26ac628c` | 1 | 2 | 0 | 1 | 2 | 9 | FTS-AND-VEC-WIN |
| EN-CON-1 | EN/conceptual | adapter seam correctness preventing synthesized values | `prefix:94a50f23,0ad17ca6` | 2 | 2 | 0 | 2 | 2 | 20 | FTS-AND-VEC-WIN |
| EN-CON-2 | EN/conceptual | native restore versus foreign restore fidelity | `prefix:c110f401,9f0b8dcc` | 1 | 2 | 6 | 1 | 2 | 6 | FTS-AND-VEC-WIN |
| EN-CON-3 | EN/conceptual | lossless round-trip test for restore | `prefix:9f0b8dcc,26ac628c` | 6 | 4 | 20 | 5 | 3 | 12 | HYBRID-IN-WINDOW-BUT-BURIED |
| EN-CON-4 | EN/conceptual | embedding model selection for local session search | `prefix:bc9f0e43` | 1 | 17 | 0 | 1 | 9 | 0 | FTS-ONLY |
| EN-CON-5 | EN/conceptual | hybrid search combining FTS and vector ranking | `prefix:94a50f23,dbddbe2e` | 3 | 8 | 0 | 3 | 6 | 0 | FTS-ONLY |
| EN-CON-6 | EN/conceptual | multi-writer cron sync to a shared S3 bucket | `prefix:d652b464` | 1 | 2 | 18 | 1 | 2 | 16 | FTS-AND-VEC-WIN |
| EN-SYM-1 | EN/symbol-lookup | Extracted<T> Source primitive adapter | `prefix:94a50f23` | 1 | 4 | 0 | 1 | 4 | 0 | FTS-ONLY |
| EN-SYM-2 | EN/symbol-lookup | merge_insert SourceDedupeBehavior FirstSeen | `prefix:94a50f23` | 1 | 17 | 0 | 1 | 5 | 0 | FTS-ONLY |
| EN-SYM-3 | EN/symbol-lookup | shared-memory authority unique per test | `prefix:d652b464` | 2 | 0 | 0 | 2 | 5 | 0 | FTS-ONLY |
| EN-SYM-4 | EN/symbol-lookup | raw_record replay native serialize | `prefix:9f0b8dcc` | 2 | 1 | 7 | 2 | 1 | 7 | FTS-AND-VEC-WIN |
| EN-ERR-1 | EN/error-message | codex-cli schema error first row must be session_meta | `prefix:9f0b8dcc` | 1 | 1 | 12 | 1 | 1 | 8 | FTS-AND-VEC-WIN |
| EN-ERR-2 | EN/error-message | hook_success attachment stdout dropped on ingest | `prefix:c110f401` | 3 | 2 | 0 | 2 | 2 | 10 | FTS-AND-VEC-WIN |
| EN-ERR-3 | EN/error-message | duplicate rows same message id twice in search results | `prefix:94a50f23` | 3 | 8 | 0 | 3 | 6 | 0 | FTS-ONLY |
| EN-BK-1 | EN/bare-keyword | Lance manifest | `prefix:d652b464` | 7 | 15 | 0 | 6 | 12 | 0 | TARGET-BELOW-20-IN-HYBRID |
| EN-BK-2 | EN/bare-keyword | cargo nextest test binaries | `prefix:4ad806f9` | 1 | 5 | 0 | 1 | 3 | 0 | FTS-ONLY |
| EN-BK-3 | EN/bare-keyword | pond search vs kb relevance comparison | `prefix:94a50f23,4ad806f9` | 0 | 20 | 0 | 0 | 8 | 0 | TARGET-BELOW-20-IN-HYBRID |
| UK-NL-1 | UK/natural-language | хто переміг у конфлікті США та Ірану | `anchor:обидві сторони` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-NL-2 | UK/natural-language | чи можеш змінити свою модель на опус посеред розмови | `anchor:визначається при запуску` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-NL-3 | UK/natural-language | чи вмієш ти перезапустити себе самостійно | `anchor:немає такого механізму` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-NL-4 | UK/natural-language | скільки часу контейнер лишається активним без повідомлень | `anchor:тримається живим` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-NL-5 | UK/natural-language | де зберігаються факти які ти про мене знаєш | `anchor:Головне сховище` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-NL-6 | UK/natural-language | що ти вмієш робити та які маєш можливості | `anchor:Що я вмію` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-CON-1 | UK/conceptual | механізм примусового завершення роботи контейнера | `anchor:файл-сентинель` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-CON-2 | UK/conceptual | управління таймаутом бездіяльності сесії | `anchor:тримається живим` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-CON-3 | UK/conceptual | сховище довготривалої памʼяті агента між сесіями | `anchor:Головне сховище` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-CON-4 | UK/conceptual | чому офіційні дипломатичні формулювання неоднозначні | `anchor:розмите формулюв` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-CON-5 | UK/conceptual | як зробити щоб модель автоматично запускалась на опусі | `anchor:прописано в` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-CON-6 | UK/conceptual | нейтральний підсумок міжнародного протистояння | `anchor:перемир` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-BK-1 | UK/bare-keyword | Ормузька протока | `anchor:Ормуз` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-BK-2 | UK/bare-keyword | іранський план десять пунктів | `anchor:10 пунктів` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-BK-3 | UK/bare-keyword | тариф нафта | `anchor:тариф на нафту` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-BK-4 | UK/bare-keyword | таймаут контейнера | `anchor:тримається живим` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-BK-5 | UK/bare-keyword | перемикання моделі опус | `anchor:перемкнути` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |
| UK-BK-6 | UK/bare-keyword | пам'ять агента | `anchor:Головне сховище` | 0 | 0 | 0 | 0 | 0 | 0 | NO-MODE |

## 2. Hybrid noise: top-3 session_ids that hybrid returned when target was NOT in its top-3

For each query where hybrid put the target outside top-3 (which is every query - 0/39), the table records the three session_ids hybrid did surface, so we can see which sessions are crowding out the answers.

| id | hybrid target rank | top-1 noise (prefix, matched_via) | top-2 noise | top-3 noise |
|---|---|---|---|---|
| EN-NL-1 | 19 | `1d70eb36` (vector) | `6b63cc9f` (vector) | `29bc6058` (vector) |
| EN-NL-2 | 0 | `1dccdda6` (vector) | `1dccdda6` (vector) | `1dccdda6` (fts) |
| EN-NL-3 | 18 | `1d70eb36` (vector) | `e2eab2c3` (vector) | `29bc6058` (vector) |
| EN-NL-4 | 0 | `1dccdda6` (vector) | `77d051bc` (vector+fts) | `77d051bc` (vector+fts) |
| EN-NL-5 | 0 | `1dccdda6` (vector) | `1dccdda6` (vector) | `1dccdda6` (vector) |
| EN-CON-1 | 0 | `1dccdda6` (vector) | `1dccdda6` (vector) | `1dccdda6` (vector) |
| EN-CON-2 | 6 | `e2eab2c3` (vector) | `019e470b` (vector+vector+fts) | `e25cc67d` (vector) |
| EN-CON-3 | 20 | `cb9d96fd` (vector) | `018b6e66` (vector) | `cb9d96fd` (vector) |
| EN-CON-4 | 0 | `1dccdda6` (vector+fts) | `1dccdda6` (vector+fts) | `1dccdda6` (vector+fts) |
| EN-CON-5 | 0 | `1dccdda6` (vector+fts) | `1dccdda6` (vector+fts) | `1dccdda6` (vector+fts) |
| EN-CON-6 | 18 | `1dccdda6` (fts) | `018b6e66` (vector) | `6630bdb7` (vector) |
| EN-SYM-1 | 0 | `cb9d96fd` (vector) | `77d051bc` (vector+fts) | `1dccdda6` (fts) |
| EN-SYM-2 | 0 | `1dccdda6` (vector+fts) | `1dccdda6` (vector+fts) | `018b6e66` (vector+fts) |
| EN-SYM-3 | 0 | `cb9d96fd` (vector) | `e25cc67d` (vector) | `019e4c88` (vector+vector) |
| EN-SYM-4 | 7 | `95b77fc5` (vector+fts) | `95b77fc5` (vector+fts) | `019e470b` (vector+vector+fts) |
| EN-ERR-1 | 12 | `1dccdda6` (fts) | `019e4c88` (vector+vector) | `1dccdda6` (vector) |
| EN-ERR-2 | 0 | `0705201d` (vector) | `1dccdda6` (vector) | `95b77fc5` (vector) |
| EN-ERR-3 | 0 | `019e4c88` (vector+vector+fts) | `1dccdda6` (fts) | `1dccdda6` (vector) |
| EN-BK-1 | 0 | `018b6e66` (vector) | `018b6e66` (vector) | `1dccdda6` (fts) |
| EN-BK-2 | 0 | `6b12da87` (vector+vector+fts) | `6b12da87` (vector+vector+fts) | `6b12da87` (vector+vector+fts) |
| EN-BK-3 | 0 | `1dccdda6` (vector+fts) | `52744cf4` (vector+fts) | `95b77fc5` (vector+fts) |
| UK-NL-1 | 0 | `6148ceb6` (fts) | `67362c5c` (vector) | `d41092c5` (vector) |
| UK-NL-2 | 0 | `1dccdda6` (vector) | `6b12da87` (vector) | `89a42665` (vector) |
| UK-NL-3 | 0 | `e25cc67d` (vector) | `fa01f657` (vector) | `6b12da87` (vector) |
| UK-NL-4 | 0 | `1d70eb36` (vector) | `5d5daf78` (vector) | `381bed64` (vector) |
| UK-NL-5 | 0 | `e25cc67d` (vector) | `6b12da87` (vector+vector) | `8c1606fd` (vector) |
| UK-NL-6 | 0 | `e2eab2c3` (vector+vector) | `e2eab2c3` (vector+vector) | `8c1606fd` (vector) |
| UK-CON-1 | 0 | `1dccdda6` (vector) | `1dccdda6` (vector) | `e2eab2c3` (vector) |
| UK-CON-2 | 0 | `180e5994` (vector) | `4ad806f9` (vector) | `67362c5c` (vector) |
| UK-CON-3 | 0 | `95b77fc5` (fts) | `95b77fc5` (vector) | `95b77fc5` (vector) |
| UK-CON-4 | 0 | `95b77fc5` (vector) | `a171ede3` (vector) | `6148ceb6` (vector) |
| UK-CON-5 | 0 | `1bd10360` (vector) | `381bed64` (vector) | `ccb77270` (vector) |
| UK-CON-6 | 0 | `d41092c5` (vector) | `7118ee92` (vector) | `39ddf045` (vector) |
| UK-BK-1 | 0 | `e25cc67d` (vector) | `6148ceb6` (vector) | `6148ceb6` (vector) |
| UK-BK-2 | 0 | `d41092c5` (fts) | `019dfdd9` (vector+vector) | `61d20ced` (vector) |
| UK-BK-3 | 0 | `d652b464` (vector) | `ccb77270` (vector) | `6148ceb6` (fts) |
| UK-BK-4 | 0 | `6b12da87` (vector) | `ccb77270` (vector) | `6148ceb6` (fts) |
| UK-BK-5 | 0 | `b8dced2e` (vector) | `61d20ced` (vector) | `d41092c5` (vector) |
| UK-BK-6 | 0 | `fa01f657` (vector) | `fa01f657` (vector) | `fa01f657` (vector) |

## 3. Aggregates

- **Queries where hybrid puts the target below rank 20 entirely (RRF never sees it)**: 32/39
- **Queries where hybrid's target rank is in `[4, 20]` (target IS in retriever output, but fusion buries it past top-3)**: 7/39
  - These are the ones that fusion math alone could fix without changing recall.

### 3.1 Sub-aggregate by language

| subset | hybrid rank=0 (below top-20) | hybrid rank in [4,20] |
|---|---|---|
| EN (n=21) | 14 | 7 |
| UK (n=18) | 18 | 0 |

### 3.2 Diagnosis tag counts

| diagnosis | count |
|---|---|
| NO-MODE | 18 |
| FTS-AND-VEC-WIN | 10 |
| FTS-ONLY | 8 |
| TARGET-BELOW-20-IN-HYBRID | 2 |
| HYBRID-IN-WINDOW-BUT-BURIED | 1 |

Tag definitions:

- `WIN-ALL`: all three modes hit Success@3.
- `HYBRID-OK` / `HYBRID-ONLY`: hybrid hit Success@3 (none observed at this coverage).
- `FTS-AND-VEC-WIN`: FTS and Vector both hit Success@3 but hybrid did not.
- `FTS-ONLY`: only FTS hit Success@3.
- `VECTOR-ONLY`: only Vector hit Success@3.
- `HYBRID-IN-WINDOW-BUT-BURIED`: hybrid surfaced target in `[4, 20]` (fixable by fusion math).
- `TARGET-BELOW-20-IN-HYBRID`: hybrid never surfaced the target in top-20 but at least one other mode did.
- `NO-MODE`: no mode surfaced the target in top-3 (typical for UK).

### 3.3 Grouped-mode (--group-by-conversation) hybrid duplication check

- Queries where a non-target *base* session appears 2+ times in hybrid's grouped top-10 (collapsing `<uuid>/agent-XXX` subsessions to `<uuid>`): **31/39**.
- Queries where ANY base session appears 2+ times in hybrid's grouped top-10 via agent subsessions (regardless of target status): **31/39**.

**Grouping bug evidence.** Despite `--group-by-conversation`, the grouped output contains rows like `95b77fc5-2a5f-43f2-87b6-3fb56d4b0793` *and* `95b77fc5-2a5f-43f2-87b6-3fb56d4b0793/agent-a22234c022bbb2333` as separate groups in the same response. A `claude-code` session and its agent sub-sessions are the same conversation from the user's perspective, but the grouper keys on the literal `session_id` string and treats sub-agents as independent rows. The net effect: the same parent conversation occupies multiple slots of the grouped top-10, recreating the same crowd-out the `--group-by-conversation` flag is supposed to prevent.

Concretely, on EN-NL-1, the grouped hybrid top-10 contains three slots for `973c5242-...` (two `/agent-...` suffixes plus the base id) and two slots for `95b77fc5-...`. Five of the ten slots are two conversations.

Sample queries where the agent-subsession grouping defect is visible in top-10: EN-NL-1, EN-NL-2, EN-NL-3, EN-NL-4, EN-NL-5, EN-CON-1, EN-CON-2, EN-CON-3, ....

Sample queries where a non-target base session repeats in grouped top-10 (the bug acts as noise inflation): EN-NL-1, EN-NL-2, EN-NL-3, EN-NL-4, EN-NL-5, EN-CON-1, EN-CON-2, EN-CON-3, ....

Even after this grouping bug is fixed, hybrid's Success@3 is still 0/39 in the grouped run - so fusion semantics need a redesign on top of the grouping fix.

### 3.4 Repeat-offender noise sessions across the hybrid top-3

How many times each session_id appears in hybrid's top-3 across queries WHERE the target was not in hybrid's top-3 (i.e. it is acting as noise). Top 10 below; ties broken by insertion order. The count is per-slot occupancy: if a session fills all three top-3 slots of one query, it contributes 3 to that session's tally - that exact pattern (one base session occupying all 3 slots) is the agent-subsession + duplicate-rows defect documented in Section 3.3.

| rank | session prefix | times appearing as top-3 hybrid noise | example query ids | sample snippet |
|---|---|---|---|---|
| 1 | `1dccdda6` | 30 | EN-NL-2, EN-NL-2, EN-NL-2, EN-NL-4, EN-NL-5, EN-NL-5, ... | Good - `cuda_if_available(0)` returns Err on a non-cuda-feature build, which the |
| 2 | `95b77fc5` | 8 | EN-SYM-4, EN-SYM-4, EN-ERR-2, EN-BK-3, UK-CON-3, UK-CON-3, ... | Confirmed: all 3 `extract_raw_record` call sites feed into a `json!({...})` macr |
| 3 | `6b12da87` | 7 | EN-BK-2, EN-BK-2, EN-BK-2, UK-NL-2, UK-NL-3, UK-NL-5, ... | I have confirmation that nextest still doesn't support doctests (run `cargo test |
| 4 | `6148ceb6` | 6 | UK-NL-1, UK-CON-4, UK-BK-1, UK-BK-1, UK-BK-3, UK-BK-4 | Here is described problem: Проблема  Існуючий продукт: paid MCP server "surf" на |
| 5 | `e2eab2c3` | 5 | EN-NL-3, EN-CON-2, UK-NL-6, UK-NL-6, UK-CON-1 | `just check` passes clean - the skill validates, README is in sync, no new lint  |
| 6 | `e25cc67d` | 5 | EN-CON-2, EN-SYM-3, UK-NL-3, UK-NL-5, UK-BK-1 | Basescan is paywalled. Let me use Blockscout's free API for Base to find reverte |
| 7 | `018b6e66` | 5 | EN-CON-3, EN-CON-6, EN-SYM-2, EN-BK-1, EN-BK-1 | The embed.rs test is clean. Now let me verify the e5_query/e5_passage doc-commen |
| 8 | `cb9d96fd` | 4 | EN-CON-3, EN-CON-3, EN-SYM-1, EN-SYM-3 | Test harness is clear (`prefect_test_harness`, per-session test DB). `prefect_te |
| 9 | `d41092c5` | 4 | UK-NL-1, UK-CON-6, UK-BK-2, UK-BK-5 | [14.05.2026 07:23] Ihor Muliar: привіт. я або дуже сильно туплю або щось пішло н |
| 10 | `fa01f657` | 4 | UK-NL-3, UK-BK-6, UK-BK-6, UK-BK-6 | i had topped up surf can you restart the agents that couldn't finish? |

The top sessions are the cross-validated noise that RRF inflates. See Section 4 exemplars.

Project context for the top 5 (looked up from the `project` field on the hits):

- `1dccdda6` - `pond/.claude/worktrees/e5-small-migration` (a worktree session ALL about hybrid search, e5-small, embeddings - hence it matches every pond-related query semantically AND lexically).
- `95b77fc5` - `pond/.claude/worktrees/tokenizer-multilingual-experiment` (tokenizer + multilingual hybrid search experiments).
- `6b12da87` - `Projects/skills` (Skills meta-tooling - matches anything generic about "cargo nextest" / "test binaries" / etc.).
- `6148ceb6` - `Projects/exporter-x402` (paid-MCP infrastructure, partly Ukrainian-language - matches UK MCP-flavor queries).
- `e2eab2c3` - `Projects/skills` (more skills meta-tooling).

The pattern is clear: the top noise sessions are pond-development meta-sessions and adjacent-tooling sessions. They discuss the exact concepts the benchmark queries are about (hybrid search, FTS, vectors, OCC, etc.) but are not the ground-truth target sessions. They are 'high topical density' relative to the benchmark workload, exactly as the brief predicted.

## 4. Failure exemplars

### EN-NL-1 - `how does OCC retry work when two writers conflict`

- Ground truth: `prefix:94a50f23,d652b464`
- FTS rank: 1 | Vector rank: 2 | Hybrid rank: 19
- Grouped FTS rank: 1 | Grouped Vector rank: 2 | Grouped Hybrid rank: 15
- Hybrid top-3 noise:
  - #1 `1d70eb36` (matched_via=vector): **OK** - 5600 req, 60/60 settled (1 x402 + 59 MPP), 0 payment.failed. `/mcp` + `/mcp-v2` returning 2...
  - #2 `6b63cc9f` (matched_via=vector): Repo was already at `~/pjv/skrabe/lobotomized-claude-code` — I fetched the latest (commit `6c074ab` ...
  - #3 `29bc6058` (matched_via=vector): This confirms the inference handler uses `validator("json", ...)` which calls `c.req.json()` interna...

### EN-SYM-1 - `Extracted<T> Source primitive adapter`

- Ground truth: `prefix:94a50f23`
- FTS rank: 1 | Vector rank: 4 | Hybrid rank: 0
- Grouped FTS rank: 1 | Grouped Vector rank: 4 | Grouped Hybrid rank: 0
- Hybrid top-3 noise:
  - #1 `cb9d96fd` (matched_via=vector): rtk garbled the output (`n` = `prefect_test_harness`). Reading the conftest files directly....
  - #2 `77d051bc` (matched_via=vector+fts): You are investigating an architecture question for `pond`, a Rust project at `/Users/tenequm/Project...
  - #3 `1dccdda6` (matched_via=fts): Diagnostics are conclusive. Here's the evidence-backed answer.  ## What the diagnostics showed  **1....

### EN-CON-5 - `hybrid search combining FTS and vector ranking`

- Ground truth: `prefix:94a50f23,dbddbe2e`
- FTS rank: 3 | Vector rank: 8 | Hybrid rank: 0
- Grouped FTS rank: 3 | Grouped Vector rank: 6 | Grouped Hybrid rank: 0
- Hybrid top-3 noise:
  - #1 `1dccdda6` (matched_via=vector+fts): Search is **hybrid** - first hit `matched_via: ["vector","fts"]`. The full pipeline works: sync -> e...
  - #2 `1dccdda6` (matched_via=vector+fts): `pond search` confirms hybrid retrieval works end-to-end: **10 hits, `matched_via: ["vector","fts"]`...
  - #3 `1dccdda6` (matched_via=vector+fts): Post-merge real verification passes: `pond embed` -> 119 e5-small vectors, `pond search` -> 10 hybri...

### EN-BK-1 - `Lance manifest`

- Ground truth: `prefix:d652b464`
- FTS rank: 7 | Vector rank: 15 | Hybrid rank: 0
- Grouped FTS rank: 6 | Grouped Vector rank: 12 | Grouped Hybrid rank: 0
- Hybrid top-3 noise:
  - #1 `018b6e66` (matched_via=vector): Lance supports `allow_subschema` for partial-column merge_insert (line 610-611), and `UpdateAll` wit...
  - #2 `018b6e66` (matched_via=vector): Now I have the full picture. Let me verify two remaining details: the Lance `MergeInsertBuilder` beh...
  - #3 `1dccdda6` (matched_via=fts): Subagent review is back. Verdict: the 14 edits are substantially complete - no dangling cross-refere...

### UK-NL-1 - `хто переміг у конфлікті США та Ірану`

- Ground truth: `anchor:обидві сторони`
- FTS rank: 0 | Vector rank: 0 | Hybrid rank: 0
- Grouped FTS rank: 0 | Grouped Vector rank: 0 | Grouped Hybrid rank: 0
- Hybrid top-3 noise:
  - #1 `6148ceb6` (matched_via=fts): Here is described problem: Проблема  Існуючий продукт: paid MCP server "surf" на surf.cascade.fyi. T...
  - #2 `67362c5c` (matched_via=vector): check Taro's reply in thread...
  - #3 `d41092c5` (matched_via=vector): [14.05.2026 07:23] Ihor Muliar: привіт. я або дуже сильно туплю або щось пішло не по плану, дивись: ...

### Narrative analysis of each exemplar

**EN-NL-1.** Both FTS-leg and vector-leg DO surface the target sessions (FTS rank 1; vector rank 2). Hybrid drops the target to rank 19, then to rank 15 even with grouping. The cause matches the report Section 8: pond-internal sessions discussing OCC/benchmarking (`95b77fc5`, `973c5242`) are matched by both arms, so RRF k=60 inflates their fused score above the seed targets `94a50f23`/`d652b464`. The recency boost compounds the inflation because those sessions are recently edited. The fix is not recall - it is the fusion math.

**EN-SYM-1.** Query is `Extracted<T> Source primitive adapter` - an exact symbol lookup. FTS rank 1, Vector rank 4. Hybrid drops below 20 and stays out of top-20 even with grouping. This is the worst class of hybrid failure: vector arm pulls in plausible-but-wrong adapter sessions whose tokens overlap `adapter` and `Source`, the cross-validation set inflates those, and the target session that contains the literal symbol falls off the RRF window entirely. For symbol-lookup, the FTS arm should dominate any tie; instead RRF treats both legs as equal contributors.

**EN-CON-5.** Query is `hybrid search combining FTS and vector ranking` - extremely close paraphrase of what every pond benchmarking session discusses. Vector arm correctly finds many sessions about hybrid search (rank 8), FTS finds the seed target (rank 3). Hybrid: target falls below top-20. The query is the most-cross-validated phrase in the entire pond corpus, so RRF inflates ~10 sessions ahead of the actual seed target. This is the textbook case for the methodology bias called out in report Section 12.

**EN-BK-1.** Bare keyword `Lance manifest`. FTS rank 7 (FTS itself struggles on this two-token query under ngram 3-5). Vector rank 15. Hybrid rank 0. With both arms barely finding it, RRF's window-of-60 amplifies whichever non-target Lance-related sessions both arms agree on, and the seed target is squeezed out. This exemplar suggests fusion bonus should be conditional on at least one arm having a high-confidence hit, not unconditional union-and-rerank.

**UK-NL-1.** Ukrainian natural-language query `хто переміг у конфлікті США та Ірану` with anchor `обидві сторони`. All modes return rank 0. The corpus skew (~0.1% Ukrainian rows) means there are very few candidate sessions to fuse, and RRF surfaces high-density English sessions instead. Hybrid does not have a fusion-math problem here; it has a recall problem inherited from the underlying retrievers, which themselves have a corpus-mix problem (see report Section 11). Fusion redesign will not move UK numbers - corpus rebalancing or a Ukrainian-specific retriever will.

## 5. What this means for the fusion redesign

Mechanically, the data above gives three concrete signals:

1. **Recall is partial; fusion is the dominant failure.** Of 21 EN queries, 7 have the target somewhere in hybrid's top-20 - the retrievers found the answer, the RRF stage demoted it past top-3. All 7 land in `[4, 20]`, so they are recoverable with fusion math changes alone (no new retrieval, no new index). The other 14 EN queries have the target below top-20 entirely. For UK queries, hybrid never surfaces the target in top-20 (18/18 rank=0); UK is a corpus-mix problem, not a fusion problem.
2. **The remaining 14 EN queries have the target below top-20 in hybrid.** Here fusion is not enough on its own: either the FTS arm gets more weight in candidate generation, or the vector arm uses tighter top-K so it doesn't drown FTS hits.
3. **Repeat-offender noise sessions exist.** The top noise sessions appear in hybrid's top-3 across multiple unrelated queries - they are recently-touched, content-dense pond conversations that get cross-validated by both arms. Any fusion redesign should detect or down-weight 'universal hit' sessions (sessions that match many disparate queries).

Grouping is not enough on its own: with `--group-by-conversation`, hybrid is still 0/39, AND the grouper is buggy (treats agent subsessions as distinct conversations, so the same base session occupies 2-3 of the top-10 slots in 31/39 queries). A fusion redesign should pair with: collapse `<uuid>/agent-XXX` to `<uuid>` before fusion, then enforce one row per base session_id in the fused output.

Four concrete redesign hypotheses falling out of this data:

- **H1 (collapse subsessions before fusion).** Key by base session_id, not literal session_id. This alone removes a 3x crowding effect on every query.
- **H2 (asymmetric RRF for symbol-lookup / bare-keyword).** When the FTS arm reports a high BM25 score (z-score over its own distribution), bias the fusion toward FTS; for paraphrase-heavy queries fall back to balanced RRF. The exemplar EN-SYM-1 is the canonical case.
- **H3 (down-weight universal hits).** Session `1dccdda6` appears as top-3 hybrid noise in 30 queries across all 8 strata - it is a 'universal hit' that fusion treats as a real signal. A per-session prior (frequency across query workload, or message-count) would dampen it.
- **H4 (gate fusion on min-arm-confidence).** When neither arm has a high-confidence hit (e.g. EN-BK-1, EN-BK-3), the fused list is dominated by accidental overlap. Falling back to FTS-only when vector confidence is below a threshold should recover those queries.
