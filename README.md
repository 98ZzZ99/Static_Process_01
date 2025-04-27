# Overview
This project implements a one-pass scheduling pipeline using a custom graph engine and a large language model (LLM).

# Directory & Core Files
| Role                  | File                         | Responsibility                                                                                 |
|-----------------------|------------------------------|------------------------------------------------------------------------------------------------|
| Entry & Graph         | `main.py`                    | Write own Graph to simulate node scheduling                                                    |
| Node – Preprocess     | `node_0_preprocessing.py`    | Temporarily empty                                                                              |
| Node – Prompting      | `node_1_prompting.py`        | Handwritten Prompt template → LLM outputs a JSON "action sequence"                             |
| Node – Execution      | `node_2_execution.py`        | Traverse actions in order, call `tool_functions.py`; maintain `current_data` & `last_scalar`   |
| Tool Library          | `tool_functions.py`          | Storage tool functions                                                                         |


# Runtime Data Flow
PreprocessNode ─→ PromptingNode ─→ ExecutionNode
    raw text        JSON plan          DataFrame / scalar

# Key Features
Minimal dependencies: Pure Python; no LangChain or LangGraph required.
Transparent pipeline: LLM’s JSON plan is logged verbatim for easy auditing and unit testing.
One-shot execution: No mid-run replanning or persistent session DataFrame.

# Tool Functions
Symbols:
✦ = DataFrame → DataFrame
■ = DataFrame → scalar
## 🛠 Tool Functions
| Name                         | Type | Description                                           | Example JSON                                                                                                 |
|------------------------------|------|-------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `select_rows`                | ✦    | Filter rows by column condition (supports AND/OR)     | `{ "function":"select_rows", "args":{"column":"Processing_Time","condition":"<= 50"} }`                       |
| `sort_rows`                  | ✦    | Sort current DataFrame                                | `{ "function":"sort_rows", "args":{"column":"Energy_Consumption","order":"desc"} }`                           |
| `top_n`                      | ✦    | Take top _n_ rows                                      | `{ "function":"top_n", "args":{"column":"Processing_Time","n":10} }`                                           |
| `group_top_n`                | ✦    | Top _n_ per group                                     | `{ "function":"group_top_n", "args":{"group_column":"Machine_ID","sort_column":"Processing_Time","n":2} }`    |
| `filter_date_range`          | ✦    | Filter by time window                                 | `{ "function":"filter_date_range", "args":{"column":"Scheduled_Start","start":"2023-03-18 10:00","end":"2023-03-18 12:00"} }` |
| `add_derived_column`         | ✦    | New column via pandas expression; supports `{last_scalar}` & time diffs | `{ "function":"add_derived_column", "args":{"name":"EE","formula":"Energy_Consumption / Processing_Time"} }`       |
| `rolling_average`            | ✦    | Global or grouped rolling mean                        | `{ "function":"rolling_average", "args":{"column":"Energy_Consumption","window":5} }`                          |
| `group_by_aggregate`         | ✦    | Group aggregate: avg/sum/min/max/count/std/var/percentile/cov/corr | `{ "function":"group_by_aggregate", "args":{"group_column":"Operation_Type","target_column":"Processing_Time","agg":"std"} }` |
| `calculate_average`          | ■    | Column mean                                           | `{ "function":"calculate_average", "args":{"column":"Processing_Time"} }`                                     |
| `calculate_median`           | ■    | Column median                                         | idem                                                                                                          |
| `calculate_mode`             | ■    | Column mode                                           | idem                                                                                                          |
| `calculate_sum`              | ■    | Column sum                                            | idem                                                                                                          |
| `calculate_min` / `calculate_max` | ■ | Column min / max                                     | idem                                                                                                          |
| `calculate_std` / `calculate_variance` | ■ | Column std / variance                            | idem                                                                                                          |
| `calculate_percentile`        | ■    | Global or grouped percentile                         | `{ "function":"calculate_percentile", "args":{"column":"Processing_Time","percentile":95} }`                    |
| `calculate_correlation` / `calculate_covariance` | ■ | Column correlation / covariance          | `{ "function":"calculate_correlation", "args":{"column1":"Processing_Time","column2":"Energy_Consumption"} }` |
| `count_rows`                  | ■    | Row count                                             | `{ "function":"count_rows", "args":{} }`                                                                       |
| `calculate_delay_avg`         | ■    | Avg. delay of completed jobs; unit selectable         | `{ "function":"calculate_delay_avg", "args":{"unit":"minutes"} }`                                              |
| `calculate_failure_rate`      | ■→DF | Failure rate per group                                | `{ "function":"calculate_failure_rate", "args":{"group_column":"Machine_ID"} }`                                |
| `calculate_delay_avg_grouped` | ✦    | Grouped avg. delay                                    | `{ "function":"calculate_delay_avg_grouped", "args":{"group_column":"Machine_ID","unit":"hours"} }`             |


# Known Issues & Required Fixes
1.Covariance for Operation_Type
LLM uses "agg":"covariance", but group_by_aggregate only recognizes cov or corr.
2.Rolling average placeholder and filtering
Placeholder {last_scalar} is skipped → column not created.
select_rows filtering uses incorrect column name; actual name is rolling_avg_Processing_Time.
Fix: Don’t insert {last_scalar} when no scalar exists; have ExecutionNode ignore unbound placeholders instead of throwing errors.
3.Complex grouped aggregation
Task: For each (Operation_Type, Machine_ID) pair, output job count and mean Energy_Consumption, keeping only groups with count ≥ 5.
Failures:
①Grouped only by Operation_Type.
②Aggregation result lacked Energy_Consumption, so add_derived_column couldn’t eval.
③Re-applying aggregation misaligned semantics.
Fix: Ensure grouping uses both columns; allow multi-metric aggregation in one call; preserve numeric columns in result.
