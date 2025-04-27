# node_1_prompting.py
# 说明：此节点负责构造提示词，调用LLM并获取返回结果

import json
import os
from string import Template
from openai import OpenAI
# 这里示例使用第二种 LangChain 方式
# from langchain_nvidia_ai_endpoints import ChatNVIDIA

from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Get NGC_API_KEY
NGC_API_KEY = os.getenv("NGC_API_KEY")

if NGC_API_KEY is None:
    raise ValueError("NGC_API_KEY is not set in the .env file")

class PromptingNode:
    def __init__(self):
        # 日志输出：节点初始化
        print("[LOG] PromptingNode initialized.")

        # 手动列出可用的列名，以及工具函数列表。
        # 注意：实际中可根据需要动态生成或更复杂处理
        self.prompt_template = Template("""
        You are an assistant that MUST translate a user's natural‑language request
        into a raw JSON command describing a sequence of data‑processing steps.
        If the question restricts rows (e.g. “for Grinding jobs”), ALWAYS start with a select_rows action that applies that filter.

        JSON schema you MUST follow  ⬇︎
        {
          "actions": [
            {
              "function": "<one of: select_rows | sort_rows | calculate_average | calculate_mode | calculate_median>",
              "args": {
                // key–value pairs required by that function
              }
            },
            ...
          ]
        }

        ––––––––––––––––––––––––––––––––––––
        Tool functions and what they do
        ––––––––––––––––––––––––––––––––––––
        select_rows
            • Select rows by comparing one column with a value.
            • args = { "column": <str>, "condition": "<op1> <value1> [AND|OR] <op2> <value2>" }
              operators: ==, !=, <, <=, >, >=
              value may be number, string, or datetime (dd/mm/yyyy HH:MM).

        sort_rows
            • Sort current rows by a column.
            • args = { "column": <str>, "order": "asc" | "desc" }

        calculate_average
            • Return the arithmetic mean of a numeric column.
            • args = { "column": <str> }

        calculate_mode
            • Return the most common value of a column.
            • args = { "column": <str> }
        
        calculate_median
            • Return the median of a numeric column.
            • args = { "column": <str> }
            
        add_derived_column
            • Create a new column from a pandas‑style formula.
            • args = { "name": <str>, "formula": "<expr>" }
            • You may use the placeholder {last_scalar} to refer to the scalar returned by the most recent scalar function.
            • You may write {last_scalar} in the formula; it will be replaced by the scalar returned by the most recent scalar‑function step.

        filter_date_range
            • Keep rows whose datetime column is between start and end.
            • args = { "column": <str>, "start": <datetime>, "end": <datetime>,
                       "inclusive": "both|left|right|neither" }

        group_top_n
            • For each group, keep the first N rows after sorting (defaults to keep_all = true,
              so you usually don't need to pass it).
            • args = { "group_column": <str>, "sort_column": <str>,
                       "order": "asc|desc", "n": <int> }
        
        group_by_aggregate
            • Group rows by one column, then aggregate another column.
            • Supports derived metrics such as time deltas.
            • group_by_aggregate does NOT support covariance/correlation.
            • args = {
                "group_column": <str>,
                // EITHER:
                "target_column": <str>,
                // OR (for derived metric):
                "derived": {
                    "name": <str>,              # name for new column
                    "type": "timedelta",
                    ""end_col": "Actual_End",
                    "start_col": "Scheduled_End"
                    "unit": "seconds" | "minutes" | "hours"
                },
                "agg": "avg" | "sum" | "min" | "max" | "count" | "std" | "var"
                "keep_all": true | false
              }
        
        calculate_sum
            • Return the sum of a numeric column.
            • args = { "column": <str> }
        
        calculate_min / calculate_max
            • Return the minimum / maximum of a numeric column.
            • args = { "column": <str> }
        
        calculate_std
            • Return the standard deviation of a numeric column.
            • args = { "column": <str> }
        
        count_rows
            • Return the number of current rows.
            • args = {}   (no args needed)
        
        top_n
            • Keep only the first N rows after sorting a column.
            • args = { "column": <str>, "order": "asc" | "desc", "n": <int> }
        
        calculate_delay_avg
            • For completed jobs, return the average delay in seconds between Actual_End and Scheduled_End.
            • args = { "unit": "seconds|minutes|hours" (default seconds),
                       "abs": true|false (default false) }

        
        rolling_average
            • Rolling mean over a window.
            • args = { "column": <str>, "window": <int>, "group_by": <str?> }

        calculate_variance
        calculate_covariance
        calculate_correlation
        calculate_percentile
        calculate_failure_rate
            • Return ( #Failed / total ) per group_column.
            • args = { "group_column": <str> }
        
        ––––––––––––––––––––––––––––––––––––
        Columns available in the CSV
        ––––––––––––––––––––––––––––––––––––
        Job_ID · Machine_ID · Operation_Type · Material_Used · Processing_Time ·
        Energy_Consumption · Machine_Availability · Scheduled_Start ·
        Scheduled_End · Actual_Start · Actual_End · Job_Status · Optimization_Category

        ––––––––––––––––––––––––––––––––––––
        Example I/O
        ––––––––––––––––––––––––––––––––––––
        User input:
        "I need all jobs whose Optimization_Category is Low Efficiency AND
        Processing_Time ≤ 50, then sort them by Machine_Availability descending."

        Expected JSON output (RAW, no markdown):
        {
          "actions": [
            { "function": "select_rows",
              "args": { "column": "Optimization_Category", "condition": "== 'Low Efficiency'" }
            },
            { "function": "select_rows",
              "args": { "column": "Processing_Time", "condition": "<= 50" }
            },
            { "function": "sort_rows",
              "args": { "column": "Machine_Availability", "order": "desc" }
            }
          ]
        }

        ––––––––––––––––––––––––––––––––––––
        Reminder: if you need the scalar you just computed, insert {last_scalar} in your formula ,but ONLY if you actually calculated a scalar in a previous step.
        Now analyse the user's request below and output ONLY the JSON, no markdown, no explanations:
        \"\"\"$user_request\"\"\"
        """)

        # ============ 真正实例化 LLM ============ #

        self.client = OpenAI(
            base_url="https://integrate.api.nvidia.com/v1",
            api_key=os.getenv("NGC_API_KEY")    # 建议放环境变量
        )

    def run(self, user_input: str) -> str:
        # 日志输出：节点执行
        print("[LOG] PromptingNode running...")

        # 在此基于模板和用户输入构造 Prompt
        final_prompt = self.prompt_template.substitute(user_request=user_input)

        # ------ 调用 LLM（流式）------
        messages = [{"role": "user", "content": final_prompt}]
        llm_response = ""
        for chunk in self.client.chat.completions.create(
            model="meta/llama-3.1-70b-instruct",
            messages=messages,
            temperature=0.0,
            top_p=0.7,
            max_tokens=10240,
            stream=True
        ):
            delta = chunk.choices[0].delta
            if delta and delta.content:
                llm_response += delta.content
        # --------------------------------

        print(f"[LOG] LLM raw output:\n{llm_response}")
        return llm_response

