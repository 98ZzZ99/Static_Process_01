# node_2_execution.py
# 说明：此节点负责解析 LLM 返回的 JSON 命令，并执行相应操作

import pandas as pd
import json
from tool_functions import load_data
from tool_functions import (
    # 行过滤 / 排序
    select_rows, sort_rows, top_n, group_top_n, group_by_aggregate, filter_date_range, rolling_average, add_derived_column,
    # 统计值
    calculate_average, calculate_mode, calculate_median,
    calculate_sum, calculate_min, calculate_max, calculate_std,
    calculate_delay_avg, calculate_variance, calculate_percentile, calculate_correlation, calculate_covariance,
    calculate_failure_rate, count_rows, calculate_delay_avg_grouped
)

class ExecutionNode:
    def __init__(self):
        # 日志输出：节点初始化
        print("[LOG] ExecutionNode initialized.")

        # DataFrame → DataFrame 类型的函数
        self.df_funcs = {
            "select_rows": select_rows,
            "sort_rows": sort_rows,
            "group_by_aggregate": group_by_aggregate,
            "top_n": top_n,
            "group_top_n": group_top_n,
            "filter_date_range": filter_date_range,
            "add_derived_column": add_derived_column,
            "rolling_average": rolling_average,
        }

        # DataFrame → 标量 的函数
        self.scalar_funcs = {
            "calculate_average": calculate_average,
            "calculate_mode": calculate_mode,
            "calculate_median": calculate_median,
            "calculate_sum": calculate_sum,
            "calculate_min": calculate_min,
            "calculate_max": calculate_max,
            "calculate_std": calculate_std,
            "calculate_variance": calculate_variance,
            "calculate_percentile": calculate_percentile,
            "calculate_correlation": calculate_correlation,
            "calculate_covariance": calculate_covariance,
            "calculate_delay_avg": calculate_delay_avg,
            "calculate_failure_rate": calculate_failure_rate,
            "count_rows": count_rows,
            "calculate_delay_avg_grouped": calculate_delay_avg_grouped,
        }

        from tool_functions import load_data
        self.orig_data = load_data()  # ★ 全量数据 ← 永远不变的“原汁原味”数据
        self.orig_data = load_data()
        self.last_scalar = None  # ① 初始化，给 last_scalar 先放个空值

    def run(self, llm_json_str: str):
        # 日志输出：节点执行
        print("[LOG] ExecutionNode running...")

        # 解析 LLM 返回的 JSON
        try:
            llm_data = json.loads(llm_json_str)
        except json.JSONDecodeError:
            print("[ERROR] Failed to decode JSON. Please check the LLM response.")
            return None

        # 验证是否包含 "actions"
        if "actions" not in llm_data:
            print("[ERROR] No 'actions' found in LLM response.")
            return None

        # 依次执行每个操作
        current_data = None                  # ★ 流水线数据
        for action in llm_data["actions"]:
            fname = action.get("function")
            args  = action.get("args", {})

            # -------- ② 如果有 {last_scalar} 就替换 --------
            if fname == "add_derived_column" and "{last_scalar}" in str(args):
                if "{last_scalar}" in json.dumps(args) and getattr(self, "last_scalar", None) is None:  # 占位符防御，在 raise 报错处改为早返回原 DF
                    print("[WARN] last_scalar not set; placeholder left untouched")
                    continue  # 跳过此 action，继续流水
                if self.last_scalar is None:
                    raise ValueError("No scalar available for {last_scalar}")
                # 把占位符换成字面量（字符串加引号，其余直接写数值）
                lit = (f"'{self.last_scalar}'"
                       if isinstance(self.last_scalar, str)
                       else str(self.last_scalar))
                args = json.loads(json.dumps(args).replace("{last_scalar}", lit))
            # ------------------------------------------------

            print(f"[LOG] Executing: {fname}  args={args}")

            if fname in self.df_funcs:           # DataFrame → DataFrame
                func = self.df_funcs[fname]
                current_data = func(current_data, args)


            elif fname in self.scalar_funcs:  # DataFrame → 标量，把最近一次得到的标量记下来
                func = self.scalar_funcs[fname]
                data_for_scalar = current_data if current_data is not None else self.orig_data
                result = func(data_for_scalar, args)
                self.last_scalar = result  # 存下来
                globals()["_LAST_SCALAR"] = result  # 提供给 add_derived_column
                # --------  新增  --------
                self.last_scalar = result  # 供后续步骤占位符替换
                # ------------------------
                print(f"[LOG] {fname} result: {result}")


            else:
                print(f"[ERROR] Unknown function: {fname}")

        # ---------- 结束后把结果展示出来 ----------
        if isinstance(current_data, pd.DataFrame):
            print("[LOG] Final DataFrame preview (first 10 rows):")
            print(current_data.head(10).to_string(index=False))


