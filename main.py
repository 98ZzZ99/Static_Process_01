# main.py
# 说明：此文件是程序的入口，负责启动所有节点并管理数据流

import sys

# 如果实际安装了 LangGraph，请使用正确的导入方式
# from langgraph import Graph, Node

from node_0_preprocessing import PreprocessingNode
from node_1_prompting import PromptingNode
from node_2_execution import ExecutionNode


class Graph:
    """
    Mock class for demonstration. In a real scenario, import and use the actual LangGraph library.
    """
    def __init__(self):
        self.nodes = []
        self.edges = []

    def add_nodes(self, node_list):
        self.nodes.extend(node_list)

    def add_edge(self, from_node, to_node):
        self.edges.append((from_node, to_node))

    def run(self, start_node, input_data):
        """
        Simple BFS-like approach: Start at start_node, pass data along edges.
        In a real scenario, you'd use more sophisticated graph execution logic.
        """
        queue = [(start_node, input_data)]
        visited = set()

        while queue:
            node, data = queue.pop(0)
            if node in visited:
                # Prevent infinite loops
                continue
            visited.add(node)

            # Run the node, get output
            output_data = node.run(data)

            # Find next nodes
            next_nodes = [edge[1] for edge in self.edges if edge[0] == node]
            for nxt in next_nodes:
                queue.append((nxt, output_data))


def main():
    # 日志输出：程序启动
    print("[LOG] Program started. Building graph...")

    # 构建节点
    pre_node = PreprocessingNode()
    prompt_node = PromptingNode()
    exec_node = ExecutionNode()

    # 构建图
    graph = Graph()
    graph.add_nodes([pre_node, prompt_node, exec_node])
    graph.add_edge(pre_node, prompt_node)
    graph.add_edge(prompt_node, exec_node)

    # 日志输出：提示用户输入
    print("[LOG] Graph construction completed. Awaiting user input...")

    # 获取用户输入
    user_input = input("Please enter your request: ")

    # 运行图
    print("[LOG] Starting graph execution...")
    graph.run(start_node=pre_node, input_data=user_input)

    # 日志输出：程序执行完毕
    print("[LOG] Program execution finished.")


if __name__ == "__main__":
    # 从这里开始执行
    main()

