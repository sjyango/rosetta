---
name: 交互模式 RERUN 功能设计
overview: 在交互模式的模式选择界面添加 RERUN 选项，允许用户通过选择或输入 RUN ID 来重跑历史测试任务，复用现有的参数和测试文件。
todos:
  - id: extend-bench-json
    content: 扩展 _save_bench_json 函数，添加 bench_file 和 database 字段
    status: completed
  - id: add-rerun-mode
    content: 修改 _select_mode 函数，添加 RERUN 选项到 MODES 列表
    status: completed
    dependencies:
      - extend-bench-json
  - id: create-runid-selector
    content: 创建 _select_rerun_run_id 函数，实现 RUN ID 选择界面
    status: completed
  - id: handle-rerun-mode
    content: 在 _enter_interactive 中添加 rerun 模式处理逻辑
    status: completed
    dependencies:
      - add-rerun-mode
      - create-runid-selector
  - id: implement-rerun-execution
    content: 实现从历史记录加载参数并调用 _run_bench 的完整流程
    status: completed
    dependencies:
      - handle-rerun-mode
---

