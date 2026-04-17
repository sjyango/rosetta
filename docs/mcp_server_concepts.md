# MCP 服务器概念

> 来源：https://modelcontextprotocol.io/docs/learn/server-concepts

## 一、MCP 服务器概述

MCP 服务器是通过**标准化协议接口**向 AI 应用暴露特定能力的程序。常见示例：

| 服务器类型 | 功能 |
|---|---|
| 文件系统服务器 | 文档访问 |
| 数据库服务器 | 数据查询 |
| GitHub 服务器 | 代码管理 |
| Slack 服务器 | 团队通信 |
| 日历服务器 | 日程安排 |

## 二、三大核心功能模块

### 1. Tools（工具）— 模型控制

**定义：** LLM 可以主动调用、并根据用户请求自主决定何时使用的函数。

**特性：**
- 可以**写入**数据库、调用外部 API、修改文件或触发其他逻辑
- 使用 **JSON Schema** 进行输入/输出验证
- 每个工具执行**单一操作**，具有明确定义的输入和输出
- 可能需要**用户同意**后才能执行

**协议操作：**

| 方法 | 用途 | 返回值 |
|---|---|---|
| `tools/list` | 发现可用工具 | 包含 schema 的工具定义数组 |
| `tools/call` | 执行特定工具 | 工具执行结果 |

**示例工具定义：**
```json
{
  "name": "searchFlights",
  "description": "Search for available flights",
  "inputSchema": {
    "type": "object",
    "properties": {
      "origin": { "type": "string", "description": "Departure city" },
      "destination": { "type": "string", "description": "Arrival city" },
      "date": { "type": "string", "format": "date", "description": "Travel date" }
    },
    "required": ["origin", "destination", "date"]
  }
}
```

**用户交互模型（人为监督机制）：**
- 在 UI 中展示可用工具，用户可定义是否在特定交互中启用
- 针对单个工具执行的**审批对话框**
- 预批准安全操作的**权限设置**
- 显示所有工具执行及结果的**活动日志**

### 2. Resources（资源）— 应用控制

**定义：** 提供只读信息访问的被动数据源，为模型提供上下文。

**特性：**
- 每个资源有唯一 **URI**（如 `file:///path/to/document.md`）
- 声明 **MIME 类型**以正确处理内容
- 应用程序自主决定如何使用资源（选择相关部分、嵌入搜索、或全部传递给模型）

**两种发现模式：**

| 模式 | 说明 | 示例 |
|---|---|---|
| **Direct Resources（直接资源）** | 指向特定数据的固定 URI | `calendar://events/2024` |
| **Resource Templates（资源模板）** | 带参数的动态 URI，支持灵活查询 | `travel://activities/{city}/{category}` |

**协议操作：**

| 方法 | 用途 | 返回值 |
|---|---|---|
| `resources/list` | 列出可用的直接资源 | 资源描述符数组 |
| `resources/templates/list` | 发现资源模板 | 资源模板定义数组 |
| `resources/read` | 检索资源内容 | 带元数据的资源数据 |
| `resources/subscribe` | 监控资源变更 | 订阅确认 |

**资源模板示例：**
```json
{
  "uriTemplate": "weather://forecast/{city}/{date}",
  "name": "weather-forecast",
  "title": "Weather Forecast",
  "description": "Get weather forecast for any city and date",
  "mimeType": "application/json"
}
```

**参数补全功能：**
- 输入 "Par" → 建议提示 "Paris" 或 "Park City"
- 输入 "JFK" → 建议提示 "JFK - John F. Kennedy International"

**用户交互模式：**
- 树形/列表视图浏览资源
- 搜索和筛选界面
- 基于启发式或 AI 选择的自动上下文包含/智能建议
- 手动或批量选择界面

### 3. Prompts（提示）— 用户控制

**定义：** 预构建的指令模板，告诉模型如何与特定工具和资源协作。

**特性：**
- **用户控制**，需显式调用而非自动触发
- 支持参数化输入
- 可引用可用资源和工具，创建综合工作流
- 支持参数补全

**协议操作：**

| 方法 | 用途 | 返回值 |
|---|---|---|
| `prompts/list` | 发现可用提示 | 提示描述符数组 |
| `prompts/get` | 检索提示详情 | 包含参数的完整提示定义 |

**示例提示定义：**
```json
{
  "name": "plan-vacation",
  "title": "Plan a vacation",
  "description": "Guide through vacation planning process",
  "arguments": [
    { "name": "destination", "type": "string", "required": true },
    { "name": "duration", "type": "number", "description": "days" },
    { "name": "budget", "type": "number", "required": false },
    { "name": "interests", "type": "array", "items": { "type": "string" } }
  ]
}
```

**常见 UI 暴露方式：**
- 斜杠命令（输入 `/` 查看可用提示，如 `/plan-vacation`）
- 命令面板
- 专用 UI 按钮
- 上下文菜单

## 三、三大功能对比

| 特性 | Tools（工具） | Resources（资源） | Prompts（提示） |
|---|---|---|---|
| **本质** | 可调用的函数 | 只读数据源 | 指令模板 |
| **控制者** | 模型 | 应用程序 | 用户 |
| **操作性质** | 主动执行操作 | 被动提供上下文 | 结构化工作流引导 |
| **读写能力** | 可读写 | 只读 | N/A |
| **需用户同意** | 可能需要 | 不需要 | 需显式调用 |

## 四、多服务器协作流程示例

**场景：** 个性化 AI 旅行规划应用，连接三个服务器：
- **Travel Server** — 航班、酒店、行程
- **Weather Server** — 气候数据和预报
- **Calendar/Email Server** — 日程和通信

**完整工作流：**

```
步骤1: 用户调用提示（带参数）
  → plan-vacation, 目的地: Barcelona, 预算: $3000, 2人

步骤2: 用户选择资源
  → calendar://my-calendar/June-2024
  → travel://preferences/europe
  → travel://past-trips/Spain-2023

步骤3: AI 处理请求
  → 读取所有选中的资源获取上下文
  → 识别可用日期、偏好的航空公司和酒店类型、之前喜欢的地点
  → 执行工具：
     - searchFlights()  — 查询航班
     - checkWeather()   — 获取天气
     - bookHotel()      — 预订酒店（需用户批准）
     - createCalendarEvent() — 添加日历事件（需用户批准）
     - sendEmail()      — 发送确认邮件（需用户批准）
```

核心设计理念：通过 **Tools（执行）、Resources（上下文）、Prompts（引导）** 三大模块的标准化协作，实现 AI 应用与外部能力的灵活、安全、可控的集成。
