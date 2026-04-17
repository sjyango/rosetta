# MCP (Model Context Protocol) 架构概述

> 来源：https://modelcontextprotocol.io/docs/learn/architecture

## 一、MCP 的范围

| 项目 | 说明 |
|------|------|
| **MCP 规范** | 定义客户端和服务器的实现要求 |
| **MCP SDK** | 不同编程语言的 SDK 实现 |
| **MCP 开发工具** | 包括 MCP Inspector 等开发调试工具 |
| **MCP 参考服务器实现** | MCP 服务器的参考实现 |

> MCP 仅关注上下文交换的协议——不规定 AI 应用如何使用 LLM 或管理提供的上下文。

## 二、核心参与者

MCP 采用**客户端-服务器架构**，关键参与者：

| 角色 | 说明 |
|------|------|
| **MCP Host（宿主）** | AI 应用（如 Claude Desktop、VS Code），协调管理一个或多个 MCP 客户端 |
| **MCP Client（客户端）** | 维护与 MCP 服务器的连接，从服务器获取上下文供宿主使用 |
| **MCP Server（服务器）** | 向 MCP 客户端提供上下文的程序 |

示例：VS Code 作为 MCP Host，连接 Sentry MCP Server 时会实例化一个 MCP Client 对象；再连接本地文件系统服务器时，会再实例化另一个 MCP Client 对象。

- **本地 MCP 服务器**：使用 STDIO 传输，通常服务单个客户端
- **远程 MCP 服务器**：使用 Streamable HTTP 传输，通常服务多个客户端

## 三、分层架构

```
┌─────────────────────────────┐
│      传输层 (Transport)      │  ← 外层
│   通信机制、认证、消息帧      │
├─────────────────────────────┤
│       数据层 (Data)          │  ← 内层
│  JSON-RPC 协议、生命周期、    │
│       原语、通知              │
└─────────────────────────────┘
```

### 3.1 数据层

基于 **JSON-RPC 2.0** 的交换协议，包含：

| 组件 | 说明 |
|------|------|
| **生命周期管理** | 处理连接初始化、能力协商和连接终止 |
| **服务器特性** | 提供 tools（AI 动作）、resources（上下文数据）、prompts（交互模板） |
| **客户端特性** | 允许服务器请求 LLM 采样、用户输入、日志记录 |
| **实用特性** | 通知（实时更新）、进度跟踪（长时间操作） |

### 3.2 传输层

管理通信通道和认证，支持两种传输机制：

| 传输方式 | 特点 |
|----------|------|
| **Stdio 传输** | 使用标准输入/输出流，本地进程直接通信，无网络开销，性能最优 |
| **Streamable HTTP 传输** | 使用 HTTP POST + 可选 SSE 流式推送，支持远程通信，支持 Bearer Token、API Key、自定义 Header 等认证，推荐使用 OAuth |

> 传输层将通信细节从协议层抽象出来，使同一 JSON-RPC 2.0 消息格式适用于所有传输机制。

## 四、数据层协议详解

### 4.1 生命周期管理

MCP 是**有状态协议**，需要生命周期管理，核心目的是**协商双方支持的能力**。

### 4.2 核心原语

原语是 MCP 中**最重要的概念**，定义了客户端和服务器能提供什么。

#### 服务器原语（Server Primitives）

| 原语 | 说明 | 示例 |
|------|------|------|
| **Tools（工具）** | AI 应用可调用的可执行函数 | 文件操作、API 调用、数据库查询 |
| **Resources（资源）** | 为 AI 应用提供上下文信息的数据源 | 文件内容、数据库记录、API 响应 |
| **Prompts（提示）** | 帮助构建与语言模型交互的可复用模板 | 系统提示、Few-shot 示例 |

每个原语类型都有关联方法：
- **发现**：`*/list`（如 `tools/list`）
- **检索**：`*/get`
- **执行**：`tools/call`

#### 客户端原语（Client Primitives）

| 原语 | 方法 | 说明 |
|------|------|------|
| **Sampling（采样）** | `sampling/complete` | 允许服务器请求客户端 AI 应用的 LLM 补全，保持模型无关性 |
| **Elicitation（询问）** | `elicitation/request` | 允许服务器向用户请求额外信息或确认操作 |
| **Logging（日志）** | — | 允许服务器向客户端发送日志消息，用于调试和监控 |

#### 跨切面实用原语

| 原语 | 说明 |
|------|------|
| **Tasks（实验性）** | 持久执行包装器，支持延迟结果检索和状态跟踪（如昂贵计算、工作流自动化、批处理、多步操作） |

### 4.3 通知

- 服务器可向客户端发送**实时通知**（如工具列表变更通知）
- 遵循 JSON-RPC 2.0 通知语义，**不需要响应**
- 通知基于能力声明，仅在初始化时声明了 `listChanged: true` 才会发送

## 五、完整交互示例

### 步骤 1：初始化（能力协商握手）

**客户端发送初始化请求：**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "2025-06-18",
    "capabilities": {
      "elicitation": {}
    },
    "clientInfo": {
      "name": "example-client",
      "version": "1.0.0"
    }
  }
}
```

初始化过程的三大目的：
1. **协议版本协商** — `protocolVersion` 确保兼容性
2. **能力发现** — `capabilities` 声明支持的功能
3. **身份交换** — `clientInfo`/`serverInfo` 用于调试

初始化成功后，客户端发送就绪通知：
```json
{
  "jsonrpc": "2.0",
  "method": "notifications/initialized"
}
```

### 步骤 2：工具发现

**客户端请求工具列表：**
```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/list"
}
```

**服务器响应**：返回 `tools` 数组，每个工具对象包含 `name`、`title`、`description`、`inputSchema`。

### 步骤 3：工具执行

**客户端调用工具：**
```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "weather_current",
    "arguments": {
      "location": "San Francisco",
      "units": "imperial"
    }
  }
}
```

关键要素：
- `name` 必须与发现响应中的工具名完全匹配
- `arguments` 遵循工具的 `inputSchema` 定义
- 响应包含 `content` 数组，支持多种内容类型（文本、图像等）

### 步骤 4：实时通知

**服务器发送工具变更通知：**
```json
{
  "jsonrpc": "2.0",
  "method": "notifications/tools/list_changed"
}
```

通知特点：
- 无 `id` 字段，不需要响应
- 仅由声明了 `listChanged: true` 的服务器发送
- 事件驱动，基于内部状态变化

通知的重要性：
1. 适应动态环境（工具可能随时增删）
2. 避免轮询，效率更高
3. 保证客户端信息一致性
4. 实现实时协作能力

## 六、工作流程总结

```
1. 初始化 → AI 应用建立连接，存储服务器能力
2. 工具发现 → 从所有 MCP 服务器收集工具，构建统一注册表
3. 工具执行 → LLM 决定使用工具时，路由到对应 MCP 服务器执行
4. 通知处理 → 收到变更通知后刷新注册表，更新 LLM 可用能力
```

整个架构设计使 AI 应用能够**动态发现、调用和跟踪**外部工具与上下文，实现 LLM 与外部世界的实时、标准化交互。
