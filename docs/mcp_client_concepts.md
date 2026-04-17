# MCP 客户端概念

> 来源：https://modelcontextprotocol.io/docs/learn/client-concepts

## 一、核心概念：Host vs Client

| 角色 | 说明 |
|------|------|
| **Host（宿主）** | 用户直接交互的应用程序（如 Claude.ai、IDE），管理整体用户体验并协调多个客户端 |
| **Client（客户端）** | 协议层面的组件，每个客户端负责与**一个**服务器的一对一通信 |

## 二、客户端三大核心功能

客户端除了使用服务器提供的上下文外，还可以向服务器提供以下三项功能：

### 1. Elicitation（信息征集）

**定义：** 允许服务器在交互过程中向用户请求特定信息，提供结构化的按需信息收集方式。

**核心特点：**
- 服务器无需预先获取所有信息，可在需要时暂停操作请求用户输入
- 创建更灵活的交互模式，服务器可适应用户需求而非遵循刚性流程
- 使用 JSON Schema 定义请求结构

**请求示例（旅行预订确认）：**
```json
{
  "method": "elicitation/requestInput",
  "params": {
    "message": "Please confirm your Barcelona vacation booking details:",
    "schema": {
      "type": "object",
      "properties": {
        "confirmBooking": { "type": "boolean", "description": "Confirm the booking (Flights + Hotel = $3,000)" },
        "seatPreference": { "type": "string", "enum": ["window", "aisle", "no preference"] },
        "roomType": { "type": "string", "enum": ["sea view", "city view", "garden view"] },
        "travelInsurance": { "type": "boolean", "default": false }
      },
      "required": ["confirmBooking"]
    }
  }
}
```

**用户交互模型：**
- **请求呈现：** 客户端展示请求时包含清晰上下文（哪个服务器在询问、为何需要、如何使用）
- **响应选项：** 用户可提供信息、拒绝（附可选说明）、或取消整个操作；客户端根据 schema 验证响应
- **隐私保护：** 永不请求密码或 API 密钥；对可疑请求发出警告；用户可在发送前审查数据

### 2. Roots（根目录）

**定义：** 允许客户端指定服务器应关注的目录范围，通过协调机制传达预期作用域。

**核心特点：**
- 由文件 URI 组成，指示服务器可操作的目录
- **仅使用 `file://` URI 方案**，专指文件系统路径
- 根列表可动态更新，变更时通过 `roots/list_changed` 通知服务器

**根结构示例：**
```json
{
  "uri": "file:///Users/agent/travel-planning",
  "name": "Travel Planning Workspace"
}
```

**旅行规划工作区示例：**
- `file:///Users/agent/travel-planning` — 主工作区（所有旅行文件）
- `file:///Users/agent/travel-templates` — 可复用的行程模板和资源
- `file:///Users/agent/client-documents` — 客户护照和旅行文件

**设计哲学（重要）：**

> Roots 是**协调机制**，不是**安全边界**！

| 要点 | 说明 |
|------|------|
| 规范用语 | 使用 "SHOULD respect"（应当尊重），而非 "MUST enforce"（必须执行） |
| 原因 | 服务器运行的代码客户端无法控制 |
| 适用场景 | 受信任/已审查的服务器；用户了解其建议性质；目标是防止意外而非阻止恶意行为 |
| 擅长领域 | 上下文范围界定、意外预防、工作流组织 |
| 安全执行 | 实际安全必须在操作系统层面通过文件权限和/或沙箱强制执行 |

**用户交互模型：**
- **自动根检测：** 用户打开文件夹时，客户端自动将其暴露为根
- **手动根配置：** 高级用户可通过配置指定根，如添加模板目录同时排除含财务记录的目录

### 3. Sampling（采样）

**定义：** 允许服务器通过客户端请求 LLM 补全，在保持安全和用户控制的同时实现智能体（agentic）工作流。

**核心特点：**
- 服务器无需直接集成或支付 AI 模型费用
- 客户端完全控制用户权限和安全措施
- 采样请求在其他操作上下文中作为独立模型调用处理，维护清晰的上下文边界
- 多个人工介入（human-in-the-loop）检查点

**请求参数示例：**
```json
{
  "messages": [
    {
      "role": "user",
      "content": "Analyze these flight options and recommend the best choice:\n[47 flights...]\nUser preferences: morning departure, max 1 layover"
    }
  ],
  "modelPreferences": {
    "hints": [{ "name": "claude-sonnet-4-20250514" }],
    "costPriority": 0.3,
    "speedPriority": 0.2,
    "intelligencePriority": 0.9
  },
  "systemPrompt": "You are a travel expert helping users find the best flights...",
  "maxTokens": 1500
}
```

**航班分析工具示例：**
- 服务器工具 `findBestFlight` 查询航空公司 API 获取 47 个航班选项
- 通过采样请求 AI 分析复杂权衡（如便宜的红眼航班 vs 便捷的早班机）
- 工具利用分析结果展示前三推荐

**用户交互模型：**
- **审批控制：** 采样请求可能需要用户明确同意；用户可批准、拒绝或修改请求
- **透明性功能：** 客户端可显示精确提示词、模型选择和 token 限制；用户可在响应返回服务器前审查
- **配置选项：** 设置模型偏好、为可信操作配置自动批准、或要求全部人工审批；可编辑敏感信息
- **安全考量：** 双方须妥善处理敏感数据；客户端应实施速率限制并验证所有消息内容
- **核心保障：** 人工介入设计确保服务器发起的 AI 交互无法在无明确用户同意下危及安全或访问敏感数据

## 三、三大功能对比

| 功能 | 目的 | 安全控制 | 交互性质 |
|------|------|----------|----------|
| **Elicitation** | 服务器向用户请求结构化信息 | 客户端验证 + 隐私保护 | 服务器 → 用户 → 服务器 |
| **Roots** | 客户端向服务器传达文件系统边界 | 建议/协调性（非强制） | 客户端 → 服务器 |
| **Sampling** | 服务器通过客户端请求 LLM 补全 | 人工介入 + 速率限制 + 数据验证 | 服务器 → 客户端(LLM) → 用户审查 → 服务器 |
