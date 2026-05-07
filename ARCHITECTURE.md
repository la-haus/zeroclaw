# ZeroClaw Architecture

Runtime de agentes autónomos en Rust. Este documento explica el flujo de ejecución completo sin asumir conocimiento de Rust.

## Crate Map

```
zeroclaw/
├── src/main.rs                  → CLI entry point (daemon, status, config)
├── crates/
│   ├── zeroclaw-api/            → Traits: Provider, Channel, Tool, TurnEvent
│   ├── zeroclaw-config/         → config.toml loading + schema validation
│   ├── zeroclaw-providers/      → LLM providers (Anthropic, OpenAI, Gemini, Bedrock)
│   │   ├── anthropic.rs         → Claude API (streaming, tools, extended thinking)
│   │   ├── multimodal.rs        → Image/PDF/Office pipeline ([IMAGE:] [DOCUMENT:] markers)
│   │   └── reliable.rs          → Fallback chain: primary → fallback models
│   ├── zeroclaw-runtime/        → Core agent execution
│   │   ├── agent/agent.rs       → Agent struct, turn(), turn_streamed()
│   │   ├── agent/loop_.rs       → Tool execution loop (run_tool_call_loop)
│   │   ├── agent/system_prompt.rs → System prompt assembly
│   │   ├── agent/dispatcher.rs  → Tool call parsing + dispatch
│   │   └── tools/               → Built-in tools (shell, file_read, http_request...)
│   ├── zeroclaw-gateway/        → HTTP + WebSocket server
│   │   ├── lib.rs               → Routes, webhook handler, auth, rate limiting
│   │   ├── ws.rs                → WebSocket /ws/chat handler + attachments
│   │   └── api.rs               → REST API (/api/status, /api/cron, /api/memory)
│   ├── zeroclaw-memory/         → Persistence (SQLite brain.db, vector search, FTS5)
│   ├── zeroclaw-channels/       → Integrations (Slack, Discord, Telegram, WhatsApp)
│   └── zeroclaw-tools/          → MCP tool support
```

## Conceptos Clave

### Turn (turno)

Un **turn** es una ejecución completa del agente: desde que recibe un mensaje del usuario hasta que produce una respuesta final. Un turn puede involucrar **múltiples LLM calls** si el agente necesita usar herramientas.

```
User message → [LLM call #1 → tool call → tool result → LLM call #2 → ...] → Final response
              └─────────────────── 1 turn ──────────────────────────────┘
```

### Session (sesión)

Una **session** es una conversación multi-turn. Cada session tiene un `session_id` (UUID) y persiste su historial de mensajes en SQLite.

```
Session "abc-123":
  Turn 1: user="Hola" → assistant="¡Hola! ¿En qué te ayudo?"
  Turn 2: user="¿Cuántas visitas tuve?" → [tool: snowflake query] → assistant="Tuviste 15 visitas"
  Turn 3: user="Envía eso a Slack" → [tool: slack_send] → assistant="Enviado ✅"
```

Cada turn ve TODO el historial previo de la sesión (hasta que se recorta por límite de tokens).

### Memory (memoria)

Separada de las sesiones. La memoria es **conocimiento persistente** que sobrevive entre sesiones (brain.db):

| Concepto | Alcance | Almacenamiento |
|---|---|---|
| **Session** | Una conversación (multi-turn) | `sessions` + `session_messages` tables |
| **Memory** | Conocimiento a largo plazo | `memories` table con embeddings + FTS5 |

La memoria usa búsqueda híbrida: **vector similarity** (embeddings) + **keyword search** (BM25 via FTS5).

## Flujo de Ejecución

### HTTP: POST /webhook → turn()

```
Client                          Gateway (lib.rs)                    Agent (agent.rs)
  │                                  │                                   │
  │  POST /webhook                   │                                   │
  │  {"message":"Hola"}              │                                   │
  │ ─────────────────────────────▶   │                                   │
  │                                  │ rate_limit_check()                │
  │                                  │ auth_check() (Bearer token)       │
  │                                  │ idempotency_check()               │
  │                                  │                                   │
  │                                  │  run_gateway_chat_simple()        │
  │                                  │ ─────────────────────────────▶    │
  │                                  │                                   │ agent.turn(&message)
  │                                  │                                   │   → build system prompt
  │                                  │                                   │   → append user message to history
  │                                  │                                   │   → prepare_messages_for_provider()
  │                                  │                                   │   → provider.chat(messages, tools)
  │                                  │                                   │   → [if tool calls: execute + loop]
  │                                  │                                   │   → return final response
  │                                  │  ◀─────────────────────────────   │
  │  {"response":"¡Hola!"}           │                                   │
  │ ◀─────────────────────────────   │                                   │
```

**Limitación HTTP**: `turn()` NO soporta herramientas en modo simple. Solo un LLM call sin tools. Para tool execution, usar WebSocket.

### WebSocket: /ws/chat → turn_streamed()

```
Client                          Gateway (ws.rs)                     Agent (agent.rs + loop_.rs)
  │                                  │                                   │
  │  WS upgrade                      │                                   │
  │  /ws/chat?session_id=abc         │                                   │
  │ ─────────────────────────────▶   │                                   │
  │                                  │ auth_check()                      │
  │                                  │ load_session("abc") from SQLite   │
  │                                  │ acquire session_queue lock        │
  │  ◀── {"type":"session_start",    │                                   │
  │       "session_id":"abc",        │                                   │
  │       "resumed":true,            │                                   │
  │       "message_count":42}        │                                   │
  │                                  │                                   │
  │  {"type":"message",              │                                   │
  │   "content":"¿Visitas?",         │                                   │
  │   "attachments":[...]}           │                                   │
  │ ─────────────────────────────▶   │                                   │
  │                                  │ append_attachment_markers()       │
  │                                  │   → [IMAGE:url] / [DOCUMENT:url]  │
  │                                  │                                   │
  │                                  │ agent.turn_streamed(&content, tx) │
  │                                  │ ─────────────────────────────▶    │
  │                                  │                                   │ seed_history(persisted_messages)
  │                                  │                                   │ build_system_prompt()
  │                                  │                                   │ prepare_messages_for_provider()
  │                                  │                                   │   → download images/PDFs
  │                                  │                                   │   → convert Office→PDF
  │                                  │                                   │   → validate MIME, base64
  │                                  │                                   │
  │                                  │                                   │ ┌── Tool Loop (max 10 iterations) ──┐
  │                                  │                                   │ │                                    │
  │  ◀── {"type":"chunk",            │ ◀── TurnEvent::TextDelta ─────── │ │ provider.stream_chat(messages)     │
  │       "content":"Esta semana"}   │                                   │ │   → streaming response             │
  │                                  │                                   │ │                                    │
  │  ◀── {"type":"tool_call",        │ ◀── TurnEvent::ToolCall ──────── │ │ [LLM requests tool: "shell"]       │
  │       "name":"shell",            │                                   │ │                                    │
  │       "args":{...}}              │                                   │ │ execute_tool("shell", args)        │
  │                                  │                                   │ │   → run command                    │
  │  ◀── {"type":"tool_result",      │ ◀── TurnEvent::ToolResult ────── │ │   → append result to history       │
  │       "output":"15 visitas"}     │                                   │ │                                    │
  │                                  │                                   │ │ [Next iteration: LLM sees result]  │
  │  ◀── {"type":"chunk",            │ ◀── TurnEvent::TextDelta ─────── │ │ provider.stream_chat(messages)     │
  │       "content":"Tuviste 15"}    │                                   │ │   → final response (no more tools) │
  │                                  │                                   │ └── Loop exits ─────────────────────┘
  │                                  │                                   │
  │  ◀── {"type":"done",             │ ◀── turn complete ─────────────── │
  │       "full_response":"..."}     │                                   │
  │                                  │ persist messages to SQLite         │
  │                                  │ release session_queue lock         │
```

### Diferencia clave: turn() vs turn_streamed()

| | `turn()` (HTTP) | `turn_streamed()` (WebSocket) |
|---|---|---|
| **Streaming** | No — espera respuesta completa | Sí — chunks en tiempo real |
| **Tool execution** | No — single LLM call | Sí — loop iterativo |
| **Session persistence** | Opcional (X-Session-Id) | Automática |
| **Eventos visibles** | Solo la respuesta final | tool_call, tool_result, chunk, done |
| **Uso** | Testing, integraciones simples | Producción, agentes con herramientas |

## Tool Execution Loop (loop_.rs)

El corazón del agente. Ejecuta iterativamente hasta que el LLM deja de pedir herramientas.

```
Input: user message + history + system prompt

for iteration in 0..max_tool_iterations (default 10):

    1. Context Management
       ├── Estimate token count of history
       ├── If > budget: trim oldest tool results (fast_trim)
       └── If still > budget: prune history (collapse/drop messages)

    2. Vision Routing
       └── If images present && provider doesn't support vision
           → Switch to vision_provider temporarily

    3. Multimodal Pipeline (prepare_messages_for_provider)
       ├── Parse [IMAGE:url] markers → download, validate MIME, base64
       ├── Parse [DOCUMENT:url] markers → download, convert Office→PDF, base64
       ├── Trim excess images from old messages (keep newest)
       └── Skip invalid/oversized with warning (non-fatal)

    4. LLM Call
       ├── Streaming: provider.stream_chat() → events via channel
       └── Non-streaming: provider.chat() → full response

    5. Parse Tool Calls
       ├── Native tools: response.tool_calls (OpenAI format)
       └── Non-native: parse XML <tool_call>{"name":"...","args":{...}}</tool_call>

    6. If no tool calls → EXIT LOOP (response is final)

    7. Execute Each Tool
       ├── Find tool spec in registry
       ├── Validate arguments
       ├── Execute (async)
       ├── Append ToolResult to history
       └── Emit TurnEvent::ToolResult

    8. Safety Checks
       ├── Loop detection (consecutive identical outputs)
       ├── Budget exhaustion (shared counter with subagents)
       └── Cancellation token (client disconnect)

    → Next iteration (LLM sees tool results, decides next action)

Output: final assistant response (last LLM output without tool calls)
```

## System Prompt Assembly

El system prompt se construye dinámicamente en cada turn:

```
┌─────────────────────────────────────────────────┐
│ 1. Tool Descriptions                            │
│    "Available tools: shell, file_read, ..."     │
│                                                 │
│ 2. Safety Guardrails                            │
│    Based on autonomy level (standard/full)      │
│                                                 │
│ 3. Workspace Bootstrap Files (max 20KB each)    │
│    ├── SOUL.md      → Personality, tone         │
│    ├── AGENTS.md    → Behavior rules, repos     │
│    ├── IDENTITY.md  → Name, emoji, metadata     │
│    ├── TOOLS.md     → Tool conventions          │
│    ├── MEMORY.md    → Curated memory index      │
│    └── USER.md      → User preferences          │
│                                                 │
│ 4. Skills                                       │
│    Full SKILL.md content for each loaded skill  │
│                                                 │
│ 5. Runtime Context                              │
│    Date, time, hostname, OS, model name         │
└─────────────────────────────────────────────────┘
```

## Multimodal Pipeline

Transforma archivos adjuntos en content blocks que el LLM puede procesar.

```
WhatsApp image → Chatwoot → conversations webhook
  → WS: {"attachments":[{"url":"https://...jpg","type":"image"}]}
    → ws.rs: append_attachment_markers()
      → content += " [IMAGE:https://...jpg]"

                    ┌──────────────────────────────────────┐
[IMAGE:url]    ──▶  │ prepare_messages_for_provider()       │
[DOCUMENT:url] ──▶  │                                      │
                    │  1. Parse markers from user messages  │
                    │  2. Download remote URLs              │
                    │  3. Detect MIME type                  │
                    │  4. Route by type:                    │
                    │     ├── image/* → base64, validate    │
                    │     │   ≤5MB → NativeContentOut::Image│
                    │     │   >5MB → skip with error detail │
                    │     ├── application/pdf → base64      │
                    │     │   → NativeContentOut::Document   │
                    │     ├── Office (docx/xlsx/pptx)       │
                    │     │   → office2pdf → PDF → Document │
                    │     └── text/csv, text/plain          │
                    │         → inline as text in message   │
                    │  5. Preserve original URLs as text    │
                    │  6. Trim old images if > max_images   │
                    └──────────────────────────────────────┘
```

**Formato que ve el modelo:**
```
USER_MESSAGE: Revisa estos archivos

Attachments:
- Attached image: https://chatwoot.lahaus.com/.../foto.jpg
- Attached document: https://chatwoot.lahaus.com/.../contrato.pdf

Skipped attachments (could not be processed):
- image https://...foto-grande.jpg: size limit exceeded 5472684 > 5242880 bytes

[IMAGE:data:image/jpeg;base64,/9j/4AAQ...]
[DOCUMENT:data:application/pdf;base64,JVBERi0...]
File content:
```
col1,col2,col3
data1,data2,data3
```
```

## Provider System

### Fallback Chain

```
config.toml:
  [providers]
  primary = "anthropic"
  fallback = "openai"

  [providers.anthropic]
  models = ["claude-sonnet-4-6", "claude-haiku-4-5"]

  [providers.openai]
  models = ["gpt-4o"]
```

Execution order when a call fails:
```
1. anthropic / claude-sonnet-4-6    → 400 error
2. anthropic / claude-haiku-4-5     → 400 error
3. openai / gpt-4o                  → success ✅
```

Each attempt: 1 retry for retryable errors (429, 500, 503), immediate skip for non-retryable (400, 401).

### stream_chat vs chat

| Method | Returns | Tool support | Used by |
|---|---|---|---|
| `chat()` | Full `ChatResponse` | Native tool_calls | `turn()` (HTTP) |
| `stream_chat()` | `Stream<StreamEvent>` | Native tool_calls via events | `turn_streamed()` (WS) |

`StreamEvent` types:
- `TextDelta(String)` — response chunk
- `ThinkingDelta(String)` — reasoning (extended thinking)
- `ToolCall { id, name, args }` — tool invocation
- `Usage { input_tokens, output_tokens }` — token counts

## Session Persistence (SQLite)

```
~/.zeroclaw/brain.db
  ├── sessions
  │   ├── id TEXT PRIMARY KEY
  │   ├── name TEXT
  │   ├── state TEXT (running/completed)
  │   └── last_updated TEXT
  │
  ├── session_messages
  │   ├── session_id TEXT → sessions.id
  │   ├── role TEXT (user/assistant/tool_call/tool_result)
  │   ├── content TEXT
  │   └── timestamp TEXT
  │
  ├── memories
  │   ├── id TEXT PRIMARY KEY
  │   ├── key TEXT UNIQUE
  │   ├── content TEXT
  │   ├── category TEXT (Conversation/Core/Procedural/Temporal)
  │   ├── embedding BLOB (vector)
  │   ├── created_at TEXT
  │   └── updated_at TEXT
  │
  ├── memories_fts (FTS5 virtual table)
  │   └── BM25 keyword search on memories.content
  │
  └── embedding_cache (LRU)
      └── content_hash → embedding vector
```

**Session flow:**
1. WS connect with `?session_id=abc`
2. Gateway loads `session_messages WHERE session_id = 'gw_abc'`
3. Agent receives history via `seed_history(messages)`
4. User message appended
5. Turn executes (may produce multiple assistant + tool messages)
6. All new messages persisted to `session_messages`
7. Next turn sees full history

## Gateway Routes

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/webhook` | Bearer/HMAC | Single-turn agent call (no tools) |
| `GET` | `/ws/chat` | Bearer/query/subprotocol | Multi-turn streaming with tools |
| `GET` | `/health` | None | Health check |
| `GET` | `/api/status` | Bearer | System status (model, memory, config) |
| `GET` | `/api/cron` | Bearer | List scheduled jobs |
| `POST` | `/api/cron` | Bearer | Create cron job |
| `GET` | `/api/sessions` | Bearer | List sessions |
| `GET` | `/api/memory` | Bearer | Search memory |
| `POST` | `/api/memory` | Bearer | Store memory |

**Auth methods** (in order of precedence):
1. `Authorization: Bearer <token>` header
2. `Sec-WebSocket-Protocol: bearer.<token>` subprotocol (WS only)
3. `?token=<token>` query param (WS only)

## Cron System

```
Cron jobs → stored in ~/.zeroclaw/cron.db
  ├── Agent jobs: execute a prompt through the agent loop
  └── Shell jobs: run an OS command directly

Scheduling:
  ├── Cron expression: "0 9 * * 1-5" (weekdays 9am)
  ├── Timezone: "America/Bogota"
  └── One-shot: "in 3600 seconds"

Execution:
  Background daemon checks due_jobs() every minute
  → Agent jobs: agent.run(prompt) → full turn with tools
  → Shell jobs: tokio::process::Command
  → Results: optional broadcast to channel (Slack/Discord)
```

## Config Reference (config.toml)

```toml
[agent]
model = "claude-sonnet-4-6"        # Default model
max_tool_iterations = 10           # Max tool calls per turn
context_token_budget = 180000      # Max tokens before history trimming

[providers]
primary = "anthropic"              # First provider to try
fallback = "openai"                # Fallback on failure

[providers.anthropic]
api_key = "sk-ant-..."
models = ["claude-sonnet-4-6", "claude-haiku-4-5"]

[gateway]
port = 42617                       # HTTP/WS port
pairing = false                    # Require pairing handshake
rate_limit_per_minute = 100        # Per-IP rate limit

[memory]
backend = "sqlite"                 # sqlite | qdrant | markdown | none
vector_weight = 0.7                # Hybrid search: vector vs keyword
keyword_weight = 0.3

[multimodal]
max_images = 10                    # Max images per turn
max_image_size_mb = 5              # Max image size (Anthropic limit)
allow_remote_fetch = true          # Download images from URLs

[autonomy]
level = "full"                     # full | standard | read-only

[tools]
profile = "full"                   # full | standard | read-only

[cron]
enabled = true
```

## Feature Flags

| Feature | Cargo flag | Effect | Binary size impact |
|---|---|---|---|
| `office-convert` | `--features office-convert` | DOCX/XLSX/PPTX → PDF via office2pdf | +10MB (~3.4MB → ~13MB) |
| `observability-otel` | `--features observability-otel` | OTLP export (Datadog/LangSmith) | +2MB |
| `agent-core` | `--features agent-core` | Core agent runtime | Base |

Default build for CX agents:
```bash
cargo install --no-default-features --features "agent-core,observability-otel,office-convert" \
  --git https://github.com/la-haus/zeroclaw.git --rev <commit>
```
