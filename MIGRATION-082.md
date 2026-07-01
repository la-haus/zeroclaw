# Migración fork la-haus/zeroclaw → upstream v0.8.2

Base: `v0.8.2` (56b5a1f7). Re-aplicar nuestras mejoras (43 commits, fork-point v0.7.0-beta.1047) con veredicto por commit del análisis detallado. **Objetivo: no perder NADA de lo nuestro que upstream no cubra igual o mejor.**

Leyenda veredicto: **KEEP** = re-aplicar lo nuestro · **FUSE** = combinar con upstream · **ADOPT** = quedarnos con upstream (descartar el nuestro) · **DROP** = obsoleto/temporal.

## Familia 1 — Config + agent-core (fundacional)
| Commit | Qué | Veredicto | Destino v0.8.2 |
|---|---|---|---|
| 7a51381f | `excluded_builtin_tools` en AgentConfig (blocklist per-agent) | **ADOPT (REDUNDANTE, verificado)** — v0.8.2 `excluded_tools` per-agente (policy.rs:240, loop_.rs:492-500) excluye cualquier built-in del registry+prompt. CX usa `excluded_tools`. NO portar. | — |
| 0c35dca7 | feature `agent-core` (build mínimo API-only) | **KEEP** | Cargo.toml features |
| ff857bdf | resolve provider name from model config | **ADOPT** (upstream `agent_provider_composite()` mejor) | — descartar |
| 6c9c3dbf | port mejoras v0.7.4 | **DROP** (obsoleto, nativo en v0.8.2) | — descartar |

## Familia 2 — Anthropic thinking
| Commit | Qué | Veredicto | Destino |
|---|---|---|---|
| d69669d0 | extended thinking + error_fallback_message | **FUSE** (adoptar thinking nativo upstream; KEEP error_fallback_message) | schema.rs + orchestrator/mod.rs |
| deac95f1 | temperature=1 con thinking | **ADOPT** (upstream idéntico, resolve_thinking:810) | — descartar |
| d85311cc | disable thinking si tool_choice fuerza tool | **KEEP — CRÍTICO** (upstream falta → 400 Bad Request) | anthropic.rs chat()+stream_chat() |
| 8dbf4018 | beta header thinking para auth API-key | **KEEP — CRÍTICO** (upstream solo OAuth → thinking no activa en prod) | anthropic.rs stream_chat() API-key branch |

## Familia 3 — Multimodal (todo aditivo; adoptar tests de imagen de upstream)
| Commit | Qué | Veredicto | Destino |
|---|---|---|---|
| 2477b68b | PDF support, resilient images, URL preservation | **KEEP** | multimodal.rs, anthropic.rs NativeContentOut::Document |
| 4bbef7d0 | CSV/text inline (no document block) | **KEEP** | multimodal.rs validate_document_mime |
| c3a58f03 | reclasificar doc→image por MIME | **KEEP** | multimodal.rs |
| 3e3c5920 | UnsupportedMime::hint | **KEEP** | multimodal.rs / providers error |
| d28fc029 | office-convert feature flag | **KEEP** | Cargo.toml + multimodal.rs |
| 52e0bda1 | office-convert binary-level feature | **KEEP** | workspace Cargo.toml |
| c8fc7d6e | User-Agent header en downloads | **KEEP** (upstream 403 silencioso sin él) | schema.rs build_runtime_proxy_client |
| 190f116c | prepare_messages_for_provider en turn/turn_streamed | **FUSE** (KEEP placement general + vision_route upstream) | agent.rs |
| a0050b95 | URL extraction + HEIC (parte multimodal) | **KEEP** | multimodal.rs inject_url_markers, mime_from_magic |
| 4dd11e47 | regex URL fix + oversized image (parte multimodal) | **KEEP** | multimodal.rs |

## Familia 4 — Structured output
| Commit | Qué | Veredicto | Destino |
|---|---|---|---|
| fdd47eda | output_schema forcing | **KEEP** | schema.rs, anthropic.rs, agent.rs, ws.rs |
| 890ee4e6 | output_schema_auto (reasoning cleanup) | **KEEP** | schema.rs ResolvedRuntime/AliasedAgentConfig, agent.rs |
| 89efef4b | cleanup threshold fix + output_schema_auto_prompt | **KEEP** | schema.rs, agent.rs |
| 277769ab | dedup en cleanup prompt | **KEEP** (parte cleanup) | agent.rs prompt |

## Familia 5 — Observabilidad (Datadog + LangSmith) — el stack es nuestro diferenciador
| Commit | Qué | Veredicto | Destino |
|---|---|---|---|
| aa3e9db4 | multi-endpoint OTEL + DatadogLogObserver | **KEEP — CORE** | schema.rs otel_endpoints; observability/datadog_log.rs (393 líneas, nuevo); otel.rs |
| 712d66f2 | correlación dd.trace_id (SharedTraceContext) | **KEEP — CRÍTICO** | datadog_log.rs, otel.rs |
| 56135b7d | session_id/message_id/input/output en traces | **FUSE** (adaptar a ObserverEvent de v0.8.2) | observability_traits.rs |
| 84fc226a | error.message en tool.call spans | **KEEP** | otel.rs |
| 5f18d073 | LangSmith OTEL compat mode | **KEEP — CRÍTICO** | otel.rs |
| 3f101fd7 | gen_ai.prompt/completion LangSmith | **KEEP** | otel.rs |
| 111addce | langsmith.usage_metadata | **KEEP** | otel.rs |
| 16b0f42c | tool.name + span name | **FUSE** (upstream ya tiene tool.name; KEEP span-name) | otel.rs |
| 8fc1419e | tool_use as response_content | **KEEP** | agent.rs |
| e46d5dbe | langsmith.metadata.* filtering | **KEEP** | otel.rs |
| 52d01785 | service.name span attribute | **KEEP** | otel.rs |
| d46ba9b2 | streaming tokens StreamEvent::Final | **FUSE** (adoptar StreamEvent::Usage de upstream + asegurar flujo) | model_provider.rs |
| 9f582de7 | ToolCallStart en WS path | **KEEP** (verificar si upstream ya emite) | agent.rs execute_tool_call |
| 9fddc52b | OTEL pipeline + ZEROCLAW_OTEL_TRACE_CONTENT | **FUSE** (singleton ya en upstream; KEEP OTEL_TRACE_CONTENT) | otel.rs, agent.rs |
| 74be8609 | diagnostic tracing temporal | **DROP** | — |

## Familia 6 — Gateway WS
| Commit | Qué | Veredicto | Destino |
|---|---|---|---|
| 4b899188 | user_id query param | **KEEP** | gateway/ws.rs, agent.rs |
| c61441d8 | TurnEvent::LlmCall (model/duration/previews en WS) | **KEEP** (upstream solo Usage interno) | zeroclaw-api agent.rs, ws.rs |
| 867a44af | structured attachments en WS | **KEEP** | ws.rs parsing |

## Familia 7 — Auto-compact + cleanup (orchestrator)
| Commit | Qué | Veredicto | Destino |
|---|---|---|---|
| a0050b95 | auto_compact_after_turns (parte orchestrator) | **KEEP** (upstream solo token-based) | schema.rs, orchestrator/mod.rs |
| 4dd11e47 | LRU turn counter (MAX_CONVERSATION_SENDERS=1000) | **KEEP** | orchestrator/mod.rs |
| 9b3c788c / 818e05e3 | per-connection observer + singleton + tool-forced cleanup | **FUSE/VERIFY** (singleton ya en upstream; KEEP tool-forced cleanup) | gateway/ws.rs, agent.rs |

## Estado de ejecución
- [ ] F1 Config + agent-core
- [ ] F2 Thinking
- [ ] F3 Multimodal
- [ ] F4 Structured output
- [ ] F5 Observabilidad
- [ ] F6 Gateway WS
- [ ] F7 Auto-compact + cleanup
- [ ] Build verde (feature set CX) + clippy -D warnings + tests
- [ ] PR → master + CI verde
- [ ] Auditoría subagente limpio: nada perdido
