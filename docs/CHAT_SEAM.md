# Chat seam — the per-chat SSE contract (for architecture B's `SessionDO`)

This is the stable boundary a per-chat front-end (the `metalcraft-do-cluster` `SessionDO`, a
hibernatable-WebSocket Durable Object) speaks to. It already exists in `workshop_api.rs`; S6 only
**formalizes and guarantees** it. Nothing here requires the caller to reconstruct agent state — the
container that runs `metalcraft-agent-r2` owns the live session; the front-end just relays.

## Endpoints (all under the OIDC/PAT-authed workshop API)

| Method | Path | Body | Response | Purpose |
|---|---|---|---|---|
| `POST` | `/api/v1/chats/{id}/turn` | `ChatTurnRequest` | **SSE** `text/event-stream` of `ChatEvent` | Run one user turn, stream it |
| `GET`  | `/api/v1/chats/{id}/events` | — | **SSE** of agent-initiated `ChatEvent` | Proactive/agent-initiated frames (gateway inbound, scheduled follow-ups) |
| `POST` | `/api/v1/chats` | `CreateChatRequest` | `ChatSummary` | Create a chat |
| `GET`  | `/api/v1/chats` / `/api/v1/chats/{id}` | — | list / one | Read |

`ChatTurnRequest`: `{ "message": string }`.

## `ChatEvent` frames (SSE, one JSON object per event)

Kind-tagged (`{"kind": "...", ...}`). Lifecycle of a turn:

```
turn_started
  → ( llm_started → llm_completed → (tool_started → tool_completed)* )*
  → done
```

- `turn_started` — `{ turn_index, user_message, session_id? }` (session_id deep-links diagnostics).
- `llm_started` / `llm_completed` — `{ messages: ChatMessageWire[], duration_ms }` (new assistant message(s)).
- `tool_started` — `{ tool_call_id, name, args }`.
- `tool_completed` — `{ tool_call_id, ... }` (the `ToolResult` appended to state).
- `done` — terminal `{ status, reason? }` (`status`: completed | failed).
- (error frames use `done` with `status: "failed"` so a client always gets one terminal frame.)

`ChatMessageWire` is the persisted message shape (`user`/`assistant`/`reasoning`/`tool_call`/`tool_result`).

## Guarantees B relies on

1. **Connection-stateless.** Every durable write goes through the `Store` port — `post_chat_turn`
   persists via `persist_chat`, which appends messages to the store (S3b). Dropping/reconnecting the
   SSE stream (or the SessionDO hibernating) loses **no** chat history; it lives in `agent.db`.
2. **Addressable per chat.** `{id}` is the chat id; the front-end keys its DO by `chat:{id}`.
3. **Single-flight per chat.** A second `/turn` while one is running returns `409` ("chat is already
   mid-turn"). The SessionDO surfaces that rather than racing.
4. **Idempotent history.** Because writes are append-by-seq (S3b), a retried turn never corrupts the
   transcript.

## How the `SessionDO` (architecture B) maps on

```
client ══WS══▶ SessionDO(chat:{id}) ──POST /pods/{slug}/api/v1/chats/{id}/turn──▶ AgentDO ▶ container
                       │  reads the SSE body, forwards each ChatEvent frame as a WS message
                       └─ holds only the socket; no durable state (history is in agent.db)
```

- **User message → turn:** SessionDO `POST …/turn` with `{message}`, pipes the SSE frames to the WS.
- **Proactive frames:** to deliver gateway/scheduled messages to a *connected* browser, a SessionDO
  subscribes to `GET …/events`. Note the hibernation trade-off: a long-lived `/events` fetch keeps the
  DO resident, so proactive-while-hibernated instead relies on the container waking the cell (future
  work); the durable record is in `agent.db` regardless and shows up on the next `/turn` or reload.

## Stability

Treat the frame `kind`s and the two SSE endpoints as the versioned contract. Additive `ChatEvent`
kinds are backward-compatible (unknown kinds should be ignored by clients); renaming/removing a kind
or changing the lifecycle ordering is a breaking change.
