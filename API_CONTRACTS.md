# API Contracts

Synchronous internal service-to-service contracts. Each section corresponds to exactly one `sync` row of the Dependency Registry in `SYSTEM_DESIGN.md`.

General rules for every contract below:
- Internal endpoints are served under `/internal/*` and are not exposed through the public ingress.
- Authentication: per-edge service token in `Authorization: Bearer <token>`, provisioned through deployment secrets. Requests without a valid service token get `401`.
- Tracing: callers propagate `traceparent` (and `X-Trace-Id`) headers; providers continue the trace.
- Timeouts: callers use a 2 s timeout with at most one retry on `5xx`/timeout. All three operations are read-only checks, so retries are safe.
- Errors use `application/problem+json` with the `shared:ErrorResponse` body.
- Permission keys (used by API-001) are the closed set: `manage_community`, `manage_channels`, `manage_roles`, `create_invites`, `mute_members`, `kick_members`, `ban_members`, `manage_automod`, `view_audit_log`, `review_appeals`.

## Contract Registry

| Contract ID | Provider | Consumers | Operation |
|---|---|---|---|
| API-001 | `community_service` | `moderation_service` | `POST /internal/communities/{community_id}/authorization-checks` |
| API-002 | `community_service` | `voice_service` | `POST /internal/communities/{community_id}/channels/{channel_id}/voice-access-checks` |
| API-003 | `messaging_service` | `realtime_gateway` | `POST /internal/realtime/user-scopes` |

## Shared Schemas

### ErrorResponse

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["error"],
  "properties": {
    "error": {"type": "string", "description": "Stable machine-readable error code, snake_case."},
    "detail": {"type": "string", "description": "Human-readable explanation."}
  }
}
```

### VoiceChannelSummary

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["channel_id", "name", "capacity"],
  "properties": {
    "channel_id": {"type": "string", "format": "uuid"},
    "name": {"type": "string", "minLength": 1, "maxLength": 100},
    "capacity": {"type": "integer", "minimum": 1, "maximum": 10}
  }
}
```

## API-001 — Community authorization check

Used by `moderation_service` before every moderator action (sanction issue/lift, appeal review, automod configuration change, audit-log read). It confirms that the actor holds the required permission and, when the action has a target, that the actor outranks the target in the community role hierarchy. Banned users who are no longer members are still reported through `target.is_banned` so that unbans and appeal reviews work.

| Field | Value |
|---|---|
| Provider | `community_service` |
| Consumers | `moderation_service` |
| Kind | REST |
| Operation | `POST /internal/communities/{community_id}/authorization-checks` |
| Auth | service token, `Authorization: Bearer` |
| Idempotency | not required (read-only check, safe to retry) |

Path parameters: `community_id` (uuid).

**Request** (`application/json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["actor_user_id", "permission"],
  "properties": {
    "actor_user_id": {"type": "string", "format": "uuid"},
    "permission": {
      "type": "string",
      "enum": ["manage_community", "manage_channels", "manage_roles", "create_invites", "mute_members", "kick_members", "ban_members", "manage_automod", "view_audit_log", "review_appeals"]
    },
    "target_user_id": {
      "type": ["string", "null"],
      "format": "uuid",
      "description": "Member the action applies to; null or omitted for non-targeted actions (automod config, audit-log read)."
    }
  }
}
```

**Response 200** (`application/json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["community_id", "actor_user_id", "permission", "allowed", "denial_reason", "actor_is_member", "actor_is_owner", "actor_permissions", "target"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "actor_user_id": {"type": "string", "format": "uuid"},
    "permission": {
      "type": "string",
      "enum": ["manage_community", "manage_channels", "manage_roles", "create_invites", "mute_members", "kick_members", "ban_members", "manage_automod", "view_audit_log", "review_appeals"]
    },
    "allowed": {"type": "boolean", "description": "True only when the actor is an active member, holds the permission, and (if a target is given) outranks the target."},
    "denial_reason": {
      "type": ["string", "null"],
      "enum": ["actor_not_member", "missing_permission", "target_outranks_actor", "target_is_owner", "target_is_self", null]
    },
    "actor_is_member": {"type": "boolean"},
    "actor_is_owner": {"type": "boolean"},
    "actor_permissions": {
      "type": "array",
      "uniqueItems": true,
      "items": {
        "type": "string",
        "enum": ["manage_community", "manage_channels", "manage_roles", "create_invites", "mute_members", "kick_members", "ban_members", "manage_automod", "view_audit_log", "review_appeals"]
      }
    },
    "target": {
      "type": ["object", "null"],
      "description": "Null when the request had no target_user_id.",
      "required": ["user_id", "is_member", "is_banned", "is_owner", "outranked_by_actor", "muted_until"],
      "properties": {
        "user_id": {"type": "string", "format": "uuid"},
        "is_member": {"type": "boolean"},
        "is_banned": {"type": "boolean"},
        "is_owner": {"type": "boolean"},
        "outranked_by_actor": {"type": "boolean"},
        "muted_until": {"type": ["string", "null"], "format": "date-time"}
      }
    }
  }
}
```

**Errors** — `400` (malformed body / unknown permission), `401` (missing or invalid service token), `404` (`community_not_found`), `500`, `503`, all `application/problem+json` referencing `shared:ErrorResponse`. A denied check is **not** an error: it returns `200` with `allowed: false` and a `denial_reason`.

## API-002 — Voice channel access check

Used by `voice_service` when a user asks to join a voice channel. It confirms active membership, that the channel exists and is a voice channel, and returns the channel capacity, the member's mute state (a muted member joins listen-only), and the community's voice channels so a full call can suggest an alternative.

| Field | Value |
|---|---|
| Provider | `community_service` |
| Consumers | `voice_service` |
| Kind | REST |
| Operation | `POST /internal/communities/{community_id}/channels/{channel_id}/voice-access-checks` |
| Auth | service token, `Authorization: Bearer` |
| Idempotency | not required (read-only check, safe to retry) |

Path parameters: `community_id` (uuid), `channel_id` (uuid).

**Request** (`application/json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["user_id"],
  "properties": {
    "user_id": {"type": "string", "format": "uuid"}
  }
}
```

**Response 200** (`application/json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["community_id", "user_id", "allowed", "denial_reason", "muted_until", "voice_channels"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "user_id": {"type": "string", "format": "uuid"},
    "allowed": {"type": "boolean", "description": "True when the user is an active, non-banned member and the channel is a voice channel. Mute does not deny access; it restricts publishing."},
    "denial_reason": {
      "type": ["string", "null"],
      "enum": ["not_a_member", "banned", "channel_not_found", "not_a_voice_channel", null]
    },
    "channel": {
      "$ref": "shared:VoiceChannelSummary",
      "description": "The requested voice channel; omitted when denial_reason is channel_not_found or not_a_voice_channel."
    },
    "muted_until": {
      "type": ["string", "null"],
      "format": "date-time",
      "description": "Non-null and in the future when the member is currently muted (listen-only)."
    },
    "voice_channels": {
      "type": "array",
      "description": "All voice channels of the community (including the requested one); empty when allowed is false.",
      "items": {"$ref": "shared:VoiceChannelSummary"}
    }
  }
}
```

**Errors** — `400` (malformed body), `401` (missing or invalid service token), `404` (`community_not_found`), `500`, `503`, all `application/problem+json` referencing `shared:ErrorResponse`. A denied access returns `200` with `allowed: false`.

## API-003 — Realtime subscription scopes

Used by `realtime_gateway` when a WebSocket connects (and on reconnect). It returns every community and private conversation the user may currently receive live events for. `messaging_service` answers from its local membership and conversation projections. After the connection opens, the gateway keeps scopes current from EVT-002 and EVT-008.

| Field | Value |
|---|---|
| Provider | `messaging_service` |
| Consumers | `realtime_gateway` |
| Kind | REST |
| Operation | `POST /internal/realtime/user-scopes` |
| Auth | service token, `Authorization: Bearer` |
| Idempotency | not required (read-only, safe to retry) |

**Request** (`application/json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["user_id"],
  "properties": {
    "user_id": {"type": "string", "format": "uuid"}
  }
}
```

**Response 200** (`application/json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["user_id", "community_ids", "conversation_ids", "generated_at"],
  "properties": {
    "user_id": {"type": "string", "format": "uuid"},
    "community_ids": {
      "type": "array",
      "items": {"type": "string", "format": "uuid"},
      "uniqueItems": true,
      "description": "Communities where the user is an active member (muted members included; removed/banned excluded)."
    },
    "conversation_ids": {
      "type": "array",
      "items": {"type": "string", "format": "uuid"},
      "uniqueItems": true,
      "description": "Private conversations (direct and group) where the user is a current participant."
    },
    "generated_at": {"type": "string", "format": "date-time"}
  }
}
```

**Errors** — `400` (malformed body), `401` (missing or invalid service token), `500`, `503`, all `application/problem+json` referencing `shared:ErrorResponse`. An unknown user returns `200` with empty lists.
