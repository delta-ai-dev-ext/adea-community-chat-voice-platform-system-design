# Event Contracts

Asynchronous internal contracts carried on Kafka. Each section corresponds to exactly one `async` row of the Dependency Registry in `SYSTEM_DESIGN.md`. A fan-out event is one contract that lists several consumers.

## Envelope (mandatory, every event)

Every message on every topic is a JSON object with exactly two top-level keys: `envelope` and `payload`.

| Field | Type | Required | Rule |
|---|---|---|---|
| `event_id` | string (uuid) | yes | Unique per event. Consumer deduplication key. |
| `event_type` | string (snake_case) | yes | Must match a registered contract. |
| `event_version` | integer | yes | >= 1. A breaking payload change bumps it. |
| `occurred_at` | string (RFC3339 UTC) | yes | Producer clock at emit time. |
| `producer` | string (snake_case) | yes | Must equal the registered producer. |
| `trace_id` | string (32 lowercase hex) | yes | **Never empty.** Propagated from the originating request; minted at the edge when absent. |
| `correlation_id` | string (uuid) | yes | Groups every event of one business flow. |
| `causation_id` | string (uuid) or null | yes | `event_id` of the causing event. `null` only for the flow's first event. |
| `schema_ref` | string | yes | `EVENT_CONTRACTS.md#<event_type>-v<version>`. |

Rules:
- The envelope is not optional and not per-topic. A topic needing a field the envelope lacks adds it to `payload`, never to the envelope.
- A payload schema MUST NOT declare an envelope field name at its top level.
- `trace_id` is mandatory even when no HTTP request originated the flow: the producer mints one at the edge of its own flow. This covers scheduled jobs such as mute expiry, LiveKit-webhook-triggered presence changes, and audit purge.
- When a consumer emits an event because of another event (for example, `community_service` emitting `community_member_changed` after applying `member_sanction_changed`), it copies `trace_id` and `correlation_id` from the causing event and sets `causation_id` to the causing `event_id`.
- The envelope is also mirrored into Kafka message headers (`trace_id`, `event_type`) for routing and observability. The JSON envelope remains the source of truth.
- Delivery is at-least-once. Every consumer dedupes on `envelope.event_id` and applies changes idempotently. Projections keep an entity version and ignore stale versions.

Envelope schema (the `envelope` object):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["event_id", "event_type", "event_version", "occurred_at", "producer", "trace_id", "correlation_id", "causation_id", "schema_ref"],
  "properties": {
    "event_id": {"type": "string", "format": "uuid"},
    "event_type": {"type": "string", "pattern": "^[a-z][a-z0-9_]*$"},
    "event_version": {"type": "integer", "minimum": 1},
    "occurred_at": {"type": "string", "format": "date-time"},
    "producer": {"type": "string", "pattern": "^[a-z][a-z0-9_]*$"},
    "trace_id": {"type": "string", "pattern": "^[0-9a-f]{32}$"},
    "correlation_id": {"type": "string", "format": "uuid"},
    "causation_id": {"type": ["string", "null"], "format": "uuid"},
    "schema_ref": {"type": "string", "pattern": "^EVENT_CONTRACTS\\.md#[a-z][a-z0-9_]*-v[0-9]+$"}
  }
}
```

## Contract Registry

| Contract ID | Event type | Version | Producer | Consumers | Topic |
|---|---|---|---|---|---|
| EVT-001 | `user_account_changed` | 1 | `identity_service` | `notification_service` | `identity_events` |
| EVT-002 | `community_member_changed` | 1 | `community_service` | `messaging_service`, `voice_service`, `realtime_gateway` | `community_events` |
| EVT-003 | `community_channel_changed` | 1 | `community_service` | `realtime_gateway`, `messaging_service`, `voice_service` | `community_events` |
| EVT-004 | `member_sanction_changed` | 1 | `moderation_service` | `community_service`, `notification_service` | `moderation_events` |
| EVT-005 | `automod_rules_changed` | 1 | `moderation_service` | `messaging_service` | `moderation_events` |
| EVT-006 | `appeal_status_changed` | 1 | `moderation_service` | `notification_service` | `moderation_events` |
| EVT-007 | `friend_request_changed` | 1 | `social_service` | `notification_service` | `social_events` |
| EVT-008 | `conversation_changed` | 1 | `social_service` | `messaging_service`, `realtime_gateway` | `social_events` |
| EVT-009 | `message_changed` | 1 | `messaging_service` | `realtime_gateway`, `notification_service` | `message_events` |
| EVT-010 | `voice_presence_changed` | 1 | `voice_service` | `realtime_gateway` | `voice_events` |
| EVT-011 | `notification_created` | 1 | `notification_service` | `realtime_gateway` | `notification_events` |

Consumer-group note: `realtime_gateway` consumes as a broadcast reader. Each replica reads all partitions independently from the latest offset and does not replay history. Every other consumer uses one shared consumer group per service.

## EVT-001 — `user_account_changed` v1

Emitted when an account's email is verified (first time it becomes reachable), its profile changes, its email changes, or it is deactivated. The payload is a full snapshot, so consumers upsert.

| Field | Value |
|---|---|
| Producer | `identity_service` |
| Consumers | `notification_service` |
| Topic | `identity_events` |
| Key | `user_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id`; apply only if `account_version` is newer |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["user_id", "change", "email", "username", "display_name", "email_verified", "status", "account_version"],
  "properties": {
    "user_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["email_verified", "profile_updated", "email_changed", "deactivated"]},
    "email": {"type": "string", "format": "email"},
    "username": {"type": "string", "minLength": 2, "maxLength": 32},
    "display_name": {"type": "string", "minLength": 1, "maxLength": 64},
    "email_verified": {"type": "boolean"},
    "status": {"type": "string", "enum": ["active", "deactivated"]},
    "account_version": {"type": "integer", "minimum": 1}
  }
}
```

## EVT-002 — `community_member_changed` v1

Emitted whenever a membership's state changes. The authoritative member state (active/removed, banned, mute, roles) is always included as a snapshot. Sanction-driven changes carry the originating `sanction_id`.

| Field | Value |
|---|---|
| Producer | `community_service` |
| Consumers | `messaging_service`, `voice_service`, `realtime_gateway` |
| Topic | `community_events` |
| Key | `community_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id`; apply only if `member_version` is newer |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["community_id", "user_id", "change", "status", "is_banned", "muted_until", "role_ids", "sanction_id", "member_version"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "user_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["joined", "left", "removed", "banned", "unbanned", "muted", "unmuted", "roles_changed"]},
    "status": {"type": "string", "enum": ["active", "removed"]},
    "is_banned": {"type": "boolean"},
    "muted_until": {"type": ["string", "null"], "format": "date-time"},
    "role_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "sanction_id": {"type": ["string", "null"], "format": "uuid"},
    "member_version": {"type": "integer", "minimum": 1}
  }
}
```

## EVT-003 — `community_channel_changed` v1

Emitted when a text or voice channel is created, updated (name, position, capacity), or deleted.

| Field | Value |
|---|---|
| Producer | `community_service` |
| Consumers | `realtime_gateway`, `messaging_service`, `voice_service` |
| Topic | `community_events` |
| Key | `community_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id`; apply only if `channel_version` is newer |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["community_id", "channel_id", "change", "kind", "name", "position", "capacity", "channel_version"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "channel_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["created", "updated", "deleted"]},
    "kind": {"type": "string", "enum": ["text", "voice"]},
    "name": {"type": "string", "minLength": 1, "maxLength": 100},
    "position": {"type": "integer", "minimum": 0},
    "capacity": {"type": ["integer", "null"], "minimum": 1, "maximum": 10, "description": "Voice channels only; null for text channels."},
    "channel_version": {"type": "integer", "minimum": 1}
  }
}
```

## EVT-004 — `member_sanction_changed` v1

Emitted for every sanction issued or lifted. Sources: a moderator action, automatic expiry of a timed mute, or an appeal that was overturned. `community_service` applies it to member state. `notification_service` sends the moderation notice to the affected member.

| Field | Value |
|---|---|
| Producer | `moderation_service` |
| Consumers | `community_service`, `notification_service` |
| Topic | `moderation_events` |
| Key | `community_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id` |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["sanction_id", "community_id", "target_user_id", "actor_user_id", "action", "source", "reason", "expires_at", "appealable"],
  "properties": {
    "sanction_id": {"type": "string", "format": "uuid"},
    "community_id": {"type": "string", "format": "uuid"},
    "target_user_id": {"type": "string", "format": "uuid"},
    "actor_user_id": {"type": ["string", "null"], "format": "uuid", "description": "Null when source is expiry."},
    "action": {"type": "string", "enum": ["mute", "unmute", "kick", "ban", "unban"]},
    "source": {"type": "string", "enum": ["moderator", "expiry", "appeal"]},
    "reason": {"type": ["string", "null"], "maxLength": 1000},
    "expires_at": {"type": ["string", "null"], "format": "date-time", "description": "Mute end time; null for permanent bans, kicks, and lift actions."},
    "appealable": {"type": "boolean", "description": "True for active mute and ban sanctions the member can appeal."}
  }
}
```

## EVT-005 — `automod_rules_changed` v1

Emitted when a community's automatic filter configuration changes. The payload is a full rule-set snapshot, so consumers replace their copy.

| Field | Value |
|---|---|
| Producer | `moderation_service` |
| Consumers | `messaging_service` |
| Topic | `moderation_events` |
| Key | `community_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id`; apply only if `rule_set_version` is newer |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["community_id", "rule_set_version", "spam", "banned_words", "exempt_role_ids"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "rule_set_version": {"type": "integer", "minimum": 1},
    "spam": {
      "type": "object",
      "additionalProperties": false,
      "required": ["enabled", "action", "max_messages_per_window", "window_seconds", "max_duplicate_messages", "max_mentions_per_message", "max_links_per_message"],
      "properties": {
        "enabled": {"type": "boolean"},
        "action": {"type": "string", "enum": ["block", "remove"]},
        "max_messages_per_window": {"type": "integer", "minimum": 1},
        "window_seconds": {"type": "integer", "minimum": 1, "maximum": 3600},
        "max_duplicate_messages": {"type": "integer", "minimum": 1},
        "max_mentions_per_message": {"type": "integer", "minimum": 1},
        "max_links_per_message": {"type": "integer", "minimum": 0}
      }
    },
    "banned_words": {
      "type": "object",
      "additionalProperties": false,
      "required": ["enabled", "action", "match_mode", "words"],
      "properties": {
        "enabled": {"type": "boolean"},
        "action": {"type": "string", "enum": ["block", "remove"]},
        "match_mode": {"type": "string", "enum": ["whole_word", "substring"]},
        "words": {"type": "array", "items": {"type": "string", "minLength": 1, "maxLength": 100}, "maxItems": 1000, "uniqueItems": true}
      }
    },
    "exempt_role_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true}
  }
}
```

## EVT-006 — `appeal_status_changed` v1

Emitted when an appeal is submitted and when moderators decide it. An `overturned` decision is always paired with an EVT-004 lift event (`source = appeal`).

| Field | Value |
|---|---|
| Producer | `moderation_service` |
| Consumers | `notification_service` |
| Topic | `moderation_events` |
| Key | `community_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id` |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["appeal_id", "sanction_id", "community_id", "appellant_user_id", "sanction_type", "status", "reviewer_user_id", "decision_note"],
  "properties": {
    "appeal_id": {"type": "string", "format": "uuid"},
    "sanction_id": {"type": "string", "format": "uuid"},
    "community_id": {"type": "string", "format": "uuid"},
    "appellant_user_id": {"type": "string", "format": "uuid"},
    "sanction_type": {"type": "string", "enum": ["mute", "ban"]},
    "status": {"type": "string", "enum": ["submitted", "upheld", "overturned"]},
    "reviewer_user_id": {"type": ["string", "null"], "format": "uuid"},
    "decision_note": {"type": ["string", "null"], "maxLength": 2000}
  }
}
```

## EVT-007 — `friend_request_changed` v1

Emitted when a friend request is sent, accepted, declined, or cancelled.

| Field | Value |
|---|---|
| Producer | `social_service` |
| Consumers | `notification_service` |
| Topic | `social_events` |
| Key | `recipient_user_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id` |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["request_id", "sender_user_id", "recipient_user_id", "status"],
  "properties": {
    "request_id": {"type": "string", "format": "uuid"},
    "sender_user_id": {"type": "string", "format": "uuid"},
    "recipient_user_id": {"type": "string", "format": "uuid"},
    "status": {"type": "string", "enum": ["pending", "accepted", "declined", "cancelled"]}
  }
}
```

## EVT-008 — `conversation_changed` v1

Emitted when a private conversation (direct or group) is created, its participants change, or it is renamed. The payload always carries the full current participant list, plus a delta.

| Field | Value |
|---|---|
| Producer | `social_service` |
| Consumers | `messaging_service`, `realtime_gateway` |
| Topic | `social_events` |
| Key | `conversation_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id`; apply only if `conversation_version` is newer |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["conversation_id", "kind", "change", "name", "participant_user_ids", "added_user_ids", "removed_user_ids", "conversation_version"],
  "properties": {
    "conversation_id": {"type": "string", "format": "uuid"},
    "kind": {"type": "string", "enum": ["direct", "group"]},
    "change": {"type": "string", "enum": ["created", "participants_changed", "renamed"]},
    "name": {"type": ["string", "null"], "maxLength": 100},
    "participant_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "minItems": 1, "maxItems": 10, "uniqueItems": true},
    "added_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "removed_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "conversation_version": {"type": "integer", "minimum": 1}
  }
}
```

## EVT-009 — `message_changed` v1

Emitted for every message creation, edit, deletion (by the author or by automod), and reaction change. The payload carries a message snapshot. On deletion, `content` is null and `attachments` is empty. Attachment download URLs are short-lived presigned URLs, and clients refresh them through message history.

| Field | Value |
|---|---|
| Producer | `messaging_service` |
| Consumers | `realtime_gateway`, `notification_service` |
| Topic | `message_events` |
| Key | `container_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id`; ordering guaranteed per container; apply only if `message_version` is newer |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["message_id", "change", "container_type", "container_id", "community_id", "author_user_id", "thread_root_id", "content", "attachments", "mentioned_user_ids", "reactions", "deletion_reason", "created_at", "edited_at", "message_version"],
  "properties": {
    "message_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["created", "edited", "deleted", "reactions_changed"]},
    "container_type": {"type": "string", "enum": ["channel", "conversation"]},
    "container_id": {"type": "string", "format": "uuid", "description": "channel_id or conversation_id."},
    "community_id": {"type": ["string", "null"], "format": "uuid", "description": "Set for channel messages; null for conversation messages."},
    "author_user_id": {"type": "string", "format": "uuid"},
    "thread_root_id": {"type": ["string", "null"], "format": "uuid"},
    "content": {"type": ["string", "null"], "maxLength": 4000},
    "attachments": {
      "type": "array",
      "maxItems": 10,
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["attachment_id", "content_type", "size_bytes", "download_url", "url_expires_at"],
        "properties": {
          "attachment_id": {"type": "string", "format": "uuid"},
          "content_type": {"type": "string", "enum": ["image/png", "image/jpeg", "image/gif", "image/webp"]},
          "size_bytes": {"type": "integer", "minimum": 1, "maximum": 10485760},
          "download_url": {"type": "string", "format": "uri"},
          "url_expires_at": {"type": "string", "format": "date-time"}
        }
      }
    },
    "mentioned_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "reactions": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["emoji", "count"],
        "properties": {
          "emoji": {"type": "string", "minLength": 1, "maxLength": 32},
          "count": {"type": "integer", "minimum": 1}
        }
      }
    },
    "deletion_reason": {"type": ["string", "null"], "enum": ["author", "automod", null]},
    "created_at": {"type": "string", "format": "date-time"},
    "edited_at": {"type": ["string", "null"], "format": "date-time"},
    "message_version": {"type": "integer", "minimum": 1}
  }
}
```

## EVT-010 — `voice_presence_changed` v1

Emitted when a participant joins or leaves a voice channel, or toggles camera or screen share. The trigger is a LiveKit webhook, an explicit leave, a moderation removal, or reconciliation. For webhook-triggered flows, the producer mints the `trace_id`.

| Field | Value |
|---|---|
| Producer | `voice_service` |
| Consumers | `realtime_gateway` |
| Topic | `voice_events` |
| Key | `channel_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id` |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["community_id", "channel_id", "user_id", "change", "camera_enabled", "screen_share_enabled", "publishing_allowed", "participant_count", "capacity"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "channel_id": {"type": "string", "format": "uuid"},
    "user_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["joined", "left", "media_changed"]},
    "camera_enabled": {"type": "boolean"},
    "screen_share_enabled": {"type": "boolean"},
    "publishing_allowed": {"type": "boolean", "description": "False when the member is muted (listen-only)."},
    "participant_count": {"type": "integer", "minimum": 0, "maximum": 10},
    "capacity": {"type": "integer", "minimum": 1, "maximum": 10}
  }
}
```

## EVT-011 — `notification_created` v1

Emitted when an in-app notification is stored for a user, so open sessions can show it immediately.

| Field | Value |
|---|---|
| Producer | `notification_service` |
| Consumers | `realtime_gateway` |
| Topic | `notification_events` |
| Key | `recipient_user_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id` |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["notification_id", "recipient_user_id", "kind", "title", "summary", "link", "unread_count"],
  "properties": {
    "notification_id": {"type": "string", "format": "uuid"},
    "recipient_user_id": {"type": "string", "format": "uuid"},
    "kind": {"type": "string", "enum": ["mention", "friend_request_received", "friend_request_accepted", "moderation_notice", "appeal_decided"]},
    "title": {"type": "string", "maxLength": 200},
    "summary": {"type": "string", "maxLength": 500},
    "link": {"type": "string", "description": "In-app deep link path, e.g. /channels/{channel_id}/messages/{message_id}."},
    "unread_count": {"type": "integer", "minimum": 0}
  }
}
```
