# Community Chat & Voice Platform — System Design

## 1. Overview

A web-based real-time communication platform where people join invitation-only communities, chat in text channels, talk and video call in voice channels (via managed LiveKit Cloud), connect with friends in direct and small group conversations, and rely on moderator tools (mute / kick / ban, automatic filters, a one-year audit log, and moderator-reviewed appeals) to keep spaces healthy.

**Scale target at launch:** tens of thousands of users overall, communities of thousands of members, voice/video calls of up to 10 participants.

**Success measure:** communities stay active and members return daily (daily active members, messages per active community, voice minutes).

### 1.1 Service map

| Service | Runtime | Core responsibility | PostgreSQL | Kafka | S3-compatible storage | External provider |
|---|---|---|---|---|---|---|
| `identity_service` | Go | Accounts, email verification, password reset, sessions, access-token issuance | yes | produce | — | Resend |
| `community_service` | Go | Communities, channels, invites, memberships, roles & permissions | yes | produce + consume | — | — |
| `moderation_service` | Go | Sanctions (mute/kick/ban), audit log, appeals, automod rules | yes | produce | — | — |
| `social_service` | Go | Friend requests, friendships, private conversations (DM / group DM) | yes | produce | — | — |
| `messaging_service` | Go | Messages, threads, reactions, mentions, image attachments, automod enforcement | yes | produce + consume | yes | — |
| `voice_service` | Go | Voice channel join/leave, capacity, presence, LiveKit integration | yes | produce + consume | — | LiveKit Cloud |
| `notification_service` | Go | In-app notification inbox, preferences, notification emails | yes | produce + consume | — | Resend |
| `realtime_gateway` | Go | WebSocket push of live events to browsers | — | consume | — | — |

### 1.2 Platform-wide conventions

- **Deployment model:** every service runs as stateless containers (`replicas >= 1`). No service relies on local disk or sticky in-memory state across requests. All services expose `GET /healthz` (liveness) and `GET /readyz` (readiness: PostgreSQL / Kafka / object storage reachability as applicable) and drain gracefully on `SIGTERM`/`SIGINT`.
- **End-user authentication (in-house):** `identity_service` issues short-lived signed access tokens (JWT, asymmetric signature, ~10 min lifetime) and long-lived rotating refresh tokens (held in an HttpOnly, Secure, SameSite cookie). Every other service verifies access tokens **locally** using identity's public verification key(s), distributed through deployment configuration (current + next key to support rotation). No service calls `identity_service` at request time to verify a user. Token claims: `sub` (user_id), `sid` (session id), `email_verified`, `exp`, `iat`, `iss`, `aud`.
- **Service-to-service authentication:** internal REST calls carry a per-edge service token (`Authorization: Bearer`) provisioned through deployment secrets. Internal endpoints live under `/internal/*` and are never exposed through the public ingress.
- **Authorization model:** community-level role-based permissions are owned by `community_service`. Permission keys: `manage_community`, `manage_channels`, `manage_roles`, `create_invites`, `mute_members`, `kick_members`, `ban_members`, `manage_automod`, `view_audit_log`, `review_appeals`. The community owner implicitly holds all permissions; roles are ordered by position (a moderator can only act on members whose highest role ranks below their own). Ordinary participation (reading/posting in text channels, joining voice) is granted to every active, non-muted member.
- **Public API style:** JSON over HTTPS, cursor-based pagination (`cursor`, `limit`), errors as `{"error": "<code>", "detail": "<human text>"}`, `Idempotency-Key` support on all creation endpoints that a client might retry.
- **Distributed tracing:** `traceparent` / `X-Trace-Id` is propagated on every HTTP call; every Kafka message carries the mandatory envelope with a non-empty `trace_id` (see `EVENT_CONTRACTS.md`). Producers mint a `trace_id` at the edge of flows that did not originate from an HTTP request (scheduled jobs, webhook-triggered flows).
- **Kafka delivery semantics:** at-least-once; every consumer deduplicates on `envelope.event_id` and applies state changes idempotently (last-writer-wins on `occurred_at` / entity version for projections). Topics are partitioned by the documented key to preserve per-entity ordering.
- **Observability:** each service exports business metrics and RED metrics (`http_requests_total{route,method,status_class}`, `http_request_duration_seconds{route,method}`), plus `kafka_consumer_lag{topic,consumer_group}` for consumers and `external_call_duration_seconds{provider,operation}` / `external_call_errors_total{provider,operation,error_class}` for provider integrations. Labels are always low-cardinality (never user, community, message, or request ids).

---

## 2. Services

### 2.1 `identity_service`

**Programming language / runtime:** Go — security-sensitive, high-request-rate token issuance and refresh with predictable latency; Resend publishes an official Go SDK, so the email integration stays first-class.

**Responsibilities**
- User registration (email, username, display name, password), email verification, sign-in, sign-out, password reset.
- Session management: issue access tokens + rotating refresh tokens; refresh-token reuse detection (reuse revokes the whole session family); user-initiated sign-out of the current session or all sessions.
- Public user profile lookup (username → user, batch user_id → display name) used by the frontend to render names and to find people to befriend.
- Owns the token signing keys and their rotation.
- Brute-force protection: per-account and per-IP sign-in throttling and temporary lockout, persisted in PostgreSQL.

**Owned domain data & infrastructure**
- **PostgreSQL:** `User` (identity, unique email, unique username, display name, email_verified flag, status), `Credential` (salted memory-hard password hash), `EmailVerificationToken` (hashed, single-use, 24 h expiry), `PasswordResetToken` (hashed, single-use, 1 h expiry), `Session` (device label, created/last-seen, revoked flag), `RefreshToken` (hashed, rotation family), `SigningKey` metadata, `LoginAttempt` throttling records.
- **Kafka:** produces `identity_events`.

**External provider integrations**
- **Resend (transactional email)** — outbound REST via official SDK: verification email on registration / resend request, password-reset email, password-changed confirmation. Each send uses an idempotency key derived from the token id. Failures are retried with exponential back-off (bounded) and surfaced to the user as "we couldn't send the email, try again"; verification and reset tokens never leave this service (they are not put on Kafka). No inbound webhook at launch.

**API endpoints**

| Method | Path | Input → Output | Business goal |
|---|---|---|---|
| POST | `/auth/register` | email, username, display_name, password → user_id, `verification_pending` | Create account and send verification email |
| POST | `/auth/verify-email` | token → verified user | Confirm email ownership |
| POST | `/auth/verify-email/resend` | email → 202 | Recover from expired/missed verification |
| POST | `/auth/login` | email, password → access_token + refresh cookie; 403 `email_not_verified` / 401 `invalid_credentials` / 429 `locked` | Sign in |
| POST | `/auth/refresh` | refresh cookie → new access_token + rotated cookie | Stay signed in across normal browser use |
| POST | `/auth/logout` | current session → 204 | End the current session |
| POST | `/auth/logout-all` | → 204 | End every session of the user |
| POST | `/auth/password-reset/request` | email → 202 (always, no account enumeration) | Start password reset |
| POST | `/auth/password-reset/confirm` | token, new_password → 204; 410 `reset_link_expired` | Complete password reset (revokes all sessions) |
| GET | `/sessions` | → list of active sessions | Let users see and end sessions |
| DELETE | `/sessions/{session_id}` | → 204 | End a specific session |
| GET | `/users/me` | → own profile | Profile display |
| PATCH | `/users/me` | display_name → profile | Update display name |
| GET | `/users/lookup?username=` | → public profile (user_id, username, display_name) | Find a person to befriend |
| GET | `/users?ids=` | up to 100 ids → public profiles | Render names for members, authors, friends |
| GET | `/.well-known/jwks.json` | → public verification keys | Key publication for operators/frontend tooling |
| GET | `/healthz`, `/readyz` | — | Probes |

**Kafka topics**
- Produces `identity_events` — `user_account_changed` (EVT-001) on verification, profile update, email change, or deactivation; payload carries user_id, email, username, display_name, email_verified, status. Consumer: `notification_service`.

**Inter-service dependencies**
- None inbound synchronous. Peers verify tokens locally. Async producer of EVT-001.

**Observability metrics & SLOs**
- Business: `identity_registrations_total{result}`, `identity_email_verifications_total{result}`, `identity_logins_total{result}` (`success|invalid_credentials|unverified|locked`), `identity_password_resets_total{stage,result}`, `identity_active_sessions` (gauge).
- Performance: `/auth/login` p95 < 300 ms (password hashing dominated), `/auth/refresh` p95 < 50 ms, 5xx rate < 0.1 %, Resend send success ≥ 99.5 % (`external_call_errors_total{provider="resend"}`).

---

### 2.2 `community_service`

**Programming language / runtime:** Go — CRUD-heavy domain service with hot internal authorization-check endpoints that must answer with low, predictable latency; no external SDKs required, so Go is the default fit.

**Responsibilities**
- Create and configure communities (name, description, icon reference as URL); owner transfer is out of scope at launch.
- Text and voice channels: create, rename, reorder, delete; voice channels carry a capacity (1–10, default 10).
- Invitations: create invite codes (expiry, max uses), preview an invite, accept an invite to join; revoke invites.
- Membership: join (via invite), leave, member listing with pagination; enforce the community ban list (banned users cannot rejoin).
- Roles & permissions: owners/admins create roles with permission sets, order them, and assign roles to members.
- Authoritative **member state**: `active` / `removed`, `muted_until`, banned flag — applied from moderation decisions (EVT-004) so every downstream service learns membership state from one source.
- Authorization oracle for peers: answers permission checks for `moderation_service` (API-001) and voice access checks for `voice_service` (API-002).

**Owned domain data & infrastructure**
- **PostgreSQL:** `Community`, `Channel` (kind `text|voice`, position, voice capacity), `Invite`, `Membership` (status, joined_at, muted_until), `CommunityBan`, `Role` (permission set, position), `MemberRole` assignment, processed-event dedupe records.
- **Kafka:** produces `community_events`; consumes `moderation_events` (`member_sanction_changed`).

**External provider integrations** — none.

**API endpoints**

| Method | Path | Input → Output | Business goal |
|---|---|---|---|
| POST | `/communities` | name, description → community with default `general` text + `General` voice channel and creator as owner | Create a community |
| GET | `/communities/mine` | → communities the caller belongs to (empty list drives the "join or create" empty state) | Home / sidebar |
| GET | `/communities/{community_id}` | → community, channels, caller's roles & effective permissions | Load a community |
| PATCH | `/communities/{community_id}` | name, description, icon_url → community (`manage_community`) | Configure community |
| POST | `/communities/{community_id}/channels` | name, kind, capacity? → channel (`manage_channels`) | Add channel |
| PATCH | `/communities/{community_id}/channels/{channel_id}` | name, position, capacity → channel | Edit channel |
| DELETE | `/communities/{community_id}/channels/{channel_id}` | → 204 | Remove channel |
| POST | `/communities/{community_id}/invites` | expires_in, max_uses → invite code (`create_invites`) | Invite others |
| DELETE | `/communities/{community_id}/invites/{code}` | → 204 | Revoke invite |
| GET | `/invites/{code}` | → community preview, validity | Invite landing page |
| POST | `/invites/{code}/accept` | → membership; 403 `banned`, 410 `invite_expired` | Join a community |
| POST | `/communities/{community_id}/leave` | → 204 | Leave a community |
| GET | `/communities/{community_id}/members` | cursor, limit → members with role ids, mute status | Member list (also used to find friends from shared communities) |
| GET | `/communities/{community_id}/roles` | → roles | Role management |
| POST / PATCH / DELETE | `/communities/{community_id}/roles[/{role_id}]` | name, permissions, position → role (`manage_roles`) | Define what moderators can do |
| PUT | `/communities/{community_id}/members/{user_id}/roles` | role_ids → member (`manage_roles`, hierarchy enforced) | Owners assign roles |
| POST | `/internal/communities/{community_id}/authorization-checks` | API-001 | Permission + hierarchy check for moderation |
| POST | `/internal/communities/{community_id}/channels/{channel_id}/voice-access-checks` | API-002 | Voice join authorization |
| GET | `/healthz`, `/readyz` | — | Probes |

**Kafka topics**
- Produces `community_events`:
  - `community_member_changed` (EVT-002) — joined / left / removed (kick) / banned / unbanned / muted / unmuted / roles_changed (role ids let messaging exempt roles from automod). Consumers: `messaging_service`, `voice_service`, `realtime_gateway`.
  - `community_channel_changed` (EVT-003) — created / updated / deleted. Consumers: `realtime_gateway`, `messaging_service`, `voice_service`.
- Consumes `moderation_events` → `member_sanction_changed` (EVT-004): kick → membership removed; ban → membership removed + ban recorded; unban → ban cleared; mute/unmute → `muted_until` updated. Each applied change emits the matching EVT-002.

**Inter-service dependencies**
- Provider of API-001 (consumer `moderation_service`) and API-002 (consumer `voice_service`).
- Producer of EVT-002, EVT-003; consumer of EVT-004.

**Observability metrics & SLOs**
- Business: `community_created_total`, `community_invites_created_total`, `community_joins_total{result}` (`joined|banned|expired|invalid`), `community_leaves_total{reason}` (`left|kicked|banned`), `community_role_assignments_total`.
- Performance: public API p95 < 150 ms; internal check endpoints p95 < 30 ms and availability ≥ 99.9 %; `kafka_consumer_lag{topic="moderation_events"}` < 1 s p95 (sanction applied within 2 s end-to-end).

---

### 2.3 `moderation_service`

**Programming language / runtime:** Go — rule-driven domain service with scheduled jobs (mute expiry, audit retention purge) and a synchronous dependency on `community_service`; no external SDK needs, Go default applies and keeps the codebase single-language.

**Responsibilities**
- Moderator actions: mute (with optional duration), unmute, kick, ban (with optional reason), unban. Each action is authorized through `community_service` (API-001: actor holds the needed permission and outranks the target).
- Sanction lifecycle: active sanctions per member; timed mutes expire via a scheduled sweep that emits an `unmute` with `source = expiry`.
- Member-facing status: a sanctioned user can read their own active sanctions (reason, expiry, whether an appeal is possible) — powers the "you are muted/banned, here is how to appeal" notice. Works for banned users who are no longer members.
- Appeals: the affected member submits one open appeal per sanction; moderators with `review_appeals` list, claim and decide (`upheld` / `overturned`). Overturning lifts the sanction (emits unmute/unban with `source = appeal`).
- Automatic filters: per-community automod configuration — spam rules (message rate per member, duplicate messages, mention flooding, link flooding) and banned-word lists, each with an action `block` (rejected before posting) or `remove` (posted, then removed after posting). Configuration is published to `messaging_service`, which enforces it on the hot path.
- **Audit log:** every moderator action (sanction issued/lifted, appeal decided, automod configuration changed) is recorded immutably with actor, target, action, reason, timestamp. Visible to members with `view_audit_log`. Records are retained for one year; a daily scheduled job purges entries older than 365 days.

**Owned domain data & infrastructure**
- **PostgreSQL:** `Sanction` (community, target, actor, type, reason, issued_at, expires_at, lifted_at, lift source), `Appeal` (sanction, appellant statement, status, reviewer, decision note, timestamps), `AutomodRuleSet` (per community: spam thresholds, banned-word list, per-rule action, version), `AuditLogEntry` (append-only, 1-year retention).
- **Kafka:** produces `moderation_events`.

**External provider integrations** — none.

**API endpoints**

| Method | Path | Input → Output | Business goal |
|---|---|---|---|
| POST | `/communities/{community_id}/sanctions` | target_user_id, type (`mute|kick|ban`), reason, duration? → sanction; 403 `insufficient_permission` / `target_outranks_actor` | Mute, remove, or ban a member |
| POST | `/communities/{community_id}/sanctions/{sanction_id}/lift` | reason → sanction | Unmute / unban |
| GET | `/communities/{community_id}/sanctions` | status, cursor → sanctions (moderators) | Review active sanctions |
| GET | `/me/sanctions` | → caller's active sanctions with appeal eligibility | Clear status notice for muted/banned members |
| POST | `/sanctions/{sanction_id}/appeals` | statement → appeal; 409 `appeal_already_open` | Member submits an appeal |
| GET | `/me/appeals` | → caller's appeals and outcomes | Appeal status tracking |
| GET | `/communities/{community_id}/appeals` | status, cursor → appeals (`review_appeals`) | Moderator appeal queue |
| POST | `/communities/{community_id}/appeals/{appeal_id}/decision` | decision (`upheld|overturned`), note → appeal | Review appeals |
| GET | `/communities/{community_id}/automod` | → rule set (`manage_automod`) | View filters |
| PUT | `/communities/{community_id}/automod` | spam rules, banned words, actions → rule set (new version) | Configure filters per community |
| GET | `/communities/{community_id}/audit-log` | action type, actor, cursor → entries (last 365 days, `view_audit_log`) | Admins review moderator actions |
| GET | `/healthz`, `/readyz` | — | Probes |

**Kafka topics**
- Produces `moderation_events`:
  - `member_sanction_changed` (EVT-004) — mute / unmute / kick / ban / unban. Consumers: `community_service`, `notification_service`.
  - `automod_rules_changed` (EVT-005) — full rule-set snapshot per version. Consumer: `messaging_service`.
  - `appeal_status_changed` (EVT-006) — submitted / upheld / overturned. Consumer: `notification_service`.

**Inter-service dependencies**
- Sync consumer of API-001 (`community_service`) — required in real time because permission and hierarchy must be correct at the moment of action. On provider unavailability the action fails closed with `503` (no unauthorized sanctions).
- Producer of EVT-004, EVT-005, EVT-006.

**Observability metrics & SLOs**
- Business: `moderation_sanctions_total{type,source}` (`source=moderator|expiry|appeal`), `moderation_appeals_total{status}`, `moderation_appeal_decision_seconds` (histogram, submit → decision), `moderation_automod_config_updates_total`, `moderation_audit_entries_purged_total`.
- Performance: sanction endpoint p95 < 250 ms (including API-001), 5xx < 0.1 %, API-001 call p95 < 50 ms, retention purge job success daily (`moderation_retention_job_last_success_timestamp`).

---

### 2.4 `social_service`

**Programming language / runtime:** Go — small, consistency-focused CRUD service; Go default, aligned with the rest of the platform.

**Responsibilities**
- Friend requests: send (by user_id resolved via identity's public lookup), accept, decline, cancel; friend list; remove friend.
- Private conversations: open (or reuse) a 1:1 direct conversation with a friend; create a small group conversation (creator + up to 9 friends, max 10 participants); add friends to / leave a group conversation; rename group.
- Owns conversation participant lists — the authority on who may read and post in a private conversation (enforced by `messaging_service` from EVT-008).
- Empty-state support: `GET /friends` returns an empty list plus hints; friend suggestions are discovered client-side from shared community member lists.

**Owned domain data & infrastructure**
- **PostgreSQL:** `FriendRequest` (sender, recipient, status), `Friendship` (unordered pair), `Conversation` (kind `direct|group`, name, created_by), `ConversationParticipant` (joined_at, left_at).
- **Kafka:** produces `social_events`.

**External provider integrations** — none.

**API endpoints**

| Method | Path | Input → Output | Business goal |
|---|---|---|---|
| POST | `/friend-requests` | recipient_user_id → request; 409 `already_friends` / `request_pending` | Send a friend request |
| GET | `/friend-requests` | direction (`incoming|outgoing`) → requests | Review requests |
| POST | `/friend-requests/{request_id}/accept` | → friendship | Accept |
| POST | `/friend-requests/{request_id}/decline` | → 204 | Decline |
| DELETE | `/friend-requests/{request_id}` | → 204 | Cancel own request |
| GET | `/friends` | → friends | Friend list |
| DELETE | `/friends/{user_id}` | → 204 | Unfriend |
| POST | `/conversations/direct` | friend_user_id → conversation (idempotent per pair) | Start a 1:1 DM |
| POST | `/conversations/group` | name?, participant_ids (friends, ≤ 9) → conversation | Start a small group chat |
| GET | `/conversations` | → caller's conversations | DM sidebar |
| POST | `/conversations/{conversation_id}/participants` | user_ids → conversation (cap 10) | Add friends to a group |
| POST | `/conversations/{conversation_id}/leave` | → 204 | Leave a group chat |
| PATCH | `/conversations/{conversation_id}` | name → conversation | Rename group |
| GET | `/healthz`, `/readyz` | — | Probes |

**Kafka topics**
- Produces `social_events`:
  - `friend_request_changed` (EVT-007) — sent / accepted / declined / cancelled. Consumer: `notification_service`.
  - `conversation_changed` (EVT-008) — created / participants_changed / renamed. Consumers: `messaging_service`, `realtime_gateway`.

**Inter-service dependencies**
- No synchronous dependencies. Producer of EVT-007, EVT-008.

**Observability metrics & SLOs**
- Business: `social_friend_requests_total{action}`, `social_friendships_total` (gauge), `social_conversations_created_total{kind}`.
- Performance: API p95 < 150 ms, 5xx < 0.1 %.

---

### 2.5 `messaging_service`

**Programming language / runtime:** Go — the platform's hottest write/read path (message send, history paging, reactions) with high concurrency and strict latency targets; Go's lightweight concurrency and predictable GC latency fit this I/O-bound, high-throughput workload.

**Responsibilities**
- Messages in **text channels** (community scope) and **private conversations** (DM / group): send, edit own, delete own (soft delete; content removed for everyone), history paging (newest-first, cursor-based; supports catch-up "after" cursor after reconnect).
- **Idempotent send:** clients attach a `client_message_id`; a repeated send after a dropped connection returns the original message instead of creating a duplicate.
- **Threads:** reply to a message to start/continue a thread; thread listing per root message with reply count.
- **Reactions:** add/remove Unicode emoji reactions; aggregated counts per message.
- **@mentions:** parse and validate mentions against the container's membership; mention data travels on EVT-009 for notifications.
- **Image attachments:** issue presigned upload URLs after validating declared type (`image/png`, `image/jpeg`, `image/gif`, `image/webp`) and size (≤ 10 MB); on send, verify the uploaded object's real size/type before attaching; serve images through short-lived presigned download URLs. Unattached uploads are garbage-collected after 24 h. Oversized/unsupported images are rejected with `413 image_too_large` / `415 unsupported_image_type` and a message explaining the limits.
- **Access enforcement from local projections (no sync calls on the hot path):**
  - Community membership, mute state and text channels — from EVT-002 / EVT-003.
  - Conversation participants — from EVT-008.
  - Muted members receive `403 member_muted` with `muted_until` and an appeal pointer; removed/banned members receive `403 not_a_member`.
- **Automod enforcement:** applies the community's current rule set (from EVT-005) on every channel message — `block` rules reject before posting (`422 blocked_by_automod`, with the rule category); `remove` rules accept then remove the message shortly after posting (emits a `deleted` change with `reason = automod`). Spam rate detection uses recent-message counts per member stored in PostgreSQL.
- **Realtime scope oracle:** answers which communities and conversations a user may subscribe to (API-003) for `realtime_gateway`.

**Owned domain data & infrastructure**
- **PostgreSQL:** `Message` (container type/id, author, content ≤ 4,000 chars, thread root, edited_at, deleted_at, deletion reason, client_message_id), `Reaction`, `Mention`, `Attachment` (object key, type, size, status), plus read-model projections owned locally: `CommunityMemberProjection`, `TextChannelProjection`, `ConversationParticipantProjection`, `AutomodRuleSetProjection`, processed-event dedupe records.
- **S3-compatible object storage:** image attachment objects (private bucket, presigned PUT/GET, 10 MB max object size enforced on upload policy).
- **Kafka:** produces `message_events`; consumes `community_events`, `social_events`, `moderation_events` (`automod_rules_changed` only).

**External provider integrations** — none.

**API endpoints**

| Method | Path | Input → Output | Business goal |
|---|---|---|---|
| POST | `/attachments` | filename, content_type, size_bytes → attachment_id, presigned upload URL; 413 / 415 | Share images up to 10 MB |
| POST | `/channels/{channel_id}/messages` | client_message_id, content, attachment_ids?, thread_root_id? → message | Chat in a text channel |
| GET | `/channels/{channel_id}/messages` | before / after cursor, limit → messages | Load history while scrolling / catch up after reconnect |
| POST | `/conversations/{conversation_id}/messages` | same as channel send → message | Chat in DM / group |
| GET | `/conversations/{conversation_id}/messages` | before / after cursor, limit → messages | DM history |
| GET | `/messages/{message_id}/thread` | cursor → root + replies | Read a thread |
| PATCH | `/messages/{message_id}` | content → message (author only) | Edit own message |
| DELETE | `/messages/{message_id}` | → 204 (author only) | Delete own message |
| PUT | `/messages/{message_id}/reactions/{emoji}` | → reaction summary | Add reaction |
| DELETE | `/messages/{message_id}/reactions/{emoji}` | → reaction summary | Remove reaction |
| POST | `/internal/realtime/user-scopes` | API-003 | Realtime subscription authorization |
| GET | `/healthz`, `/readyz` | — | Probes |

**Kafka topics**
- Produces `message_events` — `message_changed` (EVT-009): created / edited / deleted / reactions_changed with the message snapshot and mentioned user ids. Consumers: `realtime_gateway`, `notification_service`.
- Consumes `community_events` (EVT-002, EVT-003), `social_events` (EVT-008), `moderation_events` (EVT-005).

**Inter-service dependencies**
- Provider of API-003 (consumer `realtime_gateway`).
- Producer of EVT-009; consumer of EVT-002, EVT-003, EVT-005, EVT-008.

**Observability metrics & SLOs**
- Business: `messaging_messages_sent_total{container_type}` (`channel|conversation`), `messaging_messages_edited_total`, `messaging_messages_deleted_total{reason}` (`author|automod`), `messaging_reactions_total{action}`, `messaging_mentions_total`, `messaging_attachments_total{result}` (`accepted|too_large|unsupported_type`), `messaging_automod_hits_total{category,action}` (feeds the false-positive assumption review), `messaging_duplicate_sends_suppressed_total`.
- Performance: send p95 < 200 ms, history page p95 < 150 ms, 5xx < 0.1 %, projection consumer lag < 1 s p95, end-to-end send → gateway push p95 < 500 ms.

---

### 2.6 `voice_service`

**Programming language / runtime:** Go — LiveKit publishes an official, actively maintained Go server SDK (access-token minting, Room service API, webhook verification); the workload is high-concurrency, latency-sensitive token issuance and webhook ingestion.

**Responsibilities**
- Join a voice channel: authorize via `community_service` (API-002: active member, channel exists and is `voice`, mute status, capacity), reserve a seat (capacity ≤ 10, enforced transactionally in PostgreSQL with short-lived reservations, and backed by the LiveKit room's max-participant limit), then mint a short-lived LiveKit participant token. Grants: join room, subscribe, publish microphone / camera / screen share — publishing disabled when the member is muted (listen-only).
- Full call → `409 voice_channel_full` with the list of other voice channels in the community that have free seats, so the client can suggest an alternative.
- Leave a voice channel (removes the participant from the LiveKit room) and track presence ("who is here", camera/screen-share flags) from LiveKit webhooks.
- React to membership changes (EVT-002): kicked/banned members are removed from any active LiveKit room; muted members have publishing revoked in place; unmuted members regain publishing on next join. React to channel deletion (EVT-003): the LiveKit room is closed.
- Periodic reconciliation with LiveKit's participant list to heal missed webhooks.
- Device permission handling (blocked microphone/camera → audio-only fallback) is a client concern; the token always permits audio-only participation.

**Owned domain data & infrastructure**
- **PostgreSQL:** `VoiceRoom` (channel ↔ LiveKit room name, capacity), `SeatReservation` (short TTL), `VoiceParticipant` presence (user, joined_at, camera/screen flags), `MuteProjection` (from EVT-002), webhook dedupe records.
- **Kafka:** produces `voice_events`; consumes `community_events`.

**External provider integrations**
- **LiveKit Cloud (managed real-time voice/video)**:
  - Outbound: participant access-token minting (signed with LiveKit API key/secret, never exposed to clients); Room service API — create room with max participants, remove participant, update participant permissions (revoke publish), list participants (reconciliation), delete room.
  - Inbound webhook: `POST /voice/livekit/webhooks` receiving `participant_joined`, `participant_left`, `track_published`, `track_unpublished`, `room_finished`; authenticity verified by the signed Authorization header (API secret) and body hash; processed idempotently by webhook event id.
  - Media never flows through the backend; browsers connect directly to LiveKit Cloud.

**API endpoints**

| Method | Path | Input → Output | Business goal |
|---|---|---|---|
| POST | `/voice/channels/{channel_id}/join` | community_id → livekit_url, participant token, room_name, publish permissions; 403 `not_a_member`; 409 `voice_channel_full` + alternatives | Drop into voice/video |
| POST | `/voice/channels/{channel_id}/leave` | → 204 | Leave at any time |
| GET | `/communities/{community_id}/voice-presence` | → per voice channel: participants with camera/screen flags, seats used/capacity | See who is present |
| POST | `/voice/livekit/webhooks` | LiveKit webhook body (signed) → 200 | Presence tracking |
| GET | `/healthz`, `/readyz` | — | Probes |

**Kafka topics**
- Produces `voice_events` — `voice_presence_changed` (EVT-010): joined / left / media_changed with current participant count. Consumer: `realtime_gateway`.
- Consumes `community_events` (EVT-002, EVT-003).

**Inter-service dependencies**
- Sync consumer of API-002 (`community_service`) at join time — a real-time decision. Fails closed (`503`) when unavailable.
- Producer of EVT-010; consumer of EVT-002, EVT-003.

**Observability metrics & SLOs**
- Business: `voice_joins_total{result}` (`joined|full|forbidden`), `voice_participant_minutes_total{media}` (`audio|video|screen`), `voice_active_participants` (gauge), `voice_active_rooms` (gauge), `voice_moderation_removals_total{action}` (`removed|publish_revoked`). These feed the monthly LiveKit cost review.
- Performance: join p95 < 300 ms (including API-002 and token minting), webhook processing p95 < 100 ms, presence event lag p95 < 1 s, LiveKit API error rate < 0.5 %.

---

### 2.7 `notification_service`

**Programming language / runtime:** Go — event-driven fan-in consumer of five event contracts with an outbound email integration; Resend's official Go SDK keeps the integration first-class, and Go keeps the platform single-language.

**Responsibilities**
- Turn domain events into user notifications:
  - @mention in a channel or conversation (EVT-009, only `created` with mentions).
  - Friend request received / accepted (EVT-007).
  - Moderation notice to the affected member — muted / kicked / banned / unmuted / unbanned, with reason, expiry and how to appeal (EVT-004).
  - Appeal decided (EVT-006).
- Persist an in-app **notification inbox** (unread/read) and publish `notification_created` for live push.
- Send notification emails via Resend according to per-user preferences, only to verified emails, with per-recipient throttling (mention emails batched: at most one mention email per recipient per 15 minutes; moderation notices always sent).
- Keep a local projection of recipient contact data (email, verification status, display name) from EVT-001.

**Owned domain data & infrastructure**
- **PostgreSQL:** `Notification` (recipient, kind, title, summary, deep link, read_at), `NotificationPreference` (per kind: in-app / email), `EmailDelivery` (status, provider message id, attempts), `RecipientProjection` (from EVT-001), processed-event dedupe records.
- **Kafka:** produces `notification_events`; consumes `identity_events`, `moderation_events`, `social_events`, `message_events`.

**External provider integrations**
- **Resend (transactional email)** — outbound REST via official SDK for mention, friend-request, moderation-notice, and appeal-outcome emails. Idempotency key = notification id (safe under at-least-once consumption). Retries with exponential back-off on 429/5xx; permanent failures recorded on `EmailDelivery` without blocking the in-app notification. Sending rate kept under the account's per-team limit (shared with `identity_service`, which has priority for verification/reset traffic).

**API endpoints**

| Method | Path | Input → Output | Business goal |
|---|---|---|---|
| GET | `/notifications` | unread_only?, cursor → notifications + unread count | Notification inbox |
| POST | `/notifications/{notification_id}/read` | → 204 | Mark one read |
| POST | `/notifications/read-all` | → 204 | Mark all read |
| GET | `/notification-preferences` | → preferences | View preferences |
| PUT | `/notification-preferences` | per kind in-app/email toggles → preferences | Control email noise |
| GET | `/healthz`, `/readyz` | — | Probes |

**Kafka topics**
- Produces `notification_events` — `notification_created` (EVT-011). Consumer: `realtime_gateway`.
- Consumes `identity_events` (EVT-001), `moderation_events` (EVT-004, EVT-006), `social_events` (EVT-007), `message_events` (EVT-009).

**Inter-service dependencies**
- No synchronous dependencies. Producer of EVT-011; consumer of EVT-001, EVT-004, EVT-006, EVT-007, EVT-009.

**Observability metrics & SLOs**
- Business: `notification_created_total{kind}`, `notification_emails_sent_total{kind,result}`, `notification_emails_throttled_total{kind}`, `notification_read_total{kind}`.
- Performance: event → in-app notification p95 < 2 s, event → email accepted by Resend p95 < 60 s, consumer lag < 5 s p95, Resend error rate < 0.5 %.

---

### 2.8 `realtime_gateway`

**Programming language / runtime:** Go — thousands of concurrent long-lived WebSocket connections with fan-out of every live event; goroutine-per-connection concurrency and low-latency GC make Go the clear fit.

**Responsibilities**
- WebSocket endpoint for browsers; authenticates the access token locally on connect and on in-band token refresh (connections whose token expires without refresh are closed).
- On connect, asks `messaging_service` (API-003) for the user's subscription scopes (community ids, conversation ids) and subscribes the connection to those plus its personal user scope. Subscription state lives only on the connection and is rebuilt on reconnect (no sticky cross-request state).
- Consumes realtime topics as a **broadcast consumer** (each replica reads every partition independently, starting from latest, no shared consumer-group offsets) and pushes each event to locally connected sockets whose scopes match:
  - `message_events` (EVT-009) → community or conversation scope.
  - `voice_events` (EVT-010) → community scope (voice presence).
  - `community_events` (EVT-002, EVT-003) → community scope (member and channel list updates); also adds/drops the affected user's community scope on join/leave/kick/ban.
  - `social_events` (EVT-008) → adds/drops conversation scopes and pushes conversation list updates.
  - `notification_events` (EVT-011) → personal user scope.
- Heartbeats (ping/pong) to detect dead connections; the client shows a "reconnecting" indicator, reconnects with back-off, and catches up via messaging REST history (`after` cursor) — the gateway guarantees no replay.
- Client → server traffic on the socket is limited to heartbeats and in-band token refresh (typing indicators are out of scope); all writes go through domain REST APIs.

**Owned domain data & infrastructure**
- No database and no persistent domain data (connection-bound subscription state only).
- **Kafka:** consumes `message_events`, `voice_events`, `community_events`, `social_events`, `notification_events`.

**External provider integrations** — none.

**API endpoints**

| Method | Path | Input → Output | Business goal |
|---|---|---|---|
| GET | `/ws` | WebSocket upgrade with access token → server-pushed event frames `{type, scope, data}` | Live chat, presence, and notifications |
| GET | `/healthz`, `/readyz` | — | Probes (readiness includes Kafka assignment) |

**Kafka topics**
- Consumes only (see above). Produces nothing.

**Inter-service dependencies**
- Sync consumer of API-003 (`messaging_service`) at connect time; on failure the connection is refused with a retryable close code.
- Consumer of EVT-002, EVT-003, EVT-008, EVT-009, EVT-010, EVT-011.

**Observability metrics & SLOs**
- Business: `realtime_connections_active` (gauge), `realtime_connections_opened_total{result}`, `realtime_events_delivered_total{event_type}`, `realtime_reconnects_total`.
- Performance: Kafka-to-socket delivery p95 < 150 ms, consumer lag < 500 ms p95, connect (including API-003) p95 < 300 ms, dropped-frame rate (slow consumers disconnected) < 0.1 %.

---

## 3. Key Flows

1. **Sign-up:** client → `identity_service /auth/register` → Resend verification email → `/auth/verify-email` → EVT-001 → `notification_service` stores recipient contact data. Client signs in and gets an access token plus a refresh cookie.
2. **Join & chat:** `/invites/{code}/accept` (`community_service`) → EVT-002 `joined` → `messaging_service` projection allows posting; `realtime_gateway` adds the community scope to the user's live sockets. Message send (`messaging_service`) → EVT-009 → gateway pushes to everyone in the community; mentions → `notification_service` → inbox + EVT-011 push + throttled Resend email.
3. **Reconnect without duplicates:** the socket drops → client shows "reconnecting" and re-sends pending messages with the same `client_message_id` (deduplicated by messaging) → reconnect → API-003 scopes → history catch-up via `after` cursor.
4. **Voice:** `/voice/channels/{id}/join` → API-002 (`community_service`) → seat reservation → LiveKit token → browser connects to LiveKit Cloud → LiveKit webhook `participant_joined` → EVT-010 → gateway pushes presence to the community.
5. **Ban:** moderator → `moderation_service` sanctions (API-001 authorization) → audit log entry → EVT-004 → `community_service` removes membership and records the ban → EVT-002 `banned` → messaging rejects posts, voice removes the user from LiveKit, gateway drops the community scope; in parallel EVT-004 → `notification_service` sends the moderation notice with appeal instructions.
6. **Appeal:** banned user → `/sanctions/{id}/appeals` → EVT-006 `submitted`; moderator decides `overturned` → sanction lifted → EVT-004 `unban` (community clears the ban) + EVT-006 `overturned` → user notified.
7. **Automod:** admin updates rules → audit log + EVT-005 → messaging applies `block` before posting or `remove` after posting.

## 4. Communication Matrix (full producer → consumer fan-out)

The Dependency Registry below carries exactly one row per contract. Fan-out events have additional consumers that are listed here for completeness. They share the same contract.

| Contract | Producer / Provider | All consumers | Kind |
|---|---|---|---|
| API-001 | `community_service` | `moderation_service` | sync |
| API-002 | `community_service` | `voice_service` | sync |
| API-003 | `messaging_service` | `realtime_gateway` | sync |
| EVT-001 | `identity_service` | `notification_service` | async |
| EVT-002 | `community_service` | `messaging_service`, `voice_service`, `realtime_gateway` | async |
| EVT-003 | `community_service` | `realtime_gateway`, `messaging_service`, `voice_service` | async |
| EVT-004 | `moderation_service` | `community_service`, `notification_service` | async |
| EVT-005 | `moderation_service` | `messaging_service` | async |
| EVT-006 | `moderation_service` | `notification_service` | async |
| EVT-007 | `social_service` | `notification_service` | async |
| EVT-008 | `social_service` | `messaging_service`, `realtime_gateway` | async |
| EVT-009 | `messaging_service` | `realtime_gateway`, `notification_service` | async |
| EVT-010 | `voice_service` | `realtime_gateway` | async |
| EVT-011 | `notification_service` | `realtime_gateway` | async |

**Kafka topics:** `identity_events`, `community_events`, `moderation_events`, `social_events`, `message_events`, `voice_events`, `notification_events`. Every topic has at least one consumer.

## 5. Out of Scope (architectural consequences)

Mobile and desktop apps, community reporting/review queues, public discovery, payments, message search, non-image files, calls with more than 10 participants, appeals routed to owners, and typing indicators are not designed. The gateway's scope model and messaging's storage can absorb search or reporting later without changing these boundaries.

## Dependency Registry

| Edge ID | From | To | Kind | Contract |
|---|---|---|---|---|
| DEP-001 | `moderation_service` | `community_service` | sync | `API-001` |
| DEP-002 | `voice_service` | `community_service` | sync | `API-002` |
| DEP-003 | `realtime_gateway` | `messaging_service` | sync | `API-003` |
| DEP-004 | `identity_service` | `notification_service` | async | `EVT-001` |
| DEP-005 | `community_service` | `messaging_service` | async | `EVT-002` |
| DEP-006 | `community_service` | `realtime_gateway` | async | `EVT-003` |
| DEP-007 | `moderation_service` | `community_service` | async | `EVT-004` |
| DEP-008 | `moderation_service` | `messaging_service` | async | `EVT-005` |
| DEP-009 | `moderation_service` | `notification_service` | async | `EVT-006` |
| DEP-010 | `social_service` | `notification_service` | async | `EVT-007` |
| DEP-011 | `social_service` | `messaging_service` | async | `EVT-008` |
| DEP-012 | `messaging_service` | `realtime_gateway` | async | `EVT-009` |
| DEP-013 | `voice_service` | `realtime_gateway` | async | `EVT-010` |
| DEP-014 | `notification_service` | `realtime_gateway` | async | `EVT-011` |
