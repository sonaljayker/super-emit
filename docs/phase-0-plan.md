# Phase‑0 Planning Deliverable

This document contains the complete Phase‑0 planning package for Super‑Emit. It follows the product requirements and engineering constraints provided by the owner.

Table of contents
- 1. Executive summary
- 2. Architecture overview and diagram
- 3. Repository & module structure (Maven multi‑module)
- 4. Database entity relationship design (core entities)
- 5. API and frontend route conventions
- 6. Media ingest → playback design
- 7. Concurrency & scaling strategy
- 8. Security & moderation plan
- 9. Phase‑by‑phase implementation roadmap
- 10. Phase‑1 detailed vertical slice and acceptance criteria
- 11. Dependencies, risks, and mitigations
- 12. Next steps

---

1. Executive summary

Super‑Emit is designed as a modern video platform that separates the control plane (metadata, auth, feed, business logic) from the media plane (ingest, transcoding, packaging, CDN). Phase‑0 captures the architecture, data model, APIs, and a clear Phase‑1 plan so implementation is safe, testable, and incremental.

2. Architecture overview and diagram

Goals
- Separate control plane from media data plane.
- Start with a modular monolith for control plane using Spring Boot (Java 25). Media workers are separate containers.
- Use durable jobs and Kafka for events; Redis for caches and presence; Postgres for transactional data; Flyway for migrations.

High‑level components
- Control plane (modular monolith): Auth, User/Channel, Catalog, Feed, Moderation, Analytics, Notifications, Orchestration (job/event handling).
- Media plane: RTMP/SRT ingest server (MediaMTX/NGINX), Transcoding workers (FFmpeg containers), Packaging (HLS/DASH), Object storage (S3-compatible), CDN.
- Infra: Postgres, Redis, Kafka, Flyway, Docker Compose for local dev, K8s for prod.

Mermaid diagram (place in docs where rendering is supported):

```mermaid
flowchart TD
  U[Users / Creators / Admins] --> FE[Web / Mobile]
  FE --> API[API Gateway / Edge]
  API --> CP[Control Plane (Spring Boot modular monolith)]
  CP --> DB[(PostgreSQL)]
  CP --> REDIS[(Redis)]
  CP --> KAFKA[(Kafka)]
  CP --> JOBS[Job Orchestrator (Kafka + job table)]
  JOBS --> MEDIA_QUEUE[(Media Jobs Topic)]
  MEDIA_QUEUE --> TC[Transcoding Workers (FFmpeg containers)]
  TC --> S3[(S3-compatible Object Storage)]
  S3 --> CDN[CDN / Edge Delivery]
  LIVE_INGEST[RTMP/SRT Ingest Server] --> LIVE_TRANSCODER[Live Transcoder / Packager]
  LIVE_TRANSCODER --> S3
  FE --> CDN
  FE --> CHAT[WebSocket / STOMP Chat Service]
  CHAT --> REDIS
```

3. Repository & module structure (Maven multi‑module)

Rationale
- Use a Maven multi‑module project to implement a modular monolith for the control plane, enabling clear module boundaries and future extraction to microservices.
- Keep heavy CPU media tasks in separate worker containers (FFmpeg) to avoid running FFmpeg inside the JVM.

Proposed layout

```
super-emit/
  pom.xml (parent)
  docs/
    phase-0-plan.md
  infra/
    docker-compose.yml
    k8s/ (placeholders)
  control-monolith/
    pom.xml
    modules/
      auth/
      user/
      channel/
      catalog/ (videos metadata)
      feed/
      orchestration/ (job handling & events)
      moderation/
      notifications/
  media-workers/
    ffmpeg-worker/ (dockerfile + job consumer)
    live-ingest/ (config for MediaMTX or nginx-rtmp)
  web/ (React + Vite)
  mobile/ (React Native - optional)
  scripts/
  .github/
    workflows/ci.yml
```

4. Database entity relationship design (core entities)

Principles
- Use Postgres for transactional consistency.
- Add GIN and B-Tree indexes for common query patterns.
- Use UUID primary keys, created_at / updated_at, and soft delete where appropriate.

Core tables (summarized)

- users(id uuid PK, email, password_hash, display_name, role, status, created_at)
- channels(id uuid PK, owner_user_id FK users, name, handle, description, subscriber_count, created_at)
- videos(id uuid PK, channel_id FK channels, title, description, status, visibility, storage_key, poster_url, duration_seconds, created_at, published_at)
- video_renditions(id PK, video_id FK, rendition_label, s3_key, manifest_key, created_at)
- live_streams(id PK, channel_id, title, stream_key_hash, status, ingest_endpoint, playback_url, started_at, ended_at)
- likes(user_id, video_id, created_at) PK(user_id,video_id)
- comments(id PK, user_id, video_id, parent_id, content jsonb, status, created_at)
- subscriptions(subscriber_user_id, channel_id, created_at) PK(subscriber_user_id, channel_id)
- jobs(id PK, job_type, payload jsonb, status, attempts, created_at, last_attempt_at)
- audit_logs(id PK, actor_user_id, action, target_type, target_id, metadata jsonb, created_at)

Index recommendations
- videos(channel_id, status, created_at)
- GIN on videos.tags (jsonb or text[]) for tag search
- comments(video_id, created_at)

5. API and frontend route conventions

API conventions
- Versioned REST under /api/v1
- DTOs with Jakarta validation
- Cursor pagination for feeds (opaque cursor token)
- Status codes: 201 (created), 202 (accepted for async), 200, 204, 400/401/403/404/409/422/429/500 as needed
- Correlation ID header X-Request-Id

Selected endpoints (examples)
- Auth
  - POST /api/v1/auth/register
  - POST /api/v1/auth/login
  - POST /api/v1/auth/token/refresh
- Users / Channels
  - GET /api/v1/users/me
  - POST /api/v1/channels
  - GET /api/v1/channels/:id
- Videos
  - POST /api/v1/videos -> returns {videoId, uploadUrl, uploadId}
  - POST /api/v1/videos/:id/upload-complete -> 202 enqueues transcode
  - GET /api/v1/videos/:id -> metadata + renditions
- Live
  - POST /api/v1/live/streams -> returns streamId + streamKey (only to owner)
  - POST /api/v1/live/streams/:id/start
  - POST /api/v1/live/streams/:id/end
- Feed / Search
  - GET /api/v1/feed/home?cursor=
  - GET /api/v1/search?q=&type=

Frontend route conventions (React / Vite)
- / — home feed
- /watch/:videoId
- /channel/:channelId
- /upload
- /studio
- /live/new
- /live/:streamId
- /search?q=

WebSocket chat
- STOMP over WebSocket recommended for chat presence and authentication.
- Use Redis for presence and ephemeral channel state; persist chat messages asynchronously.

6. Media ingest → playback design

Principles
- Control plane issues signed credentials (presigned S3 URLs, stream keys). Media plane performs heavy work.
- No video bytes go through control plane HTTP endpoints.
- Idempotent jobs, durable events, retries with DLQ.

VOD upload flow (detailed)
1. Create metadata: POST /api/v1/videos -> server creates row with status=UPLOADED, returns presigned PUT URL and uploadId.
2. Client uploads file directly to S3 (presigned); optionally provide checksum.
3. Client calls upload-complete; server verifies (optionally checks checksum), creates job: TRANSCODE.
4. Transcode worker consumes job from Kafka, downloads source, runs FFmpeg to produce renditions and HLS manifests, uploads to S3 under videos/{videoId}/v{n}/.
5. Worker emits VideoProcessed event; control plane updates videos.status=READY and notifies Feed service to index.
6. CDN serves HLS manifests; playback URL returned in video metadata.

Live ingest flow (detailed)
1. Creator requests live session -> server generates stream_key (store only hash) and ingest endpoint.
2. Creator publishes via RTMP/SRT to ingest server which validates via control plane API.
3. Live transcoder produces HLS segments (LL‑HLS or CMAF for low latency if required) and pushes to S3/origin.
4. CDN serves segments; chat handled via WebSocket/STOMP services.

Storage layout examples
- s3://super-emit/videos/{videoId}/source/{original}
- s3://super-emit/videos/{videoId}/renditions/{bitrate}/index.m3u8
- s3://super-emit/live/{streamId}/hls/{segment.ts}

Job orchestration and reliability
- Use Kafka topics for job events; jobs table for visibility.
- Workers idempotent (job_id + versioning); retry/backoff; DLQ.

7. Concurrency & scaling strategy

Control plane
- Stateless Spring Boot pods behind LB
- Read replicas for Postgres (readonly traffic)
- Redis for caches, counters, rate limits
- Optimistic locking for concurrent edits
- Partition (shard) big tables by channel_id or hashed key when needed

Media plane
- Worker autoscaling based on Kafka lag
- Use spot nodes for non‑critical transcodes
- Use GPU instances for high-efficiency encodes when needed

Event ordering
- Use partitioning by videoId/channelId for ordered processing where necessary
- Use idempotency keys to guard duplicates

Backpressure & rate limits
- Gateway enforces quotas and per-user limits
- Use circuit breakers in internal calls

8. Security & moderation plan

Authentication & authorization
- JWT short‑lived access tokens + refresh tokens; RBAC (VIEWER, CREATOR, MODERATOR, ADMIN)
- Resource level ownership checks
- Use scopes for upload and admin actions

Stream key handling
- Only store hashed stream keys; provide rotation and revocation endpoints

Upload & content safety
- Validate mime types & sizes; transcode to normalized formats; virus/scan and ML classifiers
- Automated quarantine pipeline for flagged uploads

Moderation workflow
- Auto classification + human review queue
- Reports produce moderation tasks with priority scoring
- Audit logs for all moderation and admin actions

Logging & monitoring
- Structured logs, correlation IDs, Prometheus metrics, alerting
- Avoid logging secrets and PII

9. Phase‑by‑phase implementation roadmap

Phase 0 — Planning (this deliverable)

Phase 1 — Core control plane + E2E vertical slice (MVP)
- Auth (register/login + JWT)
- Create Channel
- Video metadata & presigned upload
- Minimal worker (mock or FFmpeg) producing one playable file
- Watch page
- Flyway migrations & CI

Phase 2 — Live streaming POC
Phase 3 — Feed, search, engagement
Phase 4 — Scaling & infra (CDN, K8s, signed URLs)
Phase 5 — Moderation, analytics, monetization

10. Phase‑1 detailed vertical slice & acceptance criteria

Scope
- Auth: register/login
- Channel create
- POST /api/v1/videos -> presigned upload URL
- POST /api/v1/videos/:id/upload-complete -> enqueue transcode
- Worker produces a single normalized MP4 (or HLS) and updates video record
- Watch page plays the produced media

Acceptance criteria
- Manual: Register -> Create Channel -> Upload -> transcode -> Play in UI locally
- Automated: Unit tests for auth and integration for upload flow using docker-compose
- Security: Password hashed, upload URL short lived, only owner can set visibility

11. Dependencies, risks, and mitigations

Dependencies
- Java 25 (verify Spring Boot compatibility; fallback plan available)
- Spring Boot (latest compatible release)
- Postgres, Redis, Kafka, MinIO/ S3, FFmpeg, MediaMTX

Risks & mitigations
- Spring Boot incompatibility with Java 25 — verify and fallback to Java 21 if necessary
- Media scaling cost — start small, use spot instances
- Copyright abuse — scanning + human moderation

12. Next steps
- Confirm Phase‑0 and approve Phase‑1 scope
- I will then scaffold the Maven multi‑module control monolith, add Flyway migrations, implement Auth module, video metadata endpoints, a simple FFmpeg worker (or mock per your choice), docker-compose for local dev, and CI. I will open a PR for Phase‑1 work and run tests.



