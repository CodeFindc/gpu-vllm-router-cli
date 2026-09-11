# API Documentation & Swagger UI Guide

`gpu-vllm-router` provides a complete, native OpenAPI 3.0.3 specification and interactive web UI for all inference, management, and observability endpoints.

---

## 1. Quick Access (Web UI)

When `gpu-vllm-router` is running (in either `mode: proxy` or `mode: run`), open your browser:

| Interface | URL | Description |
| :--- | :--- | :--- |
| **Swagger UI** | `http://<router-host>:<port>/swagger/` or `http://<router-host>:<port>/docs` | Interactive API explorer with "Try it out" execution, request schema models, parameter docs, and branded navigation bar |
| **ReDoc UI** | `http://<router-host>:<port>/redoc` | Clean, modern three-panel reference documentation |
| **OpenAPI 3.0 JSON** | `http://<router-host>:<port>/openapi.json` or `/swagger/doc.json` | Raw OpenAPI 3.0 specification in JSON format |

---

## 2. API Endpoints Overview

### OpenAI Compatible Inference Endpoints
- `POST /v1/chat/completions` — Chat completions with streaming SSE (`stream: true`) and prefix cache session affinity (`X-Session-ID`, `X-User-ID`).
- `POST /v1/completions` — Text completions.
- `POST /v1/embeddings` — Text embeddings generation.
- `GET /v1/models` — List of active models synced from GPUStack cluster.

### Control Plane & Cluster Management Endpoints
- `GET /admin/stats` — Real-time proxy status, active backend connections, circuit breaker states (`closed`, `open`, `half_open`), fail count, and cooldown timers (*available in `mode: proxy`*).
- `GET /admin/supervisor` — Supervisor status, worker breakdown, health states, and rolling reload metrics (*available in `mode: run`*).

### Observability & System Health
- `GET /health` — Kubernetes / Docker health check probe (`{"status":"ok"}`).
- `GET /metrics` — Prometheus metrics (request counters, active connections, circuit breaker trip counts, latency).

---

## 3. Static Specification Files

For offline usage, CI/CD pipelines, or importing into Postman / Insomnia / Apifox:

- **JSON Spec**: [`docs/openapi.json`](./openapi.json)
- **YAML Spec**: [`docs/openapi.yaml`](./openapi.yaml)

### Importing into Postman
1. Open Postman.
2. Click **Import** -> Select `docs/openapi.json` (or paste `http://<router-host>:<port>/openapi.json`).
3. Postman will automatically generate a complete Collection with all endpoints and example payloads.
