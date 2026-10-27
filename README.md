# A2A Invoice Action Agent (A2A 1.0)

A runnable FastAPI service implementing the assignment's HTTP+JSON surface. It creates evidence-backed proposals first, then completes only after a matching results continuation arrives. Rejected proposals are retained in history but never included in `executions`.

## Important deployment notes

- Configure `BASE_URL` to the **exact public HTTPS base URL** you submit (including its path and trailing slash, if any). The API routes mount under that path; the Agent Card remains at the origin-level discovery path. Do not include credentials, query, or fragment.
- Use a **persistent PostgreSQL database** for hosted submissions. Free web-service filesystems may be ephemeral, which would lose Tasks and decision cache on restart. The app accepts a PostgreSQL `DATABASE_URL`.
- Deploy with **one worker**. An in-process async lock makes same-process idempotency and cancel-vs-result transitions atomic. For multi-worker deployments, replace that lock with a shared distributed lock/database compare-and-swap.
- Set a real provider key in `AI_API_KEY`; no provider is hard-coded into the grader contract. The endpoint is OpenAI-chat-completions compatible and configurable via `AI_BASE_URL` and `AI_MODEL`.
- The service treats each syntactically valid Bearer token as a separate principal and stores only a hash of the token. If your deployment platform adds its own authentication, disable that layer for the grader's API routes.

## Local run

Python 3.12 is recommended.

```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # then set variables in your shell or hosting dashboard
uvicorn app:app --host 0.0.0.0 --port 8000 --workers 1
```

Windows PowerShell environment example:

```powershell
$env:BASE_URL = "http://127.0.0.1:8000"
$env:AI_BASE_URL = "https://api.openai.com/v1"
$env:AI_API_KEY = "YOUR_PROVIDER_KEY"
$env:AI_MODEL = "gpt-4o-mini"
$env:DATABASE_URL = "sqlite:///./invoice_agent.db"
uvicorn app:app --host 0.0.0.0 --port 8000 --workers 1
```

Health check:

```bash
curl http://127.0.0.1:8000/healthz
curl http://127.0.0.1:8000/.well-known/agent-card.json
```

## Contract checks performed

The local smoke tests cover the A2A 1.0 Task shape (without the legacy `kind` discriminator), initial `TASK_STATE_INPUT_REQUIRED`, one proposal artifact, stable replay when only `configuration` changes, `409 IDEMPOTENCY_CONFLICT` for changed message content, accepted-result receipt binding, terminal replay, per-token task isolation, auth/version failures, and the cancel-versus-result race. Evidence validation also requires exactly three distinct bracketed refs that co-occur in one textual paragraph.

These smoke tests use a mocked model response; they do not establish accuracy on the grader's hidden invoices. Run the hosted Check before the final Save, leaving one check available for Save.

## Routes

- `GET /.well-known/agent-card.json` — public Agent Card
- `POST /message:send` — initial batch or result continuation
- `GET /tasks/{id}` — owner-only Task read
- `GET /tasks` — owner's tasks only
- `POST /tasks/{id}:cancel` — cancel a nonterminal Task

All non-card A2A routes require `Authorization: Bearer <token>` and `A2A-Version: 1.0`. POSTs require `Content-Type: application/a2a+json`.

## Deploy (generic Docker host / Render)

1. Push this folder to a private GitHub repository.
2. Create a Docker web service from the repo. Use the included `Dockerfile` or `render.yaml`.
3. Provision persistent PostgreSQL (Neon, Supabase, Render Postgres, etc.) and set `DATABASE_URL` to its connection string. The app converts `postgres://` and `postgresql://` to SQLAlchemy's psycopg 3 driver URL.
4. Set `AI_API_KEY`, `AI_BASE_URL`, and `AI_MODEL` as hosting environment variables.
5. Deploy, then set `BASE_URL` to the assigned HTTPS origin and redeploy if needed.
6. Confirm `https://YOUR-HOST/.well-known/agent-card.json` returns the card without a redirect. The `supportedInterfaces[0].url` must exactly equal the URL you submit.
7. Test one batch via the grader's Check before using Save; every Check and Save consumes a check, and Save should be reserved for the final submission.

## Known boundary / production hardening

The supplied contract focuses on the grader's single-service protocol. Before using this with real financial records, add a fixed allowlist or signed identity system for Bearer credentials, provider-specific structured output enforcement, key rotation, audit logging, rate limits, and a distributed lock/transaction strategy for horizontally scaled workers. Do not send real invoices or secrets to a model provider without approval.
