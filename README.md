# FlowAPI

FlowAPI is a self-hosted visual workflow automation engine with a backend-first durable execution architecture and a real React Flow editor.

## Development

Copy `.env.example` to `.env`, replace both secrets (the encryption value must be a Fernet key), then run `docker compose up --build`.

Execution creation is asynchronous: API requests persist an execution pinned to an immutable `workflow_version_id`, create node state, and write queue work to the transactional outbox. Workers—not route handlers—execute nodes. PostgreSQL is the durable source of truth; Redis is coordination only.

## Security model

Flow expressions use an AST allowlist and expose only trigger, input, nodes, variables, and execution scopes. FlowAPI has no arbitrary Python, JavaScript, or shell nodes. HTTP destinations are resolved and pinned to validated addresses, redirects are revalidated, credentials use Fernet authenticated encryption, and PostgreSQL queries use separately bound parameters.

## Services

The Compose stack runs PostgreSQL, Redis, migrations, FastAPI, an outbox dispatcher, workers, the durable cron scheduler, and the Next.js editor. `make demo` publishes three workflows but deliberately creates no execution history; demos run through the normal engine.
