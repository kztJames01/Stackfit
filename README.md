# StackServe MCP

Workload-aware cloud stack advisor. An MCP server that recommends complete, compatible cloud stacks with line-item decimal cost estimates (`rust_decimal`), caveats, a cheaper baseline and a performance-oriented alternative, plus project-specific setup bundles.

## Tools

| Tool | Purpose |
| --- | --- |
| `profile_project` | Extract runtime, database, storage, and unresolved questions from a project brief or repo |
| `search_provider_docs` | Retrieve cited official provider doc chunks with `source_url` and `verified_at` |
| `recommend_stacks` | Filter incompatible services and rank feasible stacks by cost and operational trade-offs |
| `compare_stacks` | Compare itemized decimal costs across current, `3x` traffic, and `10x` storage/egress scenarios |
| `generate_setup_bundle` | Emit `.env.example`, `DEPLOYMENT.md`, `architecture-decision.md`, `provisioning-prompt.md`, and `resource-manifest.json` |

## Quick Connect (Remote MCP)

Connect directly to the hosted Streamable HTTP endpoint (`https://mcp.stackserve.net/mcp`) with zero local build:

### Claude Code
```sh
claude mcp add --scope user --transport http stackserve https://mcp.stackserve.net/mcp
```
Or install as a plugin:
```text
/plugin marketplace add kztJames01/StackServe
/plugin install stackserve@stackserve-marketplace
```

### OpenAI Codex
```sh
codex plugin marketplace add kztJames01/StackServe
codex plugin add stackserve@stackserve-marketplace
```
Or add to `~/.codex/config.toml`:
```toml
[mcp_servers.stackserve]
url = "https://mcp.stackserve.net/mcp"
```

### Cursor
Add to `.cursor/mcp.json` (or `~/.cursor/mcp.json`):
```json
{
  "mcpServers": {
    "stackserve": {
      "url": "https://mcp.stackserve.net/mcp"
    }
  }
}
```

### OpenCode & Kilo
Add to `opencode.json` or `kilo.jsonc`:
```json
{
  "mcp": {
    "stackserve": {
      "type": "remote",
      "url": "https://mcp.stackserve.net/mcp"
    }
  }
}
```

## Authentication

The hosted endpoint (`https://mcp.stackserve.net/mcp`) is protected by Cloudflare Access with managed OAuth. Add the URL to your client as usual: on first use the client opens a browser sign-in and then stores the session. There is no token to copy or rotate.

- Clients with MCP OAuth support (Claude Code, Cursor, Inspector, ChatGPT connectors) need no extra configuration.
- Clients that cannot complete an OAuth flow can use an Access service token (`CF-Access-Client-Id` and `CF-Access-Client-Secret` headers) issued by the maintainer, or run the server locally.
- `/healthz` stays unauthenticated so platform health checks keep working.

Self-hosted HTTP fails closed: `serve-http` will not start without a bearer token.

```sh
STACKSERVE_HTTP_BEARER_TOKEN="$(openssl rand -hex 32)" stackserve-mcp serve-http --bind 127.0.0.1:8787
```

Clients then send `Authorization: Bearer <token>` to `/mcp`. Set `STACKSERVE_HTTP_ALLOWED_HOSTS` (comma-separated) when serving behind a domain the server does not already allow.

## Offline Local CLI & Docker

Run completely offline without network access:

```sh
# Docker (offline stdio)
docker run --rm -i --network none ghcr.io/kztjames01/stackserve-mcp:0.1.0

# Or install the CLI onto $PATH
cargo install --git https://github.com/kztJames01/StackServe stackserve-mcp
stackserve-mcp recommend workload.json
```

## Privacy & limits

- StackServe does not provision resources, collect telemetry, or require cloud credentials. Never supply real secrets as project evidence.
- Remote MCP calls send their supplied arguments to the hosted service. Local offline execution and hosted execution have different privacy boundaries; infrastructure logging and retention must be covered by the hosted service's published privacy policy.
- Estimates use a pinned catalog snapshot—always verify each line item's `source_url` and `checked_at` against official provider pricing and terms before purchase.

## Chat plugin preparation

`plugin.json` and `mcp.json` provide portable Agent Plugins source for the existing five tools. `skills/stackserve/SKILL.md` describes chat usage; a custom embedded UI is not required. The initial intended listing is free, by Kaung Zaw Thant, in all available countries.

This source is not a published or submission-ready plugin. See [the readiness report](reports/plugin-readiness.md) for live endpoint verification, listing pages, demo, and review requirements. Only package the manifests, referenced assets, and `skills/stackserve/` for public upload; do not archive the repository, local configuration, model artifacts, or dependencies. Existing client-specific manifests remain for their respective clients.

## License

MIT OR Apache-2.0. Copyright (c) 2026 Kaung Zaw Thant.
