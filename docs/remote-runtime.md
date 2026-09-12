# Advanced: running the labs from the course site

**The default needs none of this.** `python3 lab_server.py` serves the labs at
http://localhost:3000 and talks to a runtime on the same machine. Nothing is exposed, there is no
token, and the browser never asks permission. If that works for you, stop reading.

This page is about the other shape: driving your local runtime from the course pages on
scimigo.com, so the reading and the lab sit in one place.

## Why it needs a tunnel

A page served from `https://scimigo.com` cannot reach `http://localhost` on your machine. Chrome
blocks it with `ERR_BLOCKED_BY_LOCAL_NETWORK_ACCESS_CHECKS`, for fetches and for iframes alike. That
is a deliberate protection: a public web page has no business reaching into your laptop. Depending
on your browser version you may instead be asked for permission; if you are, granting it is the
better path and you can skip the tunnel entirely.

A tunnel works because it gives your runtime a public https address, so the request is no longer a
local-network request at all. Measured working end to end: a page on scimigo.com executed Python on
a machine behind a Cloudflare quick tunnel, with a pairing token, no browser flags.

## Understand the trade before you do it

Your code-execution runtime becomes reachable from the internet. The bearer token is the only thing
standing between a stranger and a shell on your machine. The hostname is random, but that is
obscurity, not access control. Your traffic also passes through the tunnel provider.

So: start the tunnel when you begin a lab, stop it when you finish, never leave it running, and
never disable pairing while it is up.

## The recipe

```bash
# 1. runtime, with pairing ON (the default)
agent-runtime serve --port 9477

# 2. pair the course origin: the page calls POST /pairing/request, you approve in this
#    terminal, and the page receives an origin-bound token
agent-runtime pairing list        # confirm https://scimigo.com is there afterwards

# 3. a public https address for the runtime
cloudflared tunnel --url http://localhost:9477
#    → https://<random-words>.trycloudflare.com

# 4. in the course page's lab panel, paste that URL and the token
```

When you are done: stop `cloudflared`, stop the runtime, and revoke the origin with
`agent-runtime pairing revoke https://scimigo.com`.

## Known rough edges, as of 2026-09

- **The tunnel URL changes every run,** so the paste step repeats each session.
- Two earlier rough edges are fixed as of 2026-09-12: a browser can now call
  `POST /pairing/request`, which prompts you in the runtime's terminal and returns an origin-bound
  token, and the allowed-origin list is no longer frozen at startup.
- Alternatively point the local page at the remote runtime with
  `python3 lab_server.py --runtime https://<your-tunnel>`. The page prints a warning and shows a
  banner when the runtime is not on your machine, which is the situation worth noticing.
