---
"effect-agent": minor
---

Retry model calls that fail transiently before streaming any content with `AgentPolicy.make({ ..., modelRetries: 3 })`, covering retryable `AiError` failures and 429/5xx stream error parts with exponential backoff.
