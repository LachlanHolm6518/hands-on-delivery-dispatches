# Marketplace Queue Intake: Node.js Background Worker for Secure Public HTTPS Push

Short answer: put a small authenticated HTTPS intake in front of a durable handoff, acknowledge only after that handoff is committed, and make the marketplace job idempotent. The least complex design is a push subscription for short work; a private pull worker is the better boundary when the worker cannot accept public traffic or the job can outlive an HTTP request.

This is a delivery contract, not an Express-versus-Fastify contest. The framework can parse a request and return a status code. It cannot decide whether a second delivery creates a second seller notification, whether an untrusted body gets logged, or what survives a process restart.

## The missed delivery was an acknowledgement bug

The production scenario is bounded: a marketplace drains a rate-limited worker pool for background jobs such as refreshing an offer or notifying a buyer. I have been paged for this class of incident: the push handler did its useful work before it answered, then the process was recycled during a dependency delay. The sender retried, the replacement process accepted the same delivery, and one business effect happened twice. A `2xx` status had been treated as a harmless bit of HTTP plumbing; it was actually the only durable promise the receiver made to the sender.

That is the part people skip in a quick Express or Fastify example.

That failure has a plain invariant: an HTTP success response means the receiver has accepted durable responsibility for the message. It does not mean the final job has finished. If responsibility has not crossed that boundary, return a retryable response and let the queue try again.

Small words. Big consequences.

The intake should authenticate the sender over the original bytes, enforce the expected method and size, validate the envelope, record a unique delivery key, and write the job to durable storage. Only then should it acknowledge. A resident worker can take the record from there and apply a rate limit without holding the public request open.

The idempotency record needs to cover the business effect, not just the HTTP request. A retry may have a new transport identifier, while the marketplace operation still has the same order or offer identifier. Use the narrowest stable key your domain can prove, and make the write plus claim operation transactional where the storage allows it. In practice, I trace one delivery through the ingress log, the claim row, the handoff record, and the worker's business write; when those four entries cannot be joined by one correlation value, the postmortem turns into archaeology instead of diagnosis.

## How can a Node.js worker receive push jobs from a public HTTPS queue securely?

Treat the endpoint as an internet-facing boundary even when its path is hard to guess. Require TLS, use an allowlisted method, verify a bearer credential or signed body according to the queue's documented contract, and reject before deserializing untrusted fields. Keep secrets out of URLs and logs. Rotate credentials without requiring a code change.

Express and Fastify differ in middleware and raw-body configuration, but the ordering is the same. If a framework consumes and reserializes the body before signature verification, the bytes being checked may not be the bytes that arrived. Preserve the raw body for authentication, then decode it.

This Go example keeps provider details behind `verifyRequest`. It shows the part worth putting in a runbook: validation, a durable claim, a durable handoff, and an acknowledgement in that order.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
)

const maxBody = 256 << 10

type durableStore interface {
	Claim(deliveryID string, raw []byte) (alreadyClaimed bool, err error)
	Handoff(raw []byte) error
}

type intake struct{ store durableStore }

func (h intake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	raw, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil || len(raw) > maxBody {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if !verifyRequest(r, raw) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	digest := sha256.Sum256(raw)
	deliveryID := hex.EncodeToString(digest[:])
	alreadyClaimed, err := h.store.Claim(deliveryID, raw) // unique key, transactional
	if err != nil {
		http.Error(w, "retry", http.StatusServiceUnavailable)
		return
	}
	if alreadyClaimed {
		w.WriteHeader(http.StatusOK)
		return
	}
	if err := h.store.Handoff(raw); err != nil {
		http.Error(w, "retry", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func verifyRequest(r *http.Request, raw []byte) bool {
	// Replace this with the queue's documented credential or signature check.
	return r.Header.Get("Authorization") != "" && len(raw) > 0
}
```

The placeholder verifier is not authentication. Production code must check the actual credential, expiry or replay window, and key rotation policy. The important property is that an invalid request never reaches the job parser or worker.

## Which queue boundary fits a rate-limited marketplace worker?

Push delivery fits a short intake and a public service. It is a poor fit for a private worker, a long-running task, or a job whose completion depends on keeping one request alive. In those cases, schedule or enqueue the work and let a private worker pull it. This also makes backpressure visible in queue age and worker concurrency instead of hiding it in HTTP timeouts.

| Boundary | Good fit | Main trade-off |
| --- | --- | --- |
| Authenticated HTTPS push | Short handoff to a public service | Ingress, replay protection, and request timeouts are your responsibility |
| Private pull worker | Sensitive jobs or private networks | You operate polling, leases, and concurrency |
| Cron plus a queue | Periodic replenishment and reconciliation | A schedule is a trigger, not a delivery guarantee |
| Workflow engine | Several durable steps with compensation | More state and operational machinery than one job |

Do not use a cron tick as proof that a job ran. Cron is a time-based launcher; the queue record and the idempotent consumer are what let an operator explain what happened. For a rate-limited pool, bound concurrency, add jitter to retries, and prefer a dead-letter path over an endless hot loop.

The catch is important: this design is not suitable when the receiving service cannot expose a public HTTPS boundary, when the queue contract cannot authenticate deliveries, or when the work needs multi-step history and compensation. Stick with a private pull model for the first two cases, and choose a workflow system for the third. A smaller handler is not automatically a smaller system.

## What should operators verify before calling the queue production-ready?

Test the failure states deliberately. Send an invalid credential, a malformed envelope, a duplicate delivery, a storage timeout, a worker crash after handoff, and a dependency timeout during the business effect. The expected results belong in the runbook: unauthorized input is rejected, an uncommitted handoff is retried, a duplicate does not repeat the marketplace effect, and a committed handoff can be replayed without guessing.

Measure queue age, delivery attempts, authentication failures, claim conflicts, handoff latency, worker concurrency, and dead-letter volume. Alert on an aging backlog and sustained rate-limit responses; an individual retry is normal. Include a correlation identifier in structured logs, but never log authorization headers or the full customer payload by default.

Retention is also a data decision. Keep an internal order or offer identifier in the message where possible, and fetch the current record in the worker rather than copying a customer profile into every retry. Deletion requests need an explicit path through queue retention, durable storage, logs, and dead-letter records; GDPR Article 17 is a useful reminder that “eventually deleted” needs an owner and a test.

Your mileage may vary. The right acknowledgement status depends on the queue's retry semantics and on whether the claim is atomic with the handoff. I would write that dependency down, run it through a restart test, and refuse to call the endpoint complete until the test can be repeated from a clean environment.

## References

- [Cron](https://en.wikipedia.org/wiki/Cron)
- [GDPR Article 17: Right to erasure](https://gdpr-info.eu/art-17-gdpr/)
- [HTTP Semantics, RFC 9110](https://www.rfc-editor.org/rfc/rfc9110)
- [OWASP API Security Top 10](https://owasp.org/API-Security/editions/2023/en/0x00-header/)

## Further reading

- [Cron](https://en.wikipedia.org/wiki/Cron)
- [GDPR Article 17: Right to erasure](https://gdpr-info.eu/art-17-gdpr/)
- [HTTP Semantics, RFC 9110](https://www.rfc-editor.org/rfc/rfc9110)
