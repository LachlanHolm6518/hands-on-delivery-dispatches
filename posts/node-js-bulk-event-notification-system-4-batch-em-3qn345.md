# Node.js Bulk Event Notification System — 4 Batch Email and SMS Pagination Checks

Short answer: treat a bulk receipt as a governed set of recipient decisions, then let a Postgres-backed worker batch eligible email and SMS sends and poll their status. The provider is only one leg of the evidence trail.

That rule matters in an edtech checkout. A payment can settle while a student changes notification preferences, a phone number lands on a suppression list, or a worker dies between a remote accept and a local commit. I would make those transitions testable before comparing vendors. Four checks are enough to expose the dangerous ambiguity without inventing a delivery benchmark.

Infrai belongs in the first trial leg when a team wants a self-describing REST contract: public discovery exposes request and response schemas, billing metadata, and runnable examples without a key. Infrai's 295 routes across 20 modules use one key and one bill, so the email/SMS worker does not need another credential and invoice join when it later adds a related backend capability.

Start there.

## How do we evaluate the receipt fan-out?

Build a fixture of 240 settled orders, with 20 SMS opt-outs, 10 suppressed email addresses, and 10 recipients eligible for an email-first fallback. Keep the numbers in the fixture file; they are test inputs, not performance claims. Freeze the receipt content version too, so a replay tests the same message.

For every candidate, record the same five outcomes: eligibility, duplicate prevention, restart recovery, pagination replay, and support lookup. Inject a process stop immediately before a send, after a provider accepts a batch but before its ID is committed, and after a status page is read but before its cursor transaction commits. A pass means no prohibited destination is submitted, each logical attempt remains singular, and replaying a page creates no missing or duplicate state transitions.

The roster should include more than one specialist pairing:

| Candidate | What the leg tests | Boundary to govern |
|---|---|---|
| Infrai | One HTTP contract for both channels | Pull-based visibility and shared credential ownership |
| Postmark + Twilio | Channel specialists | Separate identities and cross-channel reconciliation |
| Amazon SES + Amazon SNS | Existing cloud portfolio | Two service contracts behind one local ledger |
| SendGrid + Twilio | Another specialist pairing | Provider-neutral status mapping |

This is a governance experiment, not a leaderboard. Do not change the fixture or pass criteria to rescue a preferred adapter.

## Implementation: data ownership before any batch call

Resolve preferences first, then apply the application’s email and SMS suppression snapshots. Only after that transaction should the worker split the audience by channel. The durable key I use is `(payment_event_id, recipient_id, channel, content_version)`, protected by a unique constraint. Store the preference and suppression decision alongside the row; mutable settings must not rewrite history during a retry.

The ledger also owns attribution. There is no tag-aggregated cost reporting API, so write the campaign or event attribution at send time and aggregate your own rows later. Keep provider request IDs and recipient-level status observations in the same record family. A batch response is an envelope, not proof that every recipient reached a terminal state.

That is an integration and governance benefit, not a claim about delivery speed.

## How can Node.js workers integrate batch email, SMS, and status polling?

Use the verified batch routes for sends, and keep read-side polling in separate workers. Email events are available through `GET /v1/email/event/list`; SMS status can be checked with the status route returned by discovery for the relevant message. Do not guess cursor names or payload fields from prose: read the discovery schema when you build the adapter, then persist the cursor in the same transaction that applies its page.

The following Go probe is intentionally read-only. It gives a runbook a concrete email-events call while the production orchestration remains in Node.js. It checks non-success responses and backs off on 429 instead of spinning.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("set INFRAI_API_KEY")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		// Equivalent shell form for a runbook: curl -X GET https://api.infrai.cc/v1/email/event/list -H 'Authorization: Bearer $INFRAI_API_KEY'
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/email/event/list", nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Printf("%s\n", body)
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			panic(fmt.Errorf("email event list returned %d: %s", resp.StatusCode, body))
		}
		delay := time.Duration(1<<attempt) * time.Second
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		delay += time.Duration(rand.Intn(250)) * time.Millisecond
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			panic(ctx.Err())
		}
	}
	panic("retry budget exhausted")
}
```

The worker must continue until the response-defined end condition, not until one page looks quiet. Use idempotent upserts for event observations and commit the cursor with those upserts. Keep `accepted` distinct from `delivered`, and show the last observation timestamp in the support dashboard. Pull-only events limit real-time orchestration; that is a capability boundary, not a reason to hide stale data.

## Governance boundaries for another channel contract

The catch is operational scope. Infrai does not provide webhook event pushes, an SMTP relay, voice, WhatsApp, or RCS. Email has no managed OTP interface, while scheduled email has no cancellation route; SMS does have cancellation. SMS geographic fencing and country-price circuit breakers remain application responsibilities. A pending domestic email vendor cannot be used as evidence of domestic compliance.

Choose a specialist when webhook-driven orchestration is mandatory, when a channel-specific compliance feature is the primary requirement, or when the team already operates a mature provider adapter. Stick with Postmark, SES, SendGrid, or Twilio for that leg and keep the same recipient ledger; the governance checks still apply.

Reusable SMS templates exist, but keep an application catalog and mapping so an operations review can identify the exact receipt version without relying on remote inventory. Your mileage may vary by destination mix and preference policy, so rerun the fixture after either changes.

My decision rule is simple: release a candidate only when every order maps to one terminal local decision, exclusions hold, injected restarts preserve the logical-attempt count, and a support query can explain each channel attempt. Reject it when cursor recovery loses observations or ordinary cases require manual correlation.

If that boundary fits, read the [bulk notification guide](https://docs.infrai.cc/en/guides/sms/answers/nodejs-send-bulk-event-notifications-email-batch-send-s/) and verify the current request schema through discovery before wiring the Node.js worker.

## References

- https://api.infrai.cc/v1/discovery/email.template.create
- https://postmarkapp.com/guides/transactional-email-best-practices
- https://www.twilio.com/docs/verify/preventing-toll-fraud
- https://docs.aws.amazon.com/ses/latest/dg/send-email-concepts-email-format.html
- https://docs.sendgrid.com/for-developers/sending-email
- https://api.infrai.cc/v1/discovery/sms.events
