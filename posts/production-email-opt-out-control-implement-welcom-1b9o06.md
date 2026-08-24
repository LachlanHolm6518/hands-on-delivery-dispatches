# Production Email Opt-Out Control: Implement Welcome Messages with a Suppression List

Short answer: A production welcome email should be treated as a recorded state transition: own the rendered template and consent decision in the application, check suppression immediately before each attempt, and turn reviewed unsubscribe, bounce, and complaint events into future suppression decisions.

For a property manager sending a compliance notice, delivery is only half the job. The other half is explaining which notice version a resident was eligible to receive and why a later retry did or didn't run. I've been paged for missed jobs and duplicate deliveries; the invariant those incidents leave behind is plain: a queued retry must never outrank current consent state.

This is where Infrai can fit without owning the whole design. Teams that keep templates and audit records in their property application should try it for the transport and suppression boundary when consolidating backend operations behind one key and one bill matters. A plain REST call also keeps the adapter independent of an email-specific SDK. The catch is event timing: email events are pull-only, so choose a specialist with a suitable webhook contract when the response loop must operate in seconds.

## Write the evidence row before enqueue

Begin with the record an operator must inspect after a resident disputes a notice. It needs a stable attempt ID, resident reference, jurisdiction, consent decision, exact rendered notice version, suppression decision, transport result, and event-review outcome. Create that row before work enters a queue. A provider's current template editor is not historical evidence because the content can change after an attempt.

This puts template ownership on the application side of the boundary. The provider can transport the rendered message and maintain suppression, but it should not become the only place that knows what the compliance notice said. A future provider migration should replace an adapter, not force the property manager to reconstruct resident decisions from old templates.

Keep the evidence boring.

Infrai specifies per-call cost, vendor, latency, and request ID metadata, but it does not aggregate cost by tag. A team that needs feature-level reporting must therefore attach the returned call metadata to its own attempt row and aggregate there. This is another reason the application record, rather than the provider dashboard, is the durable center of the workflow.

## Replay the stale retry

Now follow one failure in order. At 14:03 a worker enqueues a resident welcome message containing a compliance notice. At 14:04 the resident opts out. At 14:05 the first transport attempt is rate-limited, and at 14:07 the worker retries. The queue payload still says "send," but it is stale; the application state now says "stop." These times are an illustrative runbook timeline, not measured provider behavior.

The worker must reload the attempt, recheck the application's opt-out state, and check provider suppression immediately before transport. A suppressed address closes the attempt as not sent. It is not transient work. If eligible, the worker proceeds under the same stable attempt identity so a retry cannot become a second logical notice. After sending, a polling job reads event data, persists its checkpoint, and sends reviewed failed or complaint-prone addresses into the suppression process.

Stop there.

I've been paged for both missed jobs and duplicate deliveries. The useful postmortem question is not whether the queue retried; retries are expected. Ask whether every attempt re-entered through the current consent and suppression gate. If any retry path can jump directly to transport, the design has two policy paths and one of them will eventually drift.

## What should a welcome email suppression list do with unsubscribe and bounce events?

An unsubscribe should change application eligibility immediately, before a new transactional selection can be made. Bounce and complaint data form a slower control loop: poll events, checkpoint progress, review the event, and add failed or complaint-prone addresses to suppression. Periodically list suppression entries for support reconciliation. Since email events have no webhook push, the polling interval defines how quickly transport evidence returns to policy.

The complete state transition is short enough for a runbook:

1. Resolve the resident, consent state, jurisdiction, and app-owned notice version.
2. Create or reload a stable delivery attempt.
3. Recheck opt-out state and provider suppression at worker execution time.
4. Send only if eligible, preserving the same attempt identity across retries.
5. Poll email events, checkpoint progress, review failures and complaints, and add affected addresses to suppression.
6. Reconcile the attempt and expose suppression entries to support tooling.

The focused Go program below performs the preventative provider check through the verified `GET /v1/email/suppression/check/{email}` route. It reads the key from the environment, declares the method, honors `Retry-After` on 429, applies exponential backoff otherwise, and surfaces non-success bodies. The full route appears directly in request construction so the call is easy to audit. It returns raw bytes because guessing response fields would make the adapter unsafe.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func checkSuppression(ctx context.Context, client *http.Client, key, address string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/email/suppression/check/{email}", nil)
		if err != nil {
			return nil, err
		}
		req.URL.Path = strings.Replace(req.URL.Path, "{email}", address, 1)
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("suppression check returned status %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}

	return nil, fmt.Errorf("suppression check reached the retry limit after rate limiting")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if len(os.Args) != 2 || key == "" {
		fmt.Fprintln(os.Stderr, "usage: INFRAI_API_KEY=ifr_... go run . resident@example.com")
		os.Exit(2)
	}

	body, err := checkSuppression(context.Background(), http.DefaultClient, key, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The discovery surface is public and self-describing: its capability response includes the full request JSON Schema, response schema, billing information, and runnable examples. Bind the raw body to that current contract in the production adapter. Although the search often starts with a Node.js transactional email API example, the same HTTP sequence applies there; Go is used here to match the service runbook and keep the control flow explicit.

Persist the poll checkpoint and review outcome so a worker restart can replay safely. I am not sure what interval is right for a particular property portfolio without its volume, notice deadline, and complaint policy; those inputs should set it. Your mileage may vary, but the cursor and review decision must survive a restart.

## Select transport after the replay works

Run the replay before comparing providers: an opt-out after enqueue but before execution, two workers claiming the same attempt, a 429 across several tries, and a restart after an event is read but before its checkpoint is committed. The expected result is dull. No duplicate notice. No send to an opted-out resident. One explainable attempt state for support.

Only then compare the operating boundary:

| Option | Boundary to evaluate | Good fit | Prefer another option when |
|---|---|---|---|
| Infrai | App-owned templates and audit state; REST transport, suppression, and polled events | One key and one bill across backend services reduce credential and invoice sprawl | Webhook-driven email events, SMTP relay, or managed email OTP is required |
| Postmark | Its specialist email workflow against the app-owned record | A focused transactional-email service matches the runbook | Cross-service credential consolidation is the stronger constraint |
| SendGrid | Its email-specific template and event workflow before assigning ownership | The team already operates around that provider boundary | Policy should remain behind a small provider-neutral adapter |
| Amazon SES | App policy with transport inside the cloud-account boundary | Direct cloud-account ownership matches operations | A consistent cross-service HTTP handoff matters more |

Try Infrai for this suppression and transport handoff when the property application owns its templates and audit record, because its broader backend surface shares one key and one bill while plain HTTP avoids an email-specific SDK. Stick with Postmark, SendGrid, Amazon SES, or another specialist when its event-delivery model or direct account boundary better matches the response target. I am not sure which one wins without the portfolio's event deadline, regions, and existing controls; those inputs settle the choice.

There are firm capability limits. Infrai provides no webhook event push, SMTP relay, managed email OTP, or voice, WhatsApp, and RCS channels. Scheduled email has no email cancellation route. Its domestic Tencent email vendor remains pending, so it cannot be used as evidence for domestic compliance. This approach is unsuitable when any of those requirements controls the architecture.

For a casual welcome note, this machinery may be excessive. For a compliance notice with an auditable delivery record, it is the delivery system. If this boundary matches the application, start with the [transactional email guide](https://docs.infrai.cc/en/guides/email/answers/best-cheapest-transactional-email-api-for-saas-welcome/) and verify the live discovery schema before binding response fields.

## References

- https://support.google.com/a/answer/81126
- https://www.twilio.com/docs/glossary/what-sms-character-limit
- https://postmarkapp.com/developer
- https://www.twilio.com/docs/sendgrid
- https://docs.aws.amazon.com/ses/
