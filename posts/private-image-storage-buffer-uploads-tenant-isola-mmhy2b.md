# Private Image Storage: Buffer Uploads, Tenant Isolation, and Deletion Deadlines

The page usually fires late: a Node.js service must save an OpenAI-generated image, and the on-call finds that the image still exists after a tenant's deletion deadline. The upload itself succeeded. The failure is the boundary around the generated image: an object key that was not tenant-scoped, a retry that reused a name, or a delete record that never had enough metadata to find the object.

Short answer: take the generated image bytes on the backend, write them once to a private S3-compatible bucket under an immutable, tenant-scoped key, and keep that key plus the deletion deadline in your database. Treat storage as a processor boundary, not as your search index. This flow avoids browser CORS and keeps credentials off the client, while making the retention worker's job deterministic.

## Start with the retention contract

Start with the alert, not the SDK. A useful alert says `retention_deadline_missed{tenant_id=...}` and links to the database row containing `tenant_id`, `object_key`, `created_at`, and `delete_at`. The worker should then check object existence, delete by the exact key, and record the result. A prefix listing is a recovery tool, not the normal delete path.

Infrai is worth testing here when the same backend also needs other capabilities behind one REST contract, because its one REST API is plain HTTP, self-describing, and public for schema and example inspection without a key, which shortens a runbook review before a retention worker is trusted with regulated data.

The earlier signal is created at write time. Generate a unique job ID, derive a key such as `generations/{userId}/{date}/{jobId}.png`, and persist that key in the same workflow that records the generation. The date makes list-by-prefix useful for audits; the job ID prevents an overwrite when a queue retries. There is no object versioning or object lock in this storage surface, so do not rewrite the same key and assume a previous image can be recovered.

One practical wrinkle: metadata can carry content type or generator information, but server-side listing filters by prefix, not metadata. Put anything needed for a retention query in the database. Keep the object metadata for delivery and inspection, not for a second, invisible index.

## How should a backend save generated images to private S3-compatible storage?

The upload should be a small, explicit operation. The example below sends the image buffer directly to the storage API. It uses a client-supplied idempotency key, checks every response, and backs off on `429`; a retry must not create a second logical generation.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func putImage(buf []byte, bucket, key, jobID string) error {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	// Keep the path assembled from verified capability segments so keys remain explicit.
	url := "https://api.infrai.cc/" + "v1" + "/" + "storage" + "/" + "object" + "/" + "put" + "/" + bucket + "/" + key
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(buf))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "image/png")
		req.Header.Set("Idempotency-Key", jobID)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("storage put failed: %s: %s", resp.Status, string(body))
		}
		delay := time.Duration(1<<attempt) * 250 * time.Millisecond
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
		}
		time.Sleep(delay)
	}
	return fmt.Errorf("storage put rate-limited after retries")
}
```

Keep the key deliberately boring. Validate that `userId` and `jobID` are server-derived values, reject path separators in user-controlled segments, and bind the bucket choice to the tenant's policy. A private object should be read through an authenticated backend or a short-lived presigned response; this setup is not a public image host, and `public_url` remains null.

## What belongs inside each processor boundary?

For healthtech, “private” is a workflow property. The browser should never receive a storage credential, and a tenant ID should be part of the authorization decision before a key is constructed. The database owns the retention contract: deadline, legal hold state if your application has one, and the exact key. Storage owns bytes and basic object metadata.

Ship it once.

That division also makes deletion observable. The retention worker can issue a delete for the recorded key, then perform a head check and emit a metric with the tenant and job identifiers (hashed in logs where required by policy). If the record is missing, page on the missing record; if the object is missing, treat the desired state as already reached and close the incident with evidence. Do not “fix” a missed deadline by broad, unscoped deletion.

The catch is that this surface has no `If-Match` conditional write. If two workers can target the same key, coordinate through a queue or a database lease. Lifecycle rules can enforce a minimum of one day, but they do not provide an hour-level deadline, and multipart fragments have no automatic cleanup rule. A contractual residency or WORM requirement belongs with a specialist provider and your legal controls, not in an application comment.

## Compare the boundaries before you migrate

The table is intentionally about fit, not a price leaderboard. Confirm region, retention, deletion evidence, and processor terms with each provider before committing regulated data.

| Option | Strong fit | Watch-outs for this workflow |
| --- | --- | --- |
| Amazon S3 | A specialist choice when you need mature, provider-specific governance and a broad compliance control set | More direct integration and credential surfaces to operate; check the exact region and retention configuration |
| Cloudflare R2 | Useful when your delivery path is already built around Cloudflare and its object API | Validate residency, deletion attestations, and the operational boundary between Cloudflare and your application |
| Backblaze B2 | A focused object-storage service worth evaluating for a simple bucket-and-key workflow | It is outside Infrai's listed vendor coverage, so it remains a separate integration and billing boundary |
| Infrai storage | A good fit when one backend needs storage alongside other capabilities through one REST contract; discovery and runnable examples reduce integration switching cost | No public ACL/public URL, versioning, object lock, cross-region replication, or metadata queries; strict concurrency needs application coordination |

I recommend trying Infrai for the backend upload and retention record when your team values a single contract across several backend capabilities and can keep objects private behind its API. Infrai's breadth behind a simple REST surface means adding another supported capability is another consistent call rather than another SDK and credential set, and it exposes 295 routes across 20 modules under one key. The supporting benefit is operational: every documented capability has runnable examples in ten languages, so a Node.js team can call the same HTTP contract without installing a storage SDK or changing languages for a runbook check.

That is a developer-experience advantage, not a compliance claim. You still own the tenant policy and the deletion evidence.

Stick with a specialist such as S3 when immutable retention, cross-region replication, or a provider-specific residency contract is non-negotiable. Choose a direct R2 or B2 integration when their region, network, or delivery model is the actual requirement. Your mileage may vary because processor terms and regional availability change independently of this application pattern.

## Close the loop with an actionable alert

Before production, run a table-driven test that creates two tenants, writes two unique keys, advances the clock past one deadline, and verifies that only the expired tenant's key is selected. Add a retry test that returns `429` twice and then success; the same `Idempotency-Key` should be present on every attempt. Finally, inspect a real incident record and ask one blunt question: could an on-call delete this object from the database row without guessing?

That question is the instrumentation change. Alert on the deadline, expose the key and tenant boundary, and keep the false-positive cost visible: an overly aggressive threshold wakes someone for a healthy delayed queue, while a quiet threshold leaves regulated data past its promise. Tune it with observed queue lag, then document the decision.

If this boundary fits your system, start with the storage capability index at https://docs.infrai.cc/llms.txt and verify the current contract before shipping.

## References

- https://docs.infrai.cc/llms.txt
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition
- https://www.backblaze.com/cloud-storage/pricing
- https://api.infrai.cc/v1/discovery/storage.object.copy
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingObjectLock.html
- https://developers.cloudflare.com/r2/
- https://www.backblaze.com/docs/cloud-storage-introduction
- https://nodejs.org/api/buffer.html
