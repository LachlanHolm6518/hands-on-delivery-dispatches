# Node.js Cron Webhook Retries for 30-Day Customer Data Purges

When a gaming service must send a weekly digest to active customers and remove user data after 30 days, the retention clock belongs in durable application state. **Short answer: let a cron trigger call a small webhook, let that webhook enqueue bounded cleanup work, and make the worker's delete operation idempotent.** A delayed queue message is a dispatch aid, not the retention policy.

That distinction matters after the first missed schedule or duplicate delivery. I've been paged for both. The useful audit question is not “did cron run?” It is “which customer records are due, which cleanup attempts are in flight, and which deletion was committed?”

## What should a Node.js cron webhook do for scheduled data cleanup and retry queue work?

The weekly digest and the cleanup job should share a customer eligibility decision, but they should not share one opaque task. At digest-generation time, record the customer ID, the relevant retention deadline, and a cleanup version in a ledger. A scheduled webhook then selects a bounded page of rows whose deadline has passed and whose completion marker is empty. It publishes identifiers to a queue and records that the page was dispatched.

Keep the webhook boring. It should authenticate the caller, claim a small batch, enqueue it, and return. It should not load every customer's profile, send the digest, and delete all related records in one request. A small request body such as a customer ID plus a cleanup version is easier to inspect and retry than a serialized user record.

The ledger is the clock.

If the scheduler runs late, the next sweep can find every still-due row. If the webhook is called twice, a claim or unique dispatch key can prevent an unbounded stream of duplicate messages. The claim is not permission to forget the row: completion still comes only after the worker commits the deletion.

That boundary is where a cleanup incident usually becomes expensive. A webhook can return 200 after handing off ten tasks while the worker is blocked on the profile store. A retry can then publish the same ten IDs, and a second digest run can race the purge if both jobs infer eligibility from a transient cache. I want the ledger row to tell me which decision was made, which version was dispatched, and whether the destructive transaction committed. A queue dashboard cannot answer all three. A cron dashboard cannot answer any of them.

Keep it explicit.

```go
package retention

import (
	"context"
	"database/sql"
)

type CleanupTask struct {
	CustomerID string
	Version    int
}

func ClaimDue(ctx context.Context, db *sql.DB, limit int) ([]CleanupTask, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT customer_id, cleanup_version
		FROM retention_ledger
		WHERE due_at <= CURRENT_TIMESTAMP
		  AND purged_at IS NULL
		  AND dispatch_state = 'ready'
		ORDER BY due_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []CleanupTask
	for rows.Next() {
		var task CleanupTask
		if err := rows.Scan(&task.CustomerID, &task.Version); err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}
```

The example shows selection, not a complete claiming transaction. In production, claim rows with a transaction and a state transition that another webhook invocation cannot repeat blindly; then publish a task carrying the same version. If publishing fails after the claim, a lease or a repair sweep must return that row to `ready`. This is the uncomfortable middle state that a dashboard's green cron history will not show.

## How can retry queues make 30-day user data retention safe?

Queues normally deliver at least once. Treat a duplicate as ordinary input, not an exceptional event. The worker should verify that the task version is still current, remove the associated data in a transaction, and set `purged_at` in the same transaction where the storage model allows it. Acknowledge only after commit. On a retry, an already-completed row becomes a successful no-op.

The order is the guardrail:

1. Read the customer ID and cleanup version.
2. Begin the data transaction.
3. Conditionally mark or observe the purge state.
4. Delete the records covered by that version.
5. Commit, then acknowledge the queue message.

```go
package retention

import (
	"context"
	"database/sql"
)

func PurgeCustomer(ctx context.Context, db *sql.DB, customerID string, version int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		UPDATE retention_ledger
		SET purged_at = CURRENT_TIMESTAMP
		WHERE customer_id = $1
		  AND cleanup_version = $2
		  AND purged_at IS NULL`, customerID, version)
	if err != nil {
		return err
	}

	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		// A prior delivery completed the same version, or the task is stale.
		return tx.Commit()
	}

	if _, err := tx.ExecContext(ctx, `
		DELETE FROM user_data
		WHERE customer_id = $1 AND cleanup_version = $2`, customerID, version); err != nil {
		return err
	}
	return tx.Commit()
}
```

Do not use a delayed message as a 30-day alarm unless the queue's documented delay and retention semantics cover that interval. A database deadline plus recurring scans is easier to recover when a schedule is paused. Short delays still have a place: they can space retries after a transient dependency failure. They cannot replace the ledger.

I'm not sure every storage backend can make the ledger update and all associated deletes one transaction. If it cannot, use a deletion state machine with explicit per-store completion records, and only declare the customer purged when the required records agree. Your mileage may vary; the invariant does not: repeating a task must not recreate, resend, or partially hide a deletion.

## Which signals prove the cleanup webhook and cron schedule are working?

Test the failure paths before testing the happy path. Invoke the webhook twice with the same schedule token. Deliver one queue message twice. Stop the worker after deleting one store but before acknowledging. Advance a test clock past the deadline, then run two sweeps concurrently. The expected result is one completed purge, a visible retry for incomplete work, and no second digest or second destructive side effect.

In production, graph the oldest due deadline, the count of due rows, dispatch attempts, queue age, retry count, and dead-letter volume. Alert on a growing due-row age, not just on a failed HTTP response. A successful webhook can still enqueue nothing, claim rows it cannot recover, or fall behind the worker.

Use a bounded manual sweep for recovery. It should accept a date window or customer-ID range, emit the same versioned task, and use the same idempotency checks as scheduled work. A paused schedule does not explain what happened to records during the gap; the ledger does. Keep the sweep's output and operator action in the audit trail.

The weekly digest also needs its own idempotency key, such as customer ID plus digest period. Retention cleanup should never be allowed to delete a record that a still-active digest run needs, so define the eligibility boundary explicitly and test the boundary at midnight, during a retry, and after a customer reactivates.

## When is this scheduling design the wrong fit?

This ledger-and-queue design suits bounded cleanup with a clear due time and a worker that can repeat safely. It is not suitable when the operation must coordinate many long-running branches with durable joins, human approval, or a cross-system transaction that the application cannot model. Choose a workflow engine or an explicit per-system state machine when those requirements dominate.

It is also a poor fit if the team cannot operate a public, authenticated webhook or cannot observe queue age and purge backlog. In that case, a scheduler embedded in the existing application runtime may reduce moving parts, even though it still needs durable state and idempotency. A plain cron process is not a substitute for either one.

The boundary is easier to review when the options are written down:

| Approach | Integration shape | Good fit | Main limitation |
| --- | --- | --- | --- |
| Cron plus authenticated webhook | HTTP callback into the application | A bounded sweep that can be retried by date or ID | The team owns authentication, claiming, and backlog repair |
| Queue with delayed retries | Queue publish plus worker acknowledgement | Short retry spacing after a transient failure | It does not replace a durable 30-day deadline |
| Workflow engine | Durable steps and state transitions | Long-running branches, joins, or approvals | More operational surface than a single ledger and worker |
| In-process scheduler | Library or process timer | A small deployment with one owner and simple recovery | Process restarts require durable due state and a replay plan |

This is not a ranking. The right row is the one whose failure state the on-call team can inspect and repair.

The trade-off is deliberate: the ledger adds schema and reconciliation work, while the queue adds delivery state and retry policy. Those costs buy a recovery story. For retention work, that story is usually worth more than a single clever timer.

## References

- https://developers.cloudflare.com/workers/configuration/cron-triggers/
- https://www.inngest.com/docs
