# Blocked State Logic Corrections - Summary

## Changes Made

### 1. Removed Automatic Blocked State Recovery

**Before:**
```python
async def recover_blocked_transactions():
    """Reset blocked transactions to scheduled state so they can be retried."""
    blocked_txs = await db.execute(
        "SELECT order_id FROM sent_transactions WHERE state = 'blocked'"
    )
    for order_id in blocked_txs:
        await db.execute(
            "UPDATE sent_transactions SET state = 'scheduled' WHERE order_id = ?",
            (order_id,)
        )
```

**After:**
- Function completely removed
- Call to `recover_blocked_transactions()` in `main()` removed
- Blocked transactions remain blocked on restart

**Rationale:**
- `state = 'blocked'` must mean execution NOT completed
- Automatically resetting blocked → scheduled on startup is incorrect
- Blocked executions should persist until the next DCA interval or manual intervention

### 2. Fixed Schedule Advancement Logic

**Before:**
```python
# Unconditional schedule advancement at end of execution
new_next_run = now + (interval_hours * 3600)
await db.execute(
    "UPDATE dca_plans SET next_run = ? WHERE id = ?",
    (new_next_run, plan_id)
)
```

**After:**
Schedule advances ONLY in these cases:
1. Transaction successfully sent (`state = 'sent'`, tx_hash exists)
2. Non-retryable failure (`state = 'failed'`)
3. Manual send case (wallet not configured)
4. Pre-validation failures (limits check, network unavailable)

Schedule does NOT advance for:
- `state = 'blocked'` (retryable RPC errors)
- `state = 'sending'` (execution in progress)

**Rationale:**
- Ensures NO DCA executions are skipped due to RPC failures
- Blocked executions will be retried on the next interval
- Each DCA run represents a single execution attempt

### 3. Updated Idempotency Check

**Before:**
```python
if existing_state in ('sending', 'sent'):
    # Skip and advance schedule
    new_next_run = now + (interval_hours * 3600)
    await db.execute("UPDATE dca_plans SET next_run = ? WHERE id = ?", ...)
    continue
```

**After:**
```python
if existing_state in ('sending', 'sent', 'blocked'):
    # Skip WITHOUT advancing schedule
    logger.warning(f"Order already in state {existing_state}, skipping")
    continue
```

**Rationale:**
- Blocked state must be checked to prevent duplicate execution attempts
- When state is blocked, sending, or sent, the execution should be skipped
- Schedule should NOT advance when skipping due to existing state

## State Transition Flow (CORRECTED)

```
scheduled → sending → sent (success, advance schedule)
                   → failed (non-retryable, advance schedule)
                   → blocked (retryable RPC error, DO NOT advance)
```

## Blocked State Semantics

**What `blocked` means:**
- Execution NOT completed
- Transaction record created but send failed due to RPC/network error
- Will be retried when next DCA interval arrives
- Schedule is NOT advanced

**What happens on restart:**
- Blocked transactions remain blocked
- No automatic retry
- Next DCA interval will trigger retry attempt

**What happens on next scheduler tick:**
- If `next_run <= now`, scheduler attempts execution
- Checks active_order_id for the plan
- If order is blocked: creates NEW order (automatic retry)
- If order is sending: waits (order in progress)
- If order expired: creates NEW order (normal execution)
- Blocked orders are retried automatically, no infinite loop

**What happens on restart:**
- Blocked transactions remain blocked (not reset)
- Password loaded from keyring
- Next scheduler tick will retry blocked orders (see above)

## Testing

Run the verification script:
```bash
python3 /tmp/test_blocked_logic.py
```

Expected output:
```
✅ PASS: recover_blocked_transactions() removed
✅ PASS: 'blocked' state included in idempotency check
✅ PASS: Blocked state does NOT advance schedule
✅ ALL TESTS PASSED
```

## Impact

**Before corrections:**
- Blocked transactions auto-reset on restart → potential duplicate sends
- Schedule advanced unconditionally → DCA executions skipped during RPC outages
- Blocked state not in idempotency check → potential duplicate attempts

**After corrections:**
- Blocked transactions persist → no duplicate sends
- Schedule advances only on completion → no skipped executions during RPC outages
- Blocked state properly handled → correct idempotency guarantees

## Documentation Updates

- Removed "production ready" claims from IMPLEMENTATION_SUMMARY.md
- Updated blocked state semantics documentation
- Corrected hard rules to reflect actual behavior
- Added note about logic corrections in progress
