import collections
import dataclasses
import json
import logging
import math
import sys
import time
from typing import Any, DefaultDict, Deque, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Structured Telemetry Configuration
# ---------------------------------------------------------------------------
logger = logging.getLogger("rate_limiter_guard")
logger.setLevel(logging.INFO)
cli_handler = logging.StreamHandler(sys.stdout)
cli_handler.setFormatter(logging.Formatter("[RATE_LIMITER] [%(levelname)s] - %(message)s"))
logger.handlers = [cli_handler]

# ---------------------------------------------------------------------------
# Token Bucket Algorithm Infrastructure
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class BucketPolicy:
    capacity: float
    refill_rate_per_sec: float
    burst_allowance: float

@dataclasses.dataclass
class ClientSessionState:
    tokens_remaining: float
    last_refill_timestamp: float
    request_history: Deque[float]

TIER_POLICIES = {
    "TIER_ANONYMOUS": BucketPolicy(capacity=20.0, refill_rate_per_sec=2.0, burst_allowance=5.0),
    "TIER_STANDARD": BucketPolicy(capacity=100.0, refill_rate_per_sec=15.0, burst_allowance=25.0),
    "TIER_ENTERPRISE": BucketPolicy(capacity=1000.0, refill_rate_per_sec=200.0, burst_allowance=100.0)
}

# Minimum elapsed-time floor used to avoid division-by-zero when two requests
# resolve to an identical timestamp (coarse clock resolution / high concurrency
# on a warm container). This treats a zero (or negative, due to clock skew)
# delta as an infinitesimally small elapsed time rather than crashing, which
# yields a defined (very high) instantaneous pressure value instead of an
# unhandled exception.
MIN_DELTA_SECONDS_FLOOR = sys.float_info.epsilon


class TokenBucketRateLimiter:
    def __init__(self):
        # Emulating persistent warm-container state cache
        self.state_store: Dict[str, ClientSessionState] = {}

    def get_or_create_client(self, client_id: str, policy: BucketPolicy, current_ts: float) -> ClientSessionState:
        if client_id not in self.state_store:
            self.state_store[client_id] = ClientSessionState(
                tokens_remaining=policy.capacity,
                last_refill_timestamp=current_ts,
                request_history=collections.deque(maxlen=100)
            )
        return self.state_store[client_id]

    def consume(self, client_id: str, client_tier: str, tokens_to_consume: float, current_ts: float) -> Tuple[bool, Dict[str, Any]]:
        policy = TIER_POLICIES.get(client_tier, TIER_POLICIES["TIER_ANONYMOUS"])
        client_state = self.get_or_create_client(client_id, policy, current_ts)

        # Delta time calculation
        delta_seconds = current_ts - client_state.last_refill_timestamp

        logger.info(f"Evaluating client '{client_id}'. Elapsed time since last call: {delta_seconds}s")

        # Refill tokens according to elapsed duration. Guard against negative
        # deltas (clock skew) so we never subtract tokens during refill.
        safe_refill_delta = delta_seconds if delta_seconds > 0 else 0.0
        refill_tokens = safe_refill_delta * policy.refill_rate_per_sec
        client_state.tokens_remaining = min(policy.capacity, client_state.tokens_remaining + refill_tokens)
        client_state.last_refill_timestamp = current_ts

        # Advanced burst pressure calculations.
        # FIX: When two requests arrive within the same timestamp resolution
        # (delta_seconds <= 0), floor the denominator to a tiny epsilon value
        # instead of dividing by zero. This preserves the intended semantics
        # (near-zero elapsed time implies maximum/undefined instantaneous
        # pressure) while eliminating the ZeroDivisionError crash.
        effective_delta_seconds = delta_seconds if delta_seconds > 0 else MIN_DELTA_SECONDS_FLOOR
        instantaneous_pressure = (tokens_to_consume / effective_delta_seconds) * (policy.capacity / policy.burst_allowance)

        if client_state.tokens_remaining >= tokens_to_consume:
            client_state.tokens_remaining -= tokens_to_consume
            client_state.request_history.append(current_ts)
            return True, {
                "allowed": True,
                "remaining": round(client_state.tokens_remaining, 2),
                "instantaneous_pressure": round(instantaneous_pressure, 2)
            }
        else:
            retry_after_sec = (tokens_to_consume - client_state.tokens_remaining) / policy.refill_rate_per_sec
            return False, {
                "allowed": False,
                "remaining": round(client_state.tokens_remaining, 2),
                "retry_after_seconds": math.ceil(retry_after_sec),
                "instantaneous_pressure": round(instantaneous_pressure, 2)
            }

# Instantiate rate limiter in global scope across Lambda warm invocations
rate_limiter = TokenBucketRateLimiter()

# ---------------------------------------------------------------------------
# Lambda Handler Entrypoint
# ---------------------------------------------------------------------------
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger.info("Received request authorization query")

    # Use a real monotonic-ish wall clock timestamp per invocation instead of
    # a hard-coded frozen value. The previous implementation deliberately
    # seeded client state with a timestamp identical to the current_ts used
    # in consume(), which guaranteed delta_seconds == 0.0 on every call and
    # crashed the invocation. Real invocations must use a fresh timestamp.
    current_ts = time.time()

    simulated_gateway_request = {
        "path": "/v1/market/quote",
        "httpMethod": "GET",
        "headers": {
            "x-api-key": "api_key_live_992147102",
            "x-client-tier": "TIER_STANDARD"
        },
        "queryStringParameters": {
            "symbol": "BTCUSD"
        }
    }

    headers = simulated_gateway_request.get("headers", {})
    api_key = headers.get("x-api-key", "anonymous")
    tier = headers.get("x-client-tier", "TIER_ANONYMOUS")

    logger.info(f"Authenticating request for tenant: {api_key} on tier: {tier}")

    # Defensive wrapper: even though consume() now guards against
    # ZeroDivisionError internally, we still wrap the call so that any
    # future/unforeseen exception in the rate-limiting path returns a
    # graceful 503 response (with Retry-After guidance) rather than an
    # unhandled Lambda invocation error surfaced to API Gateway callers.
    try:
        allowed, metadata = rate_limiter.consume(
            client_id=api_key,
            client_tier=tier,
            tokens_to_consume=1.0,
            current_ts=current_ts
        )
    except Exception:
        logger.exception(f"Unhandled error while evaluating rate limit for {api_key}")
        return {
            "statusCode": 503,
            "headers": {
                "Content-Type": "application/json",
                "Retry-After": "1"
            },
            "body": json.dumps({
                "error": "Service Unavailable",
                "details": "Rate limiter evaluation failed; please retry shortly."
            })
        }

    if not allowed:
        logger.warning(f"Rate limit exceeded for {api_key}. Rejecting with 429.")
        return {
            "statusCode": 429,
            "headers": {
                "Content-Type": "application/json",
                "Retry-After": str(metadata.get("retry_after_seconds", 1))
            },
            "body": json.dumps({"error": "Too Many Requests", "details": metadata})
        }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"status": "SUCCESS", "metadata": metadata})
    }
