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
# Sentinel constant used when instantaneous pressure cannot be calculated
# (delta_seconds == 0.0 means simultaneous request; pressure is unbounded).
# A large finite value is used instead of float('inf') to ensure the metadata
# dict remains JSON-serialisable at all times.
# ---------------------------------------------------------------------------
PRESSURE_UNCALCULABLE: float = -1.0

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

        # Refill tokens according to elapsed duration
        refill_tokens = delta_seconds * policy.refill_rate_per_sec
        client_state.tokens_remaining = min(policy.capacity, client_state.tokens_remaining + refill_tokens)
        client_state.last_refill_timestamp = current_ts

        # Advanced burst pressure calculations.
        # Guard against delta_seconds == 0.0 (simultaneous requests arriving within the same
        # floating-point timestamp resolution, or on the very first call for a freshly seeded
        # client).  Division by zero would otherwise crash the Lambda invocation entirely.
        # When delta_seconds is zero, instantaneous throughput is theoretically unbounded;
        # we represent this with the sentinel PRESSURE_UNCALCULABLE (-1.0) which is a finite,
        # JSON-serialisable value that downstream consumers can detect and handle appropriately.
        if delta_seconds > 0.0:
            instantaneous_pressure = (tokens_to_consume / delta_seconds) * (policy.capacity / policy.burst_allowance)
        else:
            # Simultaneous request — burst pressure is unbounded; flag but do not crash.
            instantaneous_pressure = PRESSURE_UNCALCULABLE
            logger.warning(
                f"delta_seconds is 0.0 for client '{client_id}'; "
                "instantaneous_pressure set to sentinel PRESSURE_UNCALCULABLE (-1.0)."
            )

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

    # Use the current wall-clock time so that delta_seconds is naturally non-zero
    # between the state initialisation in get_or_create_client() and subsequent calls.
    # Previously a frozen timestamp was used both to seed the state store AND as
    # current_ts, forcing delta_seconds = 0.0 on every single invocation.
    frozen_epoch_now = time.time()

    # Do NOT manually pre-seed the state store with the same timestamp that will be
    # passed to consume().  Let get_or_create_client() initialise the session naturally
    # on the first call.  The previous manual injection was the direct cause of the
    # deterministic ZeroDivisionError: it set last_refill_timestamp=frozen_epoch_now
    # and then immediately called consume() with current_ts=frozen_epoch_now, making
    # delta_seconds exactly 0.0 on 100% of invocations.
    client_key = "api_key_live_992147102"

    simulated_gateway_request = {
        "path": "/v1/market/quote",
        "httpMethod": "GET",
        "headers": {
            "x-api-key": client_key,
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

    # Wrap the consume() call in a broad exception handler so that any unexpected
    # arithmetic or state-access error returns a structured HTTP 500 JSON body
    # rather than propagating an unhandled exception to the Lambda runtime and
    # causing an opaque 500 with no response body for the API Gateway caller.
    try:
        allowed, metadata = rate_limiter.consume(
            client_id=api_key,
            client_tier=tier,
            tokens_to_consume=1.0,
            current_ts=frozen_epoch_now
        )
    except Exception as exc:
        logger.error(
            f"Rate limiter internal error for client '{api_key}' on tier '{tier}' "
            f"at ts={frozen_epoch_now}: {exc}",
            exc_info=True
        )
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Internal rate limiter failure"})
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
