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

# Minimum elapsed-time floor used whenever delta_seconds is used as a divisor.
# Prevents ZeroDivisionError when two requests land on the exact same timestamp
# (e.g. near-simultaneous concurrent invocations within the same warm container tick).
MIN_SAFE_DELTA_SECONDS = 1e-6


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

        # Refill tokens according to elapsed duration (safe: multiplication, never divides)
        refill_tokens = delta_seconds * policy.refill_rate_per_sec
        client_state.tokens_remaining = min(policy.capacity, client_state.tokens_remaining + refill_tokens)
        client_state.last_refill_timestamp = current_ts

        # Advanced burst pressure calculations.
        # Guard against delta_seconds == 0.0 (e.g. two requests arriving with an identical
        # timestamp within the same warm-container tick) by clamping to a minimum safe floor
        # before using it as a divisor. This prevents ZeroDivisionError while still producing
        # a meaningful (very high) instantaneous pressure value for near-simultaneous bursts.
        safe_delta_seconds = max(delta_seconds, MIN_SAFE_DELTA_SECONDS)
        if delta_seconds <= 0.0:
            logger.warning(
                f"delta_seconds was non-positive ({delta_seconds}) for client '{client_id}'; "
                f"clamping to {MIN_SAFE_DELTA_SECONDS} to avoid division by zero."
            )
        instantaneous_pressure = (tokens_to_consume / safe_delta_seconds) * (policy.capacity / policy.burst_allowance)

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

    # Fixed reference timestamp used to seed the simulated warm-container cache entry.
    frozen_epoch_seed = 1715000000.125000

    # Seed client in state cache with a timestamp strictly earlier than the request's
    # current_ts below. Previously this seed matched current_ts exactly, forcing
    # delta_seconds to 0.0 and crashing the pressure calculation with a
    # ZeroDivisionError. current_ts is now derived from time.time() so it always
    # moves forward relative to the seeded last_refill_timestamp.
    client_key = "api_key_live_992147102"
    rate_limiter.state_store[client_key] = ClientSessionState(
        tokens_remaining=10.0,
        last_refill_timestamp=frozen_epoch_seed,
        request_history=collections.deque(maxlen=100)
    )

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

    # Use the real wall-clock time for the request's current_ts so it is always
    # strictly greater than the seeded last_refill_timestamp above, matching
    # production behaviour and avoiding an artificial delta_seconds == 0.0.
    current_ts = time.time()

    try:
        allowed, metadata = rate_limiter.consume(
            client_id=api_key,
            client_tier=tier,
            tokens_to_consume=1.0,
            current_ts=current_ts
        )
    except Exception as exc:
        logger.error(f"Unhandled error while evaluating rate limit for '{api_key}': {exc}", exc_info=True)
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Internal rate limiter error"})
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
