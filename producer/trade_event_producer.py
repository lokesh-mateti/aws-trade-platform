"""
trade_event_producer.py
-----------------------
Simulates a realistic equity trade event stream and publishes events to
AWS EventBridge. Designed for the JPMorgan AWM demo pipeline:

    EventBridge → Lambda → S3 raw → EMR Spark → RDS PostgreSQL

Usage:
    # Dry-run (print to stdout, no AWS calls)
    python trade_event_producer.py --dry-run --count 10

    # Publish 100 events to EventBridge, 50 ms between each
    python trade_event_producer.py --count 100 --interval 0.05

    # Continuous mode (Ctrl+C to stop)
    python trade_event_producer.py --continuous --interval 1.0

Dependencies:
    pip install boto3

Environment / AWS config:
    AWS_REGION         (default: us-east-1)
    EVENT_BUS_NAME     (default: trade-events-bus)
    EVENT_SOURCE       (default: com.jpmorgandemo.tradestream)
"""

import argparse
import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Market simulation parameters
# ---------------------------------------------------------------------------
TICKERS = {
    "AAPL":  {"base_price": 189.50, "avg_volume": 55_000_000, "sector": "Technology"},
    "MSFT":  {"base_price": 415.20, "avg_volume": 22_000_000, "sector": "Technology"},
    "JPM":   {"base_price": 198.75, "avg_volume": 10_000_000, "sector": "Financials"},
    "GS":    {"base_price": 450.10, "avg_volume":  3_500_000, "sector": "Financials"},
    "BAC":   {"base_price":  38.90, "avg_volume": 35_000_000, "sector": "Financials"},
    "AMZN":  {"base_price": 185.30, "avg_volume": 40_000_000, "sector": "Consumer"},
    "TSLA":  {"base_price": 242.80, "avg_volume": 90_000_000, "sector": "Automotive"},
    "NVDA":  {"base_price": 875.60, "avg_volume": 45_000_000, "sector": "Technology"},
    "BRK.B": {"base_price": 370.20, "avg_volume":  5_000_000, "sector": "Financials"},
    "XOM":   {"base_price": 118.40, "avg_volume": 18_000_000, "sector": "Energy"},
}

EXCHANGES    = ["NYSE", "NASDAQ", "BATS", "IEX", "CBOE"]
ORDER_TYPES  = ["MARKET", "LIMIT", "STOP", "STOP_LIMIT"]
TRADE_SIDES  = ["BUY", "SELL"]
TRADE_STATUS = ["FILLED", "PARTIAL_FILL", "REJECTED"]
TRADE_STATUS_WEIGHTS = [0.80, 0.15, 0.05]

# Intraday price state — persists across calls within a session
_price_state: dict[str, float] = {
    ticker: meta["base_price"] for ticker, meta in TICKERS.items()
}


# ---------------------------------------------------------------------------
# Event generation
# ---------------------------------------------------------------------------

def _next_price(ticker: str) -> float:
    """
    Geometric Brownian Motion step — simulates realistic tick-by-tick price
    movement with ±0.25 % volatility per event.
    """
    current = _price_state[ticker]
    drift      = 0.0            # zero drift for intraday simulation
    volatility = 0.0025         # 0.25 % per tick
    shock = random.gauss(drift, volatility)
    new_price = round(current * (1 + shock), 4)
    _price_state[ticker] = new_price
    return new_price


def generate_trade_event() -> dict:
    """
    Returns a single synthetic trade event dict that mirrors a real
    market data message (FIX-like fields, JSON-serialisable).
    """
    ticker   = random.choice(list(TICKERS.keys()))
    meta     = TICKERS[ticker]
    price    = _next_price(ticker)

    # Spread: 0.01–0.05 % of mid-price
    spread   = round(price * random.uniform(0.0001, 0.0005), 4)
    bid      = round(price - spread / 2, 4)
    ask      = round(price + spread / 2, 4)

    # Volume: ±40 % around avg_daily_volume / 390 trading minutes
    avg_per_event = meta["avg_volume"] / 390
    volume   = max(100, int(avg_per_event * random.uniform(0.6, 1.4)))
    # Round to nearest lot of 100
    volume   = (volume // 100) * 100 or 100

    status   = random.choices(TRADE_STATUS, weights=TRADE_STATUS_WEIGHTS, k=1)[0]
    filled_qty = (
        volume if status == "FILLED"
        else int(volume * random.uniform(0.1, 0.9)) if status == "PARTIAL_FILL"
        else 0
    )

    return {
        # Identifiers
        "event_id":       str(uuid.uuid4()),
        "trade_id":       f"TRD-{uuid.uuid4().hex[:12].upper()}",
        "order_id":       f"ORD-{uuid.uuid4().hex[:10].upper()}",

        # Instrument
        "ticker":         ticker,
        "sector":         meta["sector"],
        "exchange":       random.choice(EXCHANGES),

        # Price data
        "price":          price,
        "bid":            bid,
        "ask":            ask,
        "spread":         round(ask - bid, 4),

        # Order details
        "side":           random.choice(TRADE_SIDES),
        "order_type":     random.choice(ORDER_TYPES),
        "quantity":       volume,
        "filled_quantity": filled_qty,
        "status":         status,

        # Notional
        "notional_value": round(filled_qty * price, 2),

        # Timestamps (ISO-8601 UTC)
        "event_time":     datetime.now(timezone.utc).isoformat(),
        "trade_date":     datetime.now(timezone.utc).strftime("%Y-%m-%d"),

        # Pipeline metadata
        "schema_version": "1.0",
        "producer":       "trade_event_producer",
    }


# ---------------------------------------------------------------------------
# EventBridge publisher
# ---------------------------------------------------------------------------

class EventBridgePublisher:
    def __init__(self, region: str, bus_name: str, source: str):
        self.bus_name = bus_name
        self.source   = source
        self.client   = boto3.client("events", region_name=region)
        self._sent    = 0
        self._failed  = 0

    def publish(self, event: dict) -> bool:
        entry = {
            "Source":       self.source,
            "DetailType":   "TradeEvent",
            "Detail":       json.dumps(event),
            "EventBusName": self.bus_name,
        }
        try:
            resp = self.client.put_events(Entries=[entry])
            failed = resp.get("FailedEntryCount", 0)
            if failed:
                err = resp["Entries"][0].get("ErrorMessage", "unknown")
                log.warning("EventBridge rejected event %s: %s", event["event_id"], err)
                self._failed += 1
                return False
            self._sent += 1
            log.info(
                "Published %-8s | %s | price=%.4f | qty=%d | status=%s",
                event["ticker"],
                event["event_id"],
                event["price"],
                event["quantity"],
                event["status"],
            )
            return True
        except (BotoCoreError, ClientError) as exc:
            log.error("AWS error publishing event: %s", exc)
            self._failed += 1
            return False

    @property
    def stats(self) -> dict:
        return {"sent": self._sent, "failed": self._failed}


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Synthetic trade event producer for the JPM AWM demo pipeline."
    )
    p.add_argument(
        "--count", type=int, default=20,
        help="Number of events to produce (ignored in --continuous mode). Default: 20",
    )
    p.add_argument(
        "--interval", type=float, default=0.5,
        help="Seconds between events. Default: 0.5",
    )
    p.add_argument(
        "--continuous", action="store_true",
        help="Run until Ctrl+C instead of stopping at --count.",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Print events as JSON to stdout; do not call AWS.",
    )
    p.add_argument(
        "--region", default=os.environ.get("AWS_REGION", "us-east-1"),
        help="AWS region. Default: us-east-1 (or $AWS_REGION)",
    )
    p.add_argument(
        "--bus-name",
        default=os.environ.get("EVENT_BUS_NAME", "trade-events-bus"),
        help="EventBridge bus name. Default: trade-events-bus (or $EVENT_BUS_NAME)",
    )
    p.add_argument(
        "--source",
        default=os.environ.get("EVENT_SOURCE", "com.jpmorgandemo.tradestream"),
        help="EventBridge source string.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    publisher: Optional[EventBridgePublisher] = None
    if not args.dry_run:
        publisher = EventBridgePublisher(args.region, args.bus_name, args.source)
        log.info(
            "Publishing to EventBridge bus '%s' in %s | interval=%.2fs",
            args.bus_name, args.region, args.interval,
        )
    else:
        log.info("DRY-RUN mode — no AWS calls will be made.")

    count      = 0
    max_events = None if args.continuous else args.count

    try:
        while True:
            event = generate_trade_event()

            if args.dry_run:
                print(json.dumps(event, indent=2))
            else:
                publisher.publish(event)  # type: ignore[union-attr]

            count += 1
            if max_events and count >= max_events:
                break

            time.sleep(args.interval)

    except KeyboardInterrupt:
        log.info("Interrupted by user.")

    finally:
        log.info("Events produced: %d", count)
        if publisher:
            log.info("EventBridge stats: %s", publisher.stats)


if __name__ == "__main__":
    main()
