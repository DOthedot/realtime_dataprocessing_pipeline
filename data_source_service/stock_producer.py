import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

# ── Config from env ───────────────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC        = os.getenv("TOPIC", "stock-trades")
SYMBOLS      = os.getenv("SYMBOLS", "AAPL,GOOGL,MSFT,AMZN,TSLA,NVDA,META,NFLX").split(",")
INTERVAL     = float(os.getenv("INTERVAL", "0"))   # seconds between events
DURATION     = os.getenv("DURATION")               # seconds to run; None = run forever

# ── Base prices ───────────────────────────────────────────────────────────────
BASE_PRICES = {
    "AAPL": 182.0, "GOOGL": 140.0, "MSFT": 375.0, "AMZN": 178.0,
    "TSLA": 245.0, "NVDA":  875.0, "META": 505.0, "NFLX": 610.0,
}

_prices = {s: BASE_PRICES.get(s, 100.0) for s in SYMBOLS}


def _next_price(symbol: str) -> tuple[float, float]:
    last = _prices[symbol]
    change_pct = round(random.uniform(-0.5, 0.5), 4)
    new_price = round(last * (1 + change_pct / 100), 2)
    _prices[symbol] = new_price
    return new_price, change_pct


def _generate_trade() -> dict:
    symbol = random.choice(SYMBOLS)
    price, change_pct = _next_price(symbol)
    return {
        "symbol":     symbol,
        "price":      price,
        "volume":     random.randint(100, 10_000),
        "change_pct": change_pct,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }


def _delivery_report(err, _msg):
    if err:
        print(f"Delivery failed: {err}")


def _wait_for_broker(producer: Producer, retries: int = 10, delay: int = 3) -> None:
    print(f"Waiting for broker at {KAFKA_BROKER}...")
    for attempt in range(1, retries + 1):
        metadata = producer.list_topics(timeout=5)
        if metadata.brokers:
            print(f"Broker ready. ({attempt} attempt(s))")
            return
        print(f"Attempt {attempt}/{retries} — broker not ready, retrying in {delay}s...")
        time.sleep(delay)
    raise RuntimeError(f"Broker {KAFKA_BROKER} unreachable after {retries} attempts.")


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    print(f"Broker={KAFKA_BROKER}  Topic={TOPIC}  Symbols={SYMBOLS}  Interval={INTERVAL}s")
    _wait_for_broker(producer)

    deadline = time.monotonic() + float(DURATION) if DURATION else None

    count = 0

    try:
        while True:
            if deadline and time.monotonic() >= deadline:
                print(f"\nDuration of {DURATION}s reached. Stopping.")
                print(f"Total records produced: {count:,}")
                break

            trade = _generate_trade()
            producer.produce(
                topic=TOPIC,
                key=trade["symbol"],
                value=json.dumps(trade).encode("utf-8"),
                callback=_delivery_report,
            )
            producer.poll(0)
            count += 1

            print(f"[{trade['timestamp']}]  {trade['symbol']:5s}  "
                  f"${trade['price']:.2f}  vol={trade['volume']:,}  "
                  f"chg={trade['change_pct']:+.4f}%")
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print(f"\nStopping producer... Total records produced: {count:,}")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
