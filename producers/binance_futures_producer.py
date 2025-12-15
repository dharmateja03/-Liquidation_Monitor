#!/usr/bin/env python3
"""
Crypto Futures Data Producer
Streams funding rates, open interest, liquidations, and price data to Kafka
Uses CoinGecko API (works globally, no restrictions)
"""

import json
import time
import threading
import random
from datetime import datetime, timezone
from typing import Dict, Any
import requests
from kafka import KafkaProducer
import psycopg2
from psycopg2.extras import execute_values

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'crypto_signals',
    'user': 'postgres',
    'password': 'postgres'
}

SYMBOLS = ['BTCUSDT', 'ETHUSDT']
COINGECKO_IDS = {'BTCUSDT': 'bitcoin', 'ETHUSDT': 'ethereum'}

# CoinGecko API (free, works globally)
COINGECKO_API = 'https://api.coingecko.com/api/v3'


class CryptoFuturesProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
        self.pg_conn.autocommit = True
        self.running = True

        # Store latest prices for calculations
        self.latest_prices = {'BTCUSDT': 85000.0, 'ETHUSDT': 3000.0}
        self.prev_prices = {'BTCUSDT': 85000.0, 'ETHUSDT': 3000.0}

        # Simulated funding rates
        self.funding_rates = {'BTCUSDT': 0.0001, 'ETHUSDT': 0.0001}

    def send_to_kafka(self, topic: str, key: str, data: Dict[str, Any]):
        """Send data to Kafka topic"""
        try:
            self.producer.send(topic, key=key, value=data)
        except Exception as e:
            print(f"Kafka send error: {e}")

    def write_to_db(self, table: str, columns: list, values: list):
        """Write data directly to TimescaleDB"""
        try:
            with self.pg_conn.cursor() as cur:
                query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
                execute_values(cur, query, [values])
        except Exception as e:
            print(f"DB write error: {e}")
            try:
                self.pg_conn.rollback()
            except:
                pass

    def fetch_prices_from_coingecko(self):
        """Fetch prices from CoinGecko API"""
        try:
            response = requests.get(
                f"{COINGECKO_API}/simple/price",
                params={
                    'ids': 'bitcoin,ethereum',
                    'vs_currencies': 'usd',
                    'include_24hr_vol': 'true',
                    'include_24hr_change': 'true'
                },
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                return {
                    'BTCUSDT': {
                        'price': data.get('bitcoin', {}).get('usd', 85000),
                        'volume': data.get('bitcoin', {}).get('usd_24h_vol', 0),
                        'change_24h': data.get('bitcoin', {}).get('usd_24h_change', 0)
                    },
                    'ETHUSDT': {
                        'price': data.get('ethereum', {}).get('usd', 3000),
                        'volume': data.get('ethereum', {}).get('usd_24h_vol', 0),
                        'change_24h': data.get('ethereum', {}).get('usd_24h_change', 0)
                    }
                }
        except Exception as e:
            print(f"CoinGecko price error: {e}")
        return None

    def fetch_prices(self):
        """Fetch real-time prices every 10 seconds"""
        while self.running:
            try:
                now = datetime.now(timezone.utc)
                price_data = self.fetch_prices_from_coingecko()

                if price_data:
                    for symbol in SYMBOLS:
                        data = price_data[symbol]
                        price = data['price']
                        volume = data['volume']

                        self.prev_prices[symbol] = self.latest_prices[symbol]
                        self.latest_prices[symbol] = price

                        record = {
                            'time': now.isoformat(),
                            'symbol': symbol,
                            'price': price,
                            'volume': volume
                        }

                        self.send_to_kafka('prices', symbol, record)
                        self.write_to_db(
                            'prices',
                            ['time', 'symbol', 'price', 'volume'],
                            [now, symbol, price, volume]
                        )

                        change = ((price - self.prev_prices[symbol]) / self.prev_prices[symbol] * 100) if self.prev_prices[symbol] else 0
                        arrow = "▲" if change >= 0 else "▼"
                        print(f"[PRICE] {symbol}: ${price:,.2f} {arrow} {abs(change):.2f}%")

                        # Generate liquidations based on price movement
                        self.simulate_liquidation(symbol, price)

            except Exception as e:
                print(f"Price fetch error: {e}")

            time.sleep(10)  # CoinGecko rate limit: ~50 calls/min

    def fetch_funding_rates(self):
        """Simulate funding rates every 1 minute"""
        while self.running:
            try:
                now = datetime.now(timezone.utc)

                for symbol in SYMBOLS:
                    price = self.latest_prices.get(symbol, 0)

                    # Dynamic funding rate based on price momentum
                    price_change = (price - self.prev_prices.get(symbol, price)) / price if price else 0

                    # Funding rate tends to follow price direction
                    base_rate = 0.0001  # 0.01% base
                    momentum_factor = price_change * 10  # Amplify price change
                    noise = random.uniform(-0.0001, 0.0001)
                    funding_rate = base_rate + momentum_factor + noise

                    # Clamp to realistic range (-0.1% to 0.3%)
                    funding_rate = max(-0.001, min(0.003, funding_rate))
                    self.funding_rates[symbol] = funding_rate

                    record = {
                        'time': now.isoformat(),
                        'symbol': symbol,
                        'funding_rate': funding_rate,
                        'mark_price': price,
                        'index_price': price * (1 + random.uniform(-0.0005, 0.0005))
                    }

                    self.send_to_kafka('funding_rates', symbol, record)
                    self.write_to_db(
                        'funding_rates',
                        ['time', 'symbol', 'funding_rate', 'mark_price', 'index_price'],
                        [now, symbol, record['funding_rate'],
                         record['mark_price'], record['index_price']]
                    )

                    indicator = "🟢" if funding_rate > 0.0001 else "🔴" if funding_rate < 0 else "⚪"
                    print(f"{indicator} [FUNDING] {symbol}: {funding_rate:.6f} ({funding_rate*100:.4f}%)")

            except Exception as e:
                print(f"Funding rate error: {e}")

            time.sleep(60)

    def fetch_open_interest(self):
        """Simulate open interest every 30 seconds"""
        # Base OI values (realistic for Dec 2024)
        base_oi = {'BTCUSDT': 550000, 'ETHUSDT': 4500000}

        while self.running:
            try:
                now = datetime.now(timezone.utc)

                for symbol in SYMBOLS:
                    price = self.latest_prices.get(symbol, 100000 if 'BTC' in symbol else 4000)

                    # OI tends to increase when price moves significantly
                    price_change = abs(price - self.prev_prices.get(symbol, price)) / price if price else 0
                    oi_change = random.uniform(-0.01, 0.02) + price_change * 5

                    base_oi[symbol] = base_oi[symbol] * (1 + oi_change)
                    base_oi[symbol] = max(base_oi[symbol], 100000 if 'BTC' in symbol else 1000000)

                    oi = base_oi[symbol]
                    oi_value = oi * price

                    record = {
                        'time': now.isoformat(),
                        'symbol': symbol,
                        'open_interest': oi,
                        'open_interest_value': oi_value
                    }

                    self.send_to_kafka('open_interest', symbol, record)
                    self.write_to_db(
                        'open_interest',
                        ['time', 'symbol', 'open_interest', 'open_interest_value'],
                        [now, symbol, oi, oi_value]
                    )
                    print(f"[OI] {symbol}: {oi:,.0f} (${oi_value/1e9:.2f}B)")

            except Exception as e:
                print(f"Open interest error: {e}")

            time.sleep(30)

    def fetch_long_short_ratio(self):
        """Simulate long/short ratio every 2 minutes"""
        # Track L/S ratio with momentum
        ls_ratios = {'BTCUSDT': 1.0, 'ETHUSDT': 1.0}

        while self.running:
            try:
                now = datetime.now(timezone.utc)

                for symbol in SYMBOLS:
                    price = self.latest_prices.get(symbol, 0)
                    prev_price = self.prev_prices.get(symbol, price)

                    # L/S ratio tends to follow price (more longs when price up)
                    if prev_price and price:
                        price_momentum = (price - prev_price) / prev_price
                        ls_change = price_momentum * 2 + random.uniform(-0.05, 0.05)
                        ls_ratios[symbol] = max(0.5, min(2.0, ls_ratios[symbol] + ls_change))

                    ls_ratio = ls_ratios[symbol]
                    long_ratio = ls_ratio / (1 + ls_ratio)
                    short_ratio = 1 - long_ratio

                    record = {
                        'time': now.isoformat(),
                        'symbol': symbol,
                        'long_ratio': long_ratio,
                        'short_ratio': short_ratio,
                        'long_short_ratio': ls_ratio
                    }

                    self.send_to_kafka('long_short_ratio', symbol, record)
                    self.write_to_db(
                        'long_short_ratio',
                        ['time', 'symbol', 'long_ratio', 'short_ratio', 'long_short_ratio'],
                        [now, symbol, record['long_ratio'],
                         record['short_ratio'], record['long_short_ratio']]
                    )

                    indicator = "🐂" if ls_ratio > 1.2 else "🐻" if ls_ratio < 0.8 else "⚖️"
                    print(f"{indicator} [L/S] {symbol}: {ls_ratio:.3f} (Long: {long_ratio:.1%})")

            except Exception as e:
                print(f"Long/short ratio error: {e}")

            time.sleep(120)

    def simulate_liquidation(self, symbol: str, price: float):
        """Simulate liquidation events based on price movement"""
        # Higher probability when price moves significantly
        price_change = abs(price - self.prev_prices.get(symbol, price)) / price if price else 0
        liq_probability = min(0.3, 0.02 + price_change * 5)

        if random.random() < liq_probability:
            now = datetime.now(timezone.utc)

            # Direction: liquidate longs on price drop, shorts on price rise
            if price < self.prev_prices.get(symbol, price):
                side = 'SELL'  # Long liquidation
            else:
                side = 'BUY'  # Short liquidation

            # Size follows power law (many small, few large)
            base_size = random.paretovariate(1.5) * 0.05
            qty = min(base_size, 20)  # Cap

            if 'ETH' in symbol:
                qty *= 15

            usd_value = qty * price

            record = {
                'time': now.isoformat(),
                'symbol': symbol,
                'side': side,
                'quantity': qty,
                'price': price,
                'usd_value': usd_value
            }

            self.send_to_kafka('liquidations', symbol, record)
            self.write_to_db(
                'liquidations',
                ['time', 'symbol', 'side', 'quantity', 'price', 'usd_value'],
                [now, symbol, side, qty, price, usd_value]
            )

            emoji = "🔴" if side == "SELL" else "🟢"
            liq_type = "LONG" if side == "SELL" else "SHORT"
            print(f"{emoji} [LIQ] {symbol} {liq_type}: {qty:.4f} @ ${price:,.2f} (${usd_value:,.0f})")

    def run(self):
        """Start all data collection threads"""
        print("=" * 60)
        print("  Liquidation Cascade Monitor - Data Producer")
        print("=" * 60)
        print(f"  Symbols: {SYMBOLS}")
        print(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"  TimescaleDB: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}")
        print("=" * 60)
        print()
        print("  Data Sources:")
        print("  • Prices: CoinGecko API (real-time, every 10s)")
        print("  • Funding: Simulated with price momentum")
        print("  • Open Interest: Simulated with volatility correlation")
        print("  • Long/Short: Simulated with price direction")
        print("  • Liquidations: Simulated based on price movement")
        print("=" * 60)
        print()

        threads = [
            threading.Thread(target=self.fetch_prices, daemon=True, name="prices"),
            threading.Thread(target=self.fetch_funding_rates, daemon=True, name="funding"),
            threading.Thread(target=self.fetch_open_interest, daemon=True, name="oi"),
            threading.Thread(target=self.fetch_long_short_ratio, daemon=True, name="ls"),
        ]

        for t in threads:
            t.start()
            print(f"✓ Started thread: {t.name}")

        print()
        print("=" * 60)
        print("  Streaming data... Press Ctrl+C to stop")
        print("=" * 60)
        print()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\nShutting down...")
            self.running = False
            self.producer.close()
            self.pg_conn.close()
            print("Goodbye!")


if __name__ == "__main__":
    producer = CryptoFuturesProducer()
    producer.run()
