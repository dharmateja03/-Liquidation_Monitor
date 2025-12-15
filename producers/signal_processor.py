#!/usr/bin/env python3
"""
Signal Processor - Liquidation Risk & Stablecoin Pressure Scoring
Consumes from Kafka, computes risk scores, writes to TimescaleDB
"""

import json
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from collections import deque
from dataclasses import dataclass
import statistics
from kafka import KafkaConsumer
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


@dataclass
class MarketState:
    """Current market state for a symbol"""
    symbol: str
    current_price: float = 0.0
    funding_rate: float = 0.0
    open_interest: float = 0.0
    oi_change_1h: float = 0.0
    long_short_ratio: float = 1.0
    recent_liquidations: deque = None  # (timestamp, usd_value, side)
    price_history: deque = None  # (timestamp, price)

    def __post_init__(self):
        if self.recent_liquidations is None:
            self.recent_liquidations = deque(maxlen=1000)
        if self.price_history is None:
            self.price_history = deque(maxlen=1000)


@dataclass
class StablecoinState:
    """Current stablecoin flow state"""
    recent_inflows: deque = None  # Exchange inflows (timestamp, amount)
    recent_outflows: deque = None  # Exchange outflows (timestamp, amount)
    recent_mints: deque = None  # Mints (timestamp, amount)
    recent_burns: deque = None  # Burns (timestamp, amount)

    def __post_init__(self):
        if self.recent_inflows is None:
            self.recent_inflows = deque(maxlen=500)
        if self.recent_outflows is None:
            self.recent_outflows = deque(maxlen=500)
        if self.recent_mints is None:
            self.recent_mints = deque(maxlen=500)
        if self.recent_burns is None:
            self.recent_burns = deque(maxlen=500)


class SignalProcessor:
    def __init__(self):
        self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
        self.pg_conn.autocommit = True
        self.running = True

        # Market state per symbol
        self.market_state: Dict[str, MarketState] = {
            symbol: MarketState(symbol=symbol) for symbol in SYMBOLS
        }

        # Stablecoin state
        self.stablecoin_state = StablecoinState()

        # Historical data for baseline calculations
        self.oi_history: Dict[str, deque] = {
            symbol: deque(maxlen=500) for symbol in SYMBOLS
        }

    def write_to_db(self, table: str, columns: list, values: list):
        """Write data to TimescaleDB"""
        try:
            with self.pg_conn.cursor() as cur:
                query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
                execute_values(cur, query, [values])
        except Exception as e:
            print(f"DB write error: {e}")
            self.pg_conn.rollback()

    def calculate_liquidation_risk(self, symbol: str) -> Dict[str, Any]:
        """
        Calculate liquidation risk score (0-100)

        Components:
        1. Funding rate component (0-30): High positive = overleveraged longs
        2. OI change component (0-30): Rising OI + flat price = danger
        3. Long/Short ratio component (0-20): Extreme imbalance = risk
        4. Recent liquidation cascade component (0-20): Cascading liquidations
        """
        state = self.market_state[symbol]

        # 1. Funding rate component (0-30)
        # Normal: -0.01% to 0.01%, Elevated: 0.01% to 0.05%, High: >0.05%
        funding_abs = abs(state.funding_rate)
        if funding_abs < 0.0001:
            funding_score = 0
        elif funding_abs < 0.0005:
            funding_score = 10
        elif funding_abs < 0.001:
            funding_score = 20
        else:
            funding_score = min(30, int(funding_abs * 30000))

        # 2. OI change component (0-30)
        # Look for rising OI without corresponding price movement
        oi_score = 0
        if len(self.oi_history[symbol]) > 10:
            oi_values = [x[1] for x in self.oi_history[symbol]]
            recent_oi = oi_values[-1] if oi_values else 0
            older_oi = oi_values[-10] if len(oi_values) >= 10 else oi_values[0]

            if older_oi > 0:
                oi_change_pct = (recent_oi - older_oi) / older_oi * 100

                # Check price change over same period
                price_values = [x[1] for x in state.price_history]
                if len(price_values) >= 10:
                    recent_price = price_values[-1]
                    older_price = price_values[-10]
                    price_change_pct = (recent_price - older_price) / older_price * 100 if older_price > 0 else 0

                    # OI up but price flat = overleveraged
                    if oi_change_pct > 5 and abs(price_change_pct) < 2:
                        oi_score = min(30, int(oi_change_pct * 3))
                    elif oi_change_pct > 10:
                        oi_score = min(30, int(oi_change_pct * 2))

        # 3. Long/Short ratio component (0-20)
        ls_score = 0
        if state.long_short_ratio > 2.0:  # Very long heavy
            ls_score = min(20, int((state.long_short_ratio - 1) * 10))
        elif state.long_short_ratio < 0.5:  # Very short heavy
            ls_score = min(20, int((1 / state.long_short_ratio - 1) * 10))

        # 4. Recent liquidation cascade component (0-20)
        liq_score = 0
        now = datetime.utcnow()
        recent_liq_value = sum(
            liq[1] for liq in state.recent_liquidations
            if (now - liq[0]).total_seconds() < 300  # Last 5 minutes
        )
        if recent_liq_value > 10_000_000:  # >$10M in 5 min
            liq_score = min(20, int(recent_liq_value / 5_000_000))

        total_score = funding_score + oi_score + ls_score + liq_score

        return {
            'risk_score': min(100, total_score),
            'funding_component': funding_score,
            'oi_component': oi_score,
            'ls_component': ls_score,
            'liq_component': liq_score
        }

    def calculate_stablecoin_pressure(self) -> Dict[str, Any]:
        """
        Calculate stablecoin buying pressure score (-100 to +100)

        Positive = buying pressure (bullish)
        Negative = selling pressure (bearish)

        Components:
        1. Net exchange flow: Outflows - Inflows
        2. Mint/Burn delta: Mints - Burns
        3. Large transfer count
        """
        now = datetime.utcnow()
        lookback = timedelta(hours=4)

        # Calculate net flows in last 4 hours
        recent_inflows = sum(
            x[1] for x in self.stablecoin_state.recent_inflows
            if (now - x[0]).total_seconds() < lookback.total_seconds()
        )
        recent_outflows = sum(
            x[1] for x in self.stablecoin_state.recent_outflows
            if (now - x[0]).total_seconds() < lookback.total_seconds()
        )
        recent_mints = sum(
            x[1] for x in self.stablecoin_state.recent_mints
            if (now - x[0]).total_seconds() < lookback.total_seconds()
        )
        recent_burns = sum(
            x[1] for x in self.stablecoin_state.recent_burns
            if (now - x[0]).total_seconds() < lookback.total_seconds()
        )

        # Net exchange flow (outflow positive = bullish)
        net_exchange_flow = recent_outflows - recent_inflows

        # Mint/burn delta (mints positive = bullish)
        mint_burn_delta = recent_mints - recent_burns

        # Convert to score
        # Exchange flow: $100M = 50 points
        exchange_score = max(-50, min(50, net_exchange_flow / 2_000_000))

        # Mint/burn: $500M = 50 points
        mint_score = max(-50, min(50, mint_burn_delta / 10_000_000))

        total_score = int(exchange_score + mint_score)

        # Count large transfers
        large_transfers = (
            len([x for x in self.stablecoin_state.recent_inflows
                 if (now - x[0]).total_seconds() < lookback.total_seconds()]) +
            len([x for x in self.stablecoin_state.recent_outflows
                 if (now - x[0]).total_seconds() < lookback.total_seconds()])
        )

        return {
            'buying_pressure_score': max(-100, min(100, total_score)),
            'net_exchange_flow': net_exchange_flow,
            'mint_burn_delta': mint_burn_delta,
            'large_transfers_count': large_transfers
        }

    def check_cascade_alert(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Check if conditions warrant a cascade alert

        HIGH RISK = Liquidation Score > 70 + Buying Pressure < -30
        """
        liq_risk = self.calculate_liquidation_risk(symbol)
        stable_pressure = self.calculate_stablecoin_pressure()

        combined_score = liq_risk['risk_score'] - stable_pressure['buying_pressure_score']

        if liq_risk['risk_score'] > 70 and stable_pressure['buying_pressure_score'] < -30:
            severity = 'CRITICAL'
            message = f"🚨 HIGH CASCADE RISK: Liquidation risk {liq_risk['risk_score']}/100, " \
                      f"Stablecoin outflow pressure {stable_pressure['buying_pressure_score']}"
        elif liq_risk['risk_score'] > 50 and stable_pressure['buying_pressure_score'] < 0:
            severity = 'WARNING'
            message = f"⚠️ ELEVATED RISK: Liquidation risk {liq_risk['risk_score']}/100, " \
                      f"Neutral/negative stablecoin flow"
        elif liq_risk['risk_score'] > 30:
            severity = 'INFO'
            message = f"ℹ️ Moderate liquidation risk: {liq_risk['risk_score']}/100"
        else:
            return None

        return {
            'symbol': symbol,
            'alert_type': 'CASCADE_RISK',
            'severity': severity,
            'liquidation_risk_score': liq_risk['risk_score'],
            'buying_pressure_score': stable_pressure['buying_pressure_score'],
            'combined_score': combined_score,
            'message': message
        }

    def consume_funding_rates(self):
        """Consume funding rate updates"""
        consumer = KafkaConsumer(
            'funding_rates',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='signal_processor_funding'
        )

        for message in consumer:
            if not self.running:
                break
            try:
                data = message.value
                symbol = data['symbol']
                if symbol in self.market_state:
                    self.market_state[symbol].funding_rate = data['funding_rate']
            except Exception as e:
                print(f"Funding consume error: {e}")

    def consume_open_interest(self):
        """Consume open interest updates"""
        consumer = KafkaConsumer(
            'open_interest',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='signal_processor_oi'
        )

        for message in consumer:
            if not self.running:
                break
            try:
                data = message.value
                symbol = data['symbol']
                if symbol in self.market_state:
                    self.market_state[symbol].open_interest = data['open_interest']
                    self.oi_history[symbol].append((datetime.utcnow(), data['open_interest']))
            except Exception as e:
                print(f"OI consume error: {e}")

    def consume_long_short_ratio(self):
        """Consume long/short ratio updates"""
        consumer = KafkaConsumer(
            'long_short_ratio',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='signal_processor_ls'
        )

        for message in consumer:
            if not self.running:
                break
            try:
                data = message.value
                symbol = data['symbol']
                if symbol in self.market_state:
                    self.market_state[symbol].long_short_ratio = data['long_short_ratio']
            except Exception as e:
                print(f"L/S consume error: {e}")

    def consume_liquidations(self):
        """Consume liquidation events"""
        consumer = KafkaConsumer(
            'liquidations',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='signal_processor_liq'
        )

        for message in consumer:
            if not self.running:
                break
            try:
                data = message.value
                symbol = data['symbol']
                if symbol in self.market_state:
                    self.market_state[symbol].recent_liquidations.append(
                        (datetime.utcnow(), data['usd_value'], data['side'])
                    )
            except Exception as e:
                print(f"Liquidation consume error: {e}")

    def consume_prices(self):
        """Consume price updates"""
        consumer = KafkaConsumer(
            'prices',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='signal_processor_price'
        )

        for message in consumer:
            if not self.running:
                break
            try:
                data = message.value
                symbol = data['symbol']
                if symbol in self.market_state:
                    self.market_state[symbol].current_price = data['price']
                    self.market_state[symbol].price_history.append(
                        (datetime.utcnow(), data['price'])
                    )
            except Exception as e:
                print(f"Price consume error: {e}")

    def consume_stablecoin_flows(self):
        """Consume stablecoin flow events"""
        consumer = KafkaConsumer(
            'stablecoin_flows',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='signal_processor_stable'
        )

        for message in consumer:
            if not self.running:
                break
            try:
                data = message.value
                flow_type = data['flow_type']
                amount = data['amount']
                now = datetime.utcnow()

                if flow_type == 'EXCHANGE_INFLOW':
                    self.stablecoin_state.recent_inflows.append((now, amount))
                elif flow_type == 'EXCHANGE_OUTFLOW':
                    self.stablecoin_state.recent_outflows.append((now, amount))
                elif flow_type == 'MINT':
                    self.stablecoin_state.recent_mints.append((now, amount))
                elif flow_type == 'BURN':
                    self.stablecoin_state.recent_burns.append((now, amount))

            except Exception as e:
                print(f"Stablecoin consume error: {e}")

    def score_calculator_loop(self):
        """Periodically calculate and store risk scores"""
        while self.running:
            try:
                for symbol in SYMBOLS:
                    # Calculate liquidation risk
                    liq_risk = self.calculate_liquidation_risk(symbol)

                    self.write_to_db(
                        'liquidation_risk',
                        ['time', 'symbol', 'risk_score', 'funding_component',
                         'oi_component', 'price_component', 'liquidation_cluster_distance'],
                        [datetime.utcnow(), symbol, liq_risk['risk_score'],
                         liq_risk['funding_component'], liq_risk['oi_component'],
                         liq_risk['ls_component'], liq_risk['liq_component']]
                    )

                    # Check for alerts
                    alert = self.check_cascade_alert(symbol)
                    if alert:
                        self.write_to_db(
                            'cascade_alerts',
                            ['time', 'symbol', 'alert_type', 'severity',
                             'liquidation_risk_score', 'buying_pressure_score',
                             'combined_score', 'message'],
                            [datetime.utcnow(), symbol, alert['alert_type'],
                             alert['severity'], alert['liquidation_risk_score'],
                             alert['buying_pressure_score'], alert['combined_score'],
                             alert['message']]
                        )
                        print(alert['message'])

                # Calculate stablecoin pressure (global, not per symbol)
                stable_pressure = self.calculate_stablecoin_pressure()

                self.write_to_db(
                    'stablecoin_pressure',
                    ['time', 'buying_pressure_score', 'net_exchange_flow',
                     'large_transfers_count', 'mint_burn_delta'],
                    [datetime.utcnow(), stable_pressure['buying_pressure_score'],
                     stable_pressure['net_exchange_flow'],
                     stable_pressure['large_transfers_count'],
                     stable_pressure['mint_burn_delta']]
                )

                # Log current state
                for symbol in SYMBOLS:
                    state = self.market_state[symbol]
                    risk = self.calculate_liquidation_risk(symbol)
                    print(f"[{symbol}] Price: ${state.current_price:,.2f} | "
                          f"Funding: {state.funding_rate:.4%} | "
                          f"L/S: {state.long_short_ratio:.2f} | "
                          f"Risk: {risk['risk_score']}/100")

                print(f"[STABLE] Pressure Score: {stable_pressure['buying_pressure_score']}/100 | "
                      f"Net Flow: ${stable_pressure['net_exchange_flow']:,.0f}")
                print("-" * 80)

            except Exception as e:
                print(f"Score calculator error: {e}")

            time.sleep(10)  # Calculate every 10 seconds

    def run(self):
        """Start the signal processor"""
        print("=" * 60)
        print("Starting Signal Processor")
        print(f"Symbols: {SYMBOLS}")
        print("=" * 60)

        threads = [
            threading.Thread(target=self.consume_funding_rates, daemon=True),
            threading.Thread(target=self.consume_open_interest, daemon=True),
            threading.Thread(target=self.consume_long_short_ratio, daemon=True),
            threading.Thread(target=self.consume_liquidations, daemon=True),
            threading.Thread(target=self.consume_prices, daemon=True),
            threading.Thread(target=self.consume_stablecoin_flows, daemon=True),
            threading.Thread(target=self.score_calculator_loop, daemon=True),
        ]

        for t in threads:
            t.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")
            self.running = False
            self.pg_conn.close()


if __name__ == "__main__":
    processor = SignalProcessor()
    processor.run()
