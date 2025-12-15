#!/usr/bin/env python3
"""
Stablecoin Flow Producer
Tracks USDT/USDC large transfers and exchange flows
Uses Etherscan API for on-chain data
"""

import json
import time
import threading
from datetime import datetime
from typing import Dict, Any, List
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

# Etherscan API (free tier: 5 calls/sec)
# Get your free API key at https://etherscan.io/apis
ETHERSCAN_API_KEY = 'YourApiKeyToken'  # Replace with your key
ETHERSCAN_API = 'https://api.etherscan.io/api'

# Token contracts
TOKENS = {
    'USDT': '0xdAC17F958D2ee523a2206206994597C13D831ec7',
    'USDC': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
}

# Known exchange addresses (subset of major exchanges)
EXCHANGE_ADDRESSES = {
    # Binance
    '0x28c6c06298d514db089934071355e5743bf21d60': 'Binance',
    '0x21a31ee1afc51d94c2efccaa2092ad1028285549': 'Binance',
    '0xdfd5293d8e347dfe59e90efd55b2956a1343963d': 'Binance',
    '0x56eddb7aa87536c09ccc2793473599fd21a8b17f': 'Binance',
    # Coinbase
    '0x71660c4005ba85c37ccec55d0c4493e66fe775d3': 'Coinbase',
    '0x503828976d22510aad0201ac7ec88293211d23da': 'Coinbase',
    '0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740': 'Coinbase',
    # Kraken
    '0x2910543af39aba0cd09dbb2d50200b3e800a63d2': 'Kraken',
    '0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13': 'Kraken',
    # FTX (now bankrupt but still useful for historical)
    '0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2': 'FTX',
    # OKX
    '0x6cc5f688a315f3dc28a7781717a9a798a59fda7b': 'OKX',
    '0x236f9f97e0e62388479bf9e5ba4889e46b0273c3': 'OKX',
    # Bitfinex
    '0x1151314c646ce4e0efd76d1af4760ae66a9fe30f': 'Bitfinex',
    '0x742d35cc6634c0532925a3b844bc454e4438f44e': 'Bitfinex',
    # Huobi
    '0xab5c66752a9e8167967685f1450532fb96d5d24f': 'Huobi',
    '0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b': 'Huobi',
    # Kucoin
    '0xf16e9b0d03470827a95cdfd0cb8a8a3b46969b91': 'Kucoin',
    # Gemini
    '0xd24400ae8bfebb18ca49be86258a3c749cf46853': 'Gemini',
    # Bybit
    '0xf89d7b9c864f589bbf53a82105107622b35eaa40': 'Bybit',
}

# Tether Treasury (for mint/burn detection)
TETHER_TREASURY = '0x5754284f345afc66a98fbb0a0afe71e0f007b949'

# Minimum transfer size to track (in token units)
MIN_TRANSFER_USDT = 1_000_000  # $1M
MIN_TRANSFER_USDC = 1_000_000  # $1M


class StablecoinProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
        self.pg_conn.autocommit = True
        self.running = True
        self.last_block = {}  # Track last processed block per token

    def send_to_kafka(self, topic: str, key: str, data: Dict[str, Any]):
        """Send data to Kafka topic"""
        self.producer.send(topic, key=key, value=data)

    def write_to_db(self, table: str, columns: list, values: list):
        """Write data directly to TimescaleDB"""
        try:
            with self.pg_conn.cursor() as cur:
                query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
                execute_values(cur, query, [values])
        except Exception as e:
            print(f"DB write error: {e}")
            self.pg_conn.rollback()

    def get_latest_block(self) -> int:
        """Get latest Ethereum block number"""
        try:
            response = requests.get(ETHERSCAN_API, params={
                'module': 'proxy',
                'action': 'eth_blockNumber',
                'apikey': ETHERSCAN_API_KEY
            })
            return int(response.json()['result'], 16)
        except Exception as e:
            print(f"Error getting block: {e}")
            return 0

    def get_token_transfers(self, token: str, contract: str, from_block: int, to_block: int) -> List[Dict]:
        """Get token transfers from Etherscan"""
        try:
            response = requests.get(ETHERSCAN_API, params={
                'module': 'account',
                'action': 'tokentx',
                'contractaddress': contract,
                'startblock': from_block,
                'endblock': to_block,
                'sort': 'desc',
                'apikey': ETHERSCAN_API_KEY
            })
            data = response.json()
            if data['status'] == '1':
                return data['result']
            return []
        except Exception as e:
            print(f"Error getting transfers: {e}")
            return []

    def is_exchange_address(self, address: str) -> tuple:
        """Check if address is a known exchange"""
        addr_lower = address.lower()
        if addr_lower in EXCHANGE_ADDRESSES:
            return True, EXCHANGE_ADDRESSES[addr_lower]
        return False, None

    def classify_flow(self, from_addr: str, to_addr: str, token: str, amount: float) -> Dict:
        """Classify the type of stablecoin flow"""
        from_is_exchange, from_exchange = self.is_exchange_address(from_addr)
        to_is_exchange, to_exchange = self.is_exchange_address(to_addr)

        # Check for mint/burn
        if from_addr.lower() == '0x0000000000000000000000000000000000000000':
            return {'type': 'MINT', 'exchange': None, 'signal': 'BULLISH'}
        if to_addr.lower() == '0x0000000000000000000000000000000000000000':
            return {'type': 'BURN', 'exchange': None, 'signal': 'BEARISH'}

        # Exchange inflow (potential selling pressure)
        if to_is_exchange and not from_is_exchange:
            return {'type': 'EXCHANGE_INFLOW', 'exchange': to_exchange, 'signal': 'BEARISH'}

        # Exchange outflow (potential buying pressure)
        if from_is_exchange and not to_is_exchange:
            return {'type': 'EXCHANGE_OUTFLOW', 'exchange': from_exchange, 'signal': 'BULLISH'}

        # Exchange to exchange
        if from_is_exchange and to_is_exchange:
            return {'type': 'EXCHANGE_TRANSFER', 'exchange': f"{from_exchange}->{to_exchange}", 'signal': 'NEUTRAL'}

        # Whale transfer (non-exchange)
        return {'type': 'WHALE_TRANSFER', 'exchange': None, 'signal': 'NEUTRAL'}

    def process_transfers(self, token: str, transfers: List[Dict]):
        """Process and filter large transfers"""
        min_amount = MIN_TRANSFER_USDT if token == 'USDT' else MIN_TRANSFER_USDC
        decimals = 6  # Both USDT and USDC have 6 decimals

        for tx in transfers:
            try:
                amount = int(tx['value']) / (10 ** decimals)

                if amount < min_amount:
                    continue

                from_addr = tx['from']
                to_addr = tx['to']

                classification = self.classify_flow(from_addr, to_addr, token, amount)

                record = {
                    'time': datetime.utcnow().isoformat(),
                    'token': token,
                    'flow_type': classification['type'],
                    'amount': amount,
                    'from_address': from_addr,
                    'to_address': to_addr,
                    'is_exchange_flow': classification['type'] in ['EXCHANGE_INFLOW', 'EXCHANGE_OUTFLOW'],
                    'exchange': classification['exchange'],
                    'signal': classification['signal'],
                    'tx_hash': tx['hash']
                }

                self.send_to_kafka('stablecoin_flows', token, record)

                # Write to DB
                self.write_to_db(
                    'stablecoin_flows',
                    ['time', 'token', 'flow_type', 'amount', 'from_address', 'to_address', 'is_exchange_flow'],
                    [datetime.utcnow(), token, classification['type'], amount, from_addr, to_addr,
                     record['is_exchange_flow']]
                )

                # Log significant flows
                emoji = "🟢" if classification['signal'] == 'BULLISH' else "🔴" if classification['signal'] == 'BEARISH' else "⚪"
                exchange_info = f" ({classification['exchange']})" if classification['exchange'] else ""
                print(f"{emoji} [{token}] {classification['type']}{exchange_info}: ${amount:,.0f}")

            except Exception as e:
                print(f"Transfer processing error: {e}")

    def track_stablecoin_flows(self):
        """Main loop to track stablecoin transfers"""
        # Initialize last block
        current_block = self.get_latest_block()
        for token in TOKENS:
            self.last_block[token] = current_block - 100  # Start from 100 blocks ago

        while self.running:
            try:
                current_block = self.get_latest_block()

                for token, contract in TOKENS.items():
                    from_block = self.last_block[token] + 1
                    to_block = current_block

                    if from_block >= to_block:
                        continue

                    print(f"[{token}] Scanning blocks {from_block} to {to_block}")

                    transfers = self.get_token_transfers(token, contract, from_block, to_block)
                    if transfers:
                        self.process_transfers(token, transfers)

                    self.last_block[token] = to_block

                    # Rate limiting for free API tier
                    time.sleep(0.25)

            except Exception as e:
                print(f"Main loop error: {e}")

            # Check every 15 seconds (roughly 1 block time)
            time.sleep(15)

    def run(self):
        """Start the producer"""
        print("=" * 60)
        print("Starting Stablecoin Flow Producer")
        print(f"Tracking: {list(TOKENS.keys())}")
        print(f"Min transfer size: ${MIN_TRANSFER_USDT:,}")
        print("=" * 60)

        if ETHERSCAN_API_KEY == 'YourApiKeyToken':
            print("\n⚠️  WARNING: Using default Etherscan API key!")
            print("Get a free key at https://etherscan.io/apis")
            print("Replace 'YourApiKeyToken' in this file\n")

        thread = threading.Thread(target=self.track_stablecoin_flows, daemon=True)
        thread.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")
            self.running = False
            self.producer.close()
            self.pg_conn.close()


if __name__ == "__main__":
    producer = StablecoinProducer()
    producer.run()
