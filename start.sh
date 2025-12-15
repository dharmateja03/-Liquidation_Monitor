#!/bin/bash

echo "=========================================="
echo "  Liquidation Cascade Monitor - Startup  "
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Step 1: Start Docker services
echo -e "\n${YELLOW}[1/4] Starting Docker services...${NC}"
docker-compose up -d

# Wait for services to be healthy
echo -e "${YELLOW}Waiting for services to initialize...${NC}"
sleep 15

# Step 2: Check if Kafka is ready
echo -e "\n${YELLOW}[2/4] Checking Kafka readiness...${NC}"
max_attempts=30
attempt=0
while ! docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ $attempt -ge $max_attempts ]; then
        echo -e "${RED}Kafka failed to start. Check docker logs.${NC}"
        exit 1
    fi
    echo "Waiting for Kafka... (attempt $attempt/$max_attempts)"
    sleep 2
done
echo -e "${GREEN}Kafka is ready!${NC}"

# Step 3: Create Kafka topics
echo -e "\n${YELLOW}[3/4] Creating Kafka topics...${NC}"
topics=("funding_rates" "open_interest" "liquidations" "prices" "long_short_ratio" "stablecoin_flows")

for topic in "${topics[@]}"; do
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic "$topic" --partitions 3 --replication-factor 1 \
        --if-not-exists
    echo -e "  ${GREEN}✓${NC} Created topic: $topic"
done

# Step 4: Display service URLs
echo -e "\n${YELLOW}[4/4] Services are ready!${NC}"
echo ""
echo "=========================================="
echo -e "${GREEN}  Services Running:${NC}"
echo "=========================================="
echo -e "  Kafka UI:      ${GREEN}http://localhost:8080${NC}"
echo -e "  Grafana:       ${GREEN}http://localhost:3000${NC} (admin/admin)"
echo -e "  TimescaleDB:   ${GREEN}localhost:5432${NC}"
echo -e "  Flink UI:      ${GREEN}http://localhost:8081${NC}"
echo ""
echo "=========================================="
echo -e "${GREEN}  Next Steps:${NC}"
echo "=========================================="
echo ""
echo "1. Install Python dependencies:"
echo -e "   ${YELLOW}cd producers && pip install -r requirements.txt${NC}"
echo ""
echo "2. Start the Binance producer (Terminal 1):"
echo -e "   ${YELLOW}python producers/binance_futures_producer.py${NC}"
echo ""
echo "3. Start the Signal processor (Terminal 2):"
echo -e "   ${YELLOW}python producers/signal_processor.py${NC}"
echo ""
echo "4. (Optional) Start Stablecoin producer (Terminal 3):"
echo -e "   ${YELLOW}python producers/stablecoin_producer.py${NC}"
echo "   Note: Requires Etherscan API key"
echo ""
echo "5. Open Grafana dashboard:"
echo -e "   ${YELLOW}http://localhost:3000/d/liquidation-cascade${NC}"
echo ""
echo "=========================================="
