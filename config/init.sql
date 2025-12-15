-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Funding rates table
CREATE TABLE funding_rates (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    funding_rate DOUBLE PRECISION,
    mark_price DOUBLE PRECISION,
    index_price DOUBLE PRECISION
);
SELECT create_hypertable('funding_rates', 'time');

-- Open interest table
CREATE TABLE open_interest (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open_interest DOUBLE PRECISION,
    open_interest_value DOUBLE PRECISION
);
SELECT create_hypertable('open_interest', 'time');

-- Liquidations table
CREATE TABLE liquidations (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL,
    quantity DOUBLE PRECISION,
    price DOUBLE PRECISION,
    usd_value DOUBLE PRECISION
);
SELECT create_hypertable('liquidations', 'time');

-- Price data table
CREATE TABLE prices (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    price DOUBLE PRECISION,
    volume DOUBLE PRECISION
);
SELECT create_hypertable('prices', 'time');

-- Stablecoin flows table
CREATE TABLE stablecoin_flows (
    time TIMESTAMPTZ NOT NULL,
    token VARCHAR(10) NOT NULL,
    flow_type VARCHAR(20) NOT NULL,
    amount DOUBLE PRECISION,
    from_address VARCHAR(100),
    to_address VARCHAR(100),
    is_exchange_flow BOOLEAN DEFAULT FALSE
);
SELECT create_hypertable('stablecoin_flows', 'time');

-- Liquidation risk scores (computed by Flink)
CREATE TABLE liquidation_risk (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    risk_score INTEGER,
    funding_component DOUBLE PRECISION,
    oi_component DOUBLE PRECISION,
    price_component DOUBLE PRECISION,
    liquidation_cluster_distance DOUBLE PRECISION
);
SELECT create_hypertable('liquidation_risk', 'time');

-- Stablecoin flow scores (computed by Flink)
CREATE TABLE stablecoin_pressure (
    time TIMESTAMPTZ NOT NULL,
    buying_pressure_score INTEGER,
    net_exchange_flow DOUBLE PRECISION,
    large_transfers_count INTEGER,
    mint_burn_delta DOUBLE PRECISION
);
SELECT create_hypertable('stablecoin_pressure', 'time');

-- Combined signals table
CREATE TABLE cascade_alerts (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    liquidation_risk_score INTEGER,
    buying_pressure_score INTEGER,
    combined_score INTEGER,
    message TEXT
);
SELECT create_hypertable('cascade_alerts', 'time');

-- Long/Short ratio table
CREATE TABLE long_short_ratio (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    long_ratio DOUBLE PRECISION,
    short_ratio DOUBLE PRECISION,
    long_short_ratio DOUBLE PRECISION
);
SELECT create_hypertable('long_short_ratio', 'time');

-- Create continuous aggregates for faster queries
CREATE MATERIALIZED VIEW funding_rates_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    symbol,
    AVG(funding_rate) as avg_funding_rate,
    MAX(funding_rate) as max_funding_rate,
    MIN(funding_rate) as min_funding_rate
FROM funding_rates
GROUP BY bucket, symbol;

CREATE MATERIALIZED VIEW liquidations_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    symbol,
    side,
    SUM(usd_value) as total_usd_liquidated,
    COUNT(*) as liquidation_count
FROM liquidations
GROUP BY bucket, symbol, side;

CREATE MATERIALIZED VIEW oi_changes_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    symbol,
    FIRST(open_interest, time) as oi_start,
    LAST(open_interest, time) as oi_end,
    MAX(open_interest) - MIN(open_interest) as oi_range
FROM open_interest
GROUP BY bucket, symbol;

-- Indexes for faster queries
CREATE INDEX idx_funding_symbol ON funding_rates (symbol, time DESC);
CREATE INDEX idx_oi_symbol ON open_interest (symbol, time DESC);
CREATE INDEX idx_liquidations_symbol ON liquidations (symbol, time DESC);
CREATE INDEX idx_risk_symbol ON liquidation_risk (symbol, time DESC);
CREATE INDEX idx_alerts_severity ON cascade_alerts (severity, time DESC);
