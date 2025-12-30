-- Taiwan Stock Institutional Tracker - Database Schema

-- 股票基本資料
CREATE TABLE IF NOT EXISTS stocks (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    market VARCHAR(10) NOT NULL CHECK (market IN ('TWSE', 'TPEX')),
    total_shares BIGINT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_stocks_code ON stocks(code);
CREATE INDEX IF NOT EXISTS idx_stocks_market ON stocks(market);

-- 三大法人每日買賣超
CREATE TABLE IF NOT EXISTS institutional_flows (
    id SERIAL PRIMARY KEY,
    stock_id INTEGER REFERENCES stocks(id) ON DELETE CASCADE,
    trade_date DATE NOT NULL,
    foreign_net BIGINT DEFAULT 0,
    trust_net BIGINT DEFAULT 0,
    dealer_net BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_flows_stock_date ON institutional_flows(stock_id, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_flows_date ON institutional_flows(trade_date);

-- 外資持股
CREATE TABLE IF NOT EXISTS foreign_holdings (
    id SERIAL PRIMARY KEY,
    stock_id INTEGER REFERENCES stocks(id) ON DELETE CASCADE,
    trade_date DATE NOT NULL,
    total_shares BIGINT,
    foreign_shares BIGINT,
    foreign_ratio DECIMAL(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_holdings_stock_date ON foreign_holdings(stock_id, trade_date DESC);

-- 每日股價
CREATE TABLE IF NOT EXISTS stock_prices (
    id SERIAL PRIMARY KEY,
    stock_id INTEGER REFERENCES stocks(id) ON DELETE CASCADE,
    trade_date DATE NOT NULL,
    open_price DECIMAL(12,2),
    high_price DECIMAL(12,2),
    low_price DECIMAL(12,2),
    close_price DECIMAL(12,2),
    volume BIGINT,
    turnover BIGINT,
    change_amount DECIMAL(10,2),
    change_percent DECIMAL(8,4),
    transactions INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_prices_stock_date ON stock_prices(stock_id, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_prices_date ON stock_prices(trade_date);

-- 計算後的持股比重
CREATE TABLE IF NOT EXISTS institutional_ratios (
    id SERIAL PRIMARY KEY,
    stock_id INTEGER REFERENCES stocks(id) ON DELETE CASCADE,
    trade_date DATE NOT NULL,
    foreign_ratio DECIMAL(8,4),
    trust_ratio_est DECIMAL(8,4),
    dealer_ratio_est DECIMAL(8,4),
    three_inst_ratio_est DECIMAL(8,4),
    trust_shares_est BIGINT,
    dealer_shares_est BIGINT,
    change_5d DECIMAL(8,4),
    change_20d DECIMAL(8,4),
    change_60d DECIMAL(8,4),
    change_120d DECIMAL(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_ratios_stock_date ON institutional_ratios(stock_id, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_ratios_change_20d ON institutional_ratios(trade_date, change_20d DESC);

-- 券商分點資料
CREATE TABLE IF NOT EXISTS broker_trades (
    id SERIAL PRIMARY KEY,
    stock_id INTEGER REFERENCES stocks(id) ON DELETE CASCADE,
    trade_date DATE NOT NULL,
    broker_name VARCHAR(100) NOT NULL,
    broker_id VARCHAR(50),
    buy_vol BIGINT DEFAULT 0,
    sell_vol BIGINT DEFAULT 0,
    net_vol BIGINT DEFAULT 0,
    pct DECIMAL(8,4),
    rank INTEGER,
    side VARCHAR(10) CHECK (side IN ('buy', 'sell')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_broker_stock_date ON broker_trades(stock_id, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_broker_name_date ON broker_trades(broker_name, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_broker_date ON broker_trades(trade_date);

-- 校正基準
CREATE TABLE IF NOT EXISTS institutional_baselines (
    id SERIAL PRIMARY KEY,
    stock_id INTEGER REFERENCES stocks(id) ON DELETE CASCADE,
    baseline_date DATE NOT NULL,
    trust_shares_base BIGINT,
    dealer_shares_base BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, baseline_date)
);

-- 系統狀態追蹤
CREATE TABLE IF NOT EXISTS system_status (
    id SERIAL PRIMARY KEY,
    status_key VARCHAR(50) UNIQUE NOT NULL,
    status_value VARCHAR(50) NOT NULL,
    message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 初始化 ETL 狀態
INSERT INTO system_status (status_key, status_value, message)
VALUES ('etl_status', 'idle', '系統待機中')
ON CONFLICT (status_key) DO NOTHING;

-- 更新時間觸發器
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_stocks_updated_at
    BEFORE UPDATE ON stocks
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
