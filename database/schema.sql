-- =====================================================
-- COMPREHENSIVE PATTERN SCANNER DATABASE SCHEMA
-- =====================================================
-- Designed for: Multiple pattern scanners, future extensibility,
-- high performance, and data integrity
-- =====================================================

-- Create database
-- CREATE DATABASE pattern_scanner_db;

-- =====================================================
-- CORE TABLES: Market Data Foundation
-- =====================================================

-- 1. SYMBOLS: Master list of tradable instruments
CREATE TABLE symbols (
    symbol_id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) UNIQUE NOT NULL,
    exchange VARCHAR(10) NOT NULL DEFAULT 'NSE',
    instrument_type VARCHAR(20) NOT NULL DEFAULT 'EQUITY',
    sector VARCHAR(50),
    is_fno BOOLEAN DEFAULT FALSE,
    is_index BOOLEAN DEFAULT FALSE,
    lot_size INTEGER,
    tick_size DECIMAL(10,4) DEFAULT 0.05,
    dhan_security_id VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. DAILY OHLC: Raw daily price data
CREATE TABLE daily_ohlc (
    ohlc_id BIGSERIAL PRIMARY KEY,
    symbol_id INTEGER NOT NULL REFERENCES symbols(symbol_id),
    trade_date DATE NOT NULL,
    open DECIMAL(12,2) NOT NULL,
    high DECIMAL(12,2) NOT NULL,
    low DECIMAL(12,2) NOT NULL,
    close DECIMAL(12,2) NOT NULL,
    volume BIGINT DEFAULT 0,
    -- Derived fields for faster pattern detection
    body_size DECIMAL(12,2) GENERATED ALWAYS AS (ABS(close - open)) STORED,
    range_size DECIMAL(12,2) GENERATED ALWAYS AS (high - low) STORED,
    body_pct DECIMAL(5,2) GENERATED ALWAYS AS (
        CASE
            WHEN high = low THEN 0
            ELSE (ABS(close - open) / NULLIF(high - low, 0)) * 100
        END
    ) STORED,
    change_pct DECIMAL(7,3) GENERATED ALWAYS AS (
        CASE
            WHEN open = 0 THEN 0
            ELSE ((close - open) / open) * 100
        END
    ) STORED,
    is_bullish BOOLEAN GENERATED ALWAYS AS (close > open) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol_id, trade_date)
);

-- 3. AGGREGATED OHLC: Pre-computed weekly/monthly data
CREATE TABLE aggregated_ohlc (
    agg_id BIGSERIAL PRIMARY KEY,
    symbol_id INTEGER NOT NULL REFERENCES symbols(symbol_id),
    timeframe VARCHAR(5) NOT NULL CHECK (timeframe IN ('1W', '2W', '1M', '3M', '6M', '1Y')),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    open DECIMAL(12,2) NOT NULL,
    high DECIMAL(12,2) NOT NULL,
    low DECIMAL(12,2) NOT NULL,
    close DECIMAL(12,2) NOT NULL,
    volume BIGINT DEFAULT 0,
    trading_days INTEGER NOT NULL,
    -- Derived fields
    body_size DECIMAL(12,2) GENERATED ALWAYS AS (ABS(close - open)) STORED,
    range_size DECIMAL(12,2) GENERATED ALWAYS AS (high - low) STORED,
    body_pct DECIMAL(5,2) GENERATED ALWAYS AS (
        CASE
            WHEN high = low THEN 0
            ELSE (ABS(close - open) / NULLIF(high - low, 0)) * 100
        END
    ) STORED,
    change_pct DECIMAL(7,3) GENERATED ALWAYS AS (
        CASE
            WHEN open = 0 THEN 0
            ELSE ((close - open) / open) * 100
        END
    ) STORED,
    is_bullish BOOLEAN GENERATED ALWAYS AS (close > open) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol_id, timeframe, period_start)
);

-- =====================================================
-- PATTERN DETECTION TABLES: Flexible for Multiple Patterns
-- =====================================================

-- 4. PATTERN TYPES: Registry of all pattern types
CREATE TABLE pattern_types (
    pattern_type_id SERIAL PRIMARY KEY,
    pattern_name VARCHAR(50) UNIQUE NOT NULL,
    pattern_category VARCHAR(30) NOT NULL, -- 'reversal', 'continuation', 'neutral'
    candles_required INTEGER NOT NULL DEFAULT 2,
    description TEXT,
    detection_function VARCHAR(100), -- Function name for custom detection
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. DETECTED PATTERNS: All identified patterns
CREATE TABLE detected_patterns (
    pattern_id BIGSERIAL PRIMARY KEY,
    pattern_type_id INTEGER NOT NULL REFERENCES pattern_types(pattern_type_id),
    symbol_id INTEGER NOT NULL REFERENCES symbols(symbol_id),
    timeframe VARCHAR(5) NOT NULL DEFAULT '1D',
    pattern_date DATE NOT NULL, -- Date of pattern completion
    pattern_direction VARCHAR(10) CHECK (pattern_direction IN ('bullish', 'bearish', 'neutral')),
    confidence_score DECIMAL(5,2) DEFAULT 100.0, -- 0-100 confidence
    -- Pattern-specific metrics (flexible JSON for different patterns)
    pattern_data JSONB NOT NULL DEFAULT '{}',
    -- Common derived metrics
    breakout_level DECIMAL(12,2),
    stop_loss_level DECIMAL(12,2),
    target_level DECIMAL(12,2),
    risk_reward_ratio DECIMAL(5,2),
    scan_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE, -- Can be marked false if invalidated
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. PATTERN CANDLES: Individual candles that form patterns
CREATE TABLE pattern_candles (
    candle_id BIGSERIAL PRIMARY KEY,
    pattern_id BIGINT NOT NULL REFERENCES detected_patterns(pattern_id) ON DELETE CASCADE,
    candle_position INTEGER NOT NULL, -- 1st candle, 2nd candle, etc.
    trade_date DATE NOT NULL,
    candle_type VARCHAR(30), -- 'marubozu', 'doji', 'hammer', etc.
    open DECIMAL(12,2) NOT NULL,
    high DECIMAL(12,2) NOT NULL,
    low DECIMAL(12,2) NOT NULL,
    close DECIMAL(12,2) NOT NULL,
    volume BIGINT,
    body_pct DECIMAL(5,2),
    change_pct DECIMAL(7,3),
    notes TEXT,
    UNIQUE(pattern_id, candle_position)
);

-- =====================================================
-- SCANNER CONFIGURATION & HISTORY
-- =====================================================

-- 7. SCAN CONFIGURATIONS: Store scan parameters
CREATE TABLE scan_configurations (
    config_id SERIAL PRIMARY KEY,
    config_name VARCHAR(100) NOT NULL,
    pattern_type_id INTEGER REFERENCES pattern_types(pattern_type_id),
    timeframe VARCHAR(5) NOT NULL DEFAULT '1D',
    lookback_days INTEGER DEFAULT 30,
    min_body_move_pct DECIMAL(5,2) DEFAULT 4.0,
    min_volume BIGINT DEFAULT 0,
    additional_filters JSONB DEFAULT '{}',
    is_default BOOLEAN DEFAULT FALSE,
    created_by VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 8. SCAN HISTORY: Track all scans performed
CREATE TABLE scan_history (
    scan_id BIGSERIAL PRIMARY KEY,
    config_id INTEGER REFERENCES scan_configurations(config_id),
    scan_type VARCHAR(30) NOT NULL, -- 'full', 'today', 'custom'
    symbols_scanned INTEGER NOT NULL,
    patterns_found INTEGER NOT NULL,
    execution_time_ms INTEGER,
    scan_parameters JSONB DEFAULT '{}',
    scan_results JSONB DEFAULT '{}',
    error_log TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- ALERTS & NOTIFICATIONS (Future Feature)
-- =====================================================

-- 9. PATTERN ALERTS: Track pattern-based alerts
CREATE TABLE pattern_alerts (
    alert_id SERIAL PRIMARY KEY,
    pattern_id BIGINT REFERENCES detected_patterns(pattern_id),
    alert_type VARCHAR(30) NOT NULL, -- 'pattern_detected', 'breakout', 'target_hit'
    alert_status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'triggered', 'expired'
    trigger_condition JSONB DEFAULT '{}',
    triggered_at TIMESTAMP,
    notification_sent BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- DATA QUALITY & MAINTENANCE
-- =====================================================

-- 10. DATA UPDATE LOG: Track data freshness
CREATE TABLE data_update_log (
    update_id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    update_type VARCHAR(20) NOT NULL, -- 'full', 'incremental', 'correction'
    records_affected INTEGER,
    start_date DATE,
    end_date DATE,
    source VARCHAR(50), -- 'dhan_api', 'manual', 'csv_import'
    status VARCHAR(20) DEFAULT 'in_progress', -- 'in_progress', 'completed', 'failed'
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

-- =====================================================
-- PERFORMANCE INDEXES
-- =====================================================

-- Daily OHLC indexes
CREATE INDEX idx_daily_ohlc_symbol_date ON daily_ohlc(symbol_id, trade_date DESC);
CREATE INDEX idx_daily_ohlc_date ON daily_ohlc(trade_date DESC);
CREATE INDEX idx_daily_ohlc_body_pct ON daily_ohlc(body_pct) WHERE body_pct IS NOT NULL;
CREATE INDEX idx_daily_ohlc_change_pct ON daily_ohlc(change_pct) WHERE change_pct IS NOT NULL;

-- Aggregated OHLC indexes
CREATE INDEX idx_agg_ohlc_symbol_timeframe ON aggregated_ohlc(symbol_id, timeframe, period_start DESC);
CREATE INDEX idx_agg_ohlc_timeframe_date ON aggregated_ohlc(timeframe, period_start DESC);

-- Pattern detection indexes
CREATE INDEX idx_patterns_symbol_date ON detected_patterns(symbol_id, pattern_date DESC);
CREATE INDEX idx_patterns_type_date ON detected_patterns(pattern_type_id, pattern_date DESC);
CREATE INDEX idx_patterns_date_direction ON detected_patterns(pattern_date DESC, pattern_direction);
CREATE INDEX idx_patterns_scan_timestamp ON detected_patterns(scan_timestamp DESC);

-- Pattern data JSONB indexes (for common queries)
CREATE INDEX idx_patterns_data_gin ON detected_patterns USING gin(pattern_data);

-- Symbol lookup
CREATE INDEX idx_symbols_active ON symbols(symbol) WHERE is_active = TRUE;

-- =====================================================
-- MATERIALIZED VIEWS FOR PERFORMANCE
-- =====================================================

-- Latest patterns view for quick dashboard loading
CREATE MATERIALIZED VIEW latest_patterns AS
SELECT
    dp.pattern_id,
    s.symbol,
    pt.pattern_name,
    dp.pattern_direction,
    dp.pattern_date,
    dp.confidence_score,
    dp.pattern_data,
    dp.breakout_level,
    dp.stop_loss_level,
    dp.target_level,
    dp.scan_timestamp
FROM detected_patterns dp
JOIN symbols s ON dp.symbol_id = s.symbol_id
JOIN pattern_types pt ON dp.pattern_type_id = pt.pattern_type_id
WHERE dp.pattern_date >= CURRENT_DATE - INTERVAL '7 days'
    AND dp.is_active = TRUE
ORDER BY dp.pattern_date DESC, dp.scan_timestamp DESC;

CREATE INDEX idx_latest_patterns_symbol ON latest_patterns(symbol);
CREATE INDEX idx_latest_patterns_date ON latest_patterns(pattern_date DESC);

-- =====================================================
-- HELPER FUNCTIONS
-- =====================================================

-- Function to get latest price for a symbol
CREATE OR REPLACE FUNCTION get_latest_price(p_symbol VARCHAR)
RETURNS TABLE(symbol VARCHAR, trade_date DATE, close DECIMAL) AS $$
BEGIN
    RETURN QUERY
    SELECT s.symbol, d.trade_date, d.close
    FROM daily_ohlc d
    JOIN symbols s ON d.symbol_id = s.symbol_id
    WHERE s.symbol = p_symbol
    ORDER BY d.trade_date DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Function to check if pattern exists (avoid duplicates)
CREATE OR REPLACE FUNCTION pattern_exists(
    p_symbol_id INTEGER,
    p_pattern_type_id INTEGER,
    p_pattern_date DATE,
    p_timeframe VARCHAR DEFAULT '1D'
)
RETURNS BOOLEAN AS $$
DECLARE
    v_exists BOOLEAN;
BEGIN
    SELECT EXISTS(
        SELECT 1
        FROM detected_patterns
        WHERE symbol_id = p_symbol_id
            AND pattern_type_id = p_pattern_type_id
            AND pattern_date = p_pattern_date
            AND timeframe = p_timeframe
            AND is_active = TRUE
    ) INTO v_exists;

    RETURN v_exists;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- TRIGGERS
-- =====================================================

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_symbols_updated_at
    BEFORE UPDATE ON symbols
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- =====================================================
-- INITIAL DATA
-- =====================================================

-- Insert pattern types
INSERT INTO pattern_types (pattern_name, pattern_category, candles_required, description) VALUES
('Marubozu-Doji', 'reversal', 2, 'Strong move (Marubozu) followed by indecision (Doji)'),
('Hammer', 'reversal', 1, 'Bullish reversal pattern at bottom'),
('Shooting Star', 'reversal', 1, 'Bearish reversal pattern at top'),
('Bullish Engulfing', 'reversal', 2, 'Bullish reversal with second candle engulfing first'),
('Bearish Engulfing', 'reversal', 2, 'Bearish reversal with second candle engulfing first'),
('Morning Star', 'reversal', 3, 'Three-candle bullish reversal pattern'),
('Evening Star', 'reversal', 3, 'Three-candle bearish reversal pattern'),
('Three White Soldiers', 'continuation', 3, 'Three consecutive bullish candles'),
('Three Black Crows', 'continuation', 3, 'Three consecutive bearish candles'),
('Doji', 'neutral', 1, 'Single doji indicating indecision'),
('Spinning Top', 'neutral', 1, 'Small body indicating indecision'),
('Harami', 'reversal', 2, 'Second candle within first candle body'),
('Piercing Pattern', 'reversal', 2, 'Bullish reversal pattern'),
('Dark Cloud Cover', 'reversal', 2, 'Bearish reversal pattern'),
('Tweezer Top', 'reversal', 2, 'Double top reversal pattern'),
('Tweezer Bottom', 'reversal', 2, 'Double bottom reversal pattern');

-- =====================================================
-- PERMISSIONS (Adjust based on your user setup)
-- =====================================================

-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;
-- GRANT ALL ON ALL TABLES IN SCHEMA public TO app_user;
-- GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO app_user;

-- =====================================================
-- MAINTENANCE QUERIES
-- =====================================================

-- Query to check data freshness
/*
SELECT
    s.symbol,
    MAX(d.trade_date) as last_update,
    CURRENT_DATE - MAX(d.trade_date) as days_old
FROM symbols s
LEFT JOIN daily_ohlc d ON s.symbol_id = d.symbol_id
WHERE s.is_active = TRUE
GROUP BY s.symbol
ORDER BY days_old DESC;
*/

-- Query to find patterns detected today
/*
SELECT
    s.symbol,
    pt.pattern_name,
    dp.pattern_direction,
    dp.pattern_date,
    dp.confidence_score,
    dp.pattern_data
FROM detected_patterns dp
JOIN symbols s ON dp.symbol_id = s.symbol_id
JOIN pattern_types pt ON dp.pattern_type_id = pt.pattern_type_id
WHERE DATE(dp.scan_timestamp) = CURRENT_DATE
ORDER BY dp.scan_timestamp DESC;
*/