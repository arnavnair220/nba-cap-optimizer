-- Migration: Add ETL run tracking to player_stats and predictions tables
-- Description: Adds etl_run_id columns and indexes to track data lineage
-- Date: 2026-02-28

-- ============================================================================
-- 1. Add etl_run_id to player_stats table
-- ============================================================================

-- Add the etl_run_id column
ALTER TABLE player_stats
ADD COLUMN IF NOT EXISTS etl_run_id VARCHAR(50);

-- Create index on etl_run_id for efficient filtering
CREATE INDEX IF NOT EXISTS idx_player_stats_etl_run_id ON player_stats(etl_run_id);

-- Update existing rows to have a default etl_run_id (using current timestamp)
-- This is necessary for existing data that was loaded before this column existed
UPDATE player_stats
SET etl_run_id = 'pre_migration_' || TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD_HH24MISS')
WHERE etl_run_id IS NULL;

-- ============================================================================
-- 2. Add etl_run_id to predictions table
-- ============================================================================

-- Add the etl_run_id column
ALTER TABLE predictions
ADD COLUMN IF NOT EXISTS etl_run_id VARCHAR(50);

-- Create index on etl_run_id for efficient filtering
CREATE INDEX IF NOT EXISTS idx_predictions_etl_run_id ON predictions(etl_run_id);

-- Update existing rows to have a default etl_run_id (mark as pre-migration)
-- This is necessary for existing predictions that were made before this column existed
UPDATE predictions
SET etl_run_id = 'pre_migration_unknown'
WHERE etl_run_id IS NULL;

-- ============================================================================
-- 3. Add prediction_date index (optimization)
-- ============================================================================

-- Create index on prediction_date for efficient time-based queries
CREATE INDEX IF NOT EXISTS idx_predictions_prediction_date ON predictions(prediction_date);

-- ============================================================================
-- Verification
-- ============================================================================

-- Verify player_stats migration
SELECT 'player_stats' as table_name,
       COUNT(*) as total_rows,
       COUNT(etl_run_id) as rows_with_etl_run_id,
       COUNT(DISTINCT etl_run_id) as unique_etl_runs
FROM player_stats
UNION ALL
-- Verify predictions migration
SELECT 'predictions' as table_name,
       COUNT(*) as total_rows,
       COUNT(etl_run_id) as rows_with_etl_run_id,
       COUNT(DISTINCT etl_run_id) as unique_etl_runs
FROM predictions;
