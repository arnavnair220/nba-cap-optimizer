-- Migration: Add run tracking to predictions table for historical comparison
-- Description: Adds run_id column and updates unique constraint to allow multiple prediction runs per model version
-- Date: 2026-02-28

-- ============================================================================
-- 1. Add run_id column to predictions table
-- ============================================================================

-- Add the run_id column
ALTER TABLE predictions
ADD COLUMN IF NOT EXISTS run_id VARCHAR(100);

-- Update existing rows to have a default run_id (mark as legacy)
-- This is necessary for existing predictions that were made before this column existed
UPDATE predictions
SET run_id = 'legacy_' || TO_CHAR(prediction_date, 'YYYYMMDD_HH24MISS')
WHERE run_id IS NULL;

-- Now make run_id NOT NULL after backfilling
ALTER TABLE predictions
ALTER COLUMN run_id SET NOT NULL;

-- Create index on run_id for efficient filtering
CREATE INDEX IF NOT EXISTS idx_predictions_run_id ON predictions(run_id);

-- ============================================================================
-- 2. Update unique constraint to include run_id
-- ============================================================================

-- Drop the old unique constraint
ALTER TABLE predictions
DROP CONSTRAINT IF EXISTS predictions_player_name_season_model_version_key;

-- Add new unique constraint that includes run_id
-- This allows the same player/season/model to have multiple predictions from different runs
ALTER TABLE predictions
ADD CONSTRAINT predictions_player_name_season_model_version_run_id_key
UNIQUE (player_name, season, model_version, run_id);

-- ============================================================================
-- 3. Add prediction_date index with DESC ordering (optimization for "latest run" queries)
-- ============================================================================

-- Drop old index if exists
DROP INDEX IF EXISTS idx_predictions_prediction_date;

-- Create index on prediction_date with DESC ordering for efficient "latest run" queries
CREATE INDEX IF NOT EXISTS idx_predictions_date ON predictions(prediction_date DESC);

-- Create composite index for common query patterns (player + date for historical comparison)
CREATE INDEX IF NOT EXISTS idx_predictions_player_date ON predictions(player_name, prediction_date DESC);

-- ============================================================================
-- Verification
-- ============================================================================

-- Verify migration
SELECT
    'predictions' as table_name,
    COUNT(*) as total_rows,
    COUNT(run_id) as rows_with_run_id,
    COUNT(DISTINCT run_id) as unique_runs,
    MIN(prediction_date) as earliest_prediction,
    MAX(prediction_date) as latest_prediction
FROM predictions;

-- Show sample of run tracking data
SELECT
    run_id,
    model_version,
    COUNT(*) as player_count,
    prediction_date
FROM predictions
GROUP BY run_id, model_version, prediction_date
ORDER BY prediction_date DESC
LIMIT 10;
