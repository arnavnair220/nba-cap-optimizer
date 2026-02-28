# Database Migrations

This directory contains SQL migration scripts for the NBA Cap Optimizer database.

## Running Migrations

Migrations should be run in numerical order. To run a migration:

```bash
# Connect to your PostgreSQL database
psql -h <host> -U <username> -d nba_cap_optimizer -f <migration_file>.sql
```

Or using AWS CLI with RDS:

```bash
# Get database credentials from Secrets Manager
aws secretsmanager get-secret-value --secret-id <secret-arn> --profile personal-account

# Run migration
PGPASSWORD=<password> psql -h <host> -U <username> -d nba_cap_optimizer -f <migration_file>.sql
```

## Migration List

| Migration | Description | Date |
|-----------|-------------|------|
| 001_add_etl_run_tracking.sql | Adds etl_run_id tracking to player_stats and predictions tables for data lineage | 2026-02-28 |

## Notes

- Always backup your database before running migrations
- Migrations are designed to be idempotent (safe to run multiple times)
- After running a migration, verify the changes by checking the output
