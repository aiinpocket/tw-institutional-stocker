#!/bin/bash

# Export environment variables for cron
printenv | grep -E '^(DATABASE_URL|TZ|POSTGRES_)' >> /etc/environment

echo "=========================================="
echo "Starting ETL worker..."
echo "Timezone: $TZ"
echo "Time: $(date)"
echo "=========================================="

# Log startup
echo "$(date): ETL worker started" >> /var/log/cron.log

# Step 1: Fill any data gaps since last update (startup recovery)
echo ""
echo "[STARTUP] Checking for data gaps to backfill..."
echo "$(date): Running startup backfill..." >> /var/log/cron.log

cd /app && /usr/local/bin/python -c "
from src.etl.backfill_prices import fill_gap_since_last_update
fill_gap_since_last_update()
" 2>&1 | tee -a /var/log/cron.log

echo "$(date): Startup backfill completed" >> /var/log/cron.log

# Step 2: Recompute strategy rankings after backfill
echo ""
echo "[STARTUP] Recomputing strategy rankings..."
echo "$(date): Running strategy computation..." >> /var/log/cron.log

cd /app && /usr/local/bin/python -c "
from src.common.database import SessionLocal
from src.etl.processors.compute_strategy import run_all_computations
db = SessionLocal()
try:
    run_all_computations(db)
finally:
    db.close()
" 2>&1 | tee -a /var/log/cron.log

echo "$(date): Strategy computation completed" >> /var/log/cron.log

echo ""
echo "=========================================="
echo "[STARTUP] Initialization complete!"
echo "Starting cron scheduler..."
echo "=========================================="

# Start cron in foreground
cron -f
