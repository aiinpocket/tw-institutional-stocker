#!/bin/bash
# Data Migration Script: Local Docker PostgreSQL -> Cloud SQL
# Usage: ./gcp/migrate-data.sh

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=== Data Migration: Docker -> Cloud SQL ===${NC}"

# Check environment
if [ -z "$PROJECT_ID" ]; then
    echo -e "${RED}ERROR: PROJECT_ID not set${NC}"
    echo "Usage: PROJECT_ID=your-project REGION=asia-east1 ./gcp/migrate-data.sh"
    exit 1
fi

REGION=${REGION:-asia-east1}
INSTANCE_NAME="tw-stocker-db"
DB_NAME="tw_stocker"
BACKUP_FILE="tw_stocker_backup_$(date +%Y%m%d_%H%M%S).sql"

echo -e "\n${GREEN}[1/4] Exporting data from Docker PostgreSQL...${NC}"

# Export from Docker
docker exec tw-stocker-db pg_dump -U stocker -d tw_stocker \
    --no-owner --no-acl \
    --clean --if-exists \
    > "$BACKUP_FILE"

echo "  Exported to: $BACKUP_FILE"
echo "  Size: $(du -h $BACKUP_FILE | cut -f1)"

echo -e "\n${GREEN}[2/4] Uploading to Cloud Storage...${NC}"

# Create bucket if not exists
BUCKET_NAME="${PROJECT_ID}-db-backups"
gsutil mb -p $PROJECT_ID -l $REGION gs://$BUCKET_NAME 2>/dev/null || true

# Upload
gsutil cp "$BACKUP_FILE" "gs://$BUCKET_NAME/"
echo "  Uploaded to: gs://$BUCKET_NAME/$BACKUP_FILE"

echo -e "\n${GREEN}[3/4] Granting Cloud SQL access to bucket...${NC}"

# Get Cloud SQL service account
SQL_SA=$(gcloud sql instances describe $INSTANCE_NAME --format="value(serviceAccountEmailAddress)")
gsutil iam ch serviceAccount:${SQL_SA}:objectViewer "gs://$BUCKET_NAME"

echo -e "\n${GREEN}[4/4] Importing to Cloud SQL...${NC}"

# Import to Cloud SQL
gcloud sql import sql $INSTANCE_NAME "gs://$BUCKET_NAME/$BACKUP_FILE" \
    --database=$DB_NAME \
    --user=postgres \
    --quiet

echo -e "\n${GREEN}=== Migration Complete! ===${NC}"
echo -e "\nBackup file saved locally: $BACKUP_FILE"
echo -e "Cloud Storage: gs://$BUCKET_NAME/$BACKUP_FILE"

echo -e "\n${YELLOW}Verify migration:${NC}"
echo "gcloud sql connect $INSTANCE_NAME --user=postgres --database=$DB_NAME"
echo "SELECT COUNT(*) FROM stock_prices;"

# Cleanup local file (optional)
echo -e "\n${YELLOW}To clean up local backup:${NC}"
echo "rm $BACKUP_FILE"
