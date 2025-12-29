#!/bin/bash
# GCP Resource Cleanup Script
# Usage: ./gcp/cleanup.sh
# WARNING: This will delete ALL resources!

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${RED}=== WARNING: GCP Resource Cleanup ===${NC}"
echo -e "${RED}This will DELETE all Taiwan Stock Tracker resources!${NC}"
echo ""

if [ -z "$PROJECT_ID" ]; then
    echo -e "${YELLOW}PROJECT_ID not set. Please enter your GCP Project ID:${NC}"
    read PROJECT_ID
fi

REGION=${REGION:-asia-east1}

echo -e "\nResources to be deleted:"
echo "  - Cloud Run Service: tw-stocker-api"
echo "  - Cloud Run Job: tw-stocker-etl"
echo "  - Cloud Scheduler: tw-stocker-etl-schedule"
echo "  - Cloud SQL: tw-stocker-db"
echo "  - Secret: db-password"
echo ""
echo -e "${RED}Are you sure? (yes/no):${NC}"
read CONFIRM

if [ "$CONFIRM" != "yes" ]; then
    echo "Aborted."
    exit 0
fi

gcloud config set project $PROJECT_ID

echo -e "\n${GREEN}[1/5] Deleting Cloud Scheduler...${NC}"
gcloud scheduler jobs delete tw-stocker-etl-schedule \
    --location=$REGION --quiet 2>/dev/null || echo "  Not found, skipping..."

echo -e "\n${GREEN}[2/5] Deleting Cloud Run Job...${NC}"
gcloud run jobs delete tw-stocker-etl \
    --region=$REGION --quiet 2>/dev/null || echo "  Not found, skipping..."

echo -e "\n${GREEN}[3/5] Deleting Cloud Run Service...${NC}"
gcloud run services delete tw-stocker-api \
    --region=$REGION --quiet 2>/dev/null || echo "  Not found, skipping..."

echo -e "\n${GREEN}[4/5] Deleting Cloud SQL (this may take a few minutes)...${NC}"
gcloud sql instances delete tw-stocker-db \
    --quiet 2>/dev/null || echo "  Not found, skipping..."

echo -e "\n${GREEN}[5/5] Deleting Secret...${NC}"
gcloud secrets delete db-password \
    --quiet 2>/dev/null || echo "  Not found, skipping..."

echo -e "\n${GREEN}=== Cleanup Complete ===${NC}"
echo "All resources have been deleted."
