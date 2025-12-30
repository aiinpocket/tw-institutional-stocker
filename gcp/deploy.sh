#!/bin/bash
# GCP Deployment Script for Taiwan Stock Tracker
# Usage: ./gcp/deploy.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Taiwan Stock Tracker GCP Deployment ===${NC}"

# Check if required environment variables are set
if [ -z "$PROJECT_ID" ]; then
    echo -e "${YELLOW}PROJECT_ID not set. Please enter your GCP Project ID:${NC}"
    read PROJECT_ID
fi

if [ -z "$REGION" ]; then
    REGION="asia-east1"  # Taiwan region
    echo -e "${YELLOW}Using default region: ${REGION}${NC}"
fi

if [ -z "$DB_PASSWORD" ]; then
    echo -e "${YELLOW}DB_PASSWORD not set. Please enter a secure database password:${NC}"
    read -s DB_PASSWORD
fi

if [ -z "$OPENAI_API_KEY" ]; then
    echo -e "\n${YELLOW}OPENAI_API_KEY not set. Please enter your OpenAI API key (for AI industry classification):${NC}"
    read -s OPENAI_API_KEY
fi

# Export variables
export PROJECT_ID
export REGION
export DB_PASSWORD
export OPENAI_API_KEY

INSTANCE_NAME="tw-stocker-db"
DB_NAME="tw_stocker"
API_SERVICE="tw-stocker-api"
ETL_JOB="tw-stocker-etl"

echo -e "\n${GREEN}Configuration:${NC}"
echo "  Project ID: $PROJECT_ID"
echo "  Region: $REGION"
echo "  DB Instance: $INSTANCE_NAME"

# Set project
echo -e "\n${GREEN}[1/7] Setting GCP project...${NC}"
gcloud config set project $PROJECT_ID

# Enable APIs
echo -e "\n${GREEN}[2/7] Enabling required APIs...${NC}"
gcloud services enable \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    sqladmin.googleapis.com \
    cloudscheduler.googleapis.com \
    secretmanager.googleapis.com \
    --quiet

# Create Cloud SQL instance
echo -e "\n${GREEN}[3/7] Creating Cloud SQL instance...${NC}"
if gcloud sql instances describe $INSTANCE_NAME --quiet 2>/dev/null; then
    echo "  Cloud SQL instance already exists, skipping..."
else
    gcloud sql instances create $INSTANCE_NAME \
        --database-version=POSTGRES_15 \
        --tier=db-f1-micro \
        --region=$REGION \
        --storage-size=10GB \
        --storage-type=HDD \
        --backup-start-time=04:00 \
        --maintenance-window-day=SUN \
        --maintenance-window-hour=05 \
        --quiet

    # Wait for instance to be ready
    echo "  Waiting for instance to be ready..."
    sleep 30

    # Set password
    gcloud sql users set-password postgres \
        --instance=$INSTANCE_NAME \
        --password="$DB_PASSWORD" \
        --quiet

    # Create database
    gcloud sql databases create $DB_NAME \
        --instance=$INSTANCE_NAME \
        --quiet
fi

# Create secrets
echo -e "\n${GREEN}[4/7] Creating secrets...${NC}"

# DB Password secret
if gcloud secrets describe db-password --quiet 2>/dev/null; then
    echo "  db-password secret already exists, updating..."
    echo -n "$DB_PASSWORD" | gcloud secrets versions add db-password --data-file=-
else
    echo -n "$DB_PASSWORD" | gcloud secrets create db-password --data-file=-
fi

# OpenAI API Key secret
if gcloud secrets describe openai-api-key --quiet 2>/dev/null; then
    echo "  openai-api-key secret already exists, updating..."
    echo -n "$OPENAI_API_KEY" | gcloud secrets versions add openai-api-key --data-file=-
else
    echo -n "$OPENAI_API_KEY" | gcloud secrets create openai-api-key --data-file=-
fi

# Get project number for service account
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")

# Grant Cloud Run service account access to secrets
gcloud secrets add-iam-policy-binding db-password \
    --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor" \
    --quiet 2>/dev/null || true

gcloud secrets add-iam-policy-binding openai-api-key \
    --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor" \
    --quiet 2>/dev/null || true

# Build and deploy API
echo -e "\n${GREEN}[5/7] Building and deploying Cloud Run API...${NC}"
gcloud run deploy $API_SERVICE \
    --source=. \
    --dockerfile=gcp/Dockerfile.api \
    --region=$REGION \
    --platform=managed \
    --allow-unauthenticated \
    --memory=512Mi \
    --cpu=1 \
    --min-instances=0 \
    --max-instances=2 \
    --timeout=60 \
    --add-cloudsql-instances=$PROJECT_ID:$REGION:$INSTANCE_NAME \
    --set-env-vars="DB_HOST=/cloudsql/$PROJECT_ID:$REGION:$INSTANCE_NAME" \
    --set-env-vars="DB_NAME=$DB_NAME" \
    --set-env-vars="DB_USER=postgres" \
    --set-secrets="DB_PASSWORD=db-password:latest,OPENAI_API_KEY=openai-api-key:latest" \
    --quiet

# Get API URL
API_URL=$(gcloud run services describe $API_SERVICE --region=$REGION --format="value(status.url)")
echo -e "  ${GREEN}API deployed at: ${API_URL}${NC}"

# Build and create ETL job
echo -e "\n${GREEN}[6/7] Building and creating Cloud Run ETL Job...${NC}"
gcloud run jobs deploy $ETL_JOB \
    --source=. \
    --dockerfile=gcp/Dockerfile.etl \
    --region=$REGION \
    --memory=1Gi \
    --cpu=1 \
    --task-timeout=30m \
    --max-retries=1 \
    --add-cloudsql-instances=$PROJECT_ID:$REGION:$INSTANCE_NAME \
    --set-env-vars="DB_HOST=/cloudsql/$PROJECT_ID:$REGION:$INSTANCE_NAME" \
    --set-env-vars="DB_NAME=$DB_NAME" \
    --set-env-vars="DB_USER=postgres" \
    --set-secrets="DB_PASSWORD=db-password:latest,OPENAI_API_KEY=openai-api-key:latest" \
    --quiet

# Create Cloud Scheduler job
echo -e "\n${GREEN}[7/7] Creating Cloud Scheduler...${NC}"
if gcloud scheduler jobs describe tw-stocker-etl-schedule --location=$REGION --quiet 2>/dev/null; then
    echo "  Scheduler already exists, updating..."
    gcloud scheduler jobs update http tw-stocker-etl-schedule \
        --location=$REGION \
        --schedule="30 21 * * 1-5" \
        --time-zone="Asia/Taipei" \
        --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${ETL_JOB}:run" \
        --http-method=POST \
        --oauth-service-account-email="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
        --quiet
else
    gcloud scheduler jobs create http tw-stocker-etl-schedule \
        --location=$REGION \
        --schedule="30 21 * * 1-5" \
        --time-zone="Asia/Taipei" \
        --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${ETL_JOB}:run" \
        --http-method=POST \
        --oauth-service-account-email="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
        --quiet
fi

echo -e "\n${GREEN}=== Deployment Complete! ===${NC}"
echo -e "\nResources created:"
echo -e "  - Cloud SQL: ${INSTANCE_NAME}"
echo -e "  - Cloud Run API: ${API_URL}"
echo -e "  - Cloud Run Job: ${ETL_JOB}"
echo -e "  - Scheduler: tw-stocker-etl-schedule (21:30 Taipei Time, Mon-Fri)"

echo -e "\n${YELLOW}Next steps:${NC}"
echo "1. Run initial ETL to populate database:"
echo "   gcloud run jobs execute $ETL_JOB --region=$REGION --wait"
echo ""
echo "2. Check API health:"
echo "   curl ${API_URL}/health"
echo ""
echo "3. View API docs:"
echo "   ${API_URL}/docs"
