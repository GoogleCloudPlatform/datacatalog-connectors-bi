# tableau2datacatalog hook

Handler for Tableau web hook events.

## 1. Deploy to Cloud Run

### 1.1. Set environment variables

```bash
export TABLEAU2DC_TABLEAU_SERVER=tableau_server
export TABLEAU2DC_TABLEAU_API_VERSION=tableau_api_version
export TABLEAU2DC_TABLEAU_USERNAME=tableau_username
export TABLEAU2DC_TABLEAU_PASSWORD=tableau_password
export TABLEAU2DC_DATACATALOG_PROJECT_ID=google_cloud_project_id
export TABLEAU2DC_DATACATALOG_LOCATION_ID=us-google_cloud_location_id
export TABLEAU2DC_TABLEAU_SITE=tableau-site
export TABLEAU2DC_API_KEY=your-made-up-api-key

```

### 1.2. Run deploy

```bash
./deploy.sh
```
