# Git-Based Deployment Guide

This project is configured for **Git-based deployment** using Databricks Asset Bundles (DAB).

## 🏗️ Project Structure

```
invoice-ocr-data-platform/
├── databricks.yml              # Main bundle configuration
├── resources/
│   └── invoice_pipeline.job.yml   # Job definition (git-based)
├── config/
│   ├── dev.yml                # Dev environment overrides
│   └── prod.yml               # Prod environment overrides
├── notebooks/
│   ├── 00_setup_infrastructure.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   └── 03_gold_analytics.py
└── workflows/
    └── (deprecated - now using resources/)
```

## 🚀 Setup & Deployment

### Step 1: Push to GitHub

```bash
# Initialize git repository
git init
git add .
git commit -m "Initial commit: Invoice OCR Data Platform"

# Add your GitHub remote
git remote add origin https://github.com/YOUR_USERNAME/invoice-ocr-data-platform.git
git push -u origin main
```

### Step 2: Update Git URL

Edit `resources/invoice_pipeline.job.yml` and update the `git_url`:

```yaml
git_source:
  git_url: https://github.com/YOUR_USERNAME/invoice-ocr-data-platform
  git_provider: github
  git_branch: main
```

### Step 3: Deploy from Local

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Validate configuration
databricks bundle validate -t dev

# Deploy to dev
databricks bundle deploy -t dev
```

### Step 4: Run the Pipeline

```bash
# Trigger the job
databricks bundle run -t dev invoice_data_pipeline_job
```

## 🔄 How Git-Based Deployment Works

**Key Differences from Workspace-Based:**

1. **Notebooks from Git**: Tasks pull notebooks directly from your GitHub repository
2. **No Workspace Sync**: Notebooks aren't uploaded to workspace during deployment
3. **Version Control**: Each job run uses the specified git branch/commit
4. **Reusable**: Same code works across multiple workspaces

**What Gets Deployed:**
- ✅ Job definitions (structure, schedule, clusters)
- ✅ Git repository reference
- ❌ Notebook files (pulled from Git at runtime)

## 📝 Configuration Files

### databricks.yml (Main Config)
- Bundle name and variables
- Target environments (dev/prod)
- Includes resources and config files

### resources/invoice_pipeline.job.yml
- Job definition with git_source
- Tasks pointing to notebooks via relative paths
- Cluster configuration
- Schedule and notifications

### config/dev.yml & config/prod.yml
- Environment-specific settings
- Data quality thresholds
- Resource overrides (cluster size, retries)

## 🌿 Working with Branches

### Development Workflow

```bash
# Create feature branch
git checkout -b feature/new-transformation

# Make changes to notebooks
# Commit and push
git add .
git commit -m "feat: add new transformation logic"
git push origin feature/new-transformation

# Deploy from feature branch (optional)
# Edit resources/invoice_pipeline.job.yml to point to your branch:
# git_branch: feature/new-transformation

# Then deploy
databricks bundle deploy -t dev
```

### Promoting to Production

```bash
# Merge to main
git checkout main
git merge feature/new-transformation
git push origin main

# Deploy to prod (uses main branch)
databricks bundle deploy -t prod
```

## 🔧 Local Development

### Testing Notebooks Locally

You can still edit and test notebooks in Databricks workspace:

1. Make changes in workspace notebooks
2. Download or export updated notebooks
3. Replace files in `notebooks/` directory
4. Commit and push to Git
5. Redeploy bundle

### Sync Workspace to Git

```bash
# Export notebook from workspace
databricks workspace export /Users/your@email.com/notebook.py ./notebooks/notebook.py

# Commit changes
git add notebooks/
git commit -m "update: improved transformation logic"
git push origin main
```

## 🐛 Troubleshooting

### "Git source not found"
**Solution**: Verify git_url in `resources/invoice_pipeline.job.yml` is correct and repository is public (or add git credentials)

### "Notebook not found"
**Solution**: Check that notebook paths are relative to repo root: `notebooks/01_bronze_ingestion` (no leading slash, no .py extension)

### "Permission denied"
**Solution**: If using private repo, configure Git credentials in Databricks:
- Go to User Settings → Git Integration
- Add GitHub personal access token

### Changes not reflected in job run
**Solution**: 
1. Commit and push changes to Git
2. Check that git_branch in job config matches your branch
3. Redeploy bundle: `databricks bundle deploy -t dev`

## 📊 Monitoring

### View Job Runs

```bash
# List recent runs
databricks jobs list-runs --limit 5

# Get run details
databricks jobs get-run --run-id <run-id>
```

### Check Which Git Commit Was Used

In Databricks UI:
1. Go to Workflows → Jobs
2. Click on a run
3. View "Git commit" in run details

## 🔐 Private Repository Setup

For private repositories:

### Option 1: Personal Access Token

1. Generate GitHub PAT with `repo` scope
2. In Databricks: User Settings → Git Integration
3. Add credentials

### Option 2: SSH Key

```yaml
# In resources/invoice_pipeline.job.yml
git_source:
  git_url: git@github.com:YOUR_USERNAME/invoice-ocr-data-platform.git
  git_provider: github
  git_branch: main
```

Configure SSH key in Databricks User Settings.

## ✅ Deployment Checklist

- [ ] Code pushed to GitHub
- [ ] git_url updated in `resources/invoice_pipeline.job.yml`
- [ ] Git credentials configured (if private repo)
- [ ] Bundle validated: `databricks bundle validate -t dev`
- [ ] Unity Catalog infrastructure setup (run `00_setup_infrastructure.py`)
- [ ] Data uploaded to volumes
- [ ] Bundle deployed: `databricks bundle deploy -t dev`
- [ ] Test job run: `databricks bundle run -t dev invoice_data_pipeline_job`

## 📖 Additional Resources

- [Databricks Asset Bundles Docs](https://docs.databricks.com/dev-tools/bundles/)
- [Git Integration Guide](https://docs.databricks.com/repos/)
- [Job Configuration Reference](https://docs.databricks.com/workflows/jobs/jobs-api.html)

---

**Questions?** Check the main [README.md](README.md) or open an issue on GitHub.
