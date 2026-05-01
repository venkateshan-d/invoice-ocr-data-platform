# What Changed - Git-Based Deployment

## 🔄 Key Changes Made

### 1. databricks.yml (Simplified)
**Before**: 147 lines with inline job definitions
**After**: 40 lines, clean configuration only

**Changes**:
- ❌ Removed inline job definitions
- ✅ Added `include` directives for resources and configs
- ✅ Cleaner variable structure
- ✅ Target-specific config loading via `${bundle.target}`

### 2. Job Definition (New Location)
**Before**: Embedded in databricks.yml
**After**: Separate file at `resources/invoice_pipeline.job.yml`

**Key Changes**:
```yaml
# Added git_source
git_source:
  git_url: https://github.com/YOUR_USERNAME/invoice-ocr-data-platform
  git_provider: github
  git_branch: main

# Changed notebook paths from absolute to relative
# Before:
notebook_path: ./notebooks/01_bronze_ingestion.py

# After:
notebook_path: notebooks/01_bronze_ingestion
source: GIT
```

### 3. Configuration Files (Split by Environment)
**Before**: One large config file per environment
**After**: Clean, focused config files

**config/dev.yml**:
- Variables only
- Dev-specific job overrides (smaller cluster, fewer retries)

**config/prod.yml**:
- Production thresholds
- Larger cluster configuration
- Additional alerting

### 4. Directory Structure
**Before**:
```
invoice-ocr-data-platform/
├── databricks.yml (everything inline)
├── config/
│   ├── dev.yml
│   └── prod.yml
└── notebooks/
```

**After**:
```
invoice-ocr-data-platform/
├── databricks.yml (clean, minimal)
├── resources/
│   └── invoice_pipeline.job.yml (git-based)
├── config/
│   ├── dev.yml (environment variables)
│   └── prod.yml (environment variables)
└── notebooks/
```

## ✨ Benefits of New Structure

### 1. **Git-Based Deployment**
- Notebooks pulled from Git at runtime
- No workspace sync needed
- Version controlled execution

### 2. **Reusable Across Workspaces**
- No absolute paths
- Works in any Databricks workspace
- Easy to share and collaborate

### 3. **Better Separation of Concerns**
- Main config (databricks.yml) is clean
- Job definitions in resources/
- Environment settings in config/
- Code in notebooks/

### 4. **Easier to Maintain**
- Change notebook code → push to Git → automatic update
- Modify job structure → edit resources/invoice_pipeline.job.yml
- Adjust environment settings → edit config/dev.yml or config/prod.yml

### 5. **CI/CD Ready**
- Can deploy from CI/CD pipeline
- No manual workspace uploads
- Automated testing possible

## 📝 Migration Path

### For Existing Projects:

1. **Create resources/ directory**
   ```bash
   mkdir resources
   ```

2. **Move job definitions**
   - Extract job config from databricks.yml
   - Create resources/your_job.job.yml
   - Add git_source block
   - Update notebook paths (remove .py extension, make relative)

3. **Update databricks.yml**
   - Remove inline job definitions
   - Add include directives:
     ```yaml
     include:
       - resources/*.yml
       - config/${bundle.target}.yml
     ```

4. **Update notebook paths in job config**
   ```yaml
   # Change from:
   notebook_path: ./notebooks/01_bronze_ingestion.py
   
   # To:
   notebook_path: notebooks/01_bronze_ingestion
   source: GIT
   ```

5. **Add git_source**
   ```yaml
   git_source:
     git_url: https://github.com/YOUR_USERNAME/your-repo
     git_provider: github
     git_branch: main
   ```

## 🐛 Common Issues & Solutions

### Issue 1: "Resource not found"
**Cause**: Missing include directive
**Solution**: Add to databricks.yml:
```yaml
include:
  - resources/*.yml
```

### Issue 2: "Notebook not found"
**Cause**: Wrong notebook path format
**Solution**: Use relative path without extension:
- ✅ `notebooks/01_bronze_ingestion`
- ❌ `./notebooks/01_bronze_ingestion.py`
- ❌ `/Workspace/Users/.../01_bronze_ingestion`

### Issue 3: "Git source error"
**Cause**: Repository not accessible or wrong URL
**Solution**: 
- Verify git_url is correct
- Make repo public OR configure git credentials
- Check branch exists

### Issue 4: "Variable not found"
**Cause**: Config not being included
**Solution**: Verify include path:
```yaml
include:
  - config/${bundle.target}.yml
```

## 📚 Reference Files

| File | Purpose | Update Frequency |
|------|---------|-----------------|
| databricks.yml | Main bundle config | Rarely |
| resources/\*.job.yml | Job definitions | When changing pipeline |
| config/dev.yml | Dev settings | When tuning dev environment |
| config/prod.yml | Prod settings | When updating SLAs |
| notebooks/\*.py | Pipeline code | Frequently |

## 🎯 Quick Reference

### Deploy Commands
```bash
# Validate
databricks bundle validate -t dev

# Deploy
databricks bundle deploy -t dev

# Run
databricks bundle run -t dev invoice_data_pipeline_job
```

### File Locations
- **Job definitions**: `resources/*.job.yml`
- **Pipeline definitions**: `resources/*.pipeline.yml` (if using DLT)
- **Environment configs**: `config/<target>.yml`
- **Notebooks**: `notebooks/*.py`

### Git Integration
- **Source**: Set in job config via `git_source`
- **Branch**: Configurable per job
- **Auth**: Configure in Databricks User Settings → Git Integration

---

**Questions?** See [GIT_DEPLOYMENT.md](GIT_DEPLOYMENT.md) for detailed guide.
