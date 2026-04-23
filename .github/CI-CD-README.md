# CI/CD Pipeline Documentation

## Overview
This project uses GitHub Actions for continuous integration and deployment to Databricks.

## Workflow Triggers

### Automatic Triggers
- **Push to `main`**: Deploys to production environment
- **Push to `develop`**: Deploys to development environment
- **Pull Requests**: Runs tests and code quality checks only (no deployment)

### Manual Trigger
- Go to Actions tab → CI/CD Pipeline → Run workflow
- Select environment: dev, staging, or prod

## Pipeline Stages

### 1. Code Quality Checks
- **flake8**: PEP 8 style guide enforcement
- **black**: Code formatting validation
- **pylint**: Code analysis and linting

### 2. Unit Tests
- Runs all tests in `tests/` directory
- Generates coverage reports
- Uploads coverage to Codecov

### 3. Deployment
- Uploads Python modules to Databricks workspace
- Environment-specific paths:
  - **Dev**: `/Workspace/Users/{user}/project_voltstream`
  - **Prod**: `/Workspace/Users/{user}/project_voltstream_prod`
- Runs health check post-deployment

### 4. Notification
- Sends Slack notifications (if configured)
- Includes deployment status and commit info

## Setup Instructions

### 1. GitHub Secrets Configuration

Add these secrets in your GitHub repository:
**Settings → Secrets and variables → Actions → New repository secret**

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `DATABRICKS_HOST` | Databricks workspace URL | `https://dbc-d3a31d04-60c8.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal access token | Generate in User Settings → Access Tokens |
| `DATABRICKS_USER` | Your Databricks email | `nseekeminiudo@gmail.com` |
| `HEALTH_CHECK_JOB_ID` | Health check job ID | `1100799610778120` |
| `SLACK_WEBHOOK_URL` | Slack webhook (optional) | `https://hooks.slack.com/services/...` |

### 2. Generate Databricks Token

1. Log into Databricks workspace
2. Click User Settings (top right)
3. Go to **Developer → Access tokens**
4. Click **Generate new token**
5. Set token name: `GitHub Actions CI/CD`
6. Set lifetime: 90 days (or longer)
7. Copy token immediately (can't view again)

### 3. Branch Strategy

```
main (production)
  └── develop (development)
        └── feature/* (feature branches)
```

**Workflow:**
1. Create feature branch from `develop`
2. Make changes and commit
3. Open PR to `develop` → triggers tests
4. Merge to `develop` → deploys to dev
5. Open PR to `main` → triggers tests
6. Merge to `main` → deploys to prod

## Local Development

### Run tests locally:
```bash
pytest tests/ --cov=. --cov-report=html
```

### Check code quality:
```bash
flake8 .
black --check .
pylint **/*.py
```

### Format code:
```bash
black .
autopep8 --in-place --aggressive --aggressive --recursive .
```

## Deployment Artifacts

Each deployment:
- Uploads all Python modules to workspace
- Creates timestamped backup (optional)
- Runs health checks
- Sends notification

## Troubleshooting

### Pipeline fails on code quality
- Run `black .` to format code
- Fix flake8 warnings: `flake8 . --show-source`

### Pipeline fails on tests
- Run tests locally: `pytest tests/ -v`
- Check test logs in GitHub Actions

### Deployment fails
- Verify Databricks secrets are correct
- Check token hasn't expired
- Verify workspace path permissions

### Health check fails
- Check Databricks job logs
- Verify tables have recent data
- Review health check criteria

## Monitoring

### GitHub Actions Dashboard
- View workflow runs: Repository → Actions tab
- Download artifacts: coverage reports, test results

### Databricks
- Monitor job runs: Workflows → job_name
- Check health status: Run health check job manually

## Security Best Practices

✓ Use GitHub secrets for sensitive data (never commit tokens)
✓ Rotate Databricks tokens every 90 days
✓ Use environment-specific paths (dev/prod separation)
✓ Review deployment changes in PR before merging
✓ Monitor failed deployments and rollback if needed

## Support

For issues with CI/CD pipeline:
1. Check GitHub Actions logs
2. Verify secrets are set correctly
3. Review Databricks job run logs
4. Contact team lead or DevOps
