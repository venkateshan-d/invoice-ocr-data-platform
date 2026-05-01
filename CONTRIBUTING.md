# Contributing to Invoice OCR Data Platform

Thank you for your interest in contributing! This document provides guidelines for contributing to this project.

## Development Workflow

1. **Create a branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Edit notebooks in `notebooks/` directory
   - Update configurations in `config/` if needed
   - Test in dev environment

3. **Test your changes**
   ```bash
   # Deploy to dev
   databricks bundle deploy -t dev
   
   # Run the pipeline
   databricks bundle run -t dev invoice_data_pipeline
   ```

4. **Commit your changes**
   ```bash
   git add .
   git commit -m "feat: description of your changes"
   ```

5. **Push and create PR**
   ```bash
   git push origin feature/your-feature-name
   ```

## Commit Message Convention

We follow conventional commits:

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `refactor:` Code refactoring
- `test:` Adding tests
- `chore:` Maintenance tasks

## Code Style

### Python/PySpark
- Follow PEP 8 style guide
- Use meaningful variable names
- Add docstrings to functions
- Keep functions focused and small

### SQL
- Use uppercase for SQL keywords
- Indent nested queries
- Add comments for complex logic

## Testing Checklist

Before submitting a PR, ensure:

- [ ] Code runs successfully in dev environment
- [ ] No hardcoded credentials or sensitive data
- [ ] Configuration follows dev/prod pattern
- [ ] Documentation updated if needed
- [ ] Bronze → Silver → Gold flow works end-to-end

## Deployment Process

### To Development
- All changes deploy to dev first
- Test thoroughly before promoting

### To Production
- Requires approval from maintainers
- Deploy during maintenance window
- Monitor for 24 hours post-deployment

## Questions?

Open an issue or contact the maintainers.
