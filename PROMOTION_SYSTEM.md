# DataOps Champion/Challenger Promotion System

This system enables automated scoring, evaluation, and promotion of Airflow DAG versions through a champion/challenger framework.

## Overview

- **Champion**: The currently deployed production DAG version for a given logical pipeline
- **Challenger**: A new DAG version evaluated against the champion before promotion
- **Draft**: A newly created version not yet submitted
- **Submitted**: A version submitted as a candidate for evaluation
- **Eval**: A version in the evaluation branch being tested
- **Archived**: A previous champion that has been replaced

## Configuration

Add to your `.env` or environment:

```bash
# Promotion branches and thresholds
EVAL_BRANCH=eval                          # Branch for candidates under evaluation
PROD_BRANCH=prod                          # Branch for champion DAGs only
PROMOTION_THRESHOLD=0.05                  # Minimum 5% improvement required to promote
ALLOW_STATIC_ONLY_PROMOTION=false         # Only promote after runtime evaluation
ENABLE_AUTO_PROMOTION=false               # Require manual promotion trigger
AIRFLOW_DAGS_PATH=dags                   # Path to DAGs in repo
```

## Workflow

### 1. Create a DAG Version

Generate a new DAG version in the builder. This automatically creates a version record with:
- `promotion_status: draft`
- `score: null`
- `base_pipeline_id`: The logical pipeline identifier
- `dag_id`: The Airflow DAG ID (e.g., `customer_ingestion__v3`)

### 2. Publish to GitHub (Create PR)

Use the builder UI to publish a DAG version. This:
- Creates a feature branch
- Commits the DAG file
- Opens a PR to `main`
- Updates metadata with `github_pr_url` and `github_commit_sha`

### 3. CI/CD Validation

The `.github/workflows/dag-ci.yml` workflow runs on PR and validates:
- Python syntax
- DAG import capability
- Linting (non-blocking)
- Gitleaks scan for secrets
- Metadata completeness

If validation passes, the PR can be merged.

### 4. Promote to Eval Branch

When merged to `main`, the `.github/workflows/promote-to-eval.yml` workflow:
- Merges `main` into the `eval` branch
- Marks the version as `promotion_status: eval`
- Makes the DAG available in the eval Airflow instance

You can also manually trigger this via GitHub workflow dispatch.

### 5. Score the DAG

Score the DAG using the API or admin dashboard:

```bash
POST /api/workflows/{pipeline_id}/versions/{version_id}/score
```

This performs static analysis scoring and updates the version metadata with:
- `score`: 0-100 float
- `score_type`: "static_only"
- `score_breakdown`: Category scores

**Scoring Categories** (max 100):
- Reliability (25%): Has owner, retries, proper error handling
- Runtime Efficiency (20%): Parse time, task count optimization
- Retry Behavior (15%): Retry configuration and handling
- Scheduler Behavior (15%): Owner tags, schedule awareness
- Complexity (10%): DAG graph complexity
- Maintainability (10%): Code structure and clarity
- Resource Efficiency (5%): Resource usage optimization

### 6. Compare with Champion

Retrieve the champion and compare scores:

```bash
GET /api/workflows/{pipeline_id}/champion
GET /api/workflows/{pipeline_id}/challengers
POST /api/workflows/{pipeline_id}/versions/{version_id}/promote
```

Promotion requires:
- Challenger score ≥ (Champion score × 1.05) by default
- No critical failures
- (Optional) Runtime evaluation if `ALLOW_STATIC_ONLY_PROMOTION=false`

### 7. Promote to Prod

When ready, trigger promotion:

```bash
POST /api/workflows/{pipeline_id}/versions/{version_id}/promote
```

This workflow (`.github/workflows/promote-to-prod.yml`):
- Copies the DAG to the `prod` branch
- Archives the previous champion
- Updates metadata with `promotion_status: champion`
- Commits to `prod` branch

## Admin Dashboard Features

The admin dashboard shows:

- **Current Champion**: Score, creation date, promotion date, promoter
- **Challenger Queue**: All candidate versions sorted by score
- **Score Breakdown**: Detailed scoring for each category
- **Promotion History**: Timeline of promotions and rollovers
- **Rollback Button**: Restore a previous champion into production

## Rollback

If a promoted version has issues, rollback to the previous champion:

```bash
POST /api/workflows/{pipeline_id}/versions/{previous_version_id}/rollback
```

This:
- Archives the current champion
- Restores the previous version to `promotion_status: champion`
- Commits the change to `prod` branch
- Updates metadata with rollback details

## Metadata Fields

Each DAG version now includes promotion metadata:

```json
{
  "pipeline_id": "customer_ingestion",
  "version_id": "v3",
  "dag_id": "customer_ingestion__v3",
  "base_pipeline_id": "customer_ingestion",
  "promotion_status": "champion",
  "score": 87.5,
  "score_type": "eval_complete",
  "score_breakdown": {
    "reliability": 23.5,
    "runtime_efficiency": 17.1,
    "retry_behavior": 13.8,
    "scheduler_behavior": 12.0,
    "complexity": 8.2,
    "maintainability": 7.6,
    "resource_efficiency": 5.3
  },
  "last_eval_run_id": "run_abc123",
  "promoted_at": "2026-05-03T14:30:00Z",
  "promoted_by": "alice@example.com",
  "github_pr_url": "https://github.com/.../pull/42",
  "github_commit_sha": "abc123def456",
  "critical_failures": [],
  "archived_at": null
}
```

## Testing

Run the test suites:

```bash
pytest web_app/test/test_scoring.py -v
pytest web_app/test/test_promotion.py -v
```

Test coverage includes:
- Static scoring with various DAG configurations
- Comparison logic and thresholds
- Champion/challenger lifecycle
- Promotion and rollback operations

## GitHub Actions Workflows

### dag-ci.yml
Triggered on PR to `main` when DAG files change. Validates syntax, imports, linting, and metadata.

### promote-to-eval.yml
Triggered on merge to `main` or manually via dispatch. Moves approved DAGs to eval branch.

### evaluate-dag.yml
Manually triggered to score a DAG and collect runtime metrics. Supports static-only, single-run, and multi-run evaluation.

### promote-to-prod.yml
Manually triggered to promote a scored DAG to prod branch. Archives previous champion.

## Safety & Idempotency

- All branch operations are idempotent; running multiple times is safe
- Previous champions are never deleted, only archived
- Rollback is always possible as long as previous version metadata exists
- All promotions are logged with timestamp and actor

## Best Practices

1. **Always score before promoting**: Use static scoring as a minimum gate
2. **Monitor promotion threshold**: Adjust `PROMOTION_THRESHOLD` based on your risk tolerance
3. **Keep eval and prod branches clean**: Only champion and eval DAGs belong in those branches
4. **Automate with webhooks**: Integrate promotion API with external orchestration systems
5. **Archive rather than delete**: Maintain promotion history for auditing
6. **Test rollback procedures**: Ensure your team is comfortable with rollback workflows

## Troubleshooting

### DAG fails validation
- Check Python syntax with `python -m py_compile mydag.py`
- Ensure DAG imports are available in Airflow environment
- Add `owner` and `tags` for best practices score

### Promotion blocked by threshold
- Increase `PROMOTION_THRESHOLD` if needed
- Improve challenger score by adding retries, timeouts, owner info
- Use `ALLOW_STATIC_ONLY_PROMOTION=true` to allow static scores only

### Rollback needed after promotion
- Use the rollback endpoint to restore previous champion
- Investigate what went wrong in the promoted DAG
- Create a new challenger version with fixes

## API Reference

### Scoring
- `POST /api/workflows/{workflow_id}/versions/{version_id}/score`
  - Score a DAG using static analysis

### Querying
- `GET /api/workflows/{workflow_id}/champion`
  - Get current champion DAG
- `GET /api/workflows/{workflow_id}/challengers`
  - List challenger DAGs sorted by score

### Promotion
- `POST /api/workflows/{workflow_id}/versions/{version_id}/promote`
  - Promote a challenger to champion (if threshold met)

### Rollback
- `POST /api/workflows/{workflow_id}/versions/{version_id}/rollback`
  - Restore previous champion to prod

## Future Enhancements

- Runtime metrics collection from Airflow API
- Multi-run statistical evaluation
- Custom scoring policies per pipeline
- SLA tracking and alerts
- Integration with external monitoring systems
