# Quick Start Guide - Champion/Challenger Promotion System

## 5-Minute Setup

### 1. Configure Environment

Add to `.env`:
```bash
EVAL_BRANCH=eval
PROD_BRANCH=prod
PROMOTION_THRESHOLD=0.05         # 5% improvement required
ALLOW_STATIC_ONLY_PROMOTION=false # Require runtime metrics
ENABLE_AUTO_PROMOTION=false       # Manual promotion
```

### 2. Create a DAG in the Builder

Use the web app to generate a new DAG. This creates a version with `promotion_status: draft`.

### 3. Publish to GitHub

Click "Publish" in the builder. This:
- Creates a feature branch
- Opens a PR to `main`
- Sets `github_pr_url` and `github_commit_sha` in metadata

### 4. Merge PR

Once CI passes (dag-ci.yml validates it), merge the PR to main.

The promote-to-eval.yml workflow automatically:
- Merges main → eval
- Updates `promotion_status: eval`

### 5. Score the DAG

In admin dashboard or via API:
```bash
curl -X POST http://localhost:5000/api/workflows/{pipeline_id}/versions/{version_id}/score
```

This updates metadata with score (0-100) and score_breakdown.

### 6. Compare with Champion

Check current champion and all challengers:
```bash
# Get current champion
curl http://localhost:5000/api/workflows/{pipeline_id}/champion

# Get all challengers sorted by score
curl http://localhost:5000/api/workflows/{pipeline_id}/challengers
```

### 7. Promote if Score is Better

If challenger score ≥ (champion score × 1.05):
```bash
curl -X POST http://localhost:5000/api/workflows/{pipeline_id}/versions/{version_id}/promote
```

This workflow (promote-to-prod.yml):
- Copies DAG from eval → prod
- Archives previous champion
- Updates `promotion_status: champion`

### 8. (Optional) Rollback

If issues found:
```bash
curl -X POST http://localhost:5000/api/workflows/{pipeline_id}/versions/{previous_version_id}/rollback
```

---

## Directory Structure

```
APACHE-AIRFLOW/
├── web_app/
│   ├── services/
│   │   ├── scoring/
│   │   │   └── dag_score.py           ← Scoring engine
│   │   ├── promotion/
│   │   │   └── __init__.py            ← Promotion logic
│   │   └── pipeline_versions.py       ← Version metadata (modified)
│   ├── test/
│   │   ├── test_scoring.py            ← Scoring tests
│   │   └── test_promotion.py          ← Promotion tests
│   ├── app.py                         ← API endpoints (modified)
│   └── config.py                      ← Configuration (modified)
├── scripts/
│   ├── validation/
│   │   └── validate_dag_metadata.py   ← CI validation
│   ├── promotion/
│   │   ├── mark_eval_versions.py      ← Mark as eval
│   │   ├── mark_champion_version.py   ← Promote & archive
│   │   └── copy_dag_to_prod.py        ← Copy to prod
│   └── evaluation/
│       └── evaluate_dag.py            ← Score collection
├── .github/workflows/
│   ├── dag-ci.yml                     ← PR validation
│   ├── promote-to-eval.yml            ← Move to eval
│   ├── evaluate-dag.yml               ← Score DAG
│   └── promote-to-prod.yml            ← Move to prod
├── PROMOTION_SYSTEM.md                ← Full documentation
└── IMPLEMENTATION_SUMMARY.md          ← Detailed overview
```

---

## Scoring Formula

Each DAG gets a 0-100 score based on:

| Category | Weight | What It Checks |
|----------|--------|--|
| **Reliability** | 25% | Owner defined, retries configured, error handling |
| **Runtime Efficiency** | 20% | Fast imports, optimal task count |
| **Retry Behavior** | 15% | Retry configuration and tuning |
| **Scheduler Behavior** | 15% | Owner, tags, SLA awareness |
| **Complexity** | 10% | DAG graph complexity |
| **Maintainability** | 10% | Code structure and comments |
| **Resource Efficiency** | 5% | Resource usage optimization |

**Critical Failures** (Block Promotion):
- DAG doesn't parse (syntax error)
- DAG missing required `dag_id`
- DAG definition not found

---

## Promotion Threshold

By default, a challenger must score at least **5% higher** than the champion:

```
required_score = champion_score × 1.05
```

For example:
- Champion: 85.0
- Threshold: 85.0 × 1.05 = 89.25
- Challenger must score ≥ 89.25 to promote

Customize by setting `PROMOTION_THRESHOLD=0.10` for 10%, etc.

---

## Metadata Example

After scoring, a version looks like:

```json
{
  "pipeline_id": "customer_ingestion",
  "version_id": "v3",
  "dag_id": "customer_ingestion__v3",
  "promotion_status": "challenger",
  "score": 87.5,
  "score_type": "static_only",
  "score_breakdown": {
    "reliability": 23.5,
    "runtime_efficiency": 17.1,
    "retry_behavior": 13.8,
    "scheduler_behavior": 12.0,
    "complexity": 8.2,
    "maintainability": 7.6,
    "resource_efficiency": 5.3
  },
  "promoted_at": null,
  "promoted_by": null,
  "github_pr_url": "https://github.com/...",
  "github_commit_sha": "abc123..."
}
```

---

## API Endpoints Summary

```
POST   /api/workflows/{id}/versions/{id}/score           Score a DAG
GET    /api/workflows/{id}/champion                      Get champion
GET    /api/workflows/{id}/challengers                   List challengers
POST   /api/workflows/{id}/versions/{id}/promote         Promote to champion
POST   /api/workflows/{id}/versions/{id}/rollback        Restore previous
```

---

## Troubleshooting

### "Challenger score below threshold"
**Solution**: Improve the DAG by adding:
- `owner` field in DAG definition
- `retries` configuration (2-3 recommended)
- `tags` for categorization
- `max_active_tasks` for resource control

### "DAG failed parsing"
**Solution**: Check syntax:
```bash
python -m py_compile dags/my_dag.py
```

### "Can't find champion"
**Solution**: Promote a version first - a new pipeline has no champion until the first promotion.

### "Rollback failing"
**Solution**: Make sure the version you're rolling back to was previously a champion (has `archived_at` timestamp).

---

## Common Workflows

### Publish → Promote (Happy Path)
1. Create DAG → Publish → Merge PR
2. Admin scores in dashboard → Promotes automatically (if enabled)
3. DAG goes live to prod branch

### Compare Multiple Candidates
1. Publish multiple DAGs (all go to eval)
2. Score each one
3. Compare scores in dashboard
4. Promote the highest-scoring one

### Test & Rollback
1. Promote DAG to production
2. Monitor in Airflow
3. If issues: Rollback to previous champion
4. Create new version with fixes

---

## Next: Go Deeper

- Full guide: [PROMOTION_SYSTEM.md](PROMOTION_SYSTEM.md)
- Implementation details: [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)
- Source code: `web_app/services/scoring/dag_score.py`, `web_app/services/promotion/__init__.py`
- Tests: `web_app/test/test_scoring.py`, `web_app/test/test_promotion.py`

