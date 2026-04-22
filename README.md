# Airflow2 Orchestration Platform

This repository implements a version-aware DAG CI/CD scaffold.

## Core design

- One repo, one Airflow instance.
- Airflow scans deployable DAG views in `dags/`.
- Historical DAG versions live in `pipelines/<logical_dag_id>/versions/`.
- Scoring and promotion logic live outside DAG runtime code.
- Promotion is allowed only when candidate is accepted by policy and comparison rules.
- Rollback is metadata-driven via pipeline manifest fields.

## Key directories

- `pipelines/`: source-of-truth history, manifest, runtime config, evaluation results.
- `dags/eval`: candidate DAGs currently under evaluation.
- `dags/prod`: champion DAGs currently in production.
- `scoring/`: policies, profiles, thresholds, and decision logic.
- `scripts/`: validation, deployment, evaluation, and connection lifecycle tooling.

## Migration note

Existing DAGs under `dags/` were not removed. New `eval/` and `prod/` views were added for controlled rollout.

## Dynamic Eval and Promotion

Use these commands after publishing candidate DAGs to `dags/eval`:

```bash
python scripts/deployments/publish_eval_dag.py --root . --pipeline-id <pipeline_id>
python scripts/eval/collect_eval_metrics.py --root . --airflow-url http://localhost:8081 --airflow-api-prefix /api/v2 --airflow-username <user> --airflow-password <pass>
python scripts/deployments/promote_candidate.py --root .
```

What this does:

- waits for candidate eval DAG run completion in Airflow
- fetches runtime/task metrics through Airflow API
- scores candidate vs champion using policy by change type
- promotes only the best accepted candidate version
