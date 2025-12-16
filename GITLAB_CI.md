# GitLab CI/CD

This repo includes a GitLab pipeline in `.gitlab-ci.yml` that mirrors the existing GitHub Actions workflows:

- `lint:*`: flake8 (required), plus black/isort/mypy (allowed to fail, like `continue-on-error` in GitHub)
- `tests`: pytest + coverage across a Python version matrix, with Postgres + Mongo services
- `security:bandit`: lightweight security scan (informational; exits zero)

## Runner requirements

The `tests` job uses GitLab CI `services` (Postgres + Mongo), so use a runner with a Docker (or Kubernetes) executor that supports service containers.

### Self-hosted runner (Docker executor)

This repo also includes a ready-to-run Docker Runner template in `gitlab-runner/docker-compose.yml`.

At a minimum, the runner host needs outbound access to pull these images:

- `python:<version>` (job image)
- `postgres:15` (service)
- `mongo:7` (service)

Example registration (adjust URL/token/tags for your GitLab instance):

```bash
gitlab-runner register \
  --executor docker \
  --docker-image python:3.11 \
  --tag-list docker
```

## CI variables

- `CODECOV_TOKEN` (optional): uploads `coverage.xml` from the Python 3.11 test job to Codecov; upload failures do not fail the pipeline.

## Scheduling

GitHub's CodeQL workflow runs weekly via cron (`30 1 * * 0`). To match that behavior in GitLab, create a pipeline schedule with the same cron expression in your project's CI/CD schedules (only `security:bandit` runs on scheduled pipelines).
