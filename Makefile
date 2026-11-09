# Prefer the project Python 3.15 venv; never fall back to a different minor.
PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3.15)

.PHONY: lint test

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

test:
	$(PYTHON) -m pytest -q

TERRAFORM ?= terraform
.PHONY: infra-check infra-smoke

# Run `terraform -chdir=infra init -backend=false` once before this check.
infra-check:
	$(TERRAFORM) -chdir=infra fmt -check -recursive
	$(TERRAFORM) -chdir=infra validate
	$(TERRAFORM) -chdir=infra test

infra-smoke:
	TERRAFORM="$(TERRAFORM)" PYTHON="$(PYTHON)" bash infra/smoke.sh
