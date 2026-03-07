PYTHON ?= python

.PHONY: install bootstrap ui test daily evaluation prune

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e .[dev]

bootstrap:
	$(PYTHON) scripts/bootstrap.py

ui:
	$(PYTHON) -m streamlit run app/ui/Home.py

test:
	pytest

daily:
	$(PYTHON) scripts/run_daily_pipeline.py

evaluation:
	$(PYTHON) scripts/run_evaluation.py

prune:
	$(PYTHON) scripts/prune_storage.py
