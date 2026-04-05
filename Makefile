PYTHON ?= python3

.PHONY: validate test

validate:
	$(PYTHON) scripts/validate_repo.py

test:
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py'

