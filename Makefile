.PHONY: ci lint typecheck test format

ci: lint typecheck test

lint:
	ruff check .

typecheck:
	mypy .

test:
	pytest

format:
	ruff format .
