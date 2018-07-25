build:
	python -m compileall *.py

check:
	mypy *.py
	flake8
