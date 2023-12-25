.PHONY: create-env

create-env:
	@echo "Creating Python virtual environment..."
	python -m venv venv
	@echo "Activating the virtual environment... and Installing Poetry..."
#   see https://stackoverflow.com/questions/44052093/makefile-with-source-get-error-no-such-file-or-directory
	venv/bin/pip install poetry==1.7.1
	@echo "Configuring Poetry..."
	venv/bin/poetry config virtualenvs.create false
	@echo "Installing dependencies with Poetry..."
	venv/bin/poetry install
