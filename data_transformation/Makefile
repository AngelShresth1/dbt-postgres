export

DBT=dbt
DBT_PROFILE=data_transformation
DBT_ENV=dev
REQUIREMENT_FILE=./requirements.txt
VENV=venv
PYTHON=python

.PHONY: venv install install_dbt_packages load_yml seed run_tests run_models
# Detect the operating system
ifeq ($(OS),Windows_NT)
    VENV_ACTIVATE := $(VENV)\Scripts\activate
    PYTHON_ENV := $(PYTHON)
    ACTIVATE_CMD :=
    EXPORT_ENV :=  $(for /F "delims=" %i in ('type .env ^| findstr /v /r "^#" ^| findstr /r /c:"."') do set %i)
else
    VENV_ACTIVATE := $(VENV)/bin/activate
    PYTHON_ENV := $(PYTHON)3
    ACTIVATE_CMD := .
    EXPORT_ENV := $(export shell sed 's/=.*//' .env)
endif

all: venv install install_dbt_packages load_yml seed run_stage_models run_vault_models

# ifeq ($(OS),Windows_NT)
# 	loadenv:
#     	@for /F "delims=" %%i in ('type .env ^| findstr /v /r "^#" ^| findstr /r /c:"."') do @set %%i
# else
# 	loadenv:
#     	@export $(shell grep -v '^#' .env | xargs)
# endif

# .PHONY: loadenv

# seed run_tests run_models

venv:
	$(PYTHON_ENV) -m venv $(VENV)

install:
	$(ACTIVATE_CMD) $(VENV_ACTIVATE) && pip install -r requirements.txt 


install_dbt_packages:
	$(ACTIVATE_CMD) $(VENV_ACTIVATE) && cd data_transformation && $(DBT) deps 

load_s3_data:
	$(ACTIVATE_CMD) $(VENV_ACTIVATE) && cd data_transformation && $(PYTHON_ENV) load_s3_data.py

load_yml:
	$(ACTIVATE_CMD) $(VENV_ACTIVATE) && cd data_transformation && $(PYTHON_ENV) load_yml.py

# run_tests:
# 	$(ACTIVATE_CMD) $(VENV_ACTIVATE) && cd data_transformation && $(DBT) test --profile $(DBT_PROFILE) --target $(DBT_ENV)

seed:
	$(PYTHON_ENV) -m venv $(VENV) && $(ACTIVATE_CMD) $(VENV_ACTIVATE) && cd data_transformation && $(DBT) seed --full-refresh --profile $(DBT_PROFILE) --target $(DBT_ENV)

run_stage_models:
	$(PYTHON_ENV) -m venv $(VENV) && $(ACTIVATE_CMD) $(VENV_ACTIVATE) && pip install -r requirements.txt && cd data_transformation && $(DBT) deps && $(DBT) run -s raw stage --profile $(DBT_PROFILE) --target $(DBT_ENV)

run_vault_models:
	$(PYTHON_ENV) -m venv $(VENV) && $(ACTIVATE_CMD) $(VENV_ACTIVATE) && pip install -r requirements.txt && cd data_transformation && $(DBT) deps &&  $(DBT) run -s hubs sats links --profile $(DBT_PROFILE) --target $(DBT_ENV)
