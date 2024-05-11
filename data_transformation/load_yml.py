"""
__author__: "GritFeat Solutions - Nepal"
__copyright__: "Copyright 2022, GritFeat Solutions, Nepal."
__version__: "0.0.1"
__status__: "Dev"
__created_on__: "10/05/2024"
__maintainer__: "Krisha Maharjan"
"""


import os
from dotenv import load_dotenv
import yaml


# Load environment variables from .env file
load_dotenv()

# Accessing environment variables
postgres_host = os.getenv("POSTGRES_HOST")
postgres_user = os.getenv("POSTGRES_USER")
postgres_pwd = os.getenv("POSTGRES_PWD")
postgres_port = os.getenv("POSTGRES_PORT")
postgres_database = os.getenv("POSTGRES_DATABASE")
postgres_schema = os.getenv("POSTGRES_RAW_VAULT_SCHEMA")



postgres_stg_schema = os.getenv("POSTGRES_STAGE_SCHEMA")
postgres_lnd_schema = os.getenv("POSTGRES_LANDING_SCHEMA")

patient=os.getenv("ENV_PATIENT_TABLE")
coverage=os.getenv("ENV_COVERAGE_TABLE")
eligibility=os.getenv("ENV_ELIGIBILITY_TABLE")

lnd_patient_table = os.getenv("ENV_PATIENT_TABLE")
lnd_elig_table = os.getenv("ENV_ELIGIBILITY_TABLE")
lnd_coverage_table = os.getenv("ENV_COVERAGE_TABLE")



# Creating the data transformation dictionary
data_transformation = {
   "data_transformation":{
    "outputs": {
        "dev": {
            "type": "postgres",
            "host": postgres_host,
            "user": postgres_user,
            "password": postgres_pwd,
            "port": 4001,
            "dbname": postgres_database,
            "schema": postgres_schema,
            "threads": 1,
            "connect_timeout": 30
        }
    },
    "target": "dev"
}
}

config = {
    'sources': [
        {
            'name': 'landing',
            'database': postgres_database,
            'schema': postgres_lnd_schema,
            'tables': [
                {
                    'name': lnd_patient_table
                    , 'columns': [
                        {
                            'name': '_AIRBYTE_AB_ID',
                            'description': 'ID',
                            'tests': ['unique', 'not_null']
                        }
                    ]
                },
                {
                    'name': lnd_elig_table,
                    'columns': [
                        {
                            'name': 'patient_id',
                            'description': 'ID',
                            'tests': ['unique', 'not_null']
                        }
                    ]
                },
                {
                    'name': lnd_coverage_table,
                    'columns': [
                        {
                            'name': '_AIRBYTE_AB_ID',
                            'description': 'ID',
                            'tests': ['unique', 'not_null']
                        }
                    ]
                }
            ]
        }
    ]
}



with open("profiles.yml", "w") as yaml_file:
    yaml.dump(data_transformation, yaml_file, default_flow_style=False)


with open("models/schema.yml", "w") as yamls_file:
    yaml.dump(config, yamls_file)