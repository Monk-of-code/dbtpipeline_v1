from __future__ import annotations
import pendulum
import os
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from docker.types import Mount

# --- Configuration ---

DBT_IMAGE_NAME = "dbt_transform_image:latest"
HOST_PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR", ".")

# --- Merged, single config block for all dbt tasks ---
common_dbt_settings = {
    "image": DBT_IMAGE_NAME,
    "network_mode": "erits",
    "force_pull": False,
    "auto_remove": "success",
    "working_dir": "/usr/app/dbt/erits",
    "tty": True,
    "mount_tmp_dir": False,
    
    # --- This line passes the variables to the container ---
    "environment": {
        "DB_NAME": os.environ.get("DB_NAME"),
        "DB_USER": os.environ.get("DB_USER"),
        "DB_PASSWORD": os.environ.get("DB_PASSWORD"),
        "DB_HOST": os.environ.get("DB_HOST"),
        "DB_PORT": os.environ.get("DB_PORT"),
        "DB_SCHEMA": os.environ.get("DB_SCHEMA"),
    },

    "mounts": [
        Mount(
            source=f"{HOST_PROJECT_DIR}/dbt/erits",
            target="/usr/app/dbt/erits",
            type="bind"
        ),
        Mount(
            source=f"{HOST_PROJECT_DIR}/dbt/profiles.yml",
            target="/usr/app/dbt/.dbt/profiles.yml",
            type="bind"
        )
    ]
}
# --------------------------------------------------------
DBT_VARS_BLOCK = """
{% if params.subcounty is not none %}
--vars '{ "area": "{{ params.subcounty }}" }'
{% endif %}
"""

with DAG(
    dag_id="dbt_orchestrator", 
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    params={"subcounty": None},
    doc_md="""
    This DAG runs a complex dbt workflow using nested TaskGroups.
    It models a multi-step staging process.
    """,
    tags=["dbt", "docker", "nested-groups"],
) as dag:
    
    # --- Task Group 0: Setup ---
    with TaskGroup(group_id="setup") as setup_group:

        dbt_clean = DockerOperator(
            task_id="dbt_clean",
            command="dbt clean",
            **common_dbt_settings,
        )

        dbt_deps = DockerOperator(
            task_id="dbt_deps",
            command="dbt deps",
            **common_dbt_settings,
        )

        dbt_debug = DockerOperator(
            task_id="dbt_debug",
            command="dbt debug",
            **common_dbt_settings,
        )

        dbt_clean >> dbt_deps >> dbt_debug

    # --- Task Group 1: Staging Models (Outer Group) ---
    with TaskGroup(group_id="staging_models") as staging_models:
        
        # --- 1. Initialise Group ---
        with TaskGroup(group_id="initialise") as initialise_group:
            run_initialise_model = DockerOperator(
                task_id="run_initialise_model",
                command="dbt run --select subcounty_unit" + DBT_VARS_BLOCK, 
                **common_dbt_settings,
            )

        # --- 2. Preparation Group (6 parallel models) ---
        with TaskGroup(group_id="preparation") as preparation_group:
            models = ["addresses", "building_permits", "buildings", "cadastre_parcel","power_customers","water_meters"]
            prep_tasks = []
            for model in models:
                prep_tasks.append(
                    DockerOperator(
                        task_id=f"run_{model}",
                        command=f"dbt run --select {model} " + DBT_VARS_BLOCK,
                        **common_dbt_settings,
                    )
                )

        # --- 3. Gen UUID and Linking Group ---
        with TaskGroup(group_id="gen_uuid_and_linking") as gen_uuid_group:
            
            run_cadastre_uuid = DockerOperator(
                task_id="run_cadastre_uuid",
                command=f"dbt run --select cadastre_uuid " + DBT_VARS_BLOCK,
                **common_dbt_settings,
            )

            # --- 3a. Nested 'linking' Group ---
            with TaskGroup(group_id="linking") as linking_group:
                linking_models = ["address_locations_nlims_link", "building_nlims_link", "building_permits_nlims_link", "power_customers_nlims_link", "water_meters_nlims_link"]
                linking_tasks = []
                for model in linking_models:
                    linking_tasks.append(
                        DockerOperator(
                            task_id=f"run_{model}",
                            command=f"dbt run --select {model} " + DBT_VARS_BLOCK,
                            **common_dbt_settings,
                        )
                    )
            
            run_cadastre_uuid >> linking_group

        with TaskGroup(group_id="fusion") as fusion:

            with TaskGroup(group_id="parcel_feature_counts") as parcel_feature_counts_group:
                parcel_feature_models = ["parcel_building_count", "parcel_address_count", "parcel_watermeters_count", "parcel_powermeters_count", "parcel_permit_count"]
                parcel_feature_tasks = []
                for model in parcel_feature_models:
                    parcel_feature_tasks.append(
                        DockerOperator(
                            task_id=f"run_{model}",
                            command=f"dbt run --select {model} " + DBT_VARS_BLOCK,
                            **common_dbt_settings,
                        )
                    )
            run_fusion = DockerOperator(
                task_id="run_fusion",
                command="dbt run --select parcel_address_building_power_permits_water_counts " + DBT_VARS_BLOCK,
                **common_dbt_settings,
            ) 
            parcel_feature_counts_group >> run_fusion       

        with TaskGroup(group_id='property_interest') as interest:
            run_interests = DockerOperator(
                task_id="run_interest",
                command="dbt run --select cadastre_landrates_tradecount_property " + DBT_VARS_BLOCK,
                **common_dbt_settings,
            )
        # --- Define Dependencies *within* the 'staging_models' group ---
        initialise_group >> preparation_group >> gen_uuid_group >> fusion >> interest

    with TaskGroup(group_id='migrate_to_gold') as marts:
        gold_models=["building_permits_nlims_gold","power_customers_nlims_gold","water_meters_nlims_gold","cp_kplc_nwsc_bldg_permits_address_gold"] 
        marts_tasks=[]
        for model in gold_models:
            marts_tasks.append(
                DockerOperator(
                    task_id=f"run_{model}",
                    command=f"dbt run --select {model} " + DBT_VARS_BLOCK,
                    **common_dbt_settings
                )
            )
               
    
    # --- Define the overall DAG dependencies ---
    setup_group >> staging_models >> marts

