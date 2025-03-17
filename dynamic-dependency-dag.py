"""
Enhanced Dynamic Data Pipeline with Real-time Dependency Resolution
"""

import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.models import Variable, TaskInstance, DagRun
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from pyspark.sql import SparkSession


# Default arguments for DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    'enhanced_sql_dependency_pipeline',
    default_args=default_args,
    description='Execute SQL scripts based on file and SQL dependencies with real-time dependency resolution',
    schedule_interval=None,  # Event-driven - will be triggered by file arrivals
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['databricks', 'dependency', 'dynamic'],
)

# Configuration
STORAGE_FOLDER = Variable.get("storage_folder_path")
METADATA_TABLE = Variable.get("metadata_table_name", "sql_metadata")
EXECUTION_TABLE = Variable.get("execution_table_name", "execution_history")
DATABRICKS_CONN_ID = Variable.get("databricks_conn_id", "databricks_default")

# Function to check for new files and trigger DAG runs
def check_new_files(**kwargs):
    """
    Monitors the storage folder for new files and returns a list of new files.
    """
    # Get list of files in the folder
    file_list = os.listdir(STORAGE_FOLDER)
    
    # Get list of processed files from Variable
    processed_files = Variable.get("processed_files", default_var=json.dumps([]))
    processed_files = json.loads(processed_files)
    
    # Identify new files
    new_files = [f for f in file_list if f not in processed_files]
    
    if not new_files:
        return []
    
    # Update processed files list
    processed_files.extend(new_files)
    Variable.set("processed_files", json.dumps(processed_files))
    
    # Return new files for downstream tasks
    return new_files

# Function to initialize dependency graph
def initialize_dependency_graph(**kwargs):
    """
    Creates a dependency graph of all SQL scripts based on metadata table.
    Sets execution_ready flag for scripts that can be executed immediately.
    """
    # Create SparkSession to interact with Databricks
    spark = SparkSession.builder \
        .appName("DependencyGraphInitializer") \
        .getOrCreate()
    
    # Load metadata table
    metadata_df = spark.sql(f"SELECT * FROM {METADATA_TABLE}").toPandas()
    
    # Get current execution status
    execution_df = spark.sql(f"SELECT script_id, MAX(execution_time) as last_execution, status FROM {EXECUTION_TABLE} WHERE status = 'SUCCESS' GROUP BY script_id, status").toPandas()
    
    # Map of completed SQL scripts (for dependency checking)
    completed_scripts = {row['script_id']: True for _, row in execution_df.iterrows() if row['status'] == 'SUCCESS'}
    
    # List of all files in the folder
    all_files = os.listdir(STORAGE_FOLDER)
    
    # Initialize dependency graph
    dependency_graph = {}
    
    # Check each script in metadata table
    for _, script in metadata_df.iterrows():
        script_id = script['script_id']
        
        # Skip if already executed successfully
        if script['execution_status'] == 'SUCCESS':
            continue
            
        # Check file dependencies
        file_deps = script['dependencies'].split(',') if script['dependencies'] else []
        file_deps_met = all(dep.strip() in all_files for dep in file_deps if dep.strip())
        
        # Check SQL dependencies
        sql_deps = script['sql_dependencies'].split(',') if script['sql_dependencies'] else []
        sql_deps_list = [int(dep.strip()) for dep in sql_deps if dep.strip()]
        sql_deps_met = all(dep in completed_scripts for dep in sql_deps_list)
        
        # Create dependency entry
        dependency_graph[script_id] = {
            'script_id': script_id,
            'script_name': script['script_name'],
            'script_path': script['script_path'],
            'file_deps_met': file_deps_met,
            'sql_deps': sql_deps_list,
            'sql_deps_met': sql_deps_met,
            'execution_ready': file_deps_met and sql_deps_met,
            'downstream_scripts': []  # Will be populated in next step
        }
    
    # Populate downstream_scripts field
    for script_id, data in dependency_graph.items():
        for dep_script_id in data['sql_deps']:
            if dep_script_id in dependency_graph:
                dependency_graph[dep_script_id]['downstream_scripts'].append(script_id)
    
    # Close SparkSession
    spark.stop()
    
    # Store dependency graph in XCom for later tasks
    return dependency_graph

# Function to identify immediately executable scripts
def identify_ready_scripts(**kwargs):
    """
    Identifies which SQL scripts are ready for execution based on the dependency graph.
    """
    ti = kwargs['ti']
    dependency_graph = ti.xcom_pull(task_ids='initialize_dependency_graph')
    
    # Find scripts that are ready for execution
    executable_scripts = [
        data for script_id, data in dependency_graph.items() 
        if data['execution_ready']
    ]
    
    # Store list of all script IDs for checking completion
    all_script_ids = list(dependency_graph.keys())
    Variable.set("dag_run_all_scripts", json.dumps(all_script_ids))
    
    # Store ready scripts to variable for branching
    Variable.set("ready_scripts", json.dumps([script['script_id'] for script in executable_scripts]))
    
    return executable_scripts

# Function to determine if more scripts can be executed
def check_for_more_scripts(**kwargs):
    """
    Determines if there are more scripts that can be executed after a script completes.
    """
    ti = kwargs['ti']
    completed_script_id = kwargs['script_id']
    dependency_graph = ti.xcom_pull(task_ids='initialize_dependency_graph')
    
    # Update dependency graph with completed script
    dependency_graph[completed_script_id]['execution_ready'] = False  # Mark as no longer needing execution
    
    # Check if any downstream scripts now have all dependencies met
    newly_executable = []
    
    # For each downstream script of the completed script
    for downstream_id in dependency_graph[completed_script_id]['downstream_scripts']:
        if downstream_id in dependency_graph and not dependency_graph[downstream_id].get('execution_ready', False):
            # Check if all dependencies are now met
            sql_deps = dependency_graph[downstream_id]['sql_deps']
            
            # A dependency is met if it's not in the dependency graph (already executed before this DAG run)
            # or if it's marked as no longer needing execution (completed in this DAG run)
            all_deps_met = all(
                dep_id not in dependency_graph or 
                dependency_graph[dep_id].get('execution_ready') == False
                for dep_id in sql_deps
            )
            
            # If script has file dependencies met and now all SQL dependencies met
            if dependency_graph[downstream_id]['file_deps_met'] and all_deps_met:
                dependency_graph[downstream_id]['execution_ready'] = True
                newly_executable.append(dependency_graph[downstream_id])
    
    # Update dependency graph in XCom
    ti.xcom_push(key='dependency_graph', value=dependency_graph)
    
    return newly_executable

# Function to execute a script in Databricks
def execute_databricks_script(**kwargs):
    """
    Executes a SQL script in Databricks and updates the execution status
    """
    ti = kwargs['ti']
    script_info = kwargs['script_info']
    script_id = script_info['script_id']
    script_path = script_info['script_path']
    
    # Create Databricks notebook task
    notebook_task = {
        'notebook_path': '/Shared/sql_execution_notebook',
        'base_parameters': {
            'script_path': script_path,
            'script_id': script_id
        }
    }
    
    # Submit run to Databricks
    hook = DatabricksHook(databricks_conn_id=DATABRICKS_CONN_ID)
    run_id = hook.submit_run(notebook_task)
    
    # Wait for the run to complete
    run_info = hook.get_run(run_id)
    run_state = run_info['state']['life_cycle_state']
    
    # Update execution status based on run state
    success = run_state == 'TERMINATED' and run_info['state']['result_state'] == 'SUCCESS'
    status = 'SUCCESS' if success else 'FAILED'
    duration = run_info['execution_duration'] if 'execution_duration' in run_info else 0
    
    # Create SparkSession to update tables
    spark = SparkSession.builder \
        .appName("StatusUpdater") \
        .getOrCreate()
    
    # Update execution history
    spark.sql(f"""
        INSERT INTO {EXECUTION_TABLE} (script_id, status, execution_duration, error_message)
        VALUES ({script_id}, '{status}', {duration}, '{run_info.get('state', {}).get('state_message', '')}')
    """)
    
    # Update metadata table status
    spark.sql(f"""
        UPDATE {METADATA_TABLE}
        SET execution_status = '{status}',
            last_execution_time = CURRENT_TIMESTAMP,
            execution_duration = {duration}
        WHERE script_id = {script_id}
    """)
    
    # Close SparkSession
    spark.stop()
    
    # Return execution info for downstream tasks
    return {'script_id': script_id, 'status': status}

# Function to process newly available scripts
def process_newly_executable_scripts(**kwargs):
    """
    Creates and executes tasks for newly executable scripts
    """
    ti = kwargs['ti']
    task_id = kwargs['task_id']
    newly_executable = ti.xcom_pull(task_ids=task_id)
    
    if not newly_executable:
        return None
    
    # For demonstration, we'll just return the scripts to be handled by DAG factory
    return newly_executable

# Function to check if all scripts are complete
def check_all_complete(**kwargs):
    """
    Checks if all scripts in the dependency graph are complete
    """
    ti = kwargs['ti']
    dependency_graph = ti.xcom_pull(key='dependency_graph', task_ids=None, include_prior_dates=True)
    
    # If no dependency graph available, use the one from initialization
    if not dependency_graph:
        dependency_graph = ti.xcom_pull(task_ids='initialize_dependency_graph')
    
    # Check if any scripts are still ready for execution
    pending_scripts = [
        script_id for script_id, data in dependency_graph.items()
        if data.get('execution_ready', False)
    ]
    
    return len(pending_scripts) == 0

# Task to check for new files
check_files_task = PythonOperator(
    task_id='check_new_files',
    python_callable=check_new_files,
    provide_context=True,
    dag=dag,
)

# Task to initialize dependency graph
init_graph_task = PythonOperator(
    task_id='initialize_dependency_graph',
    python_callable=initialize_dependency_graph,
    provide_context=True,
    dag=dag,
)

# Task to identify immediately executable scripts
identify_scripts_task = PythonOperator(
    task_id='identify_ready_scripts',
    python_callable=identify_ready_scripts,
    provide_context=True,
    dag=dag,
)

# Create a factory function to create execution task groups
def create_execution_task_group(script_info, dag):
    """
    Creates a task group for executing a script and handling its downstream dependencies
    """
    script_id = script_info['script_id']
    
    with TaskGroup(group_id=f'process_script_{script_id}', dag=dag) as task_group:
        # Task to execute the script
        execute_task = PythonOperator(
            task_id=f'execute_script_{script_id}',
            python_callable=execute_databricks_script,
            op_kwargs={'script_info': script_info},
            provide_context=True,
            dag=dag,
        )
        
        # Task to check for newly executable scripts after this one completes
        check_downstream_task = PythonOperator(
            task_id=f'check_downstream_{script_id}',
            python_callable=check_for_more_scripts,
            op_kwargs={'script_id': script_id},
            provide_context=True,
            dag=dag,
        )
        
        # Task to process newly executable scripts
        process_new_task = PythonOperator(
            task_id=f'process_new_scripts_{script_id}',
            python_callable=process_newly_executable_scripts,
            op_kwargs={'task_id': f'check_downstream_{script_id}'},
            provide_context=True,
            dag=dag,
        )
        
        # Set up task dependencies within the group
        execute_task >> check_downstream_task >> process_new_task
        
    return task_group

# Dynamic task generation - create task groups for each initially executable script
def expand_tasks(**kwargs):
    """
    Dynamically creates task groups for executable scripts
    """
    ti = kwargs['ti']
    executable_scripts = ti.xcom_pull(task_ids='identify_ready_scripts')
    
    if not executable_scripts:
        return
    
    # Create a task group for each script
    task_groups = []
    for script in executable_scripts:
        task_group = create_execution_task_group(script, dag)
        task_groups.append(task_group)
        
        # Set dependency from identify_scripts_task to this task group
        identify_scripts_task >> task_group

# Task to expand initial executable scripts into task groups
expand_task = PythonOperator(
    task_id='expand_tasks',
    python_callable=expand_tasks,
    provide_context=True,
    dag=dag,
)

# Define task dependencies for the main flow
check_files_task >> init_graph_task >> identify_scripts_task >> expand_task

# Add a trigger for newly executable scripts discovered during execution
# This creates a recursive pattern that allows the DAG to continue processing
# scripts as their dependencies become satisfied
trigger_newly_discovered = TriggerDagRunOperator(
    task_id='trigger_for_new_scripts',
    trigger_dag_id='enhanced_sql_dependency_pipeline',
    reset_dag_run=True,