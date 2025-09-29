import pytest
from airflow.models import DagBag
from datetime import datetime

@pytest.fixture
def dagbag():
    """DAG bag fixture"""
    return DagBag(dag_folder="include/dags", include_examples=False)


def test_batch_dag_loaded(dagbag):
    """DAG'ın başarıyla yüklendiğini kontrol et"""
    dag = dagbag.get_dag(dag_id="nyc311_batch_backfill")
    assert dag is not None, "DAG bulunamadı"
    assert dagbag.import_errors == {}, f"DAG import hatası: {dagbag.import_errors}"


def test_batch_dag_properties(dagbag):
    """DAG özelliklerini kontrol et"""
    dag = dagbag.get_dag(dag_id="nyc311_batch_backfill")
    
    # Temel özellikler
    assert dag.schedule is None, "Schedule None olmalı (manuel tetikleme)"
    assert dag.catchup is False, "Catchup False olmalı"
    assert dag.max_active_runs == 1, "Aynı anda tek run çalışmalı"
    
    # Default args kontrol
    assert dag.default_args["owner"] == "data-engineering"
    assert dag.default_args["retries"] == 1
    
    # Tags kontrol
    assert "batch" in dag.tags
    assert "nyc311" in dag.tags
    assert "backfill" in dag.tags


def test_batch_dag_tasks(dagbag):
    """DAG task'larını kontrol et"""
    dag = dagbag.get_dag(dag_id="nyc311_batch_backfill")
    
    # Task sayısı
    assert len(dag.tasks) == 4, f"4 task olmalı, {len(dag.tasks)} bulundu"
    
    # Task isimleri
    task_ids = [task.task_id for task in dag.tasks]
    expected_tasks = [
        "start_batch_backfill",
        "process_all_months",
        "generate_summary_report", 
        "end_batch_backfill"
    ]
    
    for expected_task in expected_tasks:
        assert expected_task in task_ids, f"{expected_task} task'ı bulunamadı"


def test_batch_dag_dependencies(dagbag):
    """Task bağımlılıklarını kontrol et"""
    dag = dagbag.get_dag(dag_id="nyc311_batch_backfill")
    
    # start_batch_backfill -> process_all_months
    start_task = dag.get_task("start_batch_backfill")
    assert "process_all_months" in [t.task_id for t in start_task.downstream_list]
    
    # process_all_months -> generate_summary_report
    process_task = dag.get_task("process_all_months")
    assert "generate_summary_report" in [t.task_id for t in process_task.downstream_list]
    
    # generate_summary_report -> end_batch_backfill
    summary_task = dag.get_task("generate_summary_report")
    assert "end_batch_backfill" in [t.task_id for t in summary_task.downstream_list]
    
    # Zincir doğru mu?
    end_task = dag.get_task("end_batch_backfill")
    assert len(end_task.upstream_list) == 1, "End task'ın sadece 1 upstream'i olmalı"
    assert len(start_task.upstream_list) == 0, "Start task'ın upstream'i olmamalı"