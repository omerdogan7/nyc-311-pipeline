from airflow.operators.email import EmailOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(
    dag_id="test_fail_email_fixed",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "email"]
)
def fail_email_dag():
    
    @task
    def fail_task():
        try:
            raise Exception("Test: Bu görev kasıtlı olarak başarısız edildi")
        except Exception as e:
            # Connection ID belirterek e-posta gönder
            EmailOperator(
                task_id='send_notification',
                to=['omrdgn2212@gmail.com'],
                subject='🚨 Airflow Task Başarısız',
                html_content=f'''
                <h3>Task Başarısız Oldu</h3>
                <p><strong>Hata:</strong> {str(e)}</p>
                <p><strong>Tarih:</strong> {datetime.now()}</p>
                ''',
                conn_id='smtp_default'  # Connection ID belirt
            ).execute(context={})
            
            raise  # Task'ı failed olarak işaretle

    fail_task()

dag = fail_email_dag()