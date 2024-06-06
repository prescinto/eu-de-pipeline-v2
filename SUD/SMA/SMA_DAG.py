from datetime import datetime
from SUD.SMA.utils import prd_utils
from SUD.SMA.Extract import main
from SUD.SMA.Transform import TransformMain
from SUD.SMA.Load import LoadMain

from airflow import DAG
from airflow.operators.python import PythonOperator

import asyncio
    

dag = DAG(
    dag_id="sma-v3",
    start_date=datetime(2024,6,1),
    catchup=False,
    schedule_interval="*/5 * * * *",
)

Pipeline_Id = 'PIPE00001'
def getplantList():
    prds  = prd_utils.get_prds(Pipeline_Id)
    ret_lst = [[x[2]] for x in prds]
    print(ret_lst)
    return ret_lst


def createTaskList(plant,**kwargs):
    print(f"{plant=}")
    main.Extract(plant).get_data()
    
def transform_task_fn(plant, **kwargs):
    asyncio.run(TransformMain.Transform(plant).main())
    return

def load_task_fn(plant, **kwargs):
    asyncio.run(LoadMain.Load(plant).main())
    return

plant_List_task = PythonOperator(task_id="plant-list-for-pipeline", 
                                 python_callable=getplantList, 
                                 do_xcom_push=True, dag=dag)

create_tasks_from_plants_task = PythonOperator.partial(task_id="extract_task",
                                              python_callable=createTaskList,
                                              dag=dag).expand(op_args=plant_List_task.output)

transform_task = PythonOperator.partial(task_id="transform_task", python_callable=transform_task_fn,
                                              dag=dag).expand(op_args=plant_List_task.output)

load_task = PythonOperator.partial(task_id="load_task", python_callable=load_task_fn,
                                              dag=dag).expand(op_args=plant_List_task.output)




plant_List_task >> create_tasks_from_plants_task >> transform_task >> load_task