import logging
import time
from datetime import datetime

log = logging.getLogger()


def branch_files_to_process(
        xcom_key="list_files_in_todo",
        true_task: str = "start_cluster",
        false_task: str = "no_file", **kwargs):
    
    list_of_files = kwargs['ti'].xcom_pull(task_ids=xcom_key)
    logging.info('Number of objects: %d', len(list_of_files))
    
    return true_task if list_of_files else false_task


def xcom_push_ts(key_="timestamp", ts_format="%Y%m%d%H%M%S", **kwargs):
    timestamp = datetime.fromtimestamp(time.time()).strftime(ts_format)
    kwargs['ti'].xcom_push(key=key_, value=timestamp)
    return 0


def x_com_pusher(this_ti, keys, values):
    for k in keys:
        ind = keys.index(k)
        val = values[ind]
        # """Pushes an XCom without a specific target"""
        log.debug(f"XCom Pushing variable : Task = [{this_ti.task_id}] - Key = [{k}] - value: [{val}]")
        this_ti.xcom_push(key=k, value=val)


# function that pull XCom Variable
# ---------------------------------------------------------------------------------------
def x_com_puller(dag_id, ti, keys):
    values = []
    for k in keys:
        val = ti.xcom_pull(dag_id=dag_id, task_ids=ti.task_id, key=k)
        log.debug(f"XCom Pull : DAG = [{dag_id}] - Task = [{ti.task_id}] - Key = [{k}] - value: [{val}]")
        values.append(val)

    log.info(f"values: {values}")
    return values
