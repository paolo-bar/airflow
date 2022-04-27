import logging
import ntpath
import os
import time
import uuid

from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from core.config_utils import get_gs_jars

log = logging.getLogger(__name__)


class CustomDataProcPySparkOperator(DataProcPySparkOperator):
    
    def __init__(self, jar_bkt_id=None, *args, **kwargs):
        self.jar_bkt_id = jar_bkt_id
        # invoke super constructor
        super(CustomDataProcPySparkOperator, self).__init__(*args, **kwargs)
    
    def execute(self, context):
        # self.dataproc_pyspark_jars = get_gs_jars(self.jar_bkt_id)
        self.dataproc_jars = get_gs_jars(self.jar_bkt_id)
        # invoke super constructor
        super().execute(context=context)

