import logging
import re

from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator
from airflow.models import DagBag
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from core.config_utils import get_latest_image_id


class CustomDataprocClusterCreateOperator(DataprocClusterCreateOperator):

    def __init__(self,
                 enable_optional_components=False,
                 enable_http_port_access=True,
                 autoscaling_policy=None,
                 *args,
                 **kwargs):
        super(CustomDataprocClusterCreateOperator, self).__init__(*args, **kwargs)
        self.optional_components = enable_optional_components
        self.enable_http_port_access = enable_http_port_access
        self.autoscaling_policy = autoscaling_policy

    def _build_cluster_data(self):
        cluster_data = super(CustomDataprocClusterCreateOperator, self)._build_cluster_data()

        if self.autoscaling_policy is not None:
            cluster_data['config']['autoscalingConfig'] = {"policyUri": "{}".format(self.autoscaling_policy)}

        if self.optional_components:
            cluster_data['config']['softwareConfig']['optionalComponents'] = ['ANACONDA', 'JUPYTER', 'ZEPPELIN']

        # Enables the Access to Spark, Hadoop, YARN, Jupyter (if enabled) UIs,
        if self.enable_http_port_access | self.optional_components:
            cluster_data['config']['endpointConfig'] = {
                "enableHttpPortAccess": True
            }
        return cluster_data

