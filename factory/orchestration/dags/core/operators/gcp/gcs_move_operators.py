# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import logging
from typing import Iterable

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


def extract_job_id_to_full_path(filename):
    try:
        return filename.split('/')[-1]
    except Exception:
        logging.error('Extract name of file from full path: %s')
        return 'NO_FILE_'


class GoogleCloudStorageMoveOperator(BaseOperator):
    """
    List all objects from the bucket with the give string prefix and delimiter in name.

    This operator returns a python list with the name of objects which can be used by
     `xcom` in the downstream task.

    :param bucket: The Google cloud storage bucket to find the objects. (templated)
    :type bucket: str
    :param prefix: Prefix string which filters objects whose name begin with
           this prefix. (templated)
    :type prefix: str
    :param delimiter: The delimiter by which you want to filter the objects. (templated)
        For e.g to lists the CSV files from in a directory in GCS you would use
        delimiter='.csv'.
    :type delimiter: str
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str

    **Example**:
        The following Operator would list all the Avro files from ``sales/sales-2017``
        folder in ``data`` bucket. ::

            GCS_Files = GoogleCloudStorageMoveOperator(
                task_id='GCS_Files',
                source_bucket='data',
                source_object='sales/sales-2017/XXX.json',
                destination_bucket='data',
                destination_object='sales/sales-2017/backup/XXX.json',
                google_cloud_storage_conn_id=google_cloud_conn_id
            )
    """
    # template_fields = (
    # 'source_bucket', 'source_object', 'destination_bucket', 'destination_object')  # type: Iterable[str]
    ui_color = '#f0eee5'

    @apply_defaults
    def __init__(self,
                 source_bucket,
                 source_object,
                 destination_bucket,
                 destination_object,
                 num_object=5,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(GoogleCloudStorageMoveOperator, self).__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.source_object = source_object
        self.destination_bucket = destination_bucket
        self.destination_object = destination_object
        self.num_object = num_object
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):

        task_instance = context['task_instance']
        timestamp = task_instance.xcom_pull(key="timestamp")
        log.info('Timestamp folder: %s', timestamp)

        source = self.source_bucket + "/" + self.source_object
        destination = self.destination_bucket + "/" + self.destination_object.format(timestamp)
        logging.info('Move file in this folder: %s into another folder %s', source, destination)

        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )

        list_of_files = hook.list(bucket=self.source_bucket, prefix=self.source_object)[:self.num_object]
        logging.info('Num objects to copy: %s ', len(list_of_files))

        for filename in list_of_files:
            destination = self.destination_object.format(timestamp) + extract_job_id_to_full_path(filename)
            logging.info('Move file: %s into %s', filename, destination)

            logging.info('Move file bucket: %s source: %s into bucket: %s source: %s', self.source_bucket,
                         filename, self.destination_bucket, destination)

            hook.copy(source_bucket=self.source_bucket,
                      source_object=filename,
                      destination_bucket=self.destination_bucket,
                      destination_object=destination)

        for filename in list_of_files:
            logging.info('Delete file from %s', filename)
            hook.delete(bucket=self.source_bucket,
                        object=filename)
