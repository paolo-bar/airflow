import logging
import os
import re
import subprocess
import zipfile
from datetime import timedelta
from typing import Any, Dict, List, Tuple

import google.auth
import google.auth.transport.requests
import pendulum
import yaml
from google.cloud import storage
from jinja2 import Template
# from zipfile import ZipFile, ZipInfo


class Constants(object):
    """Framework Generic Constants"""
    CFG_PATH = "config/"
    CFG_PATH_ = "core/config/"
    CFG_COMMONS = '__commons__.yaml'
    CFG_COMMONS_HEADER = '_commons_'
    CFG_COMMONS_KEY = 'commons_'
    CFG_ENV = '__envs__.yaml'
    CFG_ENV_HEADER = '_envs_'
    CFG_ENV_KEY = 'envs_'
    CFG_TEMPLATE = '__template__.yaml'
    OP_PARAMS = 'params'
    # ------------------------------


def _get_env(cfg_dict: dict) -> Any:
    return cfg_dict[Constants.CFG_ENV_HEADER]['projects'][cfg_dict[Constants.CFG_ENV_HEADER]['project_id']]


def env_cfg_(cfg_d: dict) -> dict:
    env_ = _get_env(cfg_d)
    return cfg_d.get(Constants.CFG_ENV_KEY, {}).get(env_, cfg_d)


def _get_cfg(cfg_d: dict, key: str, section: str, sub_section: str) -> Any:
    cfg_d_ = cfg_d[section]
    return env_cfg_(cfg_d).get(key, cfg_d.get(key, cfg_d_[sub_section].get(key, cfg_d_.get(key, None))))


def get_dags_cfg_(cfg_d: dict, key: str) -> Any:
    return _get_cfg(cfg_d, key, section=Constants.CFG_COMMONS_HEADER, sub_section='dag')


def get_op_cfg_(cfg_d: dict, key: str) -> Any:
    return _get_cfg(cfg_d, key, section=Constants.CFG_COMMONS_HEADER, sub_section='base_cluster')


def get_auth_token():
    cred, _ = google.auth.default()
    # cred = google.auth.default()
    return cred


def _os_env_dict():
    return {
        "gcp_project": os.environ.get('gcp_project', "vf-it-vfa-nonlive"),
        "gce_zone": os.environ.get('gce_zone', "<gce_zone>" ),
        "gcp_region": os.environ.get('gcp_region', "<gcp_region>"),
        "gcs_bucket": os.environ.get('gcs_bucket', "<gcs_bucket>>"),
        "firewall_rules_tags": os.environ.get('firewall_rules_tags', "<firewall_rules_tags>"),
        "subnetwork_uri": os.environ.get('subnetwork_uri', "<subnetwork_uri>"),
        "opco": os.environ.get('opco', "<opco>"),
        "service_account": os.environ.get('service_account', "<service_account>")
    }


def _parse_config_yaml(property_path: str) -> str:
    with open(property_path, 'r') as f:
        try:
            return f.read()
        except IOError as err:
            raise err


def _parse_config_yaml_zip(property_path) -> str:
    zipfile_, post_zip = re.search(r'(.*\.zip)?(.*)', property_path).groups()
    # data = io.TextIOWrapper(ZipFile(zipfile).read(post_zip.lstrip('/')))
    with zipfile.ZipFile(zipfile_) as my_zip:
        with my_zip.open(post_zip.lstrip('/')) as my_file:
            return my_file.read().decode('UTF-8')


def parse_config_yaml(property_path: str) -> dict:
    try:
        if zipfile.is_zipfile(property_path):
            data = _parse_config_yaml_zip(property_path)
        else:
            data = _parse_config_yaml(property_path)
        
        t = Template(data)
        data_with_template = t.render(_os_env_dict())
        data_final = yaml.safe_load(data_with_template)
        return data_final
    except IOError as err:
        raise err


def exec_shell_command(command: str, decode='utf-8') -> Tuple:
    if not command:
        return (None, None)
    proc = subprocess \
        .Popen([command],
                stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    out = out.strip().decode(decode)  # if out else out
    err = err.strip().decode(decode) if err else err
    return (out, err)


def get_latest_image_id(image_prefix) -> str:
    out = None
    if os.environ.get("gcs_bucket", None):
        command = "gcloud compute images list --filter=\"name~'" + image_prefix + "(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})(\d{2})-(\d+)'\" --sort-by NAME | tail -n 1 | awk '{print $1}'"
        (out, _) = exec_shell_command(command)
    return out


def get_gs_jars(bkt_id) -> List[str]:
    jars = None
    gs_jars_path = f"gs://{bkt_id}/deployment/com.vodafone.it/vfait/1.0.0/jars/*.jar"
    if os.environ.get("gcs_bucket", None):
        command = f"gsutil ls {gs_jars_path}"
        (out, _) = exec_shell_command(command)
        jars = out.split("\n")
    return jars


# default arguments when creating a DAG
def get_default_args(owner, retries, retry_delay, concurrency):
    return {
        'owner': owner,
        'start_date': pendulum.datetime(year=2021, month=1, day=26).astimezone("Europe/Rome"),  # datetime.now().isoformat()[:10],
        'catchup': False,
        'depends_on_past': False,
        'retries': retries,
        'retry_delay': timedelta(minutes=retry_delay),
        'concurrency': concurrency,
        'email': ['john.doe@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'provide_context': True,
    }


def get_bkt(bkt_str, storage_client: storage.Client = None):
    try:
        # Instantiates a client
        if not storage_client: storage_client = storage.Client()
        bkt = storage_client.get_bucket(bkt_str)
    except Exception as e:
        logging.error(e)
        bkt = None
    return bkt
    

def _gcs_parse_uri(uri: str) -> (str, str):
    # e.g. "gs://bucket-name/folder/file.txt"
    # match a regular expression to extract the bucket and filename
    matches = re.match("gs://(.*?)/(.*)", uri)
    if not matches:
        raise Exception("Wrong Uri")
    else:
        bkt_id, path_str = matches.groups()
        return bkt_id, path_str
    

def _gcs_list_dir(uri: str):
    bkt_id, path_str = _gcs_parse_uri(uri)
    # Get GCS bucket
    bkt = get_bkt(bkt_id)
    # Get blobs in specific subdirectory
    blobs = list(bkt.list_blobs(prefix=path_str))
    return blobs


def get_cfg_file_list(package: str, cfg_path: str):
    # if os.environ.get("AIRFLOW_BUCKET", None):
    #     list_ = _gcs_list_dir(cfg_path)
    if zipfile.is_zipfile(package):  # .endswith(".zip"):
        with zipfile.ZipFile(package) as zip_archive:
            infolist: list[zipfile.ZipInfo] = list(filter(
                lambda x: x.is_dir() and
                          x.filename.strip(os.sep).startswith(cfg_path.strip(os.sep)) and
                          x.filename.split('.')[-1] not in ['jar', 'rar', 'zip'],
                zip_archive.infolist()
            ))
        
        list_: list[str] = list(map(lambda x: str(x.filename), infolist))
        
    else:
        list_ = os.listdir(os.sep.join(['dags', cfg_path]))

    return list_


def deprecated(*args, **kwargs):
    logging.warning("function deprecated")
    pass


@deprecated
def load_from_local_filesystem_zip(zip_file_url: str is not None, path_in_zip: str = '/') -> Dict[str, str]:
    """
    Read either a file or the files contained in a folder within the zip archive identified by the provided URL using
    the archive relative path and return a dictionary whose keys are the filenames of the read files and whose values
    are the content of the read files.
    The zip archive should be on local filesystem

    :param zip_file_url: the URL of the zip archive to read from and should be a local filesystem location.
    :param path_in_zip: the relative path to explore within to zip archive which can reference either a file to
                        read or a folder whose files are to be read.
    :return: a dictionary whose keys are the filenames of the read files and whose values are the content of the
             read files.
    """
    file_name_to_file_contents = dict()

    with zipfile.ZipFile(zip_file_url) as zip_archive:
        matched_info = list(filter(
            lambda x: ''.join(['/', x.filename.strip('/')]).startswith(''.join(['/', path_in_zip.strip('/')])) and
                      not x.is_dir() and
                      x.filename.split('.')[-1] not in ['jar', 'rar', 'zip'],
            zip_archive.infolist()
        ))

        for path_info in matched_info:
            with zip_archive.open(path_info.filename) as file:
                file_name_to_file_contents[path_info.filename] = file.read().decode('utf-8')

    return file_name_to_file_contents


@deprecated
def gcs_download_uri(uri, storage_client: storage.Client = None, bkt: storage.bucket = None):
    # Parse uri
    bkt_str, path_str = _gcs_parse_uri(uri)
    
    if not bkt:
        # Instantiates a client
        if not storage_client:
            storage_client = storage.Client()
        
        bkt = get_bkt(bkt_str, storage_client=storage_client)

    # Get blobs
    file_ = "_cfg_file"
    bkt.get_blob(path_str).download_to_filename("_cfg_file")
    return file_

