import boto3
import re
import os
import sys
from . import global_faasr as faasr_env

def faasr_get_file(local_file, remote_file, server_name="", local_folder=".", remote_folder="."):
    """
    This function downloads a file from S3
    """
    # to-do: config
    config = faasr_env.get_faasr()

    if server_name == "":
        server_name = config['DefaultDataStore']

    if server_name not in config['DataStores']:
        err_msg = '{\"faasr_get_file\":\"Invalid data server name: ' + server_name + '\"}\n'
        print(err_msg)
        sys.exit(1)

    target_s3 = config['DataStores'][server_name]
    
    # Remove "/" in the folder & file name to avoid situations:
    # 1: duplicated "/" ("/remote/folder/", "/file_name") 
    # 2: multiple "/" by user mistakes ("//remote/folder//", "file_name")
    # 3: file_name ended with "/" ("/remote/folder", "file_name/")
    remote_folder = re.sub(r'/+', '/', remote_folder.rstrip('/'))
    remote_file = re.sub(r'/+', '/', remote_file.rstrip('/'))

    if remote_folder == "":
        get_file_s3 = remote_file
    else:
        get_file_s3 = f"{remote_folder}/{remote_file}"
    
    local_folder = re.sub(r'/+', '/', local_folder.rstrip('/'))
    local_file = re.sub(r'/+', '/', local_file.rstrip('/'))
    get_file = f"{local_folder}/{local_file}"

    s3_client = boto3.client(
        's3',
        aws_access_key_id = target_s3['AccessKey'],
        aws_secret_access_key = target_s3['SecretKey'],
        region_name = target_s3['Region'],
        endpoint_url = target_s3['Endpoint']
        )

    if os.path.exists(get_file):
        os.remove(get_file)

    result = s3_client.download_file(Bucket = target_s3['Bucket'], Key = get_file_s3, Filename = get_file)