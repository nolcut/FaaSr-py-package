import re
import boto3
from . import global_faasr as faasr_env

def faasr_delete_file(remote_file, server_name="", remote_folder=""):
    """
    This function deletes a file from S3 bucket
    """
    # to-do: config
    faasr = faasr_env.get_faasr()
    config = faasr.get_payload_dict()

    if server_name == "":
        server_name = config['DefaultDataStore']

    if server_name not in config['DataStores']:
        err_msg = '{\"faasr_delete_file\":\"Invalid data server name: ' + server_name + '\"}\n'
        print(err_msg)
        quit()
    
    target_s3 = config['DataStores'][server_name]

    # Remove "/" in the folder & file name to avoid situations:x
    # 1: duplicated "/" ("/remote/folder/", "/file_name") 
    # 2: multiple "/" by user mistakes ("//remote/folder//", "file_name")
    # 3: file_name ended with "/" ("/remote/folder", "file_name/")
    remote_folder = re.sub(r'/+', '/', remote_folder.rstrip('/'))
    remote_file = re.sub(r'/+', '/', remote_file.rstrip('/'))

    delete_file_s3 = f"{remote_folder}/{remote_file}"

    s3_client = boto3.client(
        's3',
        aws_access_key_id = target_s3['AccessKey'],
        aws_secret_access_key = target_s3['SecretKey'],
        region_name = target_s3['Region'],
        endpoint_url = target_s3['Endpoint']
        )
    
    result = s3_client.delete_object(Bucket = target_s3['Bucket'], Key = delete_file_s3)
