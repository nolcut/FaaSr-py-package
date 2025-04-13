import boto3
import re
from pathlib import Path
from . import global_faasr as faasr_env


def faasr_put_file(local_file, remote_file, server_name="", local_folder=".", remote_folder="."):
    """
    This function puts an object in S3 bucket
    """
    # to-do: config
    config = faasr_env.get_faasr()

    # Get the server name from payload if it is not provided
    if server_name == "":
        server_name = config["DefaultDataStore"]

    # Ensure that the server name is valid
    if server_name not in config["DataStores"]:
        err_msg = '{"faasr_put_file":"Invalid data server name: ' + server_name + '"}\n'
        print(err_msg)
        quit()

    # Get the S3 server to put the file in
    target_s3 = config["DataStores"][server_name]

    # Remove "/" in the folder & file name to avoid situations:
    # 1: duplicated "/" ("/remote/folder/", "/file_name")
    # 2: multiple "/" by user mistakes ("//remote/folder//", "file_name")
    # 3: file_name ended with "/" ("/remote/folder", "file_name/")
    remote_folder = re.sub(r"/+", "/", remote_folder.rstrip("/"))
    remote_file = re.sub(r"/+", "/", remote_file.rstrip("/"))

    # Path for remote file
    put_file_s3 = f"{remote_folder}/{remote_file}"

    # If the local file exists in the current working directory, then set put_file to the name of the file
    # Otherwise, construct valid path to the file
    local_file_path = Path(local_file)
    # Check if local_folder is "." and local_file contains path information
    if local_folder == "." and str(local_file_path) != local_file_path.name:
        # local_file has directory components
        local_folder = str(local_file_path.parent)
        put_file = local_file
    else:
        # remove trailing '/' and replace instances of multiple '/' in a row with '/'
        local_folder = re.sub(r"/+", "/", local_folder.rstrip("/"))
        local_file = re.sub(r"/+", "/", local_file.rstrip("/"))
        put_file = f"{local_folder}/{local_file}"

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=target_s3["AccessKey"],
        aws_secret_access_key=target_s3["SecretKey"],
        region_name=target_s3["Region"],
        endpoint_url=target_s3["Endpoint"],
    )

    with open(put_file, 'rb') as put_data:
        result = s3_client.put_object(
            Bucket=target_s3["Bucket"], Body=put_data, Key=put_file_s3
        )
