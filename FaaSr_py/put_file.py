import boto3
import re
from pathlib import Path
from . import global_faasr as faasr_env


def faasr_put_file(
    local_file, remote_file, server_name="", local_folder=".", remote_folder="."
):
    """
    This function puts an object in S3 bucket
    """
    # to-do: config
    faasr = faasr_env.get_faasr()
    config = faasr.get_payload_dict()

    if server_name == "":
        server_name = config["DefaultDataStore"]

    if server_name not in config["DataStores"]:
        err_msg = '{"faasr_put_file":"Invalid data server name: ' + server_name + '"}\n'
        print(err_msg)
        quit()

    target_s3 = config["DataStores"][server_name]

    # Remove "/" in the folder & file name to avoid situations:
    # 1: duplicated "/" ("/remote/folder/", "/file_name")
    # 2: multiple "/" by user mistakes ("//remote/folder//", "file_name")
    # 3: file_name ended with "/" ("/remote/folder", "file_name/")
    remote_folder = re.sub(r"/+", "/", remote_folder.rstrip("/"))
    remote_file = re.sub(r"/+", "/", remote_file.rstrip("/"))

    put_file_s3 = f"{remote_folder}/{remote_file}"

    local_file_path = Path(local_file)
    if local_folder == "." and local_file == local_file_path.expanduser().resolve(
        strict=False
    ):
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
