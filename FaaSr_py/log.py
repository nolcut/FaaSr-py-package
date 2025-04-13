import boto3
import os
import sys
from . import global_faasr as faasr_env


def faasr_log(log_message):
    """
    This function logs a message in the FaaSr log
    """
    payload = faasr_env.get_faasr()

    # Get the logging data store from payload
    log_server_name = payload.get_logging_server()

    if log_server_name not in payload["DataStores"]:
        err_msg = '{"faasr_log":"Invalid logging server name: ' + log_server_name + '"}\n'
        print(err_msg)
        sys.exit(1)

    log_server = payload["DataStores"][log_server_name]

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=log_server["AccessKey"],
        aws_secret_access_key=log_server["SecretKey"],
        region_name=log_server["Region"],
        endpoint_url=log_server["Endpoint"],
    )

    # Path to log file
    log_folder = f"{payload['FaaSrLog']}/{payload['InvocationID']}"
    log_file = f"{log_folder}/{payload['FunctionInvoke']}.txt"

    if not os.path.isdir(log_folder):
        try:
            os.makedirs(log_folder)
        except FileExistsError:
            print("File exists: cannot make log_folder (faasr_log)")

    check_log_file = s3_client.list_objects_v2(
        Bucket=log_server["Bucket"], Prefix=log_file
    )

    # Download the log if it exists
    if "Content" in check_log_file and len(check_log_file["Content"]) != 0:
        if os.path.exists(log_file):
            os.remove(log_file)
        s3_client.download_file(
            Bucket=log_server["Bucket"], Key=log_file, Filename=log_file
        )

    # Write to log
    logs = f"{log_message}\n"
    with open(log_file, "a") as f:
        f.write(logs)

    # Upload log back to S3
    with open(log_file, "rb") as log_data:
        s3_client.put_object(Bucket=log_server["Bucket"], Body=log_data, Key=log_file)

    # to-do logging for function completion
