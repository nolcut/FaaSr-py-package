import boto3
from . import global_faasr as faasr_env

def faasr_get_folder_list(server_name="", faasr_prefix = ""):
    """
    This function gets a list of objects in the S3 bucket
    """
    # to-do: config
    faasr = faasr_env.get_faasr()
    config = faasr.get_payload_dict()


    if server_name == "":
        server_name = config['DefaultDataStore']

    if server_name not in config['DataStores']:
        err_msg = '{\"faasr_get_folder_list\":\"Invalid data server name: ' + server_name + '\"}\n'
        print(err_msg)
        quit()
    
    target_s3 = config['DataStores'][server_name]

    s3_client = boto3.client(
        's3',
        aws_access_key_id = target_s3['AccessKey'],
        aws_secret_access_key = target_s3['SecretKey'],
        region_name = target_s3['Region'],
        endpoint_url = target_s3['Endpoint']
        )
    
    result = s3_client.list_objects_v2(Bucket = target_s3['Bucket'], Prefix = faasr_prefix)
    result = [content['Key'] for content in result['Contents']]
    result = [obj for obj in result if not obj.endswith('/')]

    return result