import random
import time
import sys
import boto3



def faasr_rsm(faasr_payload):
    """
    Read and set memomry implemntation for creating a faasr lock in s3
    """

    # set env for flag and lock
    flag_content = random.randint(1, 2**31 - 1)
    flag_path = f"{faasr_payload['FaaSrLog']}/{faasr_payload['InvocationID']}/{faasr_payload['FunctionInvoke']}/flag/"
    flag_name = flag_path + str(flag_content)
    lock_name = f"{faasr_payload['FaaSrLog']}/{faasr_payload['InvocationID']}/{faasr_payload['FunctionInvoke']}./lock"

    # set env for storage
    logging_server = faasr_payload.get_logging_server()
    target_s3 = faasr_payload['DataStores'][logging_server]

    print(target_s3)

    s3_client = boto3.client(
                            's3',
                            aws_access_key_id = target_s3['AccessKey'],
                            aws_secret_access_key = target_s3['SecretKey'],
                            region_name = target_s3['Region'],
                            endpoint_url = target_s3['Endpoint']
    )

    cnt = 0
    max_cnt = 4
    max_wait = 13

    #
    while(True):
        # put an object with the name log/functionname/flag/{random_intger} into the S3 bucket
        response = s3_client.put_object(Key=flag_name, Bucket=target_s3['Bucket'])
        # if someone has a flag, then delete flag and try again
        if(anyone_else_interested(target_s3, flag_path, flag_name)):
            s3_client.delete_object(Bucket = target_s3['Bucket'], Key = flag_name)
            if(cnt > max_cnt):
                print('max spinning')
                time.sleep(2 ** max_cnt)
                cnt += 1
                # if faasr_rsm exceeds the max time to acquire, then it returns false
                if(cnt > max_wait):
                    err_msg = '{\"faasr_rsm\":\"Lock Timeout\"}\n'
                    print(err_msg)
                    sys.exit(1)
            else:
                print('spinning')
                time.sleep(2 ** cnt)
                cnt += 1
        else:
            # check if a lock exists. If it does, then return false; otherwise, write lock to S3
            check_lock = s3_client.list_objects_v2(Bucket=target_s3['Bucket'], Prefix=lock_name)
            if 'Contents' not in check_lock or len(check_lock['Contents']) == 0:
                s3_client.put_object(Bucket = target_s3['Bucket'], Key = lock_name, Body = str(flag_content))
                s3_client.delete_object(Bucket = target_s3['Bucket'], Key = flag_name)
                return True
            else:
                print('failed to acquire lock')
                return False
            

def faasr_acquire(faasr):
    """
    This function acquires the lock and leaves a lock object in s3
    """
    # call faasr_rsm to get a lock
    lock = faasr_rsm(faasr)
    cnt = 0
    max_cnt = 4
    max_wait = 13
    # if function acquires lock, then loop breaks
    while True:
        # if the lock is true (acquired), then break out of while loop
        if lock:
            return True
        else:
            if (cnt > max_cnt):
                print('max acquire spining')
                time.sleep(2 ** max_cnt)
                cnt += 1
                if (cnt > max_wait):
                    err_msg = '{\"faasr_acquire\":\"Lock Acquire Timeout\"}\n'
                    print(err_msg)
                    sys.exit(1)
            else:
                print('acquire spinning')
                time.sleep(2 ** cnt)
                cnt += 1
        lock = faasr_rsm(faasr)


def faasr_release(faasr_payload):
    """
    This function releases the lock by deleting the lock object from s3
    """

    lock_name = f"{faasr_payload['FaaSrLog']}/{faasr_payload['InvocationID']}/{faasr_payload['FunctionInvoke']}./lock"

    logging_server = faasr_payload.get_logging_server()
    target_s3 = faasr_payload['DataStores'][logging_server]

    s3_client = boto3.client(
                            's3',
                            aws_access_key_id = target_s3['AccessKey'],
                            aws_secret_access_key = target_s3['SecretKey'],
                            region_name = target_s3['Region'],
                            endpoint_url = target_s3['Endpoint']
    )

    s3_client.delete_object(Bucket = target_s3['Bucket'], Key = lock_name)



def anyone_else_interested(target_s3, flag_path, flag_name):
    """
    This function checks flags to see whether or not other
    functions are trying to acquire the lock
    """
    s3_client = boto3.client(
            's3',
            aws_access_key_id = target_s3['AccessKey'],
            aws_secret_access_key = target_s3['SecretKey'],
            region_name = target_s3['Region'],
            endpoint_url = target_s3['Endpoint']
    )

    # pool is a list of flag names
    check_pool = s3_client.list_objects_v2(Bucket=target_s3['Bucket'], Prefix=flag_path)
    pool = [x['Key'] for x in check_pool['Contents']]
    print(pool)

    if(flag_name in pool and len(pool) == 1):
        return False
    else:
        return True
