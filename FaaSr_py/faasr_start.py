import json
import uuid
from .graph_functions import *
from .faasr_payload import FaaSr
from . import global_faasr as faasr_env


def faasr_start(faasr_payload):
    # Initialize a payload object 
    # (Note: this object is a reference to the global variable in global_faasr)
    faasr_obj = faasr_env.initialize_faasr(faasr_payload)

    # Verifies that the faasr payload is a DAG (directed acyclic graph), meaning that there is no cycles
    # If the payload is a DAG, then this function returns an predecessor list for the workflow
    # If the payload is not a DAG, then the action aborts
    pre = check_dag(faasr_obj.get_payload_dict())

    # Verfies the validity of S3 data stores, checvking the server status and ensuring that the specified bucket exists
    # If any of the S3 endpoints are invalid or any data store server are unreachable, the action aborts
    faasr_obj.s3_check()

    # to-do: Initializes a log folder if this is the entry action  
    if(len(pre) == 0):
        faasr_obj.init_log_folder()

    # If there are more than 1 predecessor, then only the final action invoked will sucessfully run
    # This function validates that the current action is the last invocation; otherwise, it aborts
    if (len(pre) > 1):
        faasr_obj.abort_on_multiple_invocations(pre)

    return faasr_obj