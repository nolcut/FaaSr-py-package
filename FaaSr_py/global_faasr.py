import sys
import json
from .faasr_payload import FaaSr

faasr = None # global faasr variable used for server side functions


# Initializes global faasr variable and returns a reference to it
def initialize_faasr(payload_json):
    global faasr
    faasr = FaaSr(payload_json)
    return faasr


# Gets a reference to the global faasr variable
def get_faasr():
    if faasr is None:
        err_msg = '{\"get_faasr\":\"global faasr instance not initialized (internal issue)\"}\n'
        print(err_msg)
        sys.exit(1)
    return faasr