import sys
import json
from .faasr_payload import FaaSr

faasr = None

def initialize_faasr(payload_json: json):
    global faasr
    faasr = FaaSr(payload_json)
    return faasr

def get_faasr():
    if faasr is None:
        err_msg = '{\"get_faasr\":\"global faasr instance not initialized\"}\n'
        print(err_msg)
        sys.exit(1)
    return faasr