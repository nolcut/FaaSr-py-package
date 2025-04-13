import json
import sys
from collections import defaultdict
from jsonschema import validate
from jsonschema.exceptions import ValidationError


def validate_json(payload):
    """
    This method is used to verify that the JSON payload is compliant with the FaaSr schema
    """
    #Open FaaSr schema
    # to-do: schema path
    with open('FaaSr.schema.json') as f:
        schema = json.load(f)
    
    #Compare payload against FaaSr schema and except if they do not match
    try:
        validate(instance=payload, schema=schema)
    except ValidationError as e:
        err_msg = '{\"faasr_validate_json\":\"JSON not compliant with FaaSr schema : ' + e.message,'\"}\n'
        print(err_msg)
    return True


def is_cyclic(adj_graph, curr, visited, stack):
    """This recursive function checks if there is a cycle in a directed
    graph specified by a dictionary of (parent: list[child]) pairs

    parameters:
        adj_graph(dict): adjacency list for graph
        curr(str): current node
        visited(set): set of visited nodes 
        stack(list[]): list of nodes in recursion call stack
    """
    # if the current node is in the recursion call
    # stack then there must be a cycle in the graph
    if curr in stack:
        return True
    
    # add current node to recursion call stack and visited set
    visited.add(curr)
    stack.append(curr)

    # check each successor for cycles, recursively calling is_cyclic()
    for child in adj_graph[curr]:
        if child not in visited and is_cyclic(adj_graph, child, visited, stack):
            err = '{\"faasr_check_workflow_cycle\":\"Function loop found from node ' + curr + ' to ' + child + '\"}\n'
            print(err)
            sys.exit(1)
        elif child in stack:
            err = '{\"faasr_check_workflow_cycle\":\"Function loop found from node ' + curr + ' to ' + child + '\"}\n'
            print(err)
            sys.exit(1)
    
    # no more successors to visit for this branch and no cycles found
    # remove current node from recursion call stack
    stack.pop()
    return False


def check_dag(payload: dict):
    """
    This method checks for cycles, repeated function names, or unreachable nodes in the workflow
    and aborts if it finds any

    returns adjacency graph for the workflow
    """

    # create adjacency list
    adj_graph = defaultdict(list)

    # build the adjacency list
    for func in payload['FunctionList'].keys():
        invoke_next = payload['FunctionList'][func]['InvokeNext']
        if isinstance(invoke_next, str):
            invoke_next = [invoke_next]
        for child in invoke_next:
            adj_graph[func].append(child)

    # create empty recursion call stack
    stack = []

    # create empty visited set
    visited = set()

    # create predecessor list
    pre = predecessors_list(adj_graph)
    
    # find first function
    start = False
    for func in payload['FunctionList']:
        if len(pre[func]) == 0:
            start = True
            first_func = func

    # ensure there is an initial action
    if start is False:
        err_msg = '{\"faasr_check_workflow_cycle\":\"function loop found: no initial action\"}\n'
        print(err_msg)
        sys.exit(1)

    #check for cycles
    is_cyclic(adj_graph, first_func, visited, stack)

    for func in payload['FunctionList'].keys():
        if func not in visited:
            err = '{\"check_workflow_cycle\":\"unreachable state is found in ' + func + '\"}\n'
            print(err)
            sys.exit(1)

    return pre[payload['FunctionInvoke']]
    

def predecessors_list(adj_graph):
    """This function returns creates a graph mapping functions to their predecessor functions
    as a dictionary (function: list[predecessor])
    
    parameters:
        adj_graph(dict): adjacency list for graph (function: successor)
    """
    pre = defaultdict(list)
    for func1 in adj_graph:
        for func2 in adj_graph[func1]:
            pre[func2].append(func1)
    return pre 


# replace filler credentials in payload with real credentials
def faasr_replace_values(payload, secrets):
    ignore_keys = ["FunctionGitRepo", "FunctionList", "FunctionCRANPackage", "FunctionGitHubPackage"]
    for name in payload:
        if name not in ignore_keys:
            if isinstance(payload[name], list) or isinstance(payload[name], dict):
                payload[name] = faasr_replace_values(payload[name], secrets)
            elif payload[name] in secrets:
                payload[name] = secrets[payload[name]]
    return payload