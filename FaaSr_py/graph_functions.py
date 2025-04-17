import json
import sys
import re
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

    returns a list of predecessors for the current function
    """

    adj_graph = defaultdict(list)

    # Build the adjacency list
    for func in payload['FunctionList'].keys():
        invoke_next = payload['FunctionList'][func]['InvokeNext']
        if isinstance(invoke_next, str):
            invoke_next = [invoke_next]
        for child in invoke_next:
            child = re.sub(r"\(.*", "", child)
            adj_graph[func].append(child)

    # Initialize empty recursion call stack
    stack = []

    # Initialize empty visited set
    visited = set()

    # Initialize predecessor list
    pre = predecessors_list(adj_graph)
    
    # Find initial function in the graph
    start = False
    for func in payload['FunctionList']:
        if len(pre[func]) == 0:
            start = True
            # This function stores the first function with no predecessors
            # In the cases where there is multiple functions with no
            # predecessors, an unreachable state error will occur later
            first_func = func
            break

    # Ensure there is an initial action
    if start is False:
        err_msg = '{\"faasr_check_workflow_cycle\":\"function loop found: no initial action\"}\n'
        print(err_msg)
        sys.exit(1)

    # Check for cycles
    is_cyclic(adj_graph, first_func, visited, stack)

    # Check if all of the functions have been visited by the DFS
    # If not, then there is an unreachable state in the graph
    for func in payload['FunctionList']:
        if func.split('(')[0] not in visited:
            err = '{\"check_workflow_cycle\":\"unreachable state found: ' + func + '\"}\n'
            print(err)
            sys.exit(1)

    return pre[payload['FunctionInvoke']]
    

def predecessors_list(adj_graph):
    """This function returns creates a dict mapping functions to their predecessor functions
    
    parameters:
        adj_graph(dict): adjacency list for graph (function: successor)
    """
    pre = defaultdict(list)
    for func1 in adj_graph:
        for func2 in adj_graph[func1]:
            pre[func2].append(func1)
    return pre 


def faasr_replace_values(payload, secrets):
    """
    Replaces filler secrets in a payload with real credentials
    """
    ignore_keys = ["FunctionGitRepo", "FunctionList", "FunctionCRANPackage", "FunctionGitHubPackage"]
    for name in payload:
        if name not in ignore_keys:
            # If the value is a list or dict, recurse
            if isinstance(payload[name], list) or isinstance(payload[name], dict):
                payload[name] = faasr_replace_values(payload[name], secrets)
            # Replace value
            elif payload[name] in secrets:
                payload[name] = secrets[payload[name]]
    return payload