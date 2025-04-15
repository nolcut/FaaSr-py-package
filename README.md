# What is FaaSr_py
FaaSr_py is a package for Python that makes it easier to orchestrate serverless workflows by abstracting away provider specific APIs and allowing for DAG defined execution flow. 
Currently, FaaSr_py supports three FaaS platforms: GitHub actions, OpenWhisk (soon), and AWS Lambda (soon). Functions within the workflow can be written in either R or Python. 
Every function in the DAG is a container that will run on the user’s FaaS server of choice. 
This package provides backend tools for S3 and DAG validation, package installation, function fetching and execution, a triggering mechanism for the next functions in the DAG, 
and other validation functions to ensure proper workflow behavior. Workflows leverages S3 for persistent data-storage, with a server side API to orchestrate I/O within user functions.

# Using FaaSr_py
To use FaaSr_py, you simply need to create a workflow JSON (see below) and host your functions on GitHub. Then, you can register, invoke, and set triggers for your workflows using the CLI (soon).    

FaaSr_py abstracts away S3 interactions, so all you need to do is use the serverside API to perform I/O interactions within your functions. The available functions are the following:

```
FaaSr_py.faasr_get_file(local_file*, remote_file*, server_name, local_folder, remote_folder)
Downloads a file from specified S3 server to your local directory

FaaSr_py.faasr_put_file(local_file*, remote_file*, server_name, local_folder, remote_folder)
Uploads local_file to specified S3 server

FaaSr_py.faasr_delete_file(remote_file*, server_name, remote_folder)
Deletes remote_file from specified S3 server

FaaSr_py.faasr_log(msg*)
Logs a message to your default S3 logging server for the current workflow

FaaSr_py.get_folder_list(server_name, faasr_prefix)
Lists all of the objects in specified S3 server (within the faasr bucket) with prefix
```
An * indicates that the parameter is required

Note: if you do not specify server_name, then your default data store will be used 

# Workflow builder
The GUI for creating a workflow can be found here: [FaaSr-JSON-Builder Shiny app](https://faasr.shinyapps.io/faasr-json-builder/)

# Basic structure of an action in the workflow:
1. Workflow JSON is validated
2. InvocationID is assigned and the log folder is created (if they aren't already)
3. User function is executed
4. Subsequent actions are invoked

# JSON workflow format

## “ComputeServers”
List of FaaS compute servers. Each FaaS type requires different attributes:

#### GitHub Actions:
Required: FaaSType, UserName, ActionRepoName, Branch

#### OpenWhisk:
Required: FaaSType, Endpoint, Namespace  
Optional: SSL

#### AWS Lambda:
Required: FaaSType, Region

## “DataStores”
List of your S3 data stores

**For each data store**  
Required: Bucket, Endpoint, Region
Other attributes: Writable

## “FunctionList”
List of actions in the workflow

**For each action**  
Required: FaaSServer, FunctionName, InvokeNext, Arguments  
Optional: Rank

## “ActionContainers” 
A mapping of action names to containers

**For each container**  
Required: (action name: container)

## “FunctionGitRepo”:
A mapping of user functions to the GitHub repository that they’re hosted on (in the form username/repo)

**For each function**  
Required: (FunctionName: repo)

## “FunctionCRANPackage”:
User R code from CRAN package (for R functions)

**For each function:**  
Required: (FunctionName: package)

## “FunctionGitHubPackage”:
User code from GitHub package

**For each function**  
Required: (FunctionName: package)

## “FunctionPyPiPackage” (soon)
User code from Python package

**For each function**. 
Required: (FunctionName: package)

## Other information:
### “FunctionInvoke”  
The name of the function from function list to be invoked by this action

### “InvocationID”  
The unique ID used throughout all action invocations for this workflow

### “LoggingDataStore”  
The name of the logging server to use – must match an s3 server defined in DataStores

### “DefaultDataStore”  
The name of the default data store server to use – must match an s3 server defined in DataStores

### “FaaSrLog”  
The name of the log file’s folder
