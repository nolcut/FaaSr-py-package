# What is FaaSr
FaaSr is a serverless framework that makes it easier to orchestrate workflows by abstracting away provider specific APIs and allowing for DAG defined execution flow. 
Currently, FaaSr supports three FaaS platforms: GitHub actions, OpenWhisk, and AWS Lambda. Functions within the workflow can be written in either R or Python and are ran in a FaaSr container on the user’s FaaS server of choice. Workflows leverage S3 for persistent data-storage, with a server side API to orchestrate I/O within user functions.
This package provides backend Python tools for S3 and DAG validation, package installation, function fetching and execution, a triggering mechanism for the next functions in the DAG, 
and other validation functions to ensure proper workflow behavior.

# Using
To use FaaSr, you simply need to create a workflow JSON (see below) and host your functions on GitHub. Then, you can register, invoke, and set triggers for your workflows using the CLI (soon).    

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

# Useful containers
For running Python functions on GitHub Action, you can use the following container: 
```
ghcr.io/nolcut/github-actions-python:0.1.3
```
For running Python functions on OpenWhisk instances, use the following: 
```
nolcut/ow-faasr-image:0.1.0
```

