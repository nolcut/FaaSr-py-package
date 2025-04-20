import uuid
import json
import boto3
import random
import re
import copy
import requests
import os
import sys
import FaaSr_py
from collections import defaultdict
from .s3_helper_functions import validate_uuid
from .graph_functions import validate_json


class FaaSr:
    def __init__(self, faasr_payload: json):
        if validate_json(faasr_payload):
            self.payload_dict = faasr_payload
        else:
            ValueError("Payload failed to validate.")

    def __getitem__(self, key):
        try:
            return self.payload_dict[key]
        except KeyError as e:
            print(str(e))

    def __setitem__(self, key, value):
        self.payload_dict[key] = value

    def __contains__(self, item):
        return item in self.payload_dict

    def get_payload_dict(self):
        return self.payload_dict

    def set_payload_dict(self, payload_dict):
        if validate_json(json.dump(payload_dict)):
            self.payload_dict = payload_dict

    def get_payload_json(self):
        try:
            json_data = json.dump(self.payload_dict)
        except TypeError:
            err_msg = '{"get_payload_json":"self.payload_dict must be a dictionary"}\n'
            print(err_msg)

    def s3_check(self):
        """
        This method ensures that all of the S3 data stores are valid and reachable
        """
        
        # Iterate through all of the data stores
        for server in self.payload_dict["DataStores"].keys():
            # Get the endpoint and region
            server_endpoint = self.payload_dict["DataStores"][server]["Endpoint"]
            server_region = self.payload_dict["DataStores"][server]["Region"]
            # Ensure that endpoint is a valid http address
            if not server_endpoint.startswith("http"):
                error_message = f'{{"s3_check":"Invalid Data store server endpoint {server}}}\n'
                print(error_message)
                sys.exit(1)

            # If the region is empty, then use defualt 'us-east-1'
            if len(server_region) == 0 or server_region == "":
                self.payload_dict["DataStores"][server]["Region"] = "us-east-1"
            if (
                "Anonynmous" in self.payload_dict["DataStores"][server]
                and len(self.payload_dict["DataStores"][server]["Anonymous"]) != 0
            ):
                # to-do: continue if anonymous is true
                print("anonymous param not implemented")

            s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.payload_dict["DataStores"][server]["AccessKey"],
                aws_secret_access_key=self.payload_dict["DataStores"][server]["SecretKey"],
                region_name=self.payload_dict["DataStores"][server]["Region"],
                endpoint_url=self.payload_dict["DataStores"][server]["Endpoint"],
            )
            # Use boto3 head bucket to ensure that the bucket exists and that we have acces to it
            try:
                bucket_check = s3_client.head_bucket(
                    Bucket=self.payload_dict["DataStores"][server]["Bucket"]
                )
            except Exception as e:
                error_message = f'{{"s3_check":"S3 server {server} failed with message: {e}}}\n'
                print(error_message)
                sys.exit(1)

    def init_log_folder(self):
        """
        This method initializes a faasr log folder if one has not already been created
        """
        # Create invocation ID if one is not already present
        if validate_uuid(self.payload_dict["InvocationID"]) == False:
            ID = uuid.uuid4()
            self.payload_dict["InvocationID"] = str(ID)

        # Log invocation ID
        faasr_msg = f'{{"init_log_folder":"InvocationID for the workflow: {self.payload_dict["InvocationID"]}"}}\n'
        print(faasr_msg)

        # Get the target S3 server
        target_s3 = self.get_logging_server()
        s3_log_info = self.payload_dict["DataStores"][target_s3]

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=s3_log_info["AccessKey"],
            aws_secret_access_key=s3_log_info["SecretKey"],
            region_name=s3_log_info["Region"],
            endpoint_url=s3_log_info["Endpoint"],
        )

        # If no name for log specified, use 'FaaSrLog'
        if self.payload_dict["FaaSrLog"] is None or self.payload_dict["FaaSrLog"] == "":
            self.payload_dict["FaaSrLog"] = "FaaSrLog"

        # Get name for log folder
        idfolder = f"{self.payload_dict["FaaSrLog"]}/{self.payload_dict["InvocationID"]}/"
        
        # Check contents of log folder
        check_id_folder = s3_client.list_objects_v2(
            Prefix=idfolder, Bucket=s3_log_info["Bucket"]
        )

        # If there already is a log, log error and abort; otherwise, create log
        if "Content" in check_id_folder and len(check_id_folder["Content"]) != 0:
            err = f'{{"init_log_folder":"InvocationID already exists: {self.payload_dict["InvocationID"]}"}}\n'
            print(err)
            sys.exit(1)
        else:
            s3_client.put_object(Bucket=s3_log_info["Bucket"], Key=idfolder)

    def abort_on_multiple_invocations(self, pre):
        """
        This method is invoked when the current function has multiple predecessors
        and aborts if they have not finished or the current function instance was not
        the first to write to the candidate set
        """

        # Get S3 logging data store
        target_s3 = self.get_logging_server()

        if target_s3 not in self.payload_dict["DataStores"]:
            err = f'{"abort_on_multiple_invocation":"Invalid data server name: {target_s3}"}\n'
            print(err)
            sys.exit(1) 
        
        s3_log_info = self.payload_dict["DataStores"][target_s3]

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=s3_log_info["AccessKey"],
            aws_secret_access_key=s3_log_info["SecretKey"],
            region_name=s3_log_info["Region"],
            endpoint_url=s3_log_info["Endpoint"],
        )

        # ID folder is of the form {faasr log}/{InvocationID}
        id_folder = (
            f"{self.payload_dict['FaaSrLog']}/{self.payload_dict['InvocationID']}"
        )

        # If a predecessor has a rank attribute, then we need to ensure
        # That all concurrent invocations of that function have finished
        for pre_func in pre:
            if "Rank" in self.payload_dict["FunctionList"][pre_func] and len(self.payload_dict["FunctionList"][pre_func]["Rank"]) != 0:
                parts = self.payload_dict["FunctionList"][pre_func]["Rank"].split("/")
                pre.remove(pre_func)
                if len(parts) != 2:
                    err_msg = f'{{\"faasr_abort_on_multiple_invocation\": \"Error with rank field in function: {pre_func}\"}}'
                    print(err_msg)
                    sys.exit(1)
                for rank in range(1, int(parts[1]) + 1):
                    pre.append(f"{pre_func}.{rank}")

        # First, we check if all of the other predecessor actions are done
        # To do this, we check a file called func.done in S3, and see if all of the other actions have
        # written that they are "done"
        # If not all of the predecessor's are finished, then this action aborts
        s3_list_object_response = s3_client.list_objects_v2(
            Bucket=s3_log_info["Bucket"], Prefix=id_folder
        )
        s3_contents = s3_list_object_response["Contents"]

        # Extract paths from s3 contents
        s3_object_keys = []
        for object in s3_contents:
            if "Key" in object:
                s3_object_keys.append(object["Key"])

        for func in pre:
            # check if all of the predecessor func.done objects exist
            done_file = f"{id_folder}/{func}.done"
            # if the object does exist, do nothing
            # if it does not exist, then the current function still is waiting for
            # a predecessor and must wait
            if done_file not in s3_object_keys:
                res_msg = '{"faasr_abort_on_multiple_invocations":"not the last trigger invoked - no flag"}\n'
                print(res_msg)
                sys.exit(1)

        # Step 2: This code is reached only if all predecessors are done. Now we need to select only one Action to proceed,
        # while all other Actions should abort
        # We use a lock implementation over S3 to implement atomic read/modify/write operations and avoid a race condition
        # Between lock acquire and release, we do the following:
        # 1) download the "FunctionInvoke.candidate" file from S3. The candidate file stores random numbers generated by
        #    each action which have been invoked for this function after all predecessors are done.
        # 2) append a random number to the local file, which is generated by this Action
        # 3) upload the file back to the S3 bucket
        # 4) download the file from S3

        # to-do faasr acquire lock
        FaaSr_py.faasr_acquire(self)

        random_number = random.randint(1, 2**31 - 1)

        if not os.path.isdir(id_folder):
            os.makedirs(id_folder, exist_ok=True)

        candidate_path = f"{id_folder}/{self.payload_dict['FunctionInvoke']}.candidate"

        # Get all of the objects in S3 with the prefix {id_folder}/{FunctionInvoke}.candidate
        s3_response = s3_client.list_objects_v2(
            Bucket=s3_log_info["Bucket"], Prefix=candidate_path
        )
        if 'Contents' in s3_response and len(s3_response["Contents"]) != 0:
            # Download candidate set
            if os.path.exists(candidate_path):
                os.remove(candidate_path)          
            s3_client.download_file(
                Bucket=s3_log_info["Bucket"],
                Key=candidate_path,
                Filename=candidate_path,
            )

        # Write unique random number to candidate file
        with open(candidate_path, "a") as cf:
            cf.write(str(random_number) + "\n")

        with open(candidate_path, "rb") as cf:
            # Upload candidate file back to S3
            s3_client.put_object(
                Body=cf, Key=candidate_path, Bucket=s3_log_info["Bucket"]
            )
            
        # Download candidate file to local directory again
        if os.path.exists(candidate_path):
            os.remove(candidate_path)
        s3_client.download_file(
            Bucket=s3_log_info["Bucket"], Key=candidate_path, Filename=candidate_path
        )

        # Release the lock
        FaaSr_py.faasr_release(self)

        # Abort if current function was not the first to write to the candidate set
        with open(candidate_path, "r") as updated_candidate_file:
            first_line = updated_candidate_file.readline().strip()
            first_line = int(first_line)
        if random_number != first_line:
            res_msg = '{"abort_on_multiple_invocations":"not the last trigger invoked - random number does not match"}\n'
            print(res_msg)
            sys.exit(1)

    def get_logging_server(self):
        """
        Returns the default logging data store for the payload
        """
        if self.payload_dict["LoggingDataStore"] is None:
            logging_server = self.payload_dict["DefaultDataStore"]
        else:
            logging_server = self.payload_dict["LoggingDataStore"]
        return logging_server
    
    def run_user_function(self, imported_functions):
        """
        This method runs the user's code that was imported
        """
        faasr_dict = self.payload_dict
        curr_action = faasr_dict["FunctionInvoke"]
        func_name = faasr_dict["FunctionList"][curr_action]["FunctionName"]

        # Ensure user function is in imported_functions
        if imported_functions and func_name in imported_functions:
            user_function = imported_functions[func_name]
            # add faasr_py to user_function's namespace
            user_function.__globals__['FaaSr_py'] = FaaSr_py
        else:
            err_msg = '{"faasr_run_user_function":"Cannot find Function ' + func_name + ', check the name and sources"}\n'
            result_2 = FaaSr_py.faasr_log(err_msg)
            print(err_msg)
            sys.exit(1)

        # Get args for function
        user_args = self.get_user_function_args()

        # Run user function
        try:
            user_function(**user_args)
        except Exception as e:
            nat_err_msg = f'"faasr_run_user_function":Errors in the user function: {e}'
            err_msg = '{"faasr_run_user_function":"Errors in the user function: ' + str(self.payload_dict["FunctionInvoke"]) + ', check the log for the detail "}\n'
            result_2 = FaaSr_py.faasr_log(nat_err_msg)
            print(nat_err_msg)
            print(err_msg)
            sys.exit(1)

        # At this point, the Action has finished the invocation of the User Function
        # We flag this by uploading a file with the name FunctionInvoke.done with contents True to the S3 logs folder
        # Check if directory already exists. If not, create one
        log_folder = f"{faasr_dict['FaaSrLog']}/{faasr_dict['InvocationID']}"
        if not os.path.isdir(log_folder):
            os.makedirs(log_folder)
        curr_action = faasr_dict["FunctionInvoke"]
        if "Rank" in faasr_dict["FunctionList"][curr_action]:
            rank_unsplit = faasr_dict["FunctionList"][curr_action]["Rank"]
            if len(rank_unsplit) != 0:
                rank = rank_unsplit.split("/")[0]
                faasr_dict["FunctionInvoke"] = f"{faasr_dict['FunctionInvoke']}.{rank}"
        file_name = f"{faasr_dict['FunctionInvoke']}.done"
        file_path = f"{log_folder}/{file_name}"
        with open(file_path, "w") as f:
            f.write("True")
        
        # Put .done file in S3
        FaaSr_py.faasr_put_file(
            local_folder=log_folder,
            local_file=file_name,
            remote_folder=log_folder,
            remote_file=file_name,
        )
    
    def get_user_function_args(self):
        """
        Returns function arguments
        """
        user_action = self.payload_dict["FunctionInvoke"]

        args = self.payload_dict["FunctionList"][user_action]["Arguments"]
        if args is None:
            return []
        else:
            return args
        
    def trigger(self):
        """
        This method triggers the next actions in the DAG
        """
        # Get a list of the next functions to invoke
        faasr_dict = self.payload_dict
        curr_func = faasr_dict['FunctionInvoke']
        invoke_next = faasr_dict['FunctionList'][curr_func]['InvokeNext']
        if isinstance(invoke_next, str):
            invoke_next = [invoke_next]

        # If there is no more triggers, then return
        if len(invoke_next) == 0:
            msg = '{\"faasr_trigger\":\"no triggers for ' + curr_func + '\"}\n'
            print(msg)
            FaaSr_py.faasr_log(msg)
            return

        for next_function in invoke_next:
            # Split function name and rank if needed
            parts = re.split(r"[()]", next_function)
            if len(parts) > 1:
                next_function = parts[0]
                rank_num = int(parts[1])
            else:
                rank_num = 1

            # Change FunctionInvoke to the name of the next function
            faasr_dict['FunctionInvoke'] = next_function

            # Determine the name of the faas server for the next function
            next_server = faasr_dict['FunctionList'][next_function]['FaaSServer']

            for rank in range(1, rank_num + 1):
                # Store rank of next function
                if(rank_num > 1):
                    faasr_dict["FunctionList"][next_function]["Rank"] = f"{rank}/{rank_num}"
                
                # Abort if the next functions server is not in the server list
                if next_server not in faasr_dict["ComputeServers"]:
                    err_msg = '{\"faasr_trigger\":\"invalid server name: ' + next_server + '\"}\n'
                    print(err_msg)
                    FaaSr_py.faasr_log(err_msg)
                    break
                
                next_compute_server = faasr_dict["ComputeServers"][next_server]

                # Get faas type of next function's compute server
                next_server_type = next_compute_server["FaaSType"]

                match(next_server_type):
                    # to-do: OW and lambda testing
                    case "OpenWhisk":
                        print("OpenWhisk trigger not implemented (need to test)")
                        # Get ow credentials
                        endpoint = next_compute_server["Endpoint"]
                        api_key = next_compute_server["API.key"]
                        api_key = api_key.split(":")
                        
                        # Check if we should use ssl
                        if "SSL" not in next_compute_server or len(next_compute_server["SSL"]) == 0:
                            ssl = True
                        else:
                            if next_compute_server["SSL"].lower() != 'false': 
                                ssl = True
                            else:
                                ssl = False

                        # Get the namespace of the OW server
                        namespace = next_compute_server["Namespace"]
                        actionname = next_function
                        
                        # Append https:// front to endpoint if needed
                        if not endpoint.startswith("http"):
                            endpoint = f"https://{endpoint}"

                        # Create url for POST
                        url = f"{endpoint}/api/v1/namespaces/{namespace}/actions/{actionname}?blocking=false&result=false"

                        # Create headers for POST
                        headers = {
                            "accept": "application/json",
                            "Content-Type": "application/json"
                        }

                        # Create body for POST
                        json_payload = json.dumps(self.payload_dict)

                        # Issue POST request
                        try:
                            response = requests.post(url=url,
                                                    auth=(api_key[0], api_key[1]),
                                                    json=json_payload,
                                                    headers=headers,
                                                    verify=ssl)
                        except Exception as e:
                            if type(e) == requests.exceptions.ConnectionError:
                                err_msg = f"{{\"faasr_trigger\": \"OpenWhisk: Error invoking {faasr_dict['FunctionInvoke']} -- connection error\"}}"
                                print(err_msg)
                                sys.exit(1)
                            else:
                                err_msg = f"{{\"faasr_trigger\": \"OpenWhisk: Error invoking {faasr_dict['FunctionInvoke']} -- see logs\"}}"
                                nat_err_msg = err_msg = f"{{\"faasr_trigger\": \"OpenWhisk: Error invoking {faasr_dict['FunctionInvoke']} -- error: {e}\"}}"
                                print(err_msg)
                                FaaSr_py.faasr_log(nat_err_msg)
                                sys.exit(1)
                        
                        if response.status_code == 200 or response.status_code == 202:
                            succ_msg = f"{{\"faasr_trigger\":\"OpenWhisk: Succesfully invoked {faasr_dict['FunctionInvoke']}\"}}"
                            print(succ_msg)
                            FaaSr_py.faasr_log(succ_msg)
                        else:
                            err_msg = f"{{\"faasr_trigger\":\"OpenWhisk: Error invoking {faasr_dict['FunctionInvoke']} -- status code: {response.status_code}\"}}"
                            print(err_msg)
                            FaaSr_py.faasr_log(err_msg)
                        break

                    case "Lambda":
                        print("Lamba trigger not implemented (need to test)")
                        # Create client for invoking lambda function
                        lambda_client = boto3.client(
                            "lambda",
                            aws_access_key_id=next_compute_server["AccessKey"],
                            aws_secret_access_key=next_compute_server["SecretKey"],
                            region_name=next_compute_server["Region"],
                            )

                        # Invoke lambda function
                        try:
                            response = lambda_client.invoke(
                                FunctionName = invoke_next,
                                InvokeArgs = json.dumps(self.payload_dict),
                                InvocationType = "Event"
                                )
                        except Exception:
                                err_msg = f"{{\"faasr_trigger\": \"Error invoking function: {self.payload_dict['FunctionInvoke']} -- check API keys\"}}\n"
                                print(err_msg)
                                FaaSr_py.faasr_log(err_msg)     
                                continue                       
                        
                        if 'StatusCode' in response and str(response['StatusCode'])[0] == '2':
                            succ_msg = f"{{\"faasr_trigger\": \"Successfully invoked: {self.payload_dict['FunctionInvoke']}\"}}\n"
                            print(succ_msg)
                            FaaSr_py.faasr_log(succ_msg)
                        else:
                            try:
                                err_msg = f"{{\"faasr_trigger\": \"Error invoking function: {self.payload_dict['FunctionInvoke']} -- error: {response['FunctionError']}\"}}\n"
                                print(err_msg)
                                FaaSr_py.faasr_log(err_msg)
                            except Exception:
                                err_msg = f"{{\"faasr_trigger\": \"Error invoking function: {self.payload_dict['FunctionInvoke']} -- no response from AWS\"}}\n"
                                print(err_msg)
                                FaaSr_py.faasr_log(err_msg)

                        break
                    case "GitHubActions":
                        # Get env values for GH actions
                        pat = next_compute_server["Token"]
                        username = next_compute_server["UserName"]
                        reponame = next_compute_server["ActionRepoName"]
                        repo = f"{username}/{reponame}"
                        if not next_function.endswith('.ml') and not next_function.endswith('.yaml'):
                            workflow_file = f"{next_function}.yml"
                        else:
                            workflow_file = next_function
                        git_ref = next_compute_server["Branch"]

                        # Create copy of faasr payload
                        faasr_git = copy.deepcopy(faasr_dict)

                        # Hide credentials for compute servers before sending
                        for faas_js in faasr_git["ComputeServers"]:
                            match faasr_git["ComputeServers"][faas_js]["FaaSType"]:
                                case "GitHubActions":
                                    faasr_git["ComputeServers"][faas_js]["Token"] = f"{faas_js}_TOKEN"
                                    break
                                case "Lambda":
                                    faasr_git["ComputeServers"][faas_js]["AccessKey"] = f"{faas_js}_ACCESS_KEY"
                                    faasr_git["ComputeServers"][faas_js]["SecretKey"] = f"{faas_js}_SECRET_KEY"
                                    break
                                case "OpenWhisk":
                                    faasr_git["ComputeServers"][faas_js]["API.key"] = f"{faas_js}_API_KEY"
                                    break

                        # Hide credentials for data stores before sending
                        for data_js in faasr_git["DataStores"]:
                            faasr_git["DataStores"][data_js]["AccessKey"] = f"{data_js}_ACCESS_KEY"
                            faasr_git["DataStores"][data_js]["SecretKey"] = f"{data_js}_SECRET_KEY"

                        # Create payload input
                        json_payload = json.dumps(faasr_git, indent=4)
                        inputs = {"PAYLOAD": json_payload}

                        # Delete copy of faar payload
                        del faasr_git

                        # Create url for GitHub API
                        url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/dispatches"

                        # Create body for POST request
                        body = {"ref": git_ref, "inputs": inputs}

                        # Create headers for POST request
                        post_headers = {
                            "Authorization": f"token {pat}",
                            "Accept": "application/vnd.github.v3+json",
                            "X-GitHub-Api-Version": "2022-11-28"}

                        # Issue POST request
                        response = requests.post(
                            url = url,
                            json = body,
                            headers = post_headers
                        )

                        # Log response
                        if response.status_code == 204:
                            succ_msg = f"{{faasr_trigger: GitHub Action: Successfully invoked: {faasr_dict['FunctionInvoke']}}}\n"
                            print(succ_msg)
                            FaaSr_py.faasr_log(succ_msg)
                        elif response.status_code == 401:
                            err_msg = "{faasr_trigger: GitHub Action: Authentication failed, check the credentials}\n"
                            print(err_msg)
                            FaaSr_py.faasr_log(err_msg)
                        elif response.status_code == 404:
                            err_msg = f"{{faasr_trigger: GitHub Action: Cannot find the destination, check the repo name: {repo} and workflow name: {workflow_file}}}\n"
                            print(err_msg)
                            FaaSr_py.faasr_log(err_msg)
                        elif response.status_code == 422:
                            err_msg = f"{{faasr_trigger: GitHub Action: Cannot find the destination, check the ref: {faasr_dict["FunctionInvoke"]}\n}}"
                            print(err_msg)
                            FaaSr_py.faasr_log(err_msg)
                        else:
                            err_msg = "{faasr_trigger: GitHub Action: unknown error happens when invoke next function}\n"
                            print(err_msg)
                            FaaSr_py.faasr_log(err_msg)




                



            
