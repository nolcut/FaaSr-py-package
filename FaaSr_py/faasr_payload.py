import uuid
import json
import boto3
import random
import re
import copy
import requests
import os
import sys
from collections import defaultdict
from .s3_helper_functions import validate_uuid
from .graph_functions import validate_json
from .log import faasr_log
from .put_file import faasr_put_file



class FaaSr:
    def __init__(self, faasr_payload: json):
        if validate_json(faasr_payload):
            self.payload_dict = faasr_payload
        else:
            ValueError("Payload failed to validate.")

    def get_payload_dict(self):
        return self.payload_dict

    def set_payload_dict(self, payload_dict):
        if validate_json(json.dump(payload_dict)):
            self.payload_dict = payload_dict

    def get_payload_json(self):
        try:
            json_data = json.dump(self.payload_dict)
        except TypeError:
            err_msg = '{"get_json":"self.payload_dict must be a dictionary"}\n'
            print(err_msg)


    def s3_check(self):
        for server in self.payload_dict["DataStores"].keys():
            server_endpoint = self.payload_dict["DataStores"][server]["Endpoint"]
            server_region = self.payload_dict["DataStores"][server]["Region"]
            if len(server_endpoint) == 0 or server_endpoint == "":
                server_endpoint = self.payload_dict["DataStores"][server]["Endpoint"]
            else:
                if not server_endpoint.startswith("https://") and not server_endpoint.startswith("http://"):
                    error_message = '{"s3_check":"Invalid Data store server endpoint ' + server + '"}\n'
                    print(error_message)
                    sys.exit(1)
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
                aws_secret_access_key=self.payload_dict["DataStores"][server][
                    "SecretKey"
                ],
                region_name=self.payload_dict["DataStores"][server]["Region"],
                endpoint_url=self.payload_dict["DataStores"][server]["Endpoint"],
            )

            bucket_check = s3_client.head_bucket(
                Bucket=self.payload_dict["DataStores"][server]["Bucket"]
            )

            if not isinstance(bucket_check, dict):
                error_message = '{"s3_check":"S3 server ' + server + ' failed with message: Data store server unreachable"}' + "\n"
                print(error_message)
                sys.exit(1)

    def init_log_folder(self):
        """Initializes a faasr log folder if one has not already been created"""
        if validate_uuid(self.payload_dict["InvocationID"]) == False:
            ID = uuid.uuid4()
            self.payload_dict["InvocationID"] = str(ID)

        faasr_msg = '{"init_log_folder":"InvocationID for the workflow: ' + str(self.payload_dict["InvocationID"]) + '"}\n'

        target_s3 = self.get_logging_server()

        s3_log_info = self.payload_dict["DataStores"][target_s3]
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=s3_log_info["AccessKey"],
            aws_secret_access_key=s3_log_info["SecretKey"],
            region_name=s3_log_info["Region"],
            endpoint_url=s3_log_info["Endpoint"],
        )

        # if no name for log specified, use 'FaaSrLog'
        if self.payload_dict["FaaSrLog"] is None or self.payload_dict["FaaSrLog"] == "":
            self.payload_dict["FaaSrLog"] = "FaaSrLog"

        idfolder = (
            self.payload_dict["FaaSrLog"]
            + "/"
            + str(self.payload_dict["InvocationID"])
            + "/"
        )

        check_id_folder = s3_client.list_objects_v2(
            Prefix=idfolder, Bucket=s3_log_info["Bucket"]
        )

        if "Content" in check_id_folder and len(check_id_folder["Content"]) != 0:
            err = '{"init_log_folder":"InvocationID already exists: ' + str(self.payload_dict["InvocationID"]) + '"}\n'
            print(err)
            sys.exit(1)
        else:
            s3_client.put_object(Bucket=s3_log_info["Bucket"], Key=idfolder)

    def abort_on_multiple_invocations(self, pre):
        """
        Invoked when the current function has multiple predecessors
        
        Aborts if they have not finished or the current function was not the first
        to write to the candidate set
        """

        target_s3 = self.get_logging_server()

        if target_s3 not in self.payload_dict["DataStores"]:
            err = (
                '{"abort_on_multiple_invocation":"Invalid data server name: '
                + target_s3
                + '"}\n'
            )
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

        id_folder = (
            f"{self.payload_dict['FaaSrLog']}/{self.payload_dict['InvocationID']}"
        )

        for caller in pre:
            if "Rank" in self.payload_dict["FunctionList"][caller]:
                parts = self.payload_dict["FunctionList"][caller]["Rank"].split("/")
                pre = pre.remove(caller)
            for rank in range(1, parts[1] + 1):
                pre.append(f"{caller}.{rank}")

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
                res_msg = '{"fabort_on_multiple_invocations":"not the last trigger invoked - no flag"}\n'
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
        self.faasr_acquire()

        random_number = random.randint(1, 2**31 - 1)

        if not os.path.isdir(id_folder):
            os.mkdir(id_folder)

        candidate_path = f"{id_folder}/{self.payload_dict['FunctionInvoke']}.candidate"

        # gets all of the objects in s3 with the prefix {id_folder}/{FunctionInvoke}.candidate
        s3_response = s3_client.list_objects_v2(
            Bucket=s3_log_info["Bucket"], prefix=candidate_path
        )
        if len(s3_response["Contents"]) != 0:
            if os.path.exists(candidate_path):
                os.remove(candidate_path)
            s3_client.download_file(
                Bucket=s3_log_info["Bucket"],
                Key=candidate_path,
                Filename=candidate_path,
            )

        # append random number to candidate file
        with open(candidate_path, "a") as candidate_file:
            candidate_file.write(random_number + "\n")

        # upload candidate file back to S3
        s3_client.put_object(
            Body=candidate_file, Key=candidate_path, Bucket=s3_log_info["Bucket"]
        )

        # download candidate file to local directory again
        if os.path.exists(candidate_path):
            os.remove(candidate_path)
        s3_client.download_file(
            Bucket=s3_log_info["Bucket"], Key=candidate_path, Filename=candidate_path
        )

        # to-do faasr lock release
        self.faasr_release()

        with open(candidate_path, "a") as updated_candidate_file:
            first_line = updated_candidate_file.readline().strip()
            first_line = int(first_line)
        if random_number == first_line:
            return
        else:
            res_msg = '{"abort_on_multiple_invocations":"not the last trigger invoked - random number does not match"}\n'
            print(res_msg)
            sys.exit(1)

    def get_logging_server(self):
        if self.payload_dict["LoggingDataStore"] is None:
            logging_server = self.payload_dict["DefaultDataStore"]
        else:
            logging_server = self.payload_dict["LoggingDataStore"]
        return logging_server
    
    def run_user_function(self, imported_functions):
        """
        Runs the user's code that was imported from git repo
        """
        faasr_dict = self.payload_dict
        curr_action = faasr_dict["FunctionInvoke"]
        func_name = faasr_dict["FunctionList"][curr_action]["FunctionName"]

        # ensure
        if imported_functions and func_name in imported_functions:
            user_function = imported_functions[func_name]
        else:
            err_msg = '{"faasr_run_user_function":"Cannot find Function ' + func_name + ', check the name and sources"}\n'
            result_2 = faasr_log(err_msg)
            print(err_msg)
            sys.exit(1)

        # get args for function
        user_args = self.get_user_function_args()

        # run user function
        try:
            user_function(**user_args)
        except Exception as e:
            nat_err_msg = f'"faasr_run_user_function":Errors in the user function {repr(e)}'
            err_msg = '{"faasr_run_user_function":"Errors in the user function: ' + self.payload_dict["FunctionInvoke"] + ', check the log for the detail "}\n'
            result_2 = faasr_log(nat_err_msg)
            print(nat_err_msg)
            print(err_msg)
            sys.exit(1)

        # At this point, the Action has finished the invocation of the User Function
        # We flag this by uploading a file with name FunctionInvoke.done with contents TRUE to the S3 logs folder
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
            f.write("TRUE")
        faasr_put_file(
            local_folder=log_folder,
            local_file=file_name,
            remote_folder=log_folder,
            remote_file=file_name,
        )
    
    def get_user_function_args(self):
        """
        Gets function arguments from FaaSr JSON
        """
        user_action = self.payload_dict["FunctionInvoke"]

        args = self.payload_dict["FunctionList"][user_action]["Arguments"]
        if args is None:
            return []
        else:
            return args
        
    def trigger(self):
        """
        Triggers the next actions in the DAG
        """
        faasr_dict = self.payload_dict
        curr_func = faasr_dict['FunctionInvoke']
        invoke_next = faasr_dict['FunctionList'][curr_func]['InvokeNext']
        if isinstance(invoke_next, str):
            invoke_next = [invoke_next]

        if len(invoke_next) == 0:
            msg = '{\"faasr_trigger\":\"no triggers for ' + curr_func + '\"}\n'
            print(msg)
            faasr_log(msg)
            return


        for next_function in invoke_next:
            # split function name and rank if needed
            parts = re.split(r"[()]", next_function)
            if len(parts) > 1:
                next_function = parts[0]
                rank_num = parts[1]
            else:
                rank_num = 1

            # change FunctionInvoke to the next function
            faasr_dict['FunctionInvoke'] = next_function

            # determine faas server of next function
            next_server = faasr_dict['FunctionList'][next_function]['FaaSServer']

            for rank in range(1, rank_num + 1):
                # store rank of next function
                if(rank_num > 1):
                    faasr_dict['FunctionList'][next_function]['Rank'] = f"{rank}/{rank_num}"
                
                # determine that the function faas
                # server is in the compute server list
                # if not, then skip invoking the function
                if next_server not in faasr_dict['ComputeServers']:
                    err_msg = '{\"faasr_trigger\":\"invalid server name: ' + next_server + '\"}\n'
                    print(err_msg)
                    faasr_log(err_msg)
                    break


                # get faas type of next function
                next_server_type = faasr_dict['ComputeServers'][next_server]["FaaSType"]

                match(next_server_type):
                    # to-do: OW trigger
                    case "OpenWhisk":
                        print("OpenWhisk trigger not implemented")
                        break
                    case "Lambda":
                        print("Lamba trigger not implemented")
                        break
                    case "GitHubActions":
                        # get env values for GH actions
                        pat = faasr_dict["ComputeServers"][next_server]["Token"]
                        username = faasr_dict["ComputeServers"][next_server]["UserName"]
                        reponame = faasr_dict["ComputeServers"][next_server]["ActionRepoName"]
                        repo = f"{username}/{reponame}"
                        if not next_function.endswith('.ml') and not next_function.endswith('.yaml'):
                            workflow_file = f"{next_function}.yml"
                        else:
                            workflow_file = next_function
                        git_ref = faasr_dict["ComputeServers"][next_server]["Branch"]

                        # create copy of faasr payload
                        faasr_git = copy.deepcopy(faasr_dict)

                        # hide credentials in payload before sending
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
                        
                        for data_js in faasr_git["DataStores"]:
                            faasr_git["DataStores"][data_js]["AccessKey"] = f"{data_js}_ACCESS_KEY"
                            faasr_git["DataStores"][data_js]["SecretKey"] = f"{data_js}_SECRET_KEY"

                        # payload input
                        json_payload = json.dumps(faasr_git, indent=4)
                        inputs = {"PAYLOAD": json_payload}

                        # delete copy top ensure memory
                        del faasr_git

                        # url for github api
                        url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/dispatches"

                        # body for POST request
                        body = {"ref": git_ref, "inputs": inputs}

                        # headers for POST request
                        post_headers = {
                            "Authorization": f"token {pat}",
                            "Accept": "application/vnd.github.v3+json",
                            "X-GitHub-Api-Version": "2022-11-28"}

                        # POST request
                        response = requests.post(
                            url = url,
                            json = body,
                            headers = post_headers
                        )

                        if response.status_code == 204:
                            succ_msg = f"faasr_trigger: GitHub Action: Successfully invoked: {faasr_dict['FunctionInvoke']}\n"
                            print(succ_msg)
                            faasr_log(succ_msg)
                        elif response.status_code == 401:
                            err_msg = "faasr_trigger: GitHub Action: Authentication failed, check the credentials\n"
                            print(err_msg)
                            faasr_log(err_msg)
                        elif response.status_code == 404:
                            err_msg = "faasr_trigger: GitHub Action: Cannot find the destination, check the repo name: \"" + repo + "\" and workflow name: \"" + workflow_file + "\"\n"
                            print(err_msg)
                            faasr_log(err_msg)
                        elif response.status_code == 422:
                            err_msg = "faasr_trigger: GitHub Action: Cannot find the destination, check the ref: " + faasr_dict["FunctionInvoke"] + "\n"
                            print(err_msg)
                            faasr_log(err_msg)
                        else:
                            err_msg = "faasr_trigger: GitHub Action: unknown error happens when invoke next function\n"
                            print(err_msg)
                            faasr_log(err_msg)




                



            
