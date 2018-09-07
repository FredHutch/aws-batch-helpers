"""Python object managing task submission in AWS Batch."""
import json
import time
import boto3
import json
import logging
import pandas as pd
from tabulate import tabulate
from collections import defaultdict


class BatchTaskManager:

    def __init__(
        self,
        job_queue=None,
        s3_folder_checking_interval=30,
        monitor_interval=60,
        log_fp=None,
        dryrun=False,
    ):

        # Set up logging
        logFormatter = logging.Formatter(
            '%(asctime)s %(levelname)-8s [AWS Batch] %(message)s'
        )
        rootLogger = logging.getLogger()
        rootLogger.setLevel(logging.INFO)

        # Write to file
        if log_fp is not None:
            fileHandler = logging.FileHandler(log_fp)
            fileHandler.setFormatter(logFormatter)
            rootLogger.addHandler(fileHandler)

        # Also write to STDOUT
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        rootLogger.addHandler(consoleHandler)

        # In "dryrun" mode, jobs don't actually get submitted
        self.dryrun = dryrun
        if dryrun:
            logging.info("Dryrun mode, no jobs will be submitted")

        # Keep track of the contents of various S3 folders
        self.s3_folder_contents = {}
        self.s3_folder_contents_last_checked = {}
        self.s3_folder_checking_interval = s3_folder_checking_interval
        self.monitor_interval = monitor_interval

        # Keep track of the job queue
        assert job_queue is not None, "Must specify job queue"
        self.job_queue = job_queue
        logging.info("Job queue: " + job_queue)

        # Keep track of what jobs were submitted as part of this workflow
        self.jobs_in_workflow = set([])

        # Keep a client connection open to Batch and S3
        logging.info("Opening connections to AWS Batch and AWS S3")
        self.batch_client = boto3.client('batch')
        self.s3_client = boto3.client("s3")

        # Keep track of what jobs are currently extant on AWS Batch
        self.current_jobs = {}
        self.get_extant_jobs()

        # Keep track of the job definitions that are available
        self.get_job_definitions()

    def submit_job(
        self,
        output_files=None,
        job_name=None,
        job_definition=None,
        vcpus=None,
        memory=None,
        depends_on=[],
        parameters={},
        command=[],
        environment=[],
        retry_attempts=1,
        timeout_seconds=36000,
    ):
        # Make sure that the data types are correct
        assert isinstance(depends_on, list)
        assert isinstance(environment, list)
        assert isinstance(command, list)
        assert isinstance(parameters, dict)
        assert isinstance(retry_attempts, int)
        assert retry_attempts >= 1 and retry_attempts <= 10

        # Check and make sure that the job definition exists
        assert job_definition in self.job_definitions, "Job definition not found: {}".format(job_definition)

        # Set all the parameters to strings
        parameters = {k: str(v) for k, v in parameters.items()}

        # Remove some disallowed characters from the job name
        for k in ["-", ".", "/", "\\"]:
            job_name = job_name.replace(k, "_")

        # Set the parameters with the job definition, adding the custom parameters for this job
        for k, v in self.job_definitions[job_definition]["parameters"].items():
            if k not in parameters:
                parameters[k] = v

        # Remove None values from the depends on list
        depends_on = [d for d in depends_on if d is not None]

        # Make a hash that uniquely defines this particular new job
        job_hash_id = self.hash_job_id(
            job_definition=job_definition,
            parameters=parameters,
            vcpus=vcpus,
            memory=memory,
            environment=environment,
            timeout_seconds=timeout_seconds,
        )

        # Keep track of this job as one submitted as part of this workflow
        assert job_hash_id not in self.jobs_in_workflow, "Can't add duplicate job"
        self.jobs_in_workflow.add(job_hash_id)

        # Check to see if the outputs exist
        if all([
            self.s3_object_exists(output_s3_path)
            for output_s3_path in output_files
        ]):
            # There is no need to run this job
            logging.info("Output already exists for {}, no need to submit".format(
                job_name
            ))
            self.current_jobs[job_hash_id] = {
                "status": "SUCCEEDED",
                "job_id": None,
                "job_name": job_name,
                "job_definition": job_definition,
                "output_files": output_files
            }

            # Return None, indicating that no job was created
            return
        
        # Check to see if this job has already been created and (if so) is not FAILED
        # Note that previously submitted jobs that FAILED will be resubmitted
        if job_hash_id in self.current_jobs and self.current_jobs[job_hash_id]["status"] != "FAILED":
            # If the job has succeeded, return None
            if self.current_jobs[job_hash_id]["status"] == "SUCCEEDED":
                logging.info(
                    "Job for {} has already been completed ".format(job_name))
                return
            else:
                logging.info(
                    "Job for {} is {}".format(
                        job_name,
                        self.current_jobs[job_hash_id]["status"]
                    ))

                # Make sure that we have access to the job ID
                assert "job_id" in self.current_jobs[job_hash_id], "Job ID not found"

                # Record the output files
                self.current_jobs[job_hash_id]["output_files"] = output_files

                return self.current_jobs[job_hash_id]["job_id"]

        else:
            if self.dryrun:
                logging.info("Skipping submit for {} in dryrun mode".format(job_name))
                self.current_jobs[job_hash_id] = {
                    "status": "SKIPPED",
                    "job_id": None,
                    "job_name": job_name,
                    "job_definition": job_definition,
                    "output_files": output_files
                }
                return

            logging.info("Submitting job for " + job_name)
            r = self.batch_client.submit_job(
                jobName=job_name,
                jobQueue=self.job_queue,
                dependsOn=[
                    {
                        "jobId": dependency_job_id,
                        "type": "SEQUENTIAL"
                    }
                    for dependency_job_id in depends_on
                    if dependency_job_id is not None
                ],
                jobDefinition=job_definition,
                parameters=parameters,
                containerOverrides={
                    "vcpus": vcpus,
                    "memory": memory,
                    "command": [str(x) for x in command],
                    "environment": [str(x) for x in environment]
                },
                retryStrategy={
                    "attempts": retry_attempts
                },
                timeout={
                    "attemptDurationSeconds": timeout_seconds
                }
            )
            # Make sure that the response object contains the right fields
            assert "jobName" in r and "jobId" in r, "Job submission failed"

            # Save all of the information for this job
            self.current_jobs[job_hash_id] = {
                "status": "SUBMITTED",
                "job_id": r["jobId"],
                "depends_on": depends_on,
                "job_definition": job_definition,
                "parameters": parameters,
                "vcpus": vcpus,
                "memory": memory,
                "command": command,
                "environment": environment,
                "timeout_seconds": timeout_seconds,
                "output_files": output_files,
            }
            logging.info("{}: {}".format(
                job_name, json.dumps(self.current_jobs[job_hash_id])
            ))

            # Return the jobId for the job that was submitted
            return r["jobId"]

    def monitor_jobs(self):
        """Monitor a set of running jobs."""
        while True:
            # Keep track of the number of jobs by their status
            to_print = defaultdict(lambda: defaultdict(int))
            
            # Iterate over the jobs submitted as part of this workflow
            for job_id_hash in list(self.jobs_in_workflow):
                # If the job has succeeded, do nothing more
                if self.current_jobs[job_id_hash]["status"] in ["SUCCEEDED", "SKIPPED"]:
                    pass
                # If the outputs have been created, treat it as succeeded
                elif all([
                    self.s3_object_exists(output_s3_path)
                    for output_s3_path in self.current_jobs[job_id_hash]["output_files"]
                ]):
                    logging.info("All outputs found for {}, marking as SUCCEEDED".format(
                        self.current_jobs[job_id_hash]["job_id"]
                    ))
                    self.current_jobs[job_id_hash]["status"] = "SUCCEEDED"
                # Otherwise, check the status
                else:
                    r = self.batch_client.describe_jobs(
                        jobs=[self.current_jobs[job_id_hash]["job_id"]]
                    )
                    self.current_jobs[job_id_hash]["status"] = r["jobs"][0]["status"]

                # Add to the counters we're going to print
                to_print[
                    self.current_jobs[job_id_hash]["job_definition"]
                ][
                    self.current_jobs[job_id_hash]["status"]
                ] += 1
            
            # Print the table
            to_print = pd.DataFrame(to_print).T.fillna(0)
            if "SUCCEEDED" in to_print.columns:
                to_print.sort_values(by="SUCCEEDED", ascending=False, inplace=True)
            print(
                tabulate(
                    to_print,
                    headers="keys"
                ) + "\n\n\nWaiting {:,} seconds...\n\n\n".format(
                    self.monitor_interval
                )
            )

            # If all jobs SUCCEEDED or FAILED, finish
            if all([col_name in ["SUCCEEDED", "FAILED", "SKIPPED"] for col_name in to_print.columns]):
                break

            time.sleep(self.monitor_interval)

    def all_complete(self):
        """Check to see if all of the jobs are complete."""
        # Iterate over the jobs submitted as part of this workflow
        for job_id_hash in list(self.jobs_in_workflow):
            # If the job has succeeded, do nothing more
            if self.current_jobs[job_id_hash]["status"] != "SUCCEEDED":
                return False
        return True

    def hash_job_id(
        self,
        job_definition=None,
        parameters=None,
        vcpus=None,
        memory=None,
        environment=None,
        timeout_seconds=None,
    ):
        """Make a unique hash of this job."""
        return hash(json.dumps({
            "job_definition": job_definition,
            "parameters": parameters,
            "vcpus": vcpus,
            "memory": memory,
            "environment": environment,
            "timeout_seconds": timeout_seconds
        }, sort_keys=True))

    def s3_object_exists(self, s3_path):
        """Check whether a particular object exists on S3."""
        assert s3_path.endswith("/") is False, "Can't target a folder ({})".format(s3_path)

        # Split up the folder (includes the bucket) and the file
        s3_folder, s3_file = s3_path.rsplit("/", 1)

        # Check if the S3 folder has been checked before
        if s3_folder in self.s3_folder_contents:
            # If the file is in the folder, return True
            if s3_file in self.s3_folder_contents[s3_folder]:
                return True
            # If the folder hasn't been checked recently, check it again
            elif time.time() - self.s3_folder_contents_last_checked[s3_folder] > self.s3_folder_checking_interval:
                self.get_s3_folder_contents(s3_folder)
                return s3_file in self.s3_folder_contents[s3_folder]
            # Otherwise return False (the file doesn't exist)
            else:
                return False
        # If this is the first time, check the folder contents
        else:
            self.get_s3_folder_contents(s3_folder)
            # Return whether the file is in the folder
            return s3_file in self.s3_folder_contents[s3_folder]

    def get_s3_folder_contents(self, s3_folder):
        """Get the contents of an S3 folder."""
        # Make sure the string is properly formatted
        assert s3_folder.startswith("s3://")

        logging.info("Reading the contents of " + s3_folder)

        # Split the bucket and the folder name
        bucket_name, bucket_prefix = s3_folder[5:].split("/", 1)

        tot_objs = []
        # Retrieve in batches of 1,000
        objs = self.s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=bucket_prefix
        )

        continue_flag = True
        while continue_flag:
            continue_flag = False

            if 'Contents' not in objs:
                break

            # Add this batch to the list
            tot_objs.extend([x["Key"].rsplit("/", 1)[-1] for x in objs['Contents']])

            # Check to see if there are more to fetch
            if objs['IsTruncated']:
                continue_flag = True
                token = objs['NextContinuationToken']
                objs = self.s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=bucket_prefix,
                    ContinuationToken=token
                )
        
        self.s3_folder_contents_last_checked[s3_folder] = time.time()
        self.s3_folder_contents[s3_folder] = tot_objs

    def get_job_definitions(self):
        """Get the job definitions that are currently defined."""
        logging.info("Fetching all registered job definitions")
        # Store the job definitions as a dict of dicts, keyed on job_def_name:revision
        self.job_definitions = {}

        # Retrieve in batches of 100
        objs = self.batch_client.describe_job_definitions(
            status="ACTIVE"
        )

        continue_flag = True
        while continue_flag:
            continue_flag = False

            if 'jobDefinitions' not in objs:
                break

            # Add this batch to the list
            for jd in objs["jobDefinitions"]:
                self.job_definitions[
                    "{}:{}".format(jd["jobDefinitionName"], jd["revision"])
                ] = jd

            # Check to see if there are more to fetch
            if "nextToken" in objs and objs['NextToken'] is not None:
                continue_flag = True
                objs = self.batch_client.describe_job_definitions(
                    status="ACTIVE",
                    NextToken=objs['NextToken']
                )

    def get_extant_jobs(self):
        """Get all of the extant jobs on AWS Batch."""
        logging.info("Getting the list of jobs existing on Batch")
        for job_status in [
            "SUBMITTED", "PENDING", "RUNNABLE",
            "STARTING", "RUNNING", "SUCCEEDED"
        ]:
            job_list = self.batch_client.list_jobs(
                jobQueue=self.job_queue,
                jobStatus=job_status
            )

            continue_flag = True
            while continue_flag:
                continue_flag = False

                if 'jobSummaryList' not in job_list or len(job_list["jobSummaryList"]) == 0:
                    break

                # Get the list of IDs
                job_id_list = [
                    j["jobId"]
                    for j in job_list["jobSummaryList"]
                ]

                while len(job_id_list) > 0:

                    # Get all the details for this batch of jobs
                    for job_details in self.batch_client.describe_jobs(
                        jobs=job_id_list[:100]
                    )["jobs"]:
                        # ADD DETAILS ABOUT THIS JOB
                        job_hash_id = self.hash_job_id(
                            job_definition=job_details["jobDefinition"].split("/")[-1],
                            parameters=job_details["parameters"],
                            vcpus=job_details["container"]["vcpus"],
                            memory=job_details["container"]["memory"],
                            environment=job_details["container"]["environment"],
                            timeout_seconds=job_details.get("timeout", {}).get("attemptDurationSeconds", 0),
                        )

                        self.current_jobs[job_hash_id] = {
                            "status": job_status,
                            "job_name": job_details["jobName"],
                            "job_id": job_details["jobId"],
                            "depends_on": job_details["dependsOn"],
                            "job_definition": job_details["jobDefinition"].split("/")[-1],
                            "parameters": job_details["parameters"],
                            "vcpus": job_details["container"]["vcpus"],
                            "memory": job_details["container"]["memory"],
                            "command": job_details["container"]["command"],
                            "environment": job_details["container"]["environment"],
                            "timeout_seconds": job_details.get("timeout", {}).get("attemptDurationSeconds", 0)
                        }
                    job_id_list = job_id_list[100:]

                # Check to see if there are more to fetch
                if "nextToken" in job_list and job_list['nextToken'] is not None:
                    continue_flag = True
                    job_list = self.batch_client.list_jobs(
                        jobQueue=self.job_queue,
                        jobStatus=job_status,
                        nextToken = job_list['nextToken']
                    )
