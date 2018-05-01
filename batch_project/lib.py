#!/usr/bin/env python3
import os
import json
import re
import boto3
import argparse
import pandas as pd
from collections import defaultdict


def valid_workflow(config, verbose=True):
    """Make sure that the config object is valid."""

    if isinstance(config, dict) is False:
        if verbose:
            print("Workflow must be a dict")
            return False

    for k in ["workflow_name", "project_name", "analyses"]:
        if k not in config:
            if verbose:
                print("Workflow must have " + k)
            return False

    analysis_spec = {
        "job_definition": str,
        "outputs": list,
        "description": str,
        "queue": str,
        "parameters": dict,
        "containerOverrides": dict,
        "analyses": list,
        "samples": list,
        "timeout": int
    }
    optional = ["parameters", "containerOverrides", "analyses", "samples", "timeout"]

    for analysis in config["analyses"]:        
        for k, v in analysis.items():
            if k not in analysis_spec:
                if k in optional:
                    continue
                else:
                    print("\n\n{} not found in config file\n\n".format(k))
                    return False
            msg = "\n\nValue {} ({}) not of the correct type: {}\n\n".format(
                v, type(v), analysis_spec[k])
            if not isinstance(v, analysis_spec[k]):
                print(msg)
                return False
        # Outputs must be S3 paths
        for output in analysis["outputs"]:
            if output.startswith("s3://") is False:
                return False

    # Make sure that the project name is only alphanumeric with underscores
    msg = "\n\n{} names can only be alphanumeric with underscores\n\n"
    if not re.match("^[a-zA-Z0-9_]*$", config["project_name"]):
        print(msg.format("Project"))
        return False

    if not re.match("^[a-zA-Z0-9_]*$", config["workflow_name"]):
        print(msg.format("Workflow"))
        return False

    return True


def submit_workflow(workflow_fp):
    """Submit a set of jobs."""

    config = json.load(open(workflow_fp, "rt"))
    assert valid_workflow(config)

    if config.get('status') in ["SUBMITTED", "COMPLETED", "CANCELED"]:
        print("Project has already been submitted, exiting.")
        return

    if "jobs" in config:
        print("'jobs' already found in config, exiting.")
        return

    # Keep track of the contents of different S3 folders
    s3_contents = S3FolderContents()

    # Set up the list of jobs
    config["jobs"] = []

    # Set up the connection to Batch with boto
    client = boto3.client('batch')

    for sample_info in config["samples"]:
        if "job_ids" not in sample_info:
            sample_info["job_ids"] = []

        # Set the last job_id for sequential jobs
        last_job_id = []
        for analysis_ix, analysis_config in enumerate(config["analyses"]):
            # Fill in the values for the output paths
            sample_outputs = [
                output_template.format(
                    **sample_info,
                    **config
                )
                for output_template in analysis_config["outputs"]
            ]
            # Check to see if the outputs exist
            if all([
                s3_contents.exists(fp)
                for fp in sample_outputs
            ]):
                config["jobs"].append({
                    "outputs": sample_outputs,
                    "sample": sample_info["_sample"],
                    "job_definition": analysis_config["job_definition"],
                    "job_status": "COMPLETED",
                    "analysis_ix": analysis_ix
                })
                sample_info["job_ids"].append(None)
            else:
                # Set up the job and submit it

                # Use the parameters from the input file to submit the jobs
                job_name = "{}_{}_{}".format(
                    config["workflow_name"],
                    sample_info["_sample"],
                    analysis_config["job_definition"]
                )
                job_name = job_name.replace(".", "_").replace(":", "_")

                parameters = {
                    k: v.format(
                        **sample_info,
                        **config
                    )
                    for k, v in analysis_config["parameters"].items()
                }

                r = client.submit_job(
                    jobName=job_name,
                    jobQueue=analysis_config["queue"],
                    jobDefinition=analysis_config["job_definition"],
                    parameters=parameters,
                    containerOverrides=analysis_config.get("containerOverrides", {}),
                    dependsOn=last_job_id,
                    timeout={"attemptDurationSeconds": analysis_config.get("timeout", 21600)}
                )

                # Set the last job id to chain sequential tasks
                last_job_id = [
                    {
                        "jobId": r["jobId"],
                        "type": "SEQUENTIAL"
                    }
                ]

                # Save the response, which includes the jobName and jobId (as a dict)
                config["jobs"].append({
                    "jobName": r["jobName"],
                    "jobId": r["jobId"],
                    "outputs": sample_outputs,
                    "sample": sample_info["_sample"],
                    "job_definition": analysis_config["job_definition"],
                    "job_status": "SUBMITTED",
                    "analysis_ix": analysis_ix
                })
                sample_info["job_ids"].append(r["jobId"])

                print("Submitted {}: {}".format(job_name, r['jobId']))

    # Set the project status to "SUBMITTED"
    config["status"] = "SUBMITTED"

    # Write the config to a file
    with open(workflow_fp, "wt") as f:
        json.dump(config, f, indent=4)


def resubmit_failed_jobs(workflow_fp):
    """Resubmit any failed jobs in the project."""

    # Check the status of the jobs
    get_workflow_status(workflow_fp)

    config = json.load(open(workflow_fp, "rt"))
    assert valid_workflow(config)

    assert "jobs" in config, "'jobs' not found in config, exiting."

    # Set up the connection to Batch with boto
    client = boto3.client('batch')

    # Index the jobs by their ID
    jobs = {
        j["jobId"]: j
        for j in config["jobs"]
        if "jobId" in j
    }

    n_resubmitted = 0
    
    for sample_info in config["samples"]:
        last_job_id = []
        for analysis_ix, job_id in enumerate(sample_info["job_ids"]):
            if job_id is None:
                continue
            assert job_id in jobs
            if jobs[job_id]["job_status"] not in ["FAILED", "CANCELED"]:
                continue

            analysis_config = config["analyses"][analysis_ix]

            parameters = {
                k: v.format(
                    **sample_info,
                    **config
                )
                for k, v in analysis_config["parameters"].items()
            }

            r = client.submit_job(
                jobName=jobs[job_id]["jobName"],
                jobQueue=analysis_config["queue"],
                jobDefinition=analysis_config["job_definition"],
                parameters=parameters,
                containerOverrides=analysis_config.get(
                    "containerOverrides", {}),
                dependsOn=last_job_id,
                timeout={"attemptDurationSeconds": analysis_config.get(
                    "timeout", 21600)}
            )

            # Set the last job id to chain sequential tasks
            last_job_id = [
                {
                    "jobId": r["jobId"],
                    "type": "SEQUENTIAL"
                }
            ]
            print("Resubmitted " + r["jobId"])

            # Save the response, which includes the jobName and jobId (as a dict)
            jobs[job_id] = {
                "jobName": r["jobName"],
                "jobId": r["jobId"],
                "outputs": jobs[job_id]["outputs"],
                "sample": sample_info["_sample"],
                "job_definition": analysis_config["job_definition"],
                "job_status": "SUBMITTED",
                "analysis_ix": analysis_ix
            }
            sample_info["job_ids"][analysis_ix] = r["jobId"]

            n_resubmitted += 1

    print("Resubmitted {:,} failed jobs".format(n_resubmitted))

    config["jobs"] = list(jobs.values())

    with open(workflow_fp, "wt") as f:
        json.dump(config, f, indent=4)


def cancel_workflow_jobs(workflow_fp, status=None):
    """Cancel all of the currently pending jobs."""
    get_workflow_status(workflow_fp)

    config = json.load(open(workflow_fp, "rt"))
    assert "jobs" in config, "No jobs found in config file"

    # Prompt the user for confirmation
    response = input("Are you sure you want to cancel these jobs? (Y/N): ")
    assert response == "Y", "Do not cancel without confirmation"

    # Get a message to submit as justfication for the failure
    cancel_msg = input("What message should describe these cancellations?\n")

    # Set up the connection to Batch with boto
    client = boto3.client('batch')

    # Cancel jobs
    for j in config["jobs"]:
        if "jobId" not in j:
            continue
        if status is None or status == j["job_status"]:
            if j["job_status"] not in ["SUCCEEDED", "FAILED", "CANCELED"]:
                print("Cancelling {}".format(j["jobId"]))
                client.cancel_job(jobId=j["jobId"], reason=cancel_msg)
                client.terminate_job(jobId=j["jobId"], reason=cancel_msg)
                j["job_status"] = "CANCELED"

    config["status"] = "CANCELED"

    with open(workflow_fp, "wt") as f:
        json.dump(config, f, indent=4)


def save_workflow_logs(fp):
    """Save all of the logs to their own local file."""
    config = json.load(open(fp, "rt"))
    folder = config["project_name"]
    assert os.path.exists(folder), "project folder does not exist"

    if os.path.exists(os.path.join(folder, "logs")) is False:
        os.mkdir(os.path.join(folder, "logs"))

    assert "jobs" in config, "No jobs found in config file"

    # Get the list of IDs
    id_list = [j["jobId"] for j in config["jobs"] if "jobId" in j]

    # Set up the connection to Batch with boto
    client = boto3.client('batch')

    # Keep track of the jobs with logs
    job_log_ids = {}  # key is log_id, value is job_name+job_id

    # Check the status of each job in batches of 100
    while len(id_list) > 0:
        status = client.describe_jobs(jobs=id_list[:min(len(id_list), 100)])
        if len(id_list) < 100:
            id_list = []
        else:
            id_list = id_list[100:]

        for j in status['jobs']:
            if 'container' in j and 'logStreamName' in j['container']:
                # Save the log stream name
                fp = "{}/logs/{}.{}.{}.log".format(
                    folder,
                    j['status'],
                    j['jobName'],
                    j['jobId']
                )
                job_log_ids[j['container']['logStreamName']] = fp

    # Now get all of the logs
    client = boto3.client('logs')
    for log_id, fp in job_log_ids.items():
        try:
            response = client.get_log_events(logGroupName='/aws/batch/job', logStreamName=log_id)
        except:
            continue
        print("Writing to " + fp)
        with open(fp, 'wt') as fo:
            for event in response['events']:
                fo.write(event['message'] + '\n')


def get_workflow_status(fp, force_check=False):
    """Monitor the status of a set of jobs."""
    config = json.load(open(fp, "rt"))
    assert valid_workflow(config)

    assert "jobs" in config, "No jobs in workflow file"

    if config["status"] == "COMPLETED":
        if not force_check:
            return {"COMPLETED": len(config["jobs"])}

    # Set up a connection to S3 to check for output files
    s3_contents = S3FolderContents()

    # Mark jobs as SUCCEEDED if the outputs exist
    for j in config["jobs"]:
        if j["job_status"] == "SUCCEEDED":
            continue
        if all([
            s3_contents.exists(fp)
            for fp in j["outputs"]
        ]):
            j["job_status"] = "SUCCEEDED"
    # Get the list of IDs
    id_list = [
        j["jobId"] for j in config["jobs"]
        if j["job_status"] != "SUCCEEDED"
    ]

    # Set up the connection to Batch with boto
    client = boto3.client('batch')

    # Count up the number of jobs by status
    new_job_status = {}

    # Check the status of each job in batches of 100
    while len(id_list) > 0:
        status = client.describe_jobs(jobs=id_list[:min(len(id_list), 100)])
        if len(id_list) < 100:
            id_list = []
        else:
            id_list = id_list[100:]

        for j in status['jobs']:
            new_job_status[j["jobId"]] = j['status']
    
    # Add back to the list of jobs
    for j in config["jobs"]:
        if j.get("jobId") in new_job_status:
            j["job_status"] = new_job_status[j["jobId"]]

    # Count the job statuses
    status_counts = defaultdict(int)
    for j in config["jobs"]:
        status_counts[j["job_status"]] += 1

    # Check to see if the project is completed
    if status_counts.get("SUCCEEDED", 0) == len(config["jobs"]):
        # Set this job as COMPLETED
        print("{}: Project is COMPLETED!".format(fp))
        config["status"] = "COMPLETED"

    # Save the config
    with open(fp, "wt") as f:
        json.dump(config, f, indent=4)

    # Return the status status_counts
    return status_counts
    

def import_project_from_metadata(
    project_name,
    metadata_fp,
    file_col="file",
    sample_col="sample"
):
    """Make a project folder from a metadata CSV."""
    # Load in a metadata table
    msg = "{} does not exist"
    assert os.path.exists(metadata_fp), msg.format(metadata_fp)
    metadata = pd.read_table(metadata_fp, sep=',')

    # Check to see if another metadata object exists
    metadata_json = os.path.join(project_name, "metadata.json")
    msg = "Metadata file already exists: {}".format(metadata_json)
    assert os.path.exists(metadata_json) is False, msg

    # Make sure that the sample column and file column are present
    msg = "Column '{}' not found, please choose from: {}"
    print("Using file column: {}".format(file_col))
    print("Using sample column: {}".format(sample_col))
    assert file_col in metadata.columns, msg.format(
        file_col, "\n" + "\n".join(metadata.columns))
    assert sample_col in metadata.columns, msg.format(
        sample_col, "\n" + "\n".join(metadata.columns))

    # Make a new column named "_filepath"
    metadata["_filepath"] = metadata[file_col].values
    metadata["_sample"] = metadata[sample_col].values

    # Make sure that the file and sample are both unique
    assert len(metadata["_filepath"].unique()) == metadata.shape[0]
    assert len(metadata["_sample"].unique()) == metadata.shape[0]

    # Make a folder if it doesn't exist
    if not os.path.exists(project_name):
        os.mkdir(project_name)

    # Write out the metadata as a JSON
    with open(metadata_json, "wt") as fo:
        json.dump(metadata.to_dict(orient="records"), fo)
        print("Wrote metadata to {}".format(metadata_json))


class S3FolderContents:
    """Check whether files exist on S3, caching folder contents."""
    def __init__(self):
        self.client = boto3.client('s3')

        self.file_cache = set([])
        self.folder_cache = defaultdict(set)

    def exists(self, fp):
        """Check whether a single file exists in S3."""
        if fp in self.file_cache:
            return True

        assert fp.startswith("s3://"), "Not an S3 path"
        assert fp.endswith("/") is False, "File, should be a folder"

        bucket = fp[5:].split("/", 1)[0]
        folder = "/".join(fp[5:].split("/")[1:-1])

        # We've checked this folder, and it's not there
        if folder in self.folder_cache[bucket]:
            return False

        # Otherwise, list the contents of the folder
        for existing_fp in self.aws_s3_ls(bucket, folder):
            self.file_cache.add(
                "s3://{}/{}/{}".format(bucket, folder, existing_fp))
        self.folder_cache[bucket].add(folder)

        return fp in self.file_cache
        
    def aws_s3_ls(self, bucket, prefix):
        """List the contents of an S3 bucket."""

        print("Getting contents of s3://{}/{}".format(bucket, prefix))

        # Connect to S3
        client = boto3.client('s3')

        # Get all of the objects from S3
        tot_objs = []
        # Retrieve in batches of 1,000
        objs = client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        continue_flag = True
        while continue_flag:
            continue_flag = False

            if 'Contents' not in objs:
                break

            # Add this batch to the list
            tot_objs.extend(objs['Contents'])

            # Check to see if there are more to fetch
            if objs['IsTruncated']:
                continue_flag = True
                token = objs['NextContinuationToken']
                objs = client.list_objects_v2(Bucket=bucket,
                                            Prefix=prefix,
                                            ContinuationToken=token)
        return [d["Key"].split('/')[-1] for d in tot_objs]


def create_workflow_from_template(project_name, template_fp):
    """Make a analysis config JSON, just add samples to the workflow."""
    metadata_fp = os.path.join(project_name, "metadata.json")
    assert os.path.exists(metadata_fp)
    metadata = json.load(open(metadata_fp, "rt"))

    assert os.path.exists(template_fp)

    # Read in the skeleton for the project definition
    assert os.path.exists(template_fp)
    project = json.load(open(template_fp, "rt"))

    # Set the project_name
    project["project_name"] = project_name

    # Add the samples to the project config object
    project['samples'] = metadata

    # Write out the config file
    fp_out = os.path.join(
        project_name, '{}.json'.format(project["workflow_name"]))
    with open(fp_out, 'wt') as fo:
        json.dump(project, fo, indent=4)

    print("Wrote out {}".format(fp_out))
