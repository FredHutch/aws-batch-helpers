#!/usr/bin/python
"""Submit and monitor the status of projects on AWS Batch."""

import re
import json
import boto3
import argparse


def valid_config(config):
    """Make sure that the config object is valid."""
    spec = {
        "name": unicode,
        "description": unicode,
        "job_definition": unicode,
        "db": unicode,
        "output_folder": unicode,
        "queue": unicode,
        "samples": list
    }
    for k, v in spec.items():
        if k not in config:
            print("{} not found in config file".format(k))
            return False
        msg = "Value {} ({}) not of the correct type: {}".format(config[k], type(config[k]), v)
        if not isinstance(config[k], v):
            print(msg)
            return False

    # Make sure that the project name is only alphanumeric with underscores
    msg = "Project names can only be alphanumeric with underscores"
    if not re.match("^[a-zA-Z0-9_]*$", config["name"]):
        print(msg)
        return False

    return True


def submit_jobs(config):
    """Submit a set of jobs."""
    if config.get('status') in ["SUBMITTED", "COMPLETED", "CANCELED"]:
        print("Project has already been submitted, exiting.")
        return

    if "jobs" in config:
        print("'jobs' already found in config, exiting.")
        return

    # Set up the list of jobs
    config["jobs"] = []

    # Set up the connection to Batch with boto
    client = boto3.client('batch')

    for sample_n, sample in enumerate(config["samples"]):
        # Use the parameters from the input file to submit the jobs
        r = client.submit_job(
                jobName="{}_{}".format(config["name"], sample_n),
                jobQueue=config["queue"],
                jobDefinition=config["job_definition"],
                parameters={
                    "input": sample,
                    "ref_db": config["db"],
                    "output_folder": config["output_folder"]
                    }
            )
        # Save the response, which includes the jobName and jobId (as a dict)
        config["jobs"].append(r)

    # Set the project status to "SUBMITTED"
    config["status"] = "SUBMITTED"

    # Return the config object, which now includes the job information and the status
    return config


def monitor_jobs(config, force_check=False):
    """Monitor the status of a set of jobs."""
    assert "jobs" in config, "No jobs found in config file"

    if config["status"] == "COMPLETED":
        print("Project status is COMPLETED")
        if not force_check:
            print("Exiting, use --force-check to force check job status.")
    # Get the list of IDs
    id_list = [j["jobId"] for j in config["jobs"]]

    # Set up the connection to Batch with boto
    client = boto3.client('batch')

    # Count up the number of jobs by status
    status_counts = {}

    # Keep track of the jobs that have failed
    failed_jobs = []

    # Check the status of each job in batches of 100
    n_jobs = len(id_list)
    while len(id_list) > 0:
        status = client.describe_jobs(jobs=id_list[:min(len(id_list), 100)])
        if len(id_list) < 100:
            id_list = []
        else:
            id_list = id_list[100:]

        for j in status['jobs']:
            s = j['status']
            status_counts[s] = status_counts.get(s, 0) + 1
            if s == "FAILED":
                failed_jobs.append((j['jobId'], j['statusReason']))

    print("Total number of jobs: {}".format(n_jobs))
    print("")
    for k, v in status_counts.items():
        print("\t{}:\t{}".format(k, v))

    if len(failed_jobs) > 0:
        print("\n\nFAILED:\n")
        for f in failed_jobs:
            print(f)

    # Check to see if the project is completed
    if status_counts.get("SUCCEEDED", 0) == n_jobs:
        config["status"] = "COMPLETED"

    return config


def cancel_jobs(config):
    """Cancel all of the currently pending jobs."""
    assert "jobs" in config, "No jobs found in config file"

    # Prompt the user for confirmation
    response = raw_input("Are you sure you want to cancel these jobs? (Y/N): ")
    assert response == "Y", "Do not cancel without confirmation"

    # Get a message to submit as justfication for the failure
    cancel_msg = raw_input("What message should describe these cancellations?\n")

    # Get the list of IDs
    id_list = [j["jobId"] for j in config["jobs"]]

    # Set up the connection to Batch with boto
    client = boto3.client('batch')

    # Cancel jobs
    for job_id in id_list:
        print("Cancelling {}".format(job_id))
        client.cancel_job(jobId=job_id, reason=cancel_msg)

    config["status"] = "CANCELED"

    return config


def save_all_logs(config):
    """Save all of the logs to their own local file."""
    assert "jobs" in config, "No jobs found in config file"

    # Get the list of IDs
    id_list = [j["jobId"] for j in config["jobs"]]

    # Set up the connection to Batch with boto
    client = boto3.client('batch')

    # Keep track of the jobs with logs
    job_log_ids = {}  # key is log_id, value is job_name+job_id

    # Check the status of each job in batches of 100
    n_jobs = len(id_list)
    while len(id_list) > 0:
        status = client.describe_jobs(jobs=id_list[:min(len(id_list), 100)])
        if len(id_list) < 100:
            id_list = []
        else:
            id_list = id_list[100:]

        for j in status['jobs']:
            if 'container' in j and 'logStreamName' in j['container']:
                # Save the log stream name
                fp = "{}.{}.{}.log".format(j['status'], j['jobName'], j['jobId'])
                job_log_ids[j['container']['logStreamName']] = fp

    # Now get all of the logs
    client = boto3.client('logs')
    for log_id, fp in job_log_ids.items():
        response = client.get_log_events(logGroupName='/aws/batch/job', logStreamName=log_id)
        print("Writing to " + fp)
        with open(fp, 'wt') as fo:
            for event in response['events']:
                fo.write(event['message'] + '\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
    Submit and monitor the status of projects on AWS Batch.
    """)

    parser.add_argument("cmd",
                        type=str,
                        help="""Command to run: submit, monitor, cancel, logs""")

    parser.add_argument("project_config",
                        type=str,
                        help="""Project config file, JSON""")

    parser.add_argument("--force-check",
                        action='store_true',
                        help="""Force check job status for COMPLETED projects""")

    args = parser.parse_args()

    msg = "Please specify a command: submit, monitor, or cancel"
    assert args.cmd in ["submit", "monitor", "cancel", "logs"], msg

    # Read in the config file
    config = json.load(open(args.project_config, 'rt'))
    # Make sure that the config file is valid
    assert valid_config(config)

    if args.cmd == "submit":
        # Submit a batch of jobs
        config = submit_jobs(config)
    elif args.cmd == "monitor":
        # Monitor the progress of a set of jobs
        config = monitor_jobs(config, force_check=args.force_check)
    elif args.cmd == "cancel":
        # Cancel all of the currently pending jobs
        config = cancel_jobs(config)
    
    if args.cmd == "logs":
        # Save all of the logs to their own local file
        save_all_logs(config)
    else:
        # Update the config file
        with open(args.project_config, 'wt') as fo:
            json.dump(config, fo, indent=4)
