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
    if config.get('status') in ["SUBMITTED", "COMPLETED"]:
        print("Project has already been submitted, exiting.")
        return

    if "jobs" in config:
        print("'jobs' already found in config, exiting.")
        return

    # Set up the list of jobs
    config["jobs"] = []

    # Set up the connection to Batch with boto
    client = boto3.client('batch')

    for sample in config["samples"]:
        # Use the parameters from the input file to submit the jobs
        r = client.submit_job(
                jobName="{}_{}".format(config["name"], sample),
                jobQueue=config["queue"],
                jobDefinition=config["job_definition"],
                parameters={
                    "input": "sra://{}".format(sample),
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

    print("Total number of jobs: {}".format(n_jobs))
    print("")
    for k, v in status_counts.items():
        print("\t{}:\t{}".format(k, v))

    # Check to see if the project is completed
    if status_counts.get("SUCCEEDED", 0) == n_jobs:
        config["status"] = "COMPLETED"

    return config


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
    Submit and monitor the status of projects on AWS Batch.
    """)

    parser.add_argument("cmd",
                        type=str,
                        help="""Command to run: submit, monitor""")

    parser.add_argument("project_config",
                        type=str,
                        help="""Project config file, JSON""")

    parser.add_argument("--force-check",
                        action='store_true',
                        help="""Force check job status for COMPLETED projects""")

    args = parser.parse_args()

    assert args.cmd in ["submit", "monitor"], "Please specify a command: submit or monitor"

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

    # Update the config file
    with open(args.project_config, 'wt') as fo:
        json.dump(config, fo, indent=4)
