#!/usr/bin/python
"""Monitor the status of all project below the local directory."""

import os
import re
import json
import boto3
import argparse
import pandas as pd
from tabulate import tabulate


def valid_config(config, verbose=False):
    """Make sure that the config object is valid."""
    spec = {
        "name": unicode,
        "description": unicode,
        "job_definition": unicode,
        "db": unicode,
        "output_folder": unicode,
        "queue": unicode,
        "samples": list,
        "parameters": dict,
        "containerOverrides": dict,
    }
    optional = ["parameters", "containerOverrides"]

    for k, v in spec.items():
        if k not in config:
            if k in optional:
                continue
            else:
                if verbose:
                    print("{} not found in config file".format(k))
                return False
        msg = "Value {} ({}) not of the correct type: {}".format(config[k], type(config[k]), v)
        if not isinstance(config[k], v):
            if verbose:
                print(msg)
            return False

    # Make sure that the project name is only alphanumeric with underscores
    msg = "Project names can only be alphanumeric with underscores"
    if not re.match("^[a-zA-Z0-9_]*$", config["name"]):
        if verbose:
            print(msg)
        return False

    return True


def project_status(config, fp, force_check=False):
    """Monitor the status of a set of jobs."""
    if "jobs" not in config:
        return None

    if config["status"] == "COMPLETED":
        if not force_check:
            return {"fp": fp, "completed": 1}
    # Get the list of IDs
    id_list = [j["jobId"] for j in config["jobs"]]

    # Set up the connection to Batch with boto
    client = boto3.client('batch')

    # Count up the number of jobs by status
    status_counts = {"fp": fp}

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

    # Check to see if the project is completed
    if status_counts.get("SUCCEEDED", 0) == n_jobs:
        # Set this job as COMPLETED
        config["status"] = "COMPLETED"
        with open(fp, "wt") as fo:
            json.dump(config, fo)
        return {"fp": fp, "completed": 1}

    # Check to see how many files are in the output folder, and what the most recent ones are
    if config['output_folder'].startswith('s3://'):
        client = boto3.client('s3')
        bucket = config['output_folder'][5:].split('/')[0]
        prefix = config['output_folder'][(5 + len(bucket) + 1):]

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

    status_counts["files_in_output"] = len(tot_objs)

    return status_counts


def get_last_modified(obj):
    """Function to help sorting by datetime."""
    return int(obj['LastModified'].strftime('%s'))


def print_dashboard(dat, check_all=False):
    """Print summary information for all of the specified projects."""

    # For each project, get the number of jobs that are running, failed, etc.
    # Also get the number of files that are in the output folder

    output = []
    for fp, config in dat.items():
        status = project_status(config, fp, force_check=check_all)
        if status is not None:
            output.append(status)

    n_completed = sum([p.get("completed", False) == True for p in output])
    output = [p for p in output if p.get("completed", False) is False]
    df = pd.DataFrame(output).fillna(0)
    df.sort_values(by="fp", inplace=True)
    df.set_index("fp", inplace=True)
    print(tabulate(df, headers="keys"))

    print("\nCompleted projects: {}".format(n_completed))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
    Monitor the status of all project below the local directory.
    """)

    parser.add_argument("--all",
                        action="store_true",
                        help="""Check all results, not just pending.""")

    args = parser.parse_args()

    # Find all of the project JSONs below the currect directory
    dat = {}  # Key by path

    for root, subdirs, files in os.walk(os.getcwd()):
        for file in files:
            if file.endswith(".json"):
                fp = os.path.join(root, file)
                config = json.load(open(fp))
                if valid_config(config):
                    dat[fp] = config

    # Print a summary of all project
    print_dashboard(dat, check_all=args.all)
