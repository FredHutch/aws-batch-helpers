#!/usr/bin/python
"""Submit and monitor the status of projects on AWS Batch."""

import os
import re
import json
import boto3
import argparse
import pandas as pd


def valid_config(config):
    """Make sure that the config object is valid."""
    spec = {
        "name": unicode,
        "description": unicode,
        "job_definition": unicode,
        "output_folder": unicode,
        "queue": unicode,
        "samples": dict,
        "parameters": dict,
        "containerOverrides": dict,
    }
    optional = ["parameters", "containerOverrides"]

    for k, v in spec.items():
        if k not in config:
            if k in optional:
                continue
            else:
                print("\n\n{} not found in config file\n\n".format(k))
                return False
        msg = "\n\nValue {} ({}) not of the correct type: {}\n\n".format(
            config[k], type(config[k]), v)
        if not isinstance(config[k], v):
            print(msg)
            return False

    # Make sure that the project name is only alphanumeric with underscores
    msg = "\n\nProject names can only be alphanumeric with underscores\n\n"
    if not re.match("^[a-zA-Z0-9_]*$", config["name"]):
        print(msg)
        return False

    return True


def submit_jobs(config, force=False):
    """Submit a set of jobs."""
    if config.get('status') in ["SUBMITTED", "COMPLETED", "CANCELED"]:
        if force:
            print("Project has already been submitted, resubmitting.")
        else:
            print("Project has already been submitted, exiting.")
            return config

    if "jobs" in config:
        if force:
            print("'jobs' already found in config, resubmitting.")
        else:
            print("'jobs' already found in config, exiting.")
            return config

    # Set up the list of jobs
    config["jobs"] = []

    # Set up the connection to Batch with boto
    client = boto3.client('batch')

    for sample_name, file_names in config["samples"].items():
        # Use the parameters from the input file to submit the jobs
        job_name = "{}_{}".format(config["name"], sample_name)
        job_name = job_name.replace(".", "_")

        parameters = {
                        "input": "+".join(file_names),
                        "sample_name": sample_name,
                        "output_folder": config["output_folder"]
                     }
        if "db" in config:
            parameters["ref_db"] = config["db"]

        if "parameters" in config:
            for k, v in config["parameters"].items():
                assert k not in parameters, "Cannot repeat key {}".format(k)
                parameters[k] = v
        if "containerOverrides" in config:
            r = client.submit_job(
                    jobName=job_name,
                    jobQueue=config["queue"],
                    jobDefinition=config["job_definition"],
                    parameters=parameters,
                    containerOverrides=config["containerOverrides"]
                )
        else:
            r = client.submit_job(
                    jobName=job_name,
                    jobQueue=config["queue"],
                    jobDefinition=config["job_definition"],
                    parameters=parameters
                )
        # Save the response, which includes the jobName and jobId (as a dict)
        config["jobs"].append({"jobName": r["jobName"], "jobId": r["jobId"]})

        print("Submitted {}: {}".format(job_name, r['jobId']))

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
                # Get the reason for the failure
                if 'reason' in j['attempts'][-1]['container']:
                    reason = j['attempts'][-1]['container']['reason']
                else:
                    reason = j['statusReason']
                failed_jobs.append((j['jobId'], reason))

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

        if len(tot_objs) > 0:
            # Print the total number of objects in the folder
            print("\n\nFiles in output folder: {}\n".format(len(tot_objs)))
            # Sort by datetime
            tot_objs = sorted(tot_objs, key=get_last_modified, reverse=True)
            print("\nMost recent files in output folder:\n")
            for obj in tot_objs[:20]:
                print("{}\t{}\t{}".format(obj['LastModified'], obj['Size'], obj['Key'].split('/')[-1]))
            print('\n')

    return config


def refresh_jobs(config):
    """Reset a particular config file, removing pending and completed jobs."""

    # Get all of the objects in the output folder
    # Currently only support S3
    assert config['output_folder'].startswith('s3://')

    # Check to see how what files are in the output folder
    bucket = config['output_folder'][5:].split('/')[0]
    prefix = config['output_folder'][(5 + len(bucket) + 1):]

    # Get all of the objects from S3
    compl = aws_s3_ls(bucket, prefix)
    print("Files in output folder: {}".format(len(compl)))

    # Try to account for some alternate file endings
    compl = compl + [x.replace(".json.gz", "") for x in compl]
    compl = compl + [x.replace(".fastq", "") for x in compl]
    compl = compl + [x.replace(".gz", "") for x in compl]
    compl = compl + [x.replace(".json", "") for x in compl]
    compl = set(compl)

    print("Samples in project JSON: {}".format(len(config["samples"])))
    # Remove the samples that have already been completed
    config["samples"] = {
        k: v
        for k, v in config["samples"].items()
        if k not in compl
    }
    print("Samples remaining to process: {}".format(len(config["samples"])))

    # If there are no samples remaining, mark as COMPLETE
    if len(config["samples"]) == 0:
        config["status"] = "COMPLETED"
        return config

    # Delete the existing jobs (if any)
    if "jobs" in config:
        del config["jobs"]

    # Set the status as READY
    config["status"] = "READY"

    return config


def get_last_modified(obj):
    """Function to help sorting by datetime."""
    return int(obj['LastModified'].strftime('%s'))


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
        client.terminate_job(jobId=job_id, reason=cancel_msg)

    config["status"] = "CANCELED"

    return config


def save_all_logs(config, folder):
    """Save all of the logs to their own local file."""
    assert "jobs" in config, "No jobs found in config file"

    # Get the list of IDs
    id_list = [j["jobId"] for j in config["jobs"]]

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
                fp = "{}/{}.{}.{}.log".format(
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


def add_sra_prefix(s):
    if '/' in s or ':' in s:
        return s
    elif s[1:3] == "RR":
        return "sra://{}".format(s)
    else:
        return s


def make_project_from_metadata(project_name, metadata_fp, file_col, sample_col):
    """Make a project folder from a metadata CSV."""
    # Load in a metadata table
    msg = "{} does not exists"
    assert os.path.exists(metadata_fp), msg.format(metadata_fp)
    metadata = pd.read_table(metadata_fp, sep=',')

    # Check to see if another metadata object exists
    metadata_json = os.path.join(project_name, "metadata.json")
    msg = "Metadata file already exists: {}".format(metadata_json)
    assert os.path.exists(metadata_json) is False, msg

    # Make sure that the sample column and file column are present
    print("Using file column: {}".format(file_col))
    print("Using sample column: {}".format(sample_col))
    assert file_col in metadata.columns
    assert sample_col in metadata.columns

    # Make a new column named "_filepath"
    metadata["_filepath"] = metadata[file_col].apply(add_sra_prefix)

    # Organize the metadata as a dict of lists, keyed by sample name
    metadata_dat = {
        sample_name: sample_df.to_dict(orient="records")
        for sample_name, sample_df in metadata.groupby(sample_col)
    }

    # Write out the metadata as a JSON
    with open(metadata_json, "wt") as fo:
        json.dump(metadata_dat, fo)
        print("Wrote metadata to {}".format(metadata_json))


def aws_s3_ls(bucket, prefix):
    """List the contents of an S3 bucket."""

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


def make_config_json(template_fp, project_folder):
    """Make a analysis config JSON."""
    project_name = project_folder.rstrip("/").split("/")[-1]
    metadata_fp = os.path.join(project_folder, "metadata.json")
    assert os.path.exists(metadata_fp)
    metadata = json.load(open(metadata_fp, "rt"))

    assert os.path.exists(template_fp)

    # Read in the skeleton for the project definition
    project = json.load(open(template_fp))

    # OUTPUT LOCATION
    # Make sure the output base ends with a /
    if project["output_base"].endswith("/") is False:
        project["output_base"] = project["output_base"] + "/"

    # The following describes the path where all output files will be placed
    project["output_folder"] = "{}{}/{}/".format(project["output_base"],
                                                 project_name,     # Project name
                                                 project["name"])  # Analysis name

    # Add the project name to be used for the job names
    project["name"] = "{}_{}".format(project["name"], project_name)

    del project["output_base"]
    bucket = project["output_folder"].split('/')[2]
    prefix = '/'.join(project["output_folder"].split('/')[3:])

    samples = {
        k: [f["_filepath"] for f in files]
        for k, files in metadata.items()
    }

    # Get the contents of the output folder
    compl = aws_s3_ls(bucket, prefix)

    # Remove the samples that have already been completed
    print("Project: " + project_name)
    print("Total samples: {}".format(len(samples)))
    print("Files in output folder: {}".format(len(compl)))
    samples = {
        k: v
        for k, v in samples.items()
        if k + ".json.gz" not in compl
    }
    print("Unanalyzed samples: {}".format(len(samples)))

    # Add the samples to the project config object
    project['samples'] = samples

    # Write out the config file
    fp_out = os.path.join(project_folder, '{}.json'.format(project["name"]))
    with open(fp_out, 'wt') as fo:
        json.dump(project, fo, indent=4)

    print("Wrote out {}".format(fp_out))
    print("Analysis can be kicked off with: ")
    print("project-helper.py submit {}".format(fp_out))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
    Submit and monitor the status of projects on AWS Batch.
    """)

    parser.add_argument("cmd",
                        type=str,
                        help="""Command to run:
                        import, create, submit, monitor, cancel, logs, resubmit, or refresh""")

    parser.add_argument("project",
                        type=str,
                        help="""Project name (or path to analysis JSON).""")

    parser.add_argument("--metadata",
                        type=str,
                        help="""Metadata (CSV)""")

    parser.add_argument("--template",
                        type=str,
                        help="""Analysis template JSON""")

    parser.add_argument("--file-col",
                        type=str,
                        help="""Column name for file identifier""")

    parser.add_argument("--sample-col",
                        type=str,
                        help="""Column name for sample identifier""")

    parser.add_argument("--force-check",
                        action='store_true',
                        help="""Force check job status for COMPLETED projects""")

    args = parser.parse_args()

    valid_cmds = [
        "submit", "monitor", "cancel", "logs",
        "resubmit", "refresh", "import", "create"
    ]
    msg = "Please specify a command: {}".format(", ".join(valid_cmds))
    assert args.cmd in valid_cmds, msg

    if args.cmd == "import":
        assert args.metadata is not None, "Please specify metadata"
        assert args.sample_col is not None, "Please specify sample column"
        assert args.file_col is not None, "Please specify file column"

        make_project_from_metadata(
            args.project,
            args.metadata,
            args.file_col,
            args.sample_col,
        )
    elif args.cmd == "create":
        assert args.template is not None
        assert os.path.exists(args.project)
        assert os.path.exists(os.path.join(args.project, "metadata.json"))

        make_config_json(
            args.template,
            args.project)

    else:

        # Read in the config file
        config = json.load(open(args.project, 'rt'))
        # Make sure that the config file is valid
        assert valid_config(config)

        if args.cmd == "submit":
            # Submit a batch of jobs
            config = submit_jobs(config)
        elif args.cmd == "resubmit":
            # Submit a batch of jobs
            config = submit_jobs(config, force=True)
        elif args.cmd == "monitor":
            # Monitor the progress of a set of jobs
            config = monitor_jobs(config, force_check=args.force_check)
        elif args.cmd == "cancel":
            # Cancel all of the currently pending jobs
            config = cancel_jobs(config)
        elif args.cmd == "refresh":
            config = refresh_jobs(config)

        if args.cmd == "logs":
            # Save all of the logs to their own local file
            # Put the folder in the same directory as the config file
            if '/' not in args.project:
                log_folder = "logs"
            else:
                log_folder = '/'.join(
                    args.project.split("/")[:-1]
                ) + "/logs"

            if not os.path.exists(log_folder):
                os.mkdir(log_folder)

            save_all_logs(config, log_folder)
        else:
            # Update the config file
            with open(args.project, 'wt') as fo:
                json.dump(config, fo, indent=4)
