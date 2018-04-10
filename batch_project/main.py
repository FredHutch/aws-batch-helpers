#!/usr/bin/env python3
"""Submit and monitor the status of projects on AWS Batch."""

import os
import sys
import json
import boto3
import argparse
import pandas as pd
from tabulate import tabulate
from collections import defaultdict
from batch_project.lib import submit_workflow, get_workflow_status
from batch_project.lib import cancel_workflow_jobs, save_workflow_logs
from batch_project.lib import resubmit_failed_jobs, import_project_from_metadata
from batch_project.lib import create_workflow_from_template, valid_workflow


def clear_queue():
    parser = argparse.ArgumentParser(description="""
    Cancel or terminate all of the jobs in a queue.
    """)

    parser.add_argument("queue_name",
                        type=str,
                        help="""Name of job queue to clear""")
    parser.add_argument("--status",
                        type=str,
                        help="""Subset to jobs with a certain status""")

    # No arguments were passed in
    if len(sys.argv) < 2:
        parser.print_help()
        return

    args = parser.parse_args(sys.argv[1:2])

    # Connect to AWS Batch
    client = boto3.client("batch")

    jobs = []

    if args.status is None:
        job_status = ["SUBMITTED", "PENDING", "RUNNABLE",
                      "STARTING", "RUNNING", "SUCCEEDED"]
    else:
        job_status = args.status
        assert job_status in ["SUBMITTED", "PENDING",
                              "RUNNABLE", "STARTING", "RUNNING", "SUCCEEDED"]
        # Make into a list
        job_status = [job_status]
    for js in job_status:
        r = client.list_jobs(
            jobQueue=args.queue_name,
            jobStatus=js
        )
        jobs.extend(r["jobSummaryList"])
        while r.get("nextToken") is not None:
            r = client.list_jobs(
                jobQueue=args.queue_name,
                jobStatus=js,
                nextToken=r["nextToken"]
            )
            jobs.extend(r["jobSummaryList"])

    print("Number of jobs to clear from {}: {:,}".format(
        args.queue_name, len(jobs)
    ))
    # Prompt the user for confirmation
    response = input("Are you sure you want to cancel these jobs? (Y/N): ")
    assert response == "Y", "Do not cancel without confirmation"

    # Get a message to submit as justfication for the failure
    cancel_msg = input("What message should describe these cancellations?\n")

    for j in jobs:
        if j["job_status"] not in ["SUCCEEDED", "FAILED", "CANCELED"]:
            print("Cancelling {}".format(j["jobId"]))
            client.cancel_job(jobId=j["jobId"], reason=cancel_msg)
            client.terminate_job(jobId=j["jobId"], reason=cancel_msg)
            j["job_status"] = "CANCELED"


def queue_status():
    parser = argparse.ArgumentParser(description="""
    List the status of all of the jobs in a queue.
    """)

    parser.add_argument("queue_name",
                        type=str,
                        help="""Name of job queue""")
    parser.add_argument("--status",
                        type=str,
                        help="""Job status to check""")

    # No arguments were passed in
    if len(sys.argv) < 2:
        parser.print_help()
        return

    args = parser.parse_args(sys.argv[1:2])

    # Connect to AWS Batch
    client = boto3.client("batch")

    jobs = []

    if args.status is None:
        job_status = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING", "SUCCEEDED"]
    else:
        job_status = args.status
        assert job_status in ["SUBMITTED", "PENDING",
                              "RUNNABLE", "STARTING", "RUNNING", "SUCCEEDED"]
        # Make into a list
        job_status = [job_status]
    for js in job_status:
        r = client.list_jobs(
            jobQueue=args.queue_name,
            jobStatus=js
        )
        jobs.extend(r["jobSummaryList"])
        while r.get("nextToken") is not None:
            r = client.list_jobs(
                jobQueue=args.queue_name,
                jobStatus=js,
                nextToken=r["nextToken"]
            )
            jobs.extend(r["jobSummaryList"])

    status_counts = defaultdict(int)
    for j in jobs:
        status_counts[j["status"]] += 1
    print(pd.DataFrame({args.queue_name: status_counts}))


def dashboard():
    """Print a summary of all projects."""
    # Walk through all of the projects in the currect directory
    dat = {}  # Key by path

    n_completed = 0
    for root, subdirs, files in os.walk(os.getcwd()):
        for file in files:
            if file[0] == '_':
                continue
            if file.endswith(".json"):
                fp = os.path.join(root, file)
                try:
                    config = json.load(open(fp, "rt"))
                except ValueError:
                    raise Exception("Cannot open {}".format(fp))
                if valid_workflow(config, verbose=False):
                    if config["status"] == "COMPLETED":
                        n_completed += 1
                    else:
                        dat[fp] = get_workflow_status(fp)

    if len(dat) == 0:
        print("All projects are completed ({:,})".format(n_completed))
        return

    df = pd.DataFrame(dat).fillna(0).T
    df = df.reset_index().sort_values(by="index").set_index("index")
    print(tabulate(df, headers="keys"))

    print("\nCompleted projects: {}".format(n_completed))


def main():
    """Main function invoked by the user."""

    parser = argparse.ArgumentParser(description="""
    Submit and monitor the status of projects on AWS Batch.
    """)

    parser.add_argument("cmd",
                        type=str,
                        help="""Command to run:
                        import, create, submit, status, cancel, logs, or resubmit""")

    # No arguments were passed in
    if len(sys.argv) < 2:
        parser.print_help()
        return

    args = parser.parse_args(sys.argv[1:2])

    valid_cmds = [
        "submit", "status", "cancel", "logs",
        "resubmit", "import", "create"
    ]
    msg = "Please specify a command: {}".format(", ".join(valid_cmds))
    assert args.cmd in valid_cmds, msg

    if args.cmd == "submit":
        submit()
    elif args.cmd == "status":
        status()
    elif args.cmd == "cancel":
        cancel()
    elif args.cmd == "logs":
        logs()
    elif args.cmd == "resubmit":
        resubmit()
    elif args.cmd == "import":
        import_project()
    elif args.cmd == "create":
        create()


def submit():
    parser = argparse.ArgumentParser(description="""
    Submit a set of jobs for a project    
    """)

    parser.add_argument("workflow",
                        type=str,
                        help="""Path to JSON with workflow for project""")

    args = parser.parse_args(sys.argv[2:])

    # Submit the entire set of jobs in the workflow for analysis
    submit_workflow(args.workflow)


def status():
    parser = argparse.ArgumentParser(description="""
    Check the status of a project    
    """)

    parser.add_argument("workflow",
                        type=str,
                        help="""Path to JSON with workflow for project""")

    args = parser.parse_args(sys.argv[2:])

    print(
        json.dumps(
            get_workflow_status(args.workflow),
            indent=4
        )
    )

def cancel():
    parser = argparse.ArgumentParser(description="""
    Cancel the jobs for a project    
    """)

    parser.add_argument("workflow",
                        type=str,
                        help="""Path to JSON with workflow for project""")

    parser.add_argument("--status",
                        type=str,
                        default=None,
                        help="""If specified, only cancel jobs with this status (e.g. RUNNABLE)""")

    args = parser.parse_args(sys.argv[2:])

    cancel_workflow_jobs(args.workflow, status=args.status)



def logs():
    parser = argparse.ArgumentParser(description="""
    Get the logs for a project    
    """)

    parser.add_argument("workflow",
                        type=str,
                        help="""Path to JSON with workflow for project""")

    args = parser.parse_args(sys.argv[2:])

    save_workflow_logs(args.workflow)


def resubmit():
    parser = argparse.ArgumentParser(description="""
    Resubmit failed jobs for a project    
    """)

    parser.add_argument("workflow",
                        type=str,
                        help="""Path to JSON with workflow for project""")

    args = parser.parse_args(sys.argv[2:])

    resubmit_failed_jobs(args.workflow)



def import_project():
    parser = argparse.ArgumentParser(description="""
    Import metadata to create a project    
    """)

    parser.add_argument("project_name",
                        type=str,
                        help="""Project name""")

    parser.add_argument("--metadata",
                        required=True,
                        type=str,
                        help="""Metadata (CSV)""")

    parser.add_argument("--file-col",
                        default="file",
                        type=str,
                        help="""Column name for file identifier""")

    parser.add_argument("--sample-col",
                        default="sample",
                        type=str,
                        help="""Column name for sample identifier""")

    args = parser.parse_args(sys.argv[2:])

    import_project_from_metadata(
        args.project_name,
        args.metadata,
        sample_col=args.sample_col,
        file_col=args.file_col
    )



def create():
    parser = argparse.ArgumentParser(description="""
    Make a workflow for a project from a template    
    """)

    parser.add_argument("project_name",
                        type=str,
                        help="""Project name""")

    parser.add_argument("--template",
                        type=str,
                        help="""Workflow template JSON""")

    args = parser.parse_args(sys.argv[2:])

    create_workflow_from_template(
        args.project_name,
        args.template
    )

