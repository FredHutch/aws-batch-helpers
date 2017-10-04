#!/usr/bin/python
"""Check the status of the jobs in a job queue."""

import boto3
import argparse


def list_jobs(job_queue, status, client):
    """Return all of the jobs in a given queue, with a given status."""
    jobs = []

    r = client.list_jobs(jobQueue=job_queue, jobStatus=status)
    jobs.extend(r['jobSummaryList'])
    while "nextToken" in r and r["nextToken"] is not None:
        r = client.list_jobs(nextToken=r["nextToken"])
        jobs.extend(r['jobSummaryList'])

    return jobs


def check_queue(job_queue):
    """Check the status of the jobs in a job queue."""
    # Set up the connection to Batch with boto
    client = boto3.client('batch')

    # Count up the number of jobs by status
    status_counts = {}

    # Iterate through each potential status
    for status in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING',
                   'RUNNING', 'SUCCEEDED', 'FAILED']:
        jobs = list_jobs(job_queue, status, client)
        if len(jobs) > 0:
            status_counts[status] = len(jobs)

    for k, v in status_counts.items():
        print("{}:\t{}".format(k, v))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
    Check the status of the jobs in a job queue on AWS Batch.
    """)

    parser.add_argument("job_queue",
                        type=str,
                        help="""Name of the job queue to check""")

    args = parser.parse_args()

    check_queue(args.job_queue)
