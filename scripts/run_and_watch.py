#!/usr/bin/python
"""Run a command in Batch and watch for the output."""

import os
import json
import argparse
import datetime
import subprocess
from time import sleep


def get_job_status(job_id):
    """Get the status of a job on AWS Batch."""
    # Invoke the AWS CLI
    p = subprocess.Popen(['aws', 'batch', 'describe-jobs', '--jobs', job_id],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    stdout, stderr = p.communicate()
    exitcode = p.wait()

    assert exitcode == 0, "Problem getting status of {}".format(job_id)

    return json.loads(stdout)


def get_logs(log_stream_name):
    """Get the logs from a run on AWS Batch."""
    # Invoke the AWS CLI
    p = subprocess.Popen(['aws', 'logs', 'get-log-events', '--log-stream-name', log_stream_name,
                          '--log-group-name', '/aws/batch/job'],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    stdout, stderr = p.communicate()
    exitcode = p.wait()

    assert exitcode == 0, "Problem getting status of {}".format(job_id)

    return json.loads(stdout)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
    Run a command in Batch and watch for the output.
    """)

    parser.add_argument("--job-name",
                        type=str,
                        required=True,
                        help="""The name of the job. The first character must be alphanumeric, and
                        up to 128 letters (uppercase and lowercase), numbers, hyphens, and
                        underscores are allowed.""")

    parser.add_argument("--job-queue",
                        type=str,
                        required=True,
                        help="""The job queue into which the job will be submitted. You can specify
                        either the name or the Amazon Resource Name (ARN) of the queue.""")

    parser.add_argument("--job-definition",
                        type=str,
                        required=True,
                        help="""The job definition used by this job. This value can be either a
                        name:revision or the Amazon Resource Name (ARN) for the job definition.""")

    parser.add_argument("--depends-on",
                        type=str,
                        required=False,
                        help="""A list of job IDs on which this job depends. A job can depend upon a
                        maximum of 20 jobs..""")

    parser.add_argument("--parameters",
                        type=str,
                        required=False,
                        help="""Additional parameters passed to the job that replace parameter
                        substitution placeholders that are set in the job definition. Parameters
                        are specified as a key and value pair mapping. Parameters in a submit-job
                        request override any corresponding parameter defaults
                        from the job definition.""")

    parser.add_argument("--container-overrides",
                        type=str,
                        required=False,
                        help="""A list of container overrides in JSON format that specify the name
                        of a container in the specified job definition and the overrides it should
                        receive. You can override the default command for a container (that is
                        specified in the job definition or the Docker image) with a command
                        override. You can also override existing environment variables (that are
                        specified in the job definition or Docker image) on a container or add new
                        environment variables to it with an environment override.""")

    parser.add_argument("--retry-strategy",
                        type=str,
                        required=False,
                        help="""The retry strategy to use for failed jobs from this submit-job
                        operation. When a retry strategy is specified here, it overrides the retry
                        strategy defined in the job definition.""")

    parser.add_argument("--cli-input-json",
                        type=str,
                        required=False,
                        help="""Performs service operation based on the JSON string provided. The
                        JSON string follows the format provided by aws batch submit-job
                        --generate-cli-skeleton. If other arguments are provided on the command
                        line, the CLI values will override the JSON-provided values.""")

    args = parser.parse_args()

    # Add the required values
    cmd = ['aws', 'batch', 'submit-job',
           '--job-name', args.job_name,
           '--job-queue', args.job_queue,
           '--job-definition', args.job_definition]

    # Add the optional values
    for k, v in [('--depends-on', args.depends_on),
                 ('--parameters', args.parameters),
                 ('--container-overrides', args.container_overrides),
                 ('--retry-strategy', args.retry_strategy)]:
        if v is not None:
            cmd.extend([k, v])

    # Add the CLI input JSON, if specified
    if args.cli_input_json is not None:
        cli_input_json = json.load(open(args.cli_input_json))
        cmd.extend(['--cli-input-json', cli_input_json])

    # Run the command
    p = subprocess.Popen(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    stdout, stderr = p.communicate()
    exitcode = p.wait()

    if exitcode != 0:
        if stdout:
            print("Standard output of subprocess:\n" + stdout)
        if stderr:
            print("Standard error of subprocess:\n" + stderr)

        # Error out, printing the exit code
        assert exitcode == 0, "Exit code {}".format(exitcode)

    # Get the job ID
    job_id = json.loads(stdout)
    job_id = job_id['jobId']

    job_status = get_job_status(job_id)['jobs'][0]
    os.system('clear')
    print(json.dumps(job_status, indent=4))
    while job_status['status'] != 'FAILED' and job_status['status'] != 'SUCCEEDED':
        sleep(1)
        job_status = get_job_status(job_id)['jobs'][0]
        os.system('clear')
        print(json.dumps(job_status, indent=4))

    # Get the logs
    logs = get_logs(job_status['container']['logStreamName'])

    # Save the logs to a file
    fp = "{:%Y-%m-%d-%H:%M:%S}-{}.log".format(datetime.datetime.now(), args.job_name)
    with open(fp, 'wt') as fo:
        for event in logs['events']:
            fo.write(event['message'] + '\n')

    print("Wrote out logs to {}".format(fp))
