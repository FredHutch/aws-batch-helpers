# aws-batch-helpers
Helper scripts to run and monitor jobs on AWS Batch

The goal of this project is to make it easy to monitor and run large numbers of jobs using AWS Batch.


#### Setup

Prior to using this orchestration software, it is essential that you have set up the job definitions and
job queues (pointing to valid compute environments) needed on AWS Batch. You should also have AWS permissions
associated with your account in the local environment running this code (usually in `~/.aws`).

#### Concept and terminology

To analyze a set of data, we will run a number of `jobs`, and each `job` corresponds to a Job in AWS Batch.

Individual `jobs` may have one or more `outputs`, the presence of which determines whether the job is completed.

A `workflow` is a set of `jobs`, some of which may be connected as dependencies within the dependency structure provided by AWS Batch, where the downstream job is only started when the upstream job has finished successfully. Note that job success is determined by Batch as the task finishing with exit status 0, with no concept of whether the appropriate output files may exist. In contrast, the larger workflow management system provided in this set of tools will also take into account whether the output files for a `job` already exists, in which case there is no need to submit that `job` to AWS Batch.


#### Practial execution

A single overarching object is created within a Python script of the class `AWSBatchHelper`, and each individual `job` is added with the `AWSBatchHelper.add_job()` function, which returns a `jobId`. The larger Python script will provide the `jobId` as a dependency to any downstream jobs to create a full workflow. At the time that `add_job()` is invoked, the presence of output files are checked on S3, and if they do not exist the job will be submitted to AWS Batch. Even though the job has been submitted, it may not start if the upstream dependent jobs have not been completed.

The function `add_job()` will also check to see whether an identical job is present with a status of SUBMITTED, RUNNABLE, or RUNNING. If so, it will return the `jobId` for that previously created job instead of making a new one. 

After all of the `job` objects have been submitted, the Python script may invoke the `AWSBatchHelper.monitor()` process, which will periodically monitor `job` status and outputs, printing a summary to the screen.


### Best practices

We suggest that you keep a single CSV with the metadata for all of your samples, and then structure your workflows such that they read in the metadata sheet and start the set of jobs that are required given the project. 

### Example workflows

Examples of the workflow structure envisioned for this utility (as well as the parameters used to invoke jobs with `AWSBatchHelper.add_job()`) can be found in the `workflows` directory.
