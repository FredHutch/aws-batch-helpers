# aws-batch-helpers
Helper scripts to run and monitor jobs on AWS Batch

The goal of this project is to make it easy to monitor and run large numbers of jobs using AWS Batch.


#### Setup

Prior to using this orchestration software, it is essential that you have set up the job definitions and
job queues (pointing to valid compute environments) needed on AWS Batch. You should also have AWS permissions
associated with your account in the local environment running this code (usually in `~/.aws`).

#### Concept and terminology

To analyze a set of data, we will run a number of `jobs`, each on a single `sample`. Each `sample` is
described by `metadata` describing the sample characteristics. Each `job` corresponds to a Job in AWS Batch.

Individual `jobs` may have one or more `outputs`, the presence of which determines whether the job is completed.

A `workflow` is a series of `jobs`, each of which is executed on *every* `sample` in the project. This is
limited to perfectly sequantial workflows -- no support for branching or converging dependency graphs.


#### Practial execution

The steps needed to set up and run a project are roughly as follows:

    1. Set up a `workflow` definition describing the series of jobs to be run on each `sample`
    2. Create a project from a CSV containing information for all of its `samples`
    3. Create a `workflow` for this project from the template
    4. Submit the `workflow` for analysis
    5. Monitor `job` statuses, restarting jobs as needed



#### Workflow definitions

Workflows are defined in a single JSON file with the following structure

``` json 
{
    # Name for the workflow as a whole
    "workflow_name": "FAMLI",

    # List of analyses to be executed for each sample
    "analyses": [
        {
            # Job definition (and revision number) in AWS Batch
            "job_definition": "famli:14",

            # List of output files that will be created upon completion
            "outputs": [
                # All embedded variables (e.g. project_name), will be filled in from project data
                "s3://fh-pi-fredricks-d/lab/Sam_Minot/data/{project_name}/{workflow_name}/{sample_name}.json.gz",
            ]
            "queue": "spot-test",
            "description": "Analysis of WGS datasets with FAMLI (v0.9)",
            "parameters": {
                "sample_name": "{sample_name}",
                "file_name": "{file_name}",
                "db": "/refdbs/uniref90_diamond_20180104/uniref90_diamond_20180104.dmnd",
                "threads": "16",
                "blocks": "15",
                "min_qual": "30",
                "batchsize": "200000000",
                "temp_folder": "/scratch/"
            },
            "containerOverrides": {
                "vcpus": 16,
                "memory": 122000
            }
        },
        # Multiple analyses may be listed
        {}
    ]
}
```

#### Creating projects

To make a new project, first create a metadata CSV with a column for the `sample_name` and `file_name`
for each sample in the project. Then execute the command:

```
batch_project import <project_name> --metadata <metadata_csv>
```

This will create a folder named <project_name>, as well as a metadata file in that folder (`metadata.json`).


#### Making workflows for projects

To make a workflow for a project, execute the command:

```
batch_project create <project_name> --definition <workflow_definition_file>
```

This will create the file: <project_name>/<workflow_name>.json

#### Submitting workflows for analysis

To submit a workflow for analysis on AWS Batch, execute the command:

```
batch_project submit <project_name>/<workflow_name>.json
```

#### Resubmitting failed jobs

To resubmit a set of failed jobs (possibly with different resource requests), execute the command:

```
batch_project submit <project_name>/<workflow_name>.json
```

#### Monitoring job status

To check on the status of the jobs in a previously executed workflow, execute the command:

```
batch_project status <project_name>/<workflow_name>.json
```

#### Monitoring groups of submitted workflows

To print a summary of the status of multiple submitted project:

```
batch_dashboard
```
