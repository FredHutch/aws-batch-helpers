# aws-batch-helpers
Helper scripts to run and monitor jobs on AWS Batch


### Nomenclature

Individual jobs are executed on samples, each of which correspond to run accessions in SRA (for the most part). Each project is made up of multiple samples. Projects are described by a config file, which includes all of the details on which job definition, job queue, samples, and jobs are part of a project.


### Project Config Files

The format of each project config file is as follows:

```
{
	"name": "HMP UniRef90 Sept 26 2017",
	"description": "Analysis of HMP datasets against the UniRef90 database",
	"job_definition": "diamond:1",
	"db": "s3://bucket/db_folder/database.dmnd",
	"output_folder": "s3://bucket/output_folder/",
	"queue": "queue_16cpu_120G",
	"samples": ["SRA12345", ...],
	"status": "SUBMITTED", # or COMPLETED, added after job submission
	"jobs": ["abcd0123-456e-7f89-01g2-345hi6789j01", ...]  # added after job submission
}
```


### Scripts

    * project-helper.py: Used to submit and monitor the status of projects on AWS Batch
