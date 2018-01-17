#!/usr/bin/python

import os
import sys
import json
import boto3
import pandas as pd

# USAGE

# Arguments:
# 1. Analysis parameters (skeleton of analysis config object)
# 2. Metadata CSV with "Run_s" column containing a list of SRA accessions
# 3. Name for the project (determines the output location)

# Path to the skeleton of the job definition
skel_fp = sys.argv[1]
metadata_fp = sys.argv[2]
project_name = sys.argv[3]

# Number of samples to analyze per process
n_samples = 20

assert os.path.exists(skel_fp)
assert os.path.exists(metadata_fp)

# Read in the skeleton for the project definition
project = json.load(open(skel_fp))

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


# Read in the metadata
df = pd.read_table(metadata_fp, sep=',')
# Get the list of SRA accessions for each individual sample
sra_accessions = None
for col_name in ["Run_s", "Run"]:
    if col_name in df.columns:
        sra_accessions = list(df[col_name].values)

assert sra_accessions is not None, "No column named Run or Run_s"
sra_accessions = ["sra://" + acc for acc in sra_accessions]

# Get the contents of the output folder
compl = aws_s3_ls(bucket, prefix)

# Remove the samples that have already been completed
print("Project: " + project_name)
print("Total samples: {}".format(len(sra_accessions)))
print("Files in output folder: {}".format(len(compl)))
sra_accessions = [s for s in sra_accessions
                  if s.replace("sra://", "") + ".json.gz" not in compl]
print("Unanalyzed samples: {}".format(len(sra_accessions)))

# Add the samples to the project config object
project['samples'] = []

# Add the samples in batches
while len(sra_accessions) > 0:
    n = min(len(sra_accessions), n_samples)

    project['samples'].append(','.join(sra_accessions[:n]))

    sra_accessions = sra_accessions[n:]

# Write out the config file
fp_out = '{}.json'.format(project_name)
with open(fp_out, 'wt') as fo:
    json.dump(project, fo, indent=4)

print("Wrote out {}".format(fp_out))
print("Analysis can be kicked off with: ")
print("project-helper.py submit {}".format(fp_out))
