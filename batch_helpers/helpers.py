import sys
import json
import boto3
import shutil
import logging
import traceback
import subprocess

def exit_and_clean_up(temp_folder):
    """Log the error messages and delete the temporary folder."""
    # Capture the traceback
    logging.info("There was an unexpected failure")
    exc_type, exc_value, exc_traceback = sys.exc_info()
    for line in traceback.format_tb(exc_traceback):
        logging.info(line.encode("utf-8"))

    # Delete any files that were created for this sample
    logging.info("Removing temporary folder: " + temp_folder)
    shutil.rmtree(temp_folder)

    # Exit
    logging.info("Exit type: {}".format(exc_type))
    logging.info("Exit code: {}".format(exc_value))
    sys.exit(exc_value)


def run_cmds(commands, retry=0, catchExcept=False, stdout=None):
    """Run commands and write out the log, combining STDOUT & STDERR."""
    logging.info("Commands:")
    logging.info(' '.join(commands))
    if stdout is None:
        p = subprocess.Popen(commands,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        stdout, stderr = p.communicate()
    else:
        with open(stdout, "wt") as fo:
            p = subprocess.Popen(commands,
                                 stderr=subprocess.PIPE,
                                 stdout=fo)
            stdout, stderr = p.communicate()
        stdout = False
    exitcode = p.wait()
    if stdout:
        logging.info("Standard output of subprocess:")
        for line in stdout.decode("latin-1").split('\n'):
            logging.info(line.encode("utf-8"))
    if stderr:
        logging.info("Standard error of subprocess:")
        for line in stderr.decode("latin-1").split('\n'):
            logging.info(line.encode("utf-8"))

    # Check the exit code
    if exitcode != 0 and retry > 0:
        msg = "Exit code {}, retrying {} more times".format(exitcode, retry)
        logging.info(msg)
        run_cmds(commands, retry=retry - 1)
    elif exitcode != 0 and catchExcept:
        msg = "Exit code was {}, but we will continue anyway"
        logging.info(msg.format(exitcode))
    else:
        assert exitcode == 0, "Exit code {}".format(exitcode)


def s3_path_exists(s3_path):
    """Check if a path exists on S3."""
    s3 = boto3.resource('s3')
    assert s3_path.startswith("s3://")
    bucket, key = s3_path[5:].split("/", 1)
    bucket = s3.Bucket(bucket)
    objs = list(bucket.objects.filter(Prefix=key))
    if len(objs) > 0 and objs[0].key == key:
        return True
    else:
        return False

    
def write_s3_json(dat, s3_path):
    """Write some data in JSON format to S3."""
    s3 = boto3.resource('s3')
    dat_json = json.dumps(dat)
    assert s3_path.startswith("s3://")
    bucket, key = s3_path[5:].split("/", 1)
    object = s3.Object(bucket, key)
    object.put(Body=dat_json)
