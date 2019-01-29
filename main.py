#!/usr/bin/env python3

import subprocess
import json
import boto3.s3
import os
from datetime import date, datetime, timedelta

from log import logger
from subprocess import run

ACCOUNT = "sociallake"
AZ_BASEDIR = "/streamsets/prod"
DESTDIR = "/home/ubuntu"
CLIENTS = [ "cap", "chan", "demo", "discovermovies", "discovery", "disney", "disneymarketing", "europe",
            "ham68", "mexico", "midterms", "midterms_2", "starbucks" ]
# CLIENTS = [ "web", "stg", "dev2" ]

AZ_CLI_CMD =  "az dls fs" 
AZ_DOWNLOAD =  "download --account $1 --source-path $2 --destination-path $3 --overwrite"
AZ_FILE_LIST =  "list --account $1 --path $2"

S3_BUCKET_NAME = "nk-social-streamsets"

COMPLETED_FILE_NAME = 'completed.txt'
COMPLETED_FILE_PATH = f"{DESTDIR}/{AZ_BASEDIR}/{COMPLETED_FILE_NAME}"

# list of filepaths already loaded

completed = set()

def read_completed_file_list():
    if (os.path.exists(COMPLETED_FILE_PATH)):
        with open(COMPLETED_FILE_PATH, "r") as compfile:
            paths = compfile.readlines()
            for path in paths:
                completed.add(path.rstrip())
    else:
        with open(COMPLETED_FILE_PATH, "w+"):
            logger.info(f"Created {COMPLETED_FILE_PATH}")

def log_elapsed(func, start):
    elapsed = datetime.now() - start
    logger.info(f"{func} took {elapsed}")
    return elapsed

def get_s3_bucket():
    logger.info(f"Getting s3 bucket {S3_BUCKET_NAME}")
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET_NAME)
    return bucket

def get_uploaded_file_list(s3_bucket, prefix):
    objects = s3_bucket.objects.filter(Prefix=prefix)
    objlist = list(objects.all())
    already_uploaded = set()
    for obj in objlist:
        already_uploaded.add(obj.key)
    logger.info(f"{len(already_uploaded)} have already been uploaded to this bucket")
    return already_uploaded

def get_files(AZ_BASEDIR, client):
    """
    Given AZ_BASEDIR (/streamsets/prod) and dir (client name)
    Get the 'list' in the path. Return a list of these values (paths in az)
    """
    src_dir = os.path.join(AZ_BASEDIR, client)
    logger.debug(f"Getting files for {src_dir}")

    cmd = f"{AZ_CLI_CMD} {AZ_FILE_LIST}"
    cmd = cmd.replace("$1", ACCOUNT).replace("$2", src_dir)
    args = cmd.split(" ")

    output = subprocess.run(args, check=True, timeout=300, encoding='utf-8', stdout=subprocess.PIPE)
    entries = json.loads(output.stdout)
    paths = []
    for entry in entries:
        paths += [entry.get('name')]
    logger.info(f"Found {len(paths)} files in {src_dir}")
    return sorted(paths, reverse=True)
      
def download_files(destdir, path):
    cmd = f"{AZ_CLI_CMD} {AZ_DOWNLOAD}"
    dest_path = f"{destdir}/{path}"
    cmd = cmd.replace("$1", ACCOUNT).replace("$2", path).replace("$3", dest_path)
    args = cmd.split(" ")
    logger.debug("Downloading files...")
    return(subprocess.run(args, check=True, encoding="utf-8", stdout=subprocess.PIPE))
    
def get_downloaded_files(dest_dir, filepath):
    downloads = []
    dest = os.path.join(dest_dir, filepath)
    logger.info(f"Finding downloaded files in {dest}")
    for dirname, dirnames, filenames in os.walk(dest):
        for filename in filenames:
            downloads += [(os.path.join(dirname, filename))]
    logger.info(f"Found {len(downloads)} downloaded files for {dest}")
    return sorted(downloads)

def upload_file(tmpfile, filename, bucket):
    logger.debug(f"Attempting to upload {filename}")
    try:
        with open(tmpfile, "rb") as data:
            bucket.upload_fileobj(data, filename)
        logger.debug(f"Uploaded {filename} to s3 bucket {S3_BUCKET_NAME}")
    except Exception as e:
        logger.error("Error uploading {filename}")
        logger.exception(e)
    finally:
        delete_file(tmpfile)

def delete_file(filepath):
    if (os.path.exists(filepath)):
        try:
            os.remove(filepath)
            logger.info(f"Deleted source file {filepath}")
        except Exception as e:
            logger.error(f"Error deleting {filepath}")
            logger.exception(e)
    else:
        logger.debug(f"Did not find file {filepath} to delete")

def mark_completed(filepath):
    today = datetime.utcnow().date().strftime('%Y-%m-%d')
    elems = filepath.split("/")
    filedate = elems[len(elems)-1]
    if (today == filedate):
        logger.info("Not marking current day as completed")
    else:
        try:
            completed.add(filepath)
            with open(COMPLETED_FILE_PATH, "a") as comp:
                comp.write(filepath)
                comp.write("\n")
                logger.info(f"Marked {filepath} as completed")
        except Exception as e:
            logger.error(f"Unable to mark {filepath} as completed, continuing...")
  
def copy_files_in_path(filepath):
    start = datetime.now()
    try:
        result = download_files(DESTDIR, filepath)
        downloads = get_downloaded_files(DESTDIR, filepath)
        already_uploaded = get_uploaded_file_list(s3_bucket, filepath)
        for srcpath in downloads:
            destpath = srcpath.replace(f"{DESTDIR}/", "")
            if (destpath not in already_uploaded):
                upload_file(srcpath, destpath, s3_bucket) 
        str = f"Copying {len(downloads)} files in {filepath}"
        mark_completed(filepath)
        cleanup(downloads)
        log_elapsed(str, start)
    except Exception as e:
        logger.error(f"Error copying files from {filepath} to s3")
        logger.exception(e)
        
def cleanup(downloads):
   for path in downloads:
       delete_file(path)

if __name__ == "__main__":
    s3_bucket = get_s3_bucket()
    read_completed_file_list()
    for client in CLIENTS:
        pathlist = get_files(AZ_BASEDIR, client)
        for path in pathlist:
            logger.info(f"Path is {path}")
            if (path not in completed):
                logger.info(f"Path {path} not found in completed")
                copy_files_in_path(path)
            else:
                logger.info(f"{path} has already been copied")
