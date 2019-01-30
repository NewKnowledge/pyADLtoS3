#!/usr/bin/env python3

import subprocess
import json                     
import boto3.s3
import os
import az_utils
# from datetime import date, datetime, timedelta
from datetime import datetime
from log import logger
# from subprocess import run

ACCOUNT = "sociallake"
AZ_BASEDIR = "/streamsets/prod"
DESTDIR = "/home/dan/NewKnowledge/tmpfile"
CLIENTS = [ "cap", "chan", "demo", "discovermovies", "discovery", "disney", "disneymarketing", "europe",
            "ham68", "mexico", "midterms", "midterms_2", "starbucks" ]
# CLIENTS = [ "web", "stg", "dev2" ]

AZ_LOGIN_CMD = ""
AZ_CLI_CMD =  "az dls fs" 
AZ_DOWNLOAD =  "download --account $1 --source-path $2 --destination-path $3 --overwrite"
AZ_FILE_LIST =  "list --account $1 --path $2"

S3_BUCKET_NAME = "nk-social-streamsets"

COMPLETED_FILE_NAME = 'completed.txt'
COMPLETED_RECORD_FILE_PATH = f"{DESTDIR}/{AZ_BASEDIR}/{COMPLETED_FILE_NAME}"

# list of filepaths already loaded

completed = set()

def download_and_transfer_hour(s3_bucket, destdir, path_with_date):
    hourly_paths = hourly_filepaths(destdir, path_with_date)
    
    for hourly_path in hourly_paths:
        start = datetime.now()

        logger.info(f"downloading from {hourly_path}")
        az_utils.download_files(destdir, hourly_path)
        downloaded_file_paths = get_downloaded_files(destdir, hourly_path)
        already_uploaded = get_uploaded_file_list(s3_bucket, hourly_path)

        for downloaded_file_path in downloaded_file_paths:
            s3_destpath = downloaded_file_path.replace(f"{destdir}/", "")
            if (s3_destpath not in already_uploaded):
                logger.info(f"\t upld {downloaded_file_path[11:80]}")
                upload_file(downloaded_file_path, s3_destpath, s3_bucket)
            else:
                logger.info(f"\t skip {downloaded_file_path[11:80]}")

        str = f"Copying {len(downloaded_file_paths)} files in {path_with_date}"
        log_elapsed(str, start)
        cleanup(downloaded_file_paths)
        

def hourly_filepaths(destdir, path_with_date):
    file_list_subcommand = AZ_FILE_LIST.replace("$1", ACCOUNT).replace("$2", path_with_date)
    az_command = f"{AZ_CLI_CMD} {file_list_subcommand}"

    args = az_command.split(" ")
    cli_output = subprocess.run(args, check=True, timeout=300, encoding='utf-8', stdout=subprocess.PIPE)
    cli_jsons = json.loads(cli_output.stdout)

    hourly_paths = []
    for entry in cli_jsons:
        hourly_paths += [entry.get('name')]

    logger.info(f"Found {len(hourly_paths)} files in {path_with_date}")
    return sorted(hourly_paths, reverse=True)

def read_completed_file_list():
    if (os.path.exists(COMPLETED_RECORD_FILE_PATH)):
        with open(COMPLETED_RECORD_FILE_PATH, "r") as compfile:
            paths = compfile.readlines()
            for path in paths:
                completed.add(path.rstrip())
    else:
        with open(COMPLETED_RECORD_FILE_PATH, "w+"):
            logger.info(f"Created {COMPLETED_RECORD_FILE_PATH}")

def log_elapsed(func, start):
    """
    timing fn.
    returns elapsed time.
    """
    elapsed = datetime.now() - start
    logger.info(f"{func} took {elapsed}")
    return elapsed

def get_s3_bucket():
    logger.info(f"Getting s3 bucket {S3_BUCKET_NAME}")
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET_NAME)
    return bucket

def get_uploaded_file_list(s3_bucket, prefix):
    """
    From s3, grab SET of objects that have already been loaded
    that has the same prefix (file path on az and local)
    """
    objects = s3_bucket.objects.filter(Prefix=prefix)
    objlist = list(objects.all())
    already_uploaded = set()
    for obj in objlist:
        already_uploaded.add(obj.key)
    logger.info(f"{len(already_uploaded)} with prefix {prefix} have already been uploaded to this bucket")
    return already_uploaded


    
def get_downloaded_files(dest_dir, filepath):
    """
    Retrieve the paths to all the files downloaded from Azure to local disk
    note that filepath does not go down to the file level; hence
    os.walk is necessary to recurse down to the files
    ?: os.walk
    """
    downloaded_local_files = []
    dest = os.path.join(dest_dir, filepath)

    logger.info(f"Finding downloaded files in {dest}")
    for dirname, dirnames, filenames in os.walk(dest):
        for filename in filenames:
            downloaded_local_files += [(os.path.join(dirname, filename))]
    logger.info(f"Found {len(downloaded_local_files)} downloaded files for {dest}")
    return sorted(downloaded_local_files)

def upload_file(local_filepath, s3_destpath, bucket):
    """
    Upload file that does not exist on S3 to S3.
    """
    logger.debug(f"Attempting to upload {s3_destpath}")
    try:
        with open(local_filepath, "rb") as data:
            bucket.upload_fileobj(data, s3_destpath)
        logger.debug(f"Uploaded {s3_destpath} to s3 bucket {S3_BUCKET_NAME}")
    except Exception as e:
        logger.error("Error uploading {s3_destpath}")
        logger.exception(e)
    finally:
        delete_file(local_filepath)

def delete_file(filepath):
    """
    Delete local file
    """
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
    """
    Add the filepath as completed in the record file
    """
    today = datetime.utcnow().date().strftime('%Y-%m-%d')
    elems = filepath.split("/")
    filedate = elems[len(elems)-1]
    if (today == filedate):
        logger.info("Not marking current day as completed")
    else:
        try:
            #completed is globally defined set
            completed.add(filepath)
            with open(COMPLETED_RECORD_FILE_PATH, "a") as comp:
                comp.write(filepath)
                comp.write("\n")
                logger.info(f"Marked {filepath} as completed")
        except Exception as e:
            logger.error(f"Unable to mark {filepath} as completed, continuing...")
  
# def copy_files_in_path(filepath):
#     start = datetime.now()
#     try:
#         result = download_files(DESTDIR, filepath)
#         downloaded_file_paths = get_downloaded_files(DESTDIR, filepath)
#         already_uploaded = get_uploaded_file_list(s3_bucket, filepath)

#         for downloaded_file_path in downloaded_file_paths:
#             s3_destpath = downloaded_file_path.replace(f"{DESTDIR}/", "") #remove local directory
#             if (s3_destpath not in already_uploaded):
#                 upload_file(downloaded_file_path, s3_destpath, s3_bucket) 

#         str = f"Copying {len(downloaded_file_paths)} files in {filepath}"
#         mark_completed(filepath)
#         cleanup(downloaded_file_paths)
#         log_elapsed(str, start)
#     except Exception as e:
#         logger.error(f"Error copying files from {filepath} to s3")
#         logger.exception(e)
        
def cleanup(downloads):
    """
    Wait, why is this necessary?
    """
    for path in downloads:
        delete_file(path)

if __name__ == "__main__":
    s3_bucket = get_s3_bucket()
    read_completed_file_list()
    
    for client in ["discovery"]:#CLIENTS: #TODO
        logger.info("client: {}".format(client))
        try:
            pathlist = az_utils.get_directories_az(AZ_BASEDIR, client)
        except Exception as e:
            logger.warning("Could not get directories from az: {}".format(e))
            continue

        for path in pathlist: #paths with date
            if (path in completed):
                logger.info(f"{path} has already been copied")
                continue

            logger.info(f"Path {path} is new")
            start = datetime.now()

            try:
                download_and_transfer_hour(s3_bucket, DESTDIR, path)
            except Exception as e:
                logger.warning("Something has gone wrong. {}".format(e))
                break

            # result = az_utils.download_files(DESTDIR, path)
            # downloaded_file_paths = get_downloaded_files(DESTDIR,path)
            # already_uploaded = get_uploaded_file_list(s3_bucket, path)

            # for downloaded_file_path in downloaded_file_paths:
            #     s3_destpath = downloaded_file_path.replace(f"{DESTDIR}/", "")
            #     if (s3_destpath not in already_uploaded):
            #         upload_file(downloaded_file_path, s3_destpath, s3_bucket)
            
            
            mark_completed(path)
            # cleanup(downloaded_file_paths)
            log_elapsed(str, start)
            # copy_files_in_path(path)
            
