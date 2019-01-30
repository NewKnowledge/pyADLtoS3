import os
import json
from log import logger
import subprocess

ACCOUNT = "sociallake"
AZ_CLI_CMD =  "az dls fs" 
AZ_FILE_LIST =  "list --account $1 --path $2"
AZ_DOWNLOAD =  "download --account $1 --source-path $2 --destination-path $3 --overwrite"
def get_directories_az(AZ_BASEDIR, client):
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
    logger.info(f"Found {len(paths)} files in {src_dir} on azure")
    return sorted(paths, reverse=True)


def download_files(destdir, path):
    """
    Downloads file from Azure to desired destination
    Returns console output
    The 'path' argument is the directory in az. The local
    destination directory copies the structure found on az
    """
    cmd = f"{AZ_CLI_CMD} {AZ_DOWNLOAD}"
    dest_path = f"{destdir}/{path}" 
    cmd = cmd.replace("$1", ACCOUNT).replace("$2", path).replace("$3", dest_path)
    args = cmd.split(" ")
    logger.debug("Downloading files...")
    return(subprocess.run(args, check=True, encoding="utf-8", stdout=subprocess.PIPE))
