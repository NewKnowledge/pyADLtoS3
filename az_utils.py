import os
import json
from log import logger
import subprocess

ACCOUNT = "sociallake"
AZ_CLI_CMD =  "az dls fs" 
AZ_FILE_LIST =  "list --account $1 --path $2"
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
