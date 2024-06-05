import json
import sys
from SUD.SMA.utils import prd_utils, blob_utils
import asyncio

class Transform():
    def __init__(self, plant_id):
        print(plant_id)
        files = asyncio.run(blob_utils.read_files_from_blob(plant_id,'Extract'))
        print(files)
        # print(f"{files=}")
        # blob_utils.move_files_to_processed(plant_id, files, 'Extract')
        return files

Xform = Transform()