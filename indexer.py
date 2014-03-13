import base64
from genericpath import isfile, isdir
import json
from os import listdir
import os
from os.path import join
from elasticsearch import Elasticsearch
import time
import config
from utils import get_file_base64
import logging

logging.basicConfig(level=config.LOGGING_LEVEL, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger("indexer")
es = Elasticsearch()
indexed_images = 0

# disable info log for unneeded
logging.getLogger("elasticsearch").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)


def create_mapping():
    features = {}
    for f in config.INDEX_FEATURES:
        features[f] = {
            "hash": "BIT_SAMPLING"
        }
    mapping = {
        config.TYPE_NAME: {
            "_source": {
                "excludes": ["img"]
            },
            "properties": {
                "img": {
                    "type": "image",
                    "feature": features
                },
                "filename": {
                    "type": "string",
                    "index": "not_analyzed"
                }
            }
        }
    }
    if not es.indices.exists(index=config.INDEX_NAME):  # only create index if not exist
        es.indices.create(index=config.INDEX_NAME)
    es.indices.put_mapping(doc_type=config.TYPE_NAME, body=json.dumps(mapping), index=config.INDEX_NAME)


def index_image(folder_path, file_name):
    path = join(folder_path, file_name)
    file_name = os.path.relpath(path, config.IMAGE_FOLDER)
    doc = {
        "img": get_file_base64(path),
        "filename": file_name
    }
    index_id = base64.encodestring(file_name).strip()
    if config.IGNORE_EXIST and es.exists(index=config.INDEX_NAME, doc_type=config.TYPE_NAME, id=index_id):
        logger.debug("%s already exist, ignore", file_name)
        return  # already indexed ignore
    es.index(index=config.INDEX_NAME, doc_type=config.TYPE_NAME, body=json.dumps(doc), id=index_id)
    global indexed_images
    indexed_images += 1
    if indexed_images % config.INDEX_LOG_INTERVAL == 0:
        logger.info("Indexed %d images", indexed_images)


def index_image_folder(folder_path):
    for f in listdir(folder_path):
        path = join(folder_path, f)
        if isfile(path):
            is_image = any(path.endswith("." + ext) for ext in config.IMAGE_EXTENSIONS)   # check extension, only index images
            if is_image:
                index_image(folder_path=folder_path, file_name=f)
        elif isdir(path):
            index_image_folder(path)



if __name__ == "__main__":
    start = time.time()
    logger.info("Start indexer: %d", start)
    create_mapping()
    index_image_folder(config.IMAGE_FOLDER)
    end = time.time()
    logger.info("Finish indexer: %d", start)
    logger.info("Time used: %d s", end - start)
