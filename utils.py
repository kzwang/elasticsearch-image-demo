import base64
import config


def get_es_url():
    return config.ES_SERVER_ENDPOINT + "/" + config.INDEX_NAME + "/" + config.TYPE_NAME + "/"

def get_file_base64(file_path):
    with open(file_path, "rb") as image_file:
        return base64.b64encode(image_file.read())
