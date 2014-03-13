import base64
import json
import logging
from os.path import join
from jinja2 import Environment, FileSystemLoader, FileSystemBytecodeCache
import tornado
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.options import define, options
from tornado.web import HTTPError
from tornado import gen
import config
import utils


cache = None
if config.TEMPLATE_CACHE_DIR:
    cache = FileSystemBytecodeCache(directory=config.TEMPLATE_CACHE_DIR)
env = Environment(loader=FileSystemLoader("template"),
                  bytecode_cache=cache, cache_size=100,
                  trim_blocks=True)


class BaseHandler(tornado.web.RequestHandler):
    def get(self, *args, **kwargs):
        raise HTTPError(404)

    def render_template(self, template_name, template_args=None):
        if not template_args:
            template_args = {}
        template = env.get_template(template_name)
        html = template.render(template_args)
        self.write(html)
        self.finish()

    def get_single_argument(self, name):
        l = self.get_arguments(name=name)
        if l and len(l) > 0:
            return l[0]
        return None


class IndexPageHandler(BaseHandler):
    @gen.coroutine
    def get(self):
        filename = self.get_single_argument("filename")
        http_client = AsyncHTTPClient()
        search_request = {
            "fields": [
                "filename"
            ]
        }
        if filename is not None:
            feature = self.get_single_argument("feature") or "CEDD"
            path = join(config.IMAGE_FOLDER, filename)
            search_request['query'] = {
                "image": {
                    "img": {
                        "image": utils.get_file_base64(path),
                        "feature": feature,
                        "hash": "BIT_SAMPLING",
                        "limit": config.SEARCH_HASH_LIMIT
                    }
                }
            }
        request = HTTPRequest(url=utils.get_es_url() + "_search?size=" + str(config.RESULT_SIZE), method='POST', body=json.dumps(search_request))
        response = yield http_client.fetch(request)
        search_result = json.loads(response.body)
        args = {
            "search_result": search_result,
            "features": config.INDEX_FEATURES,
            "image_base_url": config.IMAGE_BASE_URL
        }
        self.render_template("index.html", template_args=args)




settings = {
    "cookie_secret": "61oETzKXQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=",
}
application = tornado.web.Application([
    (r"/", IndexPageHandler),
    (r'/static/(.*)', tornado.web.StaticFileHandler, {'path': config.IMAGE_FOLDER}),
], **settings)




define("port", default=8001, help="run on the given port", type=int)


if __name__ == "__main__":
    #options.logging = config.LOG_CONSOLE_LEVEL
    options.parse_command_line()

    logging.info("Start on port: " + str(options.port))

    from tornado.httpserver import HTTPServer
    server = HTTPServer(application, xheaders=True)
    loop = tornado.ioloop.IOLoop.instance()

    server.listen(port=options.port)
    loop.start()