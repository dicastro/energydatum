import os

from jinja2 import Environment


class EsiosBase:
    def __init__(self, jinja_env: Environment):
        self.headers = {
            'Accept': 'application/json; application/vnd.esios-api-v1+json',
            'Content-Type': 'application/json',
            'Host': 'api.esios.ree.es',
            'Authorization': 'Token token=' + os.environ.get("ESIOS_TOKEN")
        }
        self.jinja_env = jinja_env
