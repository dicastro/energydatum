import os
from typing import Dict

from jinja2 import Environment


class EsiosBase:
    def __init__(self, jinja_env: Environment, jinja_common_context: Dict[str, any]):
        self.headers = {
            'Accept': 'application/json; application/vnd.esios-api-v1+json',
            'Content-Type': 'application/json',
            'Host': 'api.esios.ree.es',
            'Authorization': 'Token token=' + os.environ.get("ESIOS_TOKEN")
        }
        self.jinja_env = jinja_env
        self.jinja_common_context = jinja_common_context
