import json
from typing import Dict

import pytz
import requests
import datetime as dt
from jinja2 import Environment

from services.esios.esios_base import EsiosBase


class EsiosPrice(EsiosBase):
    def __init__(self, jinja_env: Environment, jinja_common_context: Dict[str, any]):
        super().__init__(jinja_env, jinja_common_context)

    def _get_esios_price_url(self):
        PRICE_20TD_DATE_MIN = dt.date(2021, 6, 1)
        range_e = dt.date(2022, 4, 30)

        range_s_str = pytz.timezone('europe/madrid').localize(dt.datetime.combine(PRICE_20TD_DATE_MIN, dt.datetime.min.time())).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        range_e_str = pytz.timezone('europe/madrid').localize(dt.datetime.combine(range_e, dt.datetime.max.time())).strftime('%Y-%m-%dT%H:%M:%S.%f%z')

        return f'https://api.esios.ree.es/indicators/1739?start_date={range_s_str}&end_date={range_e_str}&geo_ids[]=3'

    def get_prices(self) -> None:
        response = requests.get(self._get_esios_price_url(), headers=self.headers)

        if response.status_code == 200:
            print(response.text)
            #json_data = json.loads(response.text)
