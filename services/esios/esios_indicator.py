import datetime as dt
import glob
import json
import os
from typing import Optional

import requests
from jinja2 import Environment

import constants
from services.esios.esios_base import EsiosBase


class EsiosIndicator(EsiosBase):
    def __init__(self, jinja_env: Environment):
        super().__init__(jinja_env)

    def _get_new_esios_indicators_file_name(self, refresh_date: dt.date) -> str:
        return f"docs/data/esios/indicators_{refresh_date.strftime('%Y%m%d')}.json"

    def _get_esios_indicators(self, original_indicators_file, new_indicators_file) -> Optional[str]:
        esios_indicators_url = 'https://api.esios.ree.es/indicators'

        response = requests.get(esios_indicators_url, headers=self.headers)

        if response.status_code == 200:
            response_json = json.loads(response.text.replace('espa?', 'espa&ntilde;').replace('Espa?', 'Espa&ntilde;'))

            with open(new_indicators_file, 'w', encoding='utf-8') as outfile:
                json.dump(response_json, outfile, ensure_ascii=False, indent=4)

            if original_indicators_file:
                os.remove(original_indicators_file)

            return new_indicators_file
        else:
            raise Exception(response.status_code)

    def refresh_esios_indicators(self) -> None:
        today = dt.date.today()

        indicators_files = glob.glob("docs/data/esios/indicators_*.json")

        update_indicators_file = False
        indicators_file = None
        indicators_date = None

        if indicators_files:
            # given that the filename of the indicators is in the format: indicators_YYYY-MM-DD.csv, parse date from filename
            indicators_file = indicators_files[0]
            indicators_date = dt.datetime.strptime(os.path.basename(indicators_file).split("_")[1].split('.')[0], "%Y%m%d").date()

            # if the indicators file is older than 1 month, download new indicators
            if (today - indicators_date).days > 30:
                update_indicators_file = True
        else:
            update_indicators_file = True

        if update_indicators_file:
            try:
                print("INFO: Refreshing E-SIOS indicators...")

                indicators_file = self._get_esios_indicators(indicators_file, self._get_new_esios_indicators_file_name(today))
                indicators_date = today
            except Exception as e:
                print(f"ERROR: Failed to refresh E-SIOS indicators: {e}")

        if indicators_file:
            with open(indicators_file, 'r', encoding='utf-8') as json_file:
                json_data = json.loads(json_file.read())

            self.jinja_env.get_template('esios/esios_indicators.html')\
                .stream(
                    update_date=indicators_date.strftime('%d/%m/%Y'),
                    esios_indicators_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
                    indicators=sorted(json_data['indicators'], key=lambda i: i['id']),
                ).dump(os.path.join('docs', 'esios', 'esios_indicators.html'))

            indicators_to_export = [10391]

            for indicator_id in indicators_to_export:
                indicator = next(filter(lambda i: i['id'] == indicator_id, json_data['indicators']))

                self.jinja_env.get_template('esios/esios_indicator_card.html') \
                    .stream(
                        id=indicator['id'],
                        name=indicator['name'],
                        description=indicator['description'],
                    ).dump(os.path.join('templates', 'parts', f'esios_indicator_card_{indicator_id}.html'))
