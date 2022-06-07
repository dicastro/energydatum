from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

import constants
from services.rates.rate_base import RateInfoBase


class RateInfo20TDInfo(RateInfoBase):
    def __init__(self):
        super().__init__('20td')

        self.PERIOD_COLOR = {
            'P1': '#ef553b',
            'P2': '#636efa',
            'P3': '#00cc96'
        }

        self.SERIES = [{
                'title': 'L-V',
                'period_seq': [
                    'P3', 'P3', 'P3', 'P3', 'P3', 'P3', 'P3', 'P3',
                    'P2', 'P2', 'P1', 'P1', 'P1', 'P1', 'P2', 'P2',
                    'P2', 'P2', 'P1', 'P1', 'P1', 'P1', 'P2', 'P2'
                ]
            }, {
                'title': 'S-D + F',
                'period_seq': [
                    'P3', 'P3', 'P3', 'P3', 'P3', 'P3', 'P3', 'P3',
                    'P3', 'P3', 'P3', 'P3', 'P3', 'P3', 'P3', 'P3',
                    'P3', 'P3', 'P3', 'P3', 'P3', 'P3', 'P3', 'P3',
                ]
            }]

        self.PERIODS = list(self.PERIOD_COLOR.keys())

    def get_color_sequence(self):
        return list(self.PERIOD_COLOR.values())

    def get_period_color_rgba(self, period, alpha=1):
        return f"rgba{(*self._hex_to_rgb(self.PERIOD_COLOR[period]), alpha)}"

    def get_series(self):
        return self.SERIES

    def get_periods(self):
        return self.PERIODS

    def get_period_colors(self):
        return self.PERIOD_COLOR

    @staticmethod
    @udf(returnType=StringType())
    def get_period(hour, dow, is_bank_day):
        if is_bank_day or dow in constants.DOW_WEEKEND_DAYS:
            return 'P3'
        elif (10 < hour <= 14) or (18 < hour <= 22):
            return 'P1'
        elif (8 < hour <= 10) or (14 < hour <= 18) or (22 < hour <= 24):
            return 'P2'
        else:
            return 'P3'
