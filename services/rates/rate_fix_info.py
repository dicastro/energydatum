from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from services.rates.rate_base import RateInfoBase


class RateFixInfo(RateInfoBase):
    def __init__(self):
        super().__init__('fix')

        self.PERIOD_COLOR = {
            'P1': '#ef553b'
        }

        self.SERIES = [{
                'title': 'L-V',
                'period_seq': [
                    'P1', 'P1', 'P1', 'P1', 'P1', 'P1', 'P1', 'P1',
                    'P1', 'P1', 'P1', 'P1', 'P1', 'P1', 'P1', 'P1',
                    'P1', 'P1', 'P1', 'P1', 'P1', 'P1', 'P1', 'P1'
                ]
            }, {
                'title': 'S-D + F',
                'period_seq': [
                    'P1', 'P1', 'P1', 'P1', 'P1', 'P1', 'P1', 'P1',
                    'P1', 'P1', 'P1', 'P1', 'P1', 'P1', 'P1', 'P1',
                    'P1', 'P1', 'P1', 'P1', 'P1', 'P1', 'P1', 'P1',
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
        return 'P1'
