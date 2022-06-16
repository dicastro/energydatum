import datetime as dt
import decimal
import glob
import json
import os
from typing import Dict, List, Tuple

import pandas as pd
import pytz
import requests
from dateutil.relativedelta import relativedelta
from jinja2 import Environment
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import to_date, col, min as ps_min, max as ps_max, year, month, when, dayofmonth, dayofweek, \
    lit, date_format, concat, to_timestamp, lpad, row_number, count as ps_count, avg, round as ps_round
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, DecimalType, TimestampType, StringType

import utils
from services.esios.esios_base import EsiosBase


class EsiosPrice(EsiosBase):
    """
    10229 PEAJE 2.0.A
    10230 PEAJE 2.0 DHA
    10231 PEAJE 2.0.DHS
    10391 PEAJE 2.0TD

    geos 8741 - Peninsula | 8742 - Canarias | 8743 - Baleares | 8744 - Ceuta | 8745 - Melilla
    """
    def __init__(self, jinja_env: Environment, jinja_common_context: Dict[str, any], spark: SparkSession):
        super().__init__(jinja_env, jinja_common_context)
        self.spark = spark

        self.DATE_FORMAT_URL = '%Y-%m-%dT%H:%M:%S.%f%z'
        self.PRICE_DATE_MIN = dt.date(2021, 6, 1)

        self.input_schema = StructType([
            StructField('index', IntegerType()),
            StructField('date', DateType(), False),
            StructField('hour', IntegerType(), False),
            StructField('price_mwh', DecimalType(scale=2), False)
        ])

        self.output_schema = StructType([
            StructField('date', DateType(), False),
            StructField('hour', IntegerType(), False),
            StructField('hour_pricebuy_emwh', DecimalType(scale=2), False),
            StructField('hour_pricesell_emwh', DecimalType(scale=2), False)
        ])

        self.window_date = Window.partitionBy(col('date'))
        self.window_date_ordered_by_index = Window.partitionBy(col('date')).orderBy(col('index'))

        self.sdf = None
        self.price_years = []
        self.price_date_min = None
        self.price_date_max = None
        self.price_buy_scale = None
        self.price_sell_scale = None

        self._load_prices()

    def _claculate_price_years(self) -> None:
        self.price_years = list(self.sdf.select(year('date').alias('year')).distinct().orderBy('year').toPandas()['year'])

    def _calculate_price_buy_scale(self) -> None:
        self.price_buy_scale = len(str(self.sdf.select(ps_max('hour_pricebuy_emwh').alias('price_mwh_max')).first()['price_mwh_max'])) - 1

    def _calculate_price_sell_scale(self) -> None:
        self.price_sell_scale = len(str(self.sdf.select(ps_max('hour_pricesell_emwh').alias('price_mwh_max')).first()['price_mwh_max'])) - 1

    def _calculate_price_scale(self) -> None:
        self._calculate_price_buy_scale()
        self._calculate_price_sell_scale()

    def _calculate_price_dates(self) -> None:
        self.price_date_min = self.sdf.select(ps_min('date').alias('date_min')).first()['date_min']
        self.price_date_max = self.sdf.select(ps_max('date').alias('date_max')).first()['date_max']

    def _calculate_meta_info(self):
        self._claculate_price_years()
        self._calculate_price_scale()
        self._calculate_price_dates()

    def _load_prices(self):
        price_files = glob.glob(os.path.join('docs', 'data', 'esios', 'price', 'raw', 'esios_price_20td_*.json'))

        self.sdf = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema=self.output_schema)

        if len(price_files) > 0:
            for price_file in price_files:
                price_file_df = pd.read_json(price_file, orient='split', convert_dates=False)
                price_file_df['date'] = price_file_df['date'].apply(lambda d: d[0:10])

                price_file_sdf = self.spark.createDataFrame(price_file_df)\
                    .withColumn('date', to_date(col('date'), 'yyyy-MM-dd'))\
                    .withColumn('hour', col('hour').cast(IntegerType()))\
                    .withColumn('hour_pricebuy_emwh', col('hour_pricebuy_emwh').cast(DecimalType(scale=2)))\
                    .withColumn('hour_pricesell_emwh', col('hour_pricesell_emwh').cast(DecimalType(scale=2)))

                # new spark DataFrame is created in order to ensure that it has the same schema as the previous ones
                self.sdf = self.sdf.union(self.spark.createDataFrame(price_file_sdf.toPandas(), schema=self.output_schema))

            self._calculate_meta_info()

    def _encode_date_to_url_param(self, date: dt.date, max_time: bool = False) -> str:
        datetime = dt.datetime.combine(date, dt.datetime.max.time() if max_time else dt.datetime.min.time())
        return pytz.timezone('europe/madrid').localize(datetime).strftime(self.DATE_FORMAT_URL)

    def _get_esios_price_url(self, indicator_id: int, range_s: dt.date, range_e: dt.date, geo_ids: List[int]) -> Tuple[str, Dict[str, any]]:
        range_s_str = self._encode_date_to_url_param(range_s)
        range_e_str = self._encode_date_to_url_param(range_e, max_time=True)

        params = {
            'start_date': range_s_str,
            'end_date': range_e_str,
            'geo_ids[]': geo_ids
        }

        return f'https://api.esios.ree.es/indicators/{indicator_id}', params

    def _get_esios_price_sell_url(self, range_s: dt.date, range_e: dt.date) -> Tuple[str, Dict[str, any]]:
        return self._get_esios_price_url(1739, range_s, range_e, [3])

    def _get_esios_price_buy_url(self, range_s: dt.date, range_e: dt.date) -> Tuple[str, Dict[str, any]]:
        return self._get_esios_price_url(10391, range_s, range_e, [8741])

    def _get_price_date_max(self) -> dt.date:
        if self.sdf.count() == 0:
            price_date_max = self.PRICE_DATE_MIN + relativedelta(days=-1)
        else:
            price_date_max = self.sdf.select(ps_max('date').alias('date_max')).first()['date_max']

        return price_date_max

    def _prices_should_be_updated(self, upper_date_range: dt.date):
        return self._get_price_date_max() < upper_date_range

    def _get_missing_prices_date_rage(self, until: dt.date) -> Tuple[dt.date, dt.date]:
        price_date_max = self._get_price_date_max()

        return price_date_max + relativedelta(days=1), until

    def _download_data(self, url: str, params: Dict[str, any]) -> DataFrame:
        response = requests.get(url, params=params, headers=self.headers)

        subset = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema=self.input_schema)

        if response.status_code == 200:
            json_data = json.loads(response.text)

            parsed_values = [[
                i,
                dt.datetime.strptime(value['datetime'][0:10], '%Y-%m-%d').date(),
                int(value['datetime'][11:13]),
                decimal.Decimal(str(value['value']))] for i, value in enumerate(json_data['indicator']['values'])]

            subset = self.spark.createDataFrame(parsed_values, schema=self.input_schema)

            # in days with 25 hours (when there was an hour change from 03:00 to 02:00), hour values are from 0 to 23
            # and hour 2 (02:00) appears twice
            # to implement an equivalent logic to which has been done with consumptions,
            # the extra hour 2 (the second occurrence, the fourth lecture of the day) is discarded from dataset
            subset = subset\
                .withColumn('hour_changed', when(ps_count('hour').over(self.window_date) == 25, -1)
                            .when(ps_count('hour').over(self.window_date) == 23, 1)
                            .otherwise(0))\
                .withColumn('hour_number', row_number().over(self.window_date_ordered_by_index))\
                .withColumn('keep_row', when(col('hour_changed') == 0, 1)
                            .when((col('hour_changed') == 1), 1)
                            .when((col('hour_changed') == -1) & (col('hour_number') != 4), 1)
                            .otherwise(0))

            subset = subset\
                .filter(col('keep_row') == 1)\
                .drop('index', 'hour_changed', 'hour_number', 'keep_row')\
                .cache()

        return subset

    def _refresh_prices(self, until: dt.date) -> None:
        if self._prices_should_be_updated(until):
            range_s, range_e = self._get_missing_prices_date_rage(until)
            print(f'[WARNING]: missing prices fom {range_s.strftime("%d/%m/%Y")} until {range_e.strftime("%d/%m/%Y")}, getting them from E-SIOS ...')

            buy_url, buy_params = self._get_esios_price_buy_url(range_s, range_e)

            print('[DEBUG]: downloading buy prices ...')
            buy_price_subset = self._download_data(buy_url, buy_params)\
                .withColumnRenamed('price_mwh', 'hour_pricebuy_emwh')

            sell_url, sell_params = self._get_esios_price_sell_url(range_s, range_e)

            print('[DEBUG]: downloading sell prices ...')
            sell_price_subset = self._download_data(sell_url, sell_params)\
                .withColumnRenamed('price_mwh', 'hour_pricesell_emwh')

            print('[DEBUG]: merging buy and sell prices ...')
            price_subset = buy_price_subset\
                .join(sell_price_subset, on=['date', 'hour'], how='left')

            self.sdf = self.sdf.union(price_subset)

            self._calculate_meta_info()

            print('[DEBUG]: persisting prices ...')
            for price_year in self.price_years:
                utils.df_to_json_file(self.sdf
                                      .filter(year('date') == price_year)
                                      .orderBy(col('date'), col('hour')),
                                      os.path.join('docs', 'data', 'esios', 'price', 'raw', f'esios_price_20td_{price_year}.json'), (0,))

            print('[DEBUG]: reloading prices from persisted files ...')
            self._load_prices()
        else:
            print(f'[INFO]: all prices present until {until.strftime("%d/%m/%Y")}')

    def get_price_date_range(self) -> Tuple[dt.date, dt.date]:
        return self.price_date_min, self.price_date_max

    def get_prices(self, until: dt.date) -> DataFrame:
        self._refresh_prices(until)

        window_year_month = Window.partitionBy(col('year'), col('month'))

        # consumption hour goes from 1 to 24, however, esios prices are for the hour 0 to 23
        # that is why we need to add 1 to the hour column
        return self.sdf\
            .withColumn('datetime', to_timestamp(concat(date_format(col('date'), 'yyyy-MM-dd'), lit(' '), lpad(col('hour'), 2, '0'), lit(':00:00')), format='yyyy-MM-dd HH:mm:ss'))\
            .withColumn('hour_pricebuy_ekwh', (col('hour_pricebuy_emwh') / 1000).cast(DecimalType(scale=self.price_buy_scale)))\
            .withColumn('hour_pricesell_ekwh', (col('hour_pricesell_emwh') / 1000).cast(DecimalType(scale=self.price_sell_scale)))\
            .withColumn('year', year('date'))\
            .withColumn('month', month('date'))\
            .withColumn('month_avg_pricebuy_ekwh', ps_round(avg('hour_pricebuy_ekwh').over(window_year_month), self.price_buy_scale).cast(DecimalType(scale=self.price_buy_scale)))\
            .withColumn('month_avg_pricesell_ekwh', ps_round(avg('hour_pricesell_ekwh').over(window_year_month), self.price_buy_scale).cast(DecimalType(scale=self.price_buy_scale)))\
            .withColumn('hour', col('hour') + 1)\
            .withColumn('month_text',
                        when(col('month') == 1, 'Enero')
                        .when(col('month') == 2, 'Febrero')
                        .when(col('month') == 3, 'Marzo')
                        .when(col('month') == 4, 'Abril')
                        .when(col('month') == 5, 'Mayo')
                        .when(col('month') == 6, 'Junio')
                        .when(col('month') == 7, 'Julio')
                        .when(col('month') == 8, 'Agosto')
                        .when(col('month') == 9, 'Septiembre')
                        .when(col('month') == 10, 'Octubre')
                        .when(col('month') == 11, 'Noviembre')
                        .when(col('month') == 12, 'Diciembre'))\
            .withColumn('dom', dayofmonth('date'))\
            .withColumn('dow', dayofweek('date'))\
            .withColumn('dow_text',
                        when(col('dow') == 1, 'Domingo')
                        .when(col('dow') == 2, 'Lunes')
                        .when(col('dow') == 3, 'Martes')
                        .when(col('dow') == 4, 'Miércoles')
                        .when(col('dow') == 5, 'Jueves')
                        .when(col('dow') == 6, 'Viernes')
                        .when(col('dow') == 7, 'Sábado'))\
            .withColumn('dow_order',
                        when(col('dow') == 1, 7)
                        .when(col('dow') == 2, 1)
                        .when(col('dow') == 3, 2)
                        .when(col('dow') == 4, 3)
                        .when(col('dow') == 5, 4)
                        .when(col('dow') == 6, 5)
                        .when(col('dow') == 7, 6))\
            .cache()
