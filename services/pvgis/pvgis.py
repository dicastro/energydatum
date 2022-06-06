import glob
import itertools
import json
import os
import re
from decimal import Decimal
from typing import List, Dict, Any

import dict_hash
import numpy as np
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, to_timestamp, hour, round as ps_round, month, dayofmonth, \
    avg, year, when, sum, abs as ps_abs, concat, lpad, lit, expr
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, FloatType, IntegerType

import utils


class Pvgis:
    def __init__(self, pvgis_config, spark):
        self.range_definition_regex = r'range\((?P<start>-?\d+(?:\.\d+)?),\s*(?P<end>-?\d+(?:\.\d+)?),\s*(?P<step>\d+(?:\.\d+)?)\)'

        self.url = 'https://re.jrc.ec.europa.eu/api/v5_2/seriescalc'
        self.pvgis_config = pvgis_config
        self.spark = spark

        self.pvgis_schema = StructType([
            StructField('time', StringType(), False),
            StructField('P', FloatType(), True),
            StructField('G(i)', FloatType(), True),
            StructField('H_sum', FloatType(), True),
            StructField('T2m', FloatType(), True),
            StructField('WS10m', FloatType(), True),
            StructField('Int', FloatType(), True),
        ])

        self.schema = StructType([
            StructField('month', IntegerType(), False),
            StructField('dom', IntegerType(), False),
            StructField('hour', IntegerType(), False),
            StructField('hour_production_kwh', DecimalType(precision=10, scale=3), False),
        ])

        self.default_params = {
            'peakpower': 1,
            'pvtechchoice': 'crystSi',
            'mountingplace': 'free',
            'loss': 0,
            'angle': 30,
            'aspect': 0
        }

        self.fixed_params = {
            'browser': 0,
            'outputformat': 'json',
            'raddatabase': 'PVGIS-SARAH2',
            'pvcalculation': 1
        }

        self.runtime_params = {}

        coordinates = os.getenv('COORDINATES')

        if coordinates:
            self.coordinates_present = True
            self.runtime_params['lat'], self.runtime_params['lon'] = [c.strip() for c in coordinates.split(',')]

        self.calibrations = None
        self.calibrations_file = os.path.join('docs', 'data', 'pvgis', 'pvgis_calibrations.json')

        if os.path.exists(self.calibrations_file):
            with open(self.calibrations_file, 'r') as f:
                self.calibrations = json.load(f)

        self.calibration_done = False
        # to_combine = []
        # to_combine_params = []
        #
        # if 'params_series' in self.pvgis_config:
        #     if 'peakpower' in self.pvgis_config['params_series'] and self.pvgis_config['params_series']['peakpower']:
        #         peakpower_raw_series = self.pvgis_config['params_series']['peakpower']
        #
        #         peakpower_series = self._get_range_values(peakpower_raw_series)
        #         to_combine.append(peakpower_series)
        #         to_combine_params.append('peakpower')
        #
        #     if 'angle' in self.pvgis_config['params_series'] and self.pvgis_config['params_series']['angle']:
        #         peakpower_raw_series = self.pvgis_config['params_series']['angle']
        #
        #         peakpower_series = self._get_range_values(peakpower_raw_series)
        #         to_combine.append(peakpower_series)
        #         to_combine_params.append('angle')
        #
        #     if 'aspect' in self.pvgis_config['params_series'] and self.pvgis_config['params_series']['aspect']:
        #         peakpower_raw_series = self.pvgis_config['params_series']['aspect']
        #
        #         peakpower_series = self._get_range_values(peakpower_raw_series)
        #         to_combine.append(peakpower_series)
        #         to_combine_params.append('aspect')
        #
        # params = []
        #
        # if len(to_combine) > 0:
        #     all_combinations = list(itertools.product(*to_combine))
        #
        #     for combination in all_combinations:
        #         series_params = {}
        #
        #         for i, v in enumerate(combination):
        #             series_params[to_combine_params[i]] = v
        #
        #         params.append(self.default_params | self.pvgis_config['params'] | self.fixed_params | self.runtime_params | series_params)
        # else:
        #     params.append(self.default_params | self.pvgis_config['params'] | self.fixed_params | self.runtime_params)
        #
        # self.production_sdfs = []
        #
        # for i, p in enumerate(params):
        #     print(f'{i+1}/{len(params)}: peakpower={p["peakpower"]}, angle={p["angle"]}, aspect={p["aspect"]}')
        #     self._load_data(p)

    def _get_range_values(self, start, end, step):
        start = Decimal(start) if isinstance(start, float) else start

        step = Decimal(step) if isinstance(step, float) else step

        end = (Decimal(end) if isinstance(end, float) else end) + step

        values = []
        current = start
        while current < end:
            values.append(current)
            current += step

        return [float(v) if isinstance(v, Decimal) else v for v in values]

    def _get_data_id(self, params) -> str:
        file_id = {
            'peakpower': params['peakpower'],
            'angle': params['angle'],
            'aspect': params['aspect']
        }

        return dict_hash.sha256(file_id)

    def _get_file_name(self, data_id: str) -> str:
        return f'pvgis_{data_id}.json'

    def _load_data(self, params: Dict[str, any], data_id: str) -> DataFrame:
        file_name = self._get_file_name(data_id)
        found_files = glob.glob(os.path.join('docs', 'data', 'pvgis', file_name))

        if len(found_files) == 0:
            sdf = self._download_data(params, file_name)
        else:
            sdf = utils.json_file_to_df(found_files[0], self.spark, self.schema)

        return sdf

    def _download_data(self, params: Dict[str, Any], file_name: str) -> DataFrame:
        if self.coordinates_present:
            response = requests.get(self.url, params=params, timeout=30)

            if response.status_code == 200:
                sdf = self.spark.createDataFrame(response.json()['outputs']['hourly'], self.pvgis_schema)\
                    .withColumn('timestamp', to_timestamp(col('time'), 'yyyyMMdd:HHmm'))\
                    .withColumn('date', to_date(col('time'), 'yyyyMMdd:HHmm'))\
                    .withColumn('year', year(col('timestamp')))\
                    .withColumn('month', month(col('timestamp'))) \
                    .withColumn('dom', dayofmonth(col('timestamp')))\
                    .withColumn('hour', hour(col('timestamp')) + 1)\
                    .withColumn('hour_production_kwh', ps_round(col('P').cast(DecimalType(scale=3)) / 1000, 3))\
                    .select(col('date'), col('year'), col('month'), col('dom'), col('hour'), col('hour_production_kwh'))

                sdf_hourly = sdf\
                    .groupBy(col('month'), col('dom'), col('hour'))\
                    .agg(ps_round(avg(col('hour_production_kwh')), 3).alias('hour_production_kwh'))\
                    .orderBy(col('month'), col('dom'), col('hour'))

                utils.df_to_json_file(sdf_hourly, os.path.join('docs', 'data', 'pvgis', file_name))

                return sdf_hourly
            else:
                print(f'[ERROR] Error getting PVGIS data: {response.status_code}')
        else:
            print('[WARN] COORDINATES environment variable not set, PVGIS data will not be retrieved')

    def _calibrate_angle(self, calibration_date_scope, consumption_sdf: DataFrame) -> None:
        to_combine = [[1.0], self._get_range_values(25, 50, 1), [0]]

        params_combinations = []

        for combination in itertools.product(*to_combine):
            series_params = {
                'peakpower': combination[0],
                'angle': combination[1],
                'aspect': combination[2]
            }

            params_combinations.append(series_params)

        for params in params_combinations:
            data_id = self._get_data_id(params)

            if self.calibrations and calibration_date_scope in self.calibrations and 'angle' in self.calibrations[calibration_date_scope] and data_id in self.calibrations[calibration_date_scope]['angle']:
                current_calibration = self.calibrations[calibration_date_scope]['angle'][data_id]

                print(f'[DEBUG] already calibrated angle ({params["angle"]}) (peakpower: {params["peakpower"]} - aspect: {params["aspect"]}) >> selfsupply: {current_calibration["summary"]["selfsupply_kwh"]} - exceeding: {current_calibration["summary"]["exceeding_kwh"]} - consumption: {current_calibration["summary"]["total_consumption_kwh"]}')
                continue
            else:
                pvgis_hourly_production_sdf = self._load_data(self.default_params | self.pvgis_config['params'] | self.fixed_params | self.runtime_params | params, data_id)

                calibration_sdf = consumption_sdf\
                    .join(pvgis_hourly_production_sdf, on=['month', 'dom', 'hour'], how='left')\
                    .withColumn('consumption_result_kwh', col('hour_production_kwh') - col('hour_consumption_kwh'))\
                    .groupBy(col('year'), col('month'), col('month_text'))\
                    .agg(
                        sum(col('hour_consumption_kwh')).cast(DecimalType(precision=10, scale=3)).alias('month_consumption_kwh_total'),
                        sum(col('hour_production_kwh')).cast(DecimalType(precision=10, scale=3)).alias('month_production_kwh_total'),
                        sum(when(col('consumption_result_kwh') < 0, ps_abs(col('consumption_result_kwh'))).otherwise(0)).cast(DecimalType(precision=10, scale=3)).alias('month_consumption_kwh_inludingselfsupply'),
                        sum(when(col('consumption_result_kwh') > 0, col('consumption_result_kwh')).otherwise(0)).cast(DecimalType(precision=10, scale=3)).alias('month_exceding_kwh'),
                    )\
                    .withColumn('month_selfsupply_kwh', (col('month_consumption_kwh_total') - col('month_consumption_kwh_inludingselfsupply')).cast(DecimalType(precision=10, scale=3)))

                if not self.calibrations or calibration_date_scope not in self.calibrations:
                    self.calibrations = {calibration_date_scope: {'angle': {data_id: {}}}}
                elif 'angle' not in self.calibrations[calibration_date_scope]:
                    self.calibrations[calibration_date_scope] = {'angle': {data_id: {}}}
                elif data_id in self.calibrations[calibration_date_scope]['angle']:
                    self.calibrations[calibration_date_scope]['angle'] = {data_id: {}}

                selfsupply = calibration_sdf.select('month_selfsupply_kwh').toPandas()['month_selfsupply_kwh'].sum()
                exceeding = calibration_sdf.select('month_exceding_kwh').toPandas()['month_exceding_kwh'].sum()
                consumption = calibration_sdf.select('month_consumption_kwh_total').toPandas()['month_consumption_kwh_total'].sum()

                self.calibrations[calibration_date_scope]['angle'][data_id] = {
                    'id': data_id,
                    'params': params,
                    'dataframe': utils.df_to_json(calibration_sdf),
                    'summary': {
                        'selfsupply_kwh': float(selfsupply),
                        'exceeding_kwh': float(exceeding),
                        'total_consumption_kwh': float(consumption)
                    }
                }

                self.calibration_done = True

                print(f'[DEBUG] calibrated angle ({params["angle"]}) (peakpower: {params["peakpower"]} - aspect: {params["aspect"]}) >> selfsupply: {selfsupply} - exceeding: {exceeding} - consumption: {consumption}')

    def calibrate(self, consumption_sdf: DataFrame, consumption_date_min, consumption_date_max) -> None:
        if self.pvgis_config['calibrate_angle_and_aspect']:
            calibration_date_scope = f'{consumption_date_min.strftime("%Y%m%d")}_{consumption_date_max.strftime("%Y%m%d")}'

            self._calibrate_angle(calibration_date_scope, consumption_sdf)

            if self.calibration_done:
                for c in self.calibrations.values():
                    c['is_last'] = False

                self.calibrations[calibration_date_scope]['is_last'] = True

                with open(self.calibrations_file, 'w', encoding='utf-8') as f:
                    json.dump(self.calibrations, f, ensure_ascii=False)

    def get_calibrations(self) -> DataFrame:
        calibration = next(c for c in self.calibrations.values() if c['is_last'])

        calibrations = [{
            'peakpower': c['params']['peakpower'],
            'angle': c['params']['angle'],
            'aspect': c['params']['aspect'],
            'selfsupply': c['summary']['selfsupply_kwh'],
            'exceeding': c['summary']['exceeding_kwh'],
            'consumption': c['summary']['total_consumption_kwh'],
        } for c in calibration['angle'].values()]

        return self.spark.createDataFrame(calibrations).orderBy(col('angle'))

    def get_data(self, index: int) -> Dict[str, Any]:
        calibration = filter(lambda c: c['is_last'], self.calibrations.values())

        if not self.calibrated:
            raise Exception('PVGIS data not calibrated')

        return self.production_sdfs[index]

    def unpivot(self, sdf: DataFrame) -> DataFrame:
        unpivot_expr = "stack(3, 'Autoconsumido', month_selfsupply_kwh, 'Consumido', month_consumption_kwh_inludingselfsupply, 'Sobreproducido', month_exceding_kwh) as (energy_type, energy_qty_kwh)"

        return sdf\
            .orderBy(col('year'), col('month'))\
            .withColumn('month_year', concat(lpad(col('month'), 2, '0'), lit('-'), col('year')))\
            .drop('month', 'year')\
            .select('month_year', 'month_consumption_kwh_total', 'month_selfsupply_kwh', 'month_consumption_kwh_inludingselfsupply', 'month_exceding_kwh')\
            .select('month_year', 'month_consumption_kwh_total', expr(unpivot_expr))\
            .withColumn('energy_pct', ps_round(col('energy_qty_kwh') / col('month_consumption_kwh_total') * 100, 2).cast(DecimalType(precision=10, scale=2)))
