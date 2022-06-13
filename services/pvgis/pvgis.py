import datetime as dt
import glob
import itertools
import json
import os
from decimal import Decimal
from typing import List, Dict, Any, Tuple

import plotly.colors
import plotly.express as px
import plotly.graph_objects as go
import requests
from plotly.subplots import make_subplots
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, to_timestamp, hour, round as ps_round, month, dayofmonth, \
    avg, year, when, sum, lit, expr, least
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, FloatType, IntegerType

import constants
import utils


class Pvgis:
    def __init__(self, pvgis_config, spark, consumption_sdf: DataFrame, consumption_date_min: dt.date, consumption_date_max: dt.date):
        self.range_definition_regex = r'range\((?P<start>-?\d+(?:\.\d+)?),\s*(?P<end>-?\d+(?:\.\d+)?),\s*(?P<step>\d+(?:\.\d+)?)\)'

        self.url = 'https://re.jrc.ec.europa.eu/api/v5_2/seriescalc'
        self.pvgis_config = pvgis_config
        self.spark = spark
        self.consumption_sdf = consumption_sdf
        self.consumption_date_min = consumption_date_min
        self.consumption_date_max = consumption_date_max
        self.calibration_date_scope = f'{consumption_date_min.strftime("%Y%m%d")}_{consumption_date_max.strftime("%Y%m%d")}'

        self.pvgis_schema = StructType([
            StructField('time', StringType(), False),
            StructField('P', FloatType(), True),
            StructField('G(i)', FloatType(), True),
            StructField('H_sum', FloatType(), True),
            StructField('T2m', FloatType(), True),
            StructField('WS10m', FloatType(), True),
            StructField('Int', FloatType(), True),
        ])

        self.pvgis_persisted_schema = StructType([
            StructField('month', IntegerType(), False),
            StructField('dom', IntegerType(), False),
            StructField('hour', IntegerType(), False),
            StructField('hour_production_kwh', DecimalType(precision=10, scale=3), False),
        ])

        self.calibration_m_schemas = {
            'angle': StructType([
                StructField('angle', IntegerType(), False),
                StructField('year', IntegerType(), False),
                StructField('month', IntegerType(), False),
                StructField('month_consumption_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_production_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_selfsupply_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_exceeding_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_finalconsumption_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_avg_price_buy_kwh', DecimalType(precision=10, scale=5), False),
                StructField('month_avg_price_sell_kwh', DecimalType(precision=10, scale=5), False),
                StructField('price_sell_vs_buy', DecimalType(precision=10, scale=3), False),
                StructField('price_buy_vs_sell', DecimalType(precision=10, scale=3), False),
                StructField('scoring', DecimalType(precision=10, scale=3), False),
            ]),
            'aspect': StructType([
                StructField('aspect', IntegerType(), False),
                StructField('year', IntegerType(), False),
                StructField('month', IntegerType(), False),
                StructField('month_consumption_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_production_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_selfsupply_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_exceeding_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_finalconsumption_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_avg_price_buy_kwh', DecimalType(precision=10, scale=5), False),
                StructField('month_avg_price_sell_kwh', DecimalType(precision=10, scale=5), False),
                StructField('price_sell_vs_buy', DecimalType(precision=10, scale=3), False),
                StructField('price_buy_vs_sell', DecimalType(precision=10, scale=3), False),
                StructField('scoring', DecimalType(precision=10, scale=3), False),
            ]),
            'angle+aspect': StructType([
                StructField('angle', IntegerType(), False),
                StructField('aspect', IntegerType(), False),
                StructField('year', IntegerType(), False),
                StructField('month', IntegerType(), False),
                StructField('month_consumption_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_production_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_selfsupply_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_exceeding_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_finalconsumption_kwh', DecimalType(precision=10, scale=3), False),
                StructField('month_avg_price_buy_kwh', DecimalType(precision=10, scale=5), False),
                StructField('month_avg_price_sell_kwh', DecimalType(precision=10, scale=5), False),
                StructField('price_sell_vs_buy', DecimalType(precision=10, scale=3), False),
                StructField('price_buy_vs_sell', DecimalType(precision=10, scale=3), False),
                StructField('scoring', DecimalType(precision=10, scale=3), False),
            ])
        }

        self.calibration_y_schemas = {
            'angle': StructType([
                StructField('angle', IntegerType(), False),
                StructField('year_consumption_kwh', DecimalType(precision=10, scale=3), False),
                StructField('year_production_kwh', DecimalType(precision=10, scale=3), False),
                StructField('year_selfsupply_kwh', DecimalType(precision=10, scale=3), False),
                StructField('year_exceeding_kwh', DecimalType(precision=10, scale=3), False),
                StructField('year_finalconsumption_kwh', DecimalType(precision=10, scale=3), False),
                StructField('scoring', DecimalType(precision=10, scale=3), False),
            ]),
            'aspect': StructType([
                StructField('aspect', IntegerType(), False),
                StructField('year_consumption_kwh', DecimalType(precision=10, scale=3), False),
                StructField('year_production_kwh', DecimalType(precision=10, scale=3), False),
                StructField('year_selfsupply_kwh', DecimalType(precision=10, scale=3), False),
                StructField('year_exceeding_kwh', DecimalType(precision=10, scale=3), False),
                StructField('year_finalconsumption_kwh', DecimalType(precision=10, scale=3), False),
                StructField('scoring', DecimalType(precision=10, scale=3), False),
            ]),
            'angle+aspect': StructType([
                StructField('angle', IntegerType(), False),
                StructField('aspect', IntegerType(), False),
                StructField('year_consumption_kwh', DecimalType(precision=10, scale=3), False),
                StructField('year_production_kwh', DecimalType(precision=10, scale=3), False),
                StructField('year_selfsupply_kwh', DecimalType(precision=10, scale=3), False),
                StructField('year_exceeding_kwh', DecimalType(precision=10, scale=3), False),
                StructField('year_finalconsumption_kwh', DecimalType(precision=10, scale=3), False),
                StructField('scoring', DecimalType(precision=10, scale=3), False),
            ])
        }

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
        self.calibrations_cache: Dict[str, DataFrame] = dict()

        if os.path.exists(self.calibrations_file):
            with open(self.calibrations_file, 'r') as f:
                self.calibrations = json.load(f)

        self.calibration_done = False

        self.figure_y_titles = {
            'angle': {
                'xaxis': 'Inclinacción',
                'legend': 'Energía'
            },
            'aspect': {
                'xaxis': 'Azimuth',
                'legend': 'Energía'
            },
            'angle+aspect': {
                'xaxis': 'Azimuth',
                'legend': 'Inclinacción'
            }
        }

        self._calibrate()

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
        return f'PP{params["peakpower"]}_AN{params["angle"]}_AS{params["aspect"]}'.replace('.', 'P').replace('-', 'M')

    def _get_file_name(self, data_id: str) -> str:
        return f'pvgis_{data_id}.json'

    def _load_data(self, params: Dict[str, any], data_id: str) -> DataFrame:
        file_name = self._get_file_name(data_id)
        found_files = glob.glob(os.path.join('docs', 'data', 'pvgis', file_name))

        if len(found_files) == 0:
            sdf = self._download_data(params, file_name)
        else:
            sdf = utils.json_file_to_df(found_files[0], self.spark, self.pvgis_persisted_schema)

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

    def _get_value_from_combination(self, combination_value):
        return combination_value[0] if isinstance(combination_value, Tuple) else combination_value

    def _calibrate_attribute(self, attribute: str, params_to_combine: List[List[any]], best_count: int = 5) -> List[int]:
        attribute_parts = attribute.split('+')

        params_combinations = []

        for combination in itertools.product(*params_to_combine):
            series_params = {
                'peakpower': self._get_value_from_combination(combination[0]),
                'angle': self._get_value_from_combination(combination[1]),
                'aspect': self._get_value_from_combination(combination[2])
            }

            params_combinations.append(series_params)

        for params in params_combinations:
            data_id = self._get_data_id(params)

            if self.calibrations\
                    and self.calibration_date_scope in self.calibrations\
                    and attribute in self.calibrations[self.calibration_date_scope]\
                    and any([True for a in self.calibrations[self.calibration_date_scope][attribute] if a['id'] == data_id]):
                current_calibration = next(a for a in self.calibrations[self.calibration_date_scope][attribute] if a['id'] == data_id)

                print(f'[DEBUG] already calibrated {attribute} ({json.dumps(current_calibration["params"])}) with a score {current_calibration["scoring"]}')
                continue
            else:
                pvgis_hourly_production_sdf = self._load_data(self.default_params | self.pvgis_config['params'] | self.fixed_params | self.runtime_params | params, data_id)

                calibration_m_sdf = self.consumption_sdf\
                    .join(pvgis_hourly_production_sdf, on=['month', 'dom', 'hour'], how='left')\
                    .withColumn('hour_selfsupply_kwh', when(col('hour_production_kwh') <= col('hour_consumption_kwh'), col('hour_production_kwh')).otherwise(col('hour_consumption_kwh')))\
                    .withColumn('hour_exceeding_kwh', when(col('hour_production_kwh') <= col('hour_consumption_kwh'), 0.0).otherwise(col('hour_production_kwh') - col('hour_consumption_kwh')))\
                    .withColumn('hour_finalconsumption_kwh', when(col('hour_production_kwh') >= col('hour_consumption_kwh'), 0.0).otherwise(col('hour_consumption_kwh') - col('hour_production_kwh')))\
                    .groupBy(col('year'), col('month'))\
                    .agg(
                        sum(col('hour_consumption_kwh')).cast(DecimalType(precision=10, scale=3)).alias('month_consumption_kwh'),
                        sum(col('hour_production_kwh')).cast(DecimalType(precision=10, scale=3)).alias('month_production_kwh'),
                        sum(col('hour_selfsupply_kwh')).cast(DecimalType(precision=10, scale=3)).alias('month_selfsupply_kwh'),
                        sum(col('hour_exceeding_kwh')).cast(DecimalType(precision=10, scale=3)).alias('month_exceeding_kwh'),
                        sum(col('hour_finalconsumption_kwh')).cast(DecimalType(precision=10, scale=3)).alias('month_finalconsumption_kwh'),
                        ps_round(avg(col('price_buy_kwh')), 5).cast(DecimalType(precision=10, scale=5)).alias('month_avg_price_buy_kwh'),
                        ps_round(avg(col('price_sell_kwh')), 5).cast(DecimalType(precision=10, scale=5)).alias('month_avg_price_sell_kwh'),
                    )\
                    .withColumn('price_sell_vs_buy', ps_round(col('month_avg_price_sell_kwh') / col('month_avg_price_buy_kwh'), 3).cast(DecimalType(precision=10, scale=3)))\
                    .withColumn('price_buy_vs_sell', ps_round(col('month_avg_price_buy_kwh') / col('month_avg_price_sell_kwh'), 3).cast(DecimalType(precision=10, scale=3)))\
                    .withColumn('scoring', ps_round(col('month_selfsupply_kwh') + (least(col('month_exceeding_kwh'), col('month_finalconsumption_kwh') * col('price_buy_vs_sell')) * col('price_sell_vs_buy')), 3).cast(DecimalType(precision=10, scale=3)))

                attribute_cols = []

                for a in attribute_parts:
                    attribute_cols.append(col(a))

                    calibration_m_sdf = calibration_m_sdf\
                        .withColumn(a, lit(params[a]))\

                calibration_m_sdf = calibration_m_sdf\
                    .orderBy(col('year'), col('month'))\
                    .select(*attribute_cols, col('year'), col('month'), col('month_consumption_kwh'), col('month_production_kwh'), col('month_selfsupply_kwh'), col('month_exceeding_kwh'), col('month_finalconsumption_kwh'), col('month_avg_price_buy_kwh'), col('month_avg_price_sell_kwh'), col('price_sell_vs_buy'), col('price_buy_vs_sell'), col('scoring'))

                if not self.calibrations or self.calibration_date_scope not in self.calibrations:
                    self.calibrations = {self.calibration_date_scope: {attribute: []}}
                elif attribute not in self.calibrations[self.calibration_date_scope]:
                    self.calibrations[self.calibration_date_scope][attribute] = []

                calibration_y_sdf = calibration_m_sdf\
                    .groupBy(*attribute_cols)\
                    .agg(
                        sum(col('month_consumption_kwh')).cast(DecimalType(precision=10, scale=3)).alias('year_consumption_kwh'),
                        sum(col('month_production_kwh')).cast(DecimalType(precision=10, scale=3)).alias('year_production_kwh'),
                        sum(col('month_selfsupply_kwh')).cast(DecimalType(precision=10, scale=3)).alias('year_selfsupply_kwh'),
                        sum(col('month_exceeding_kwh')).cast(DecimalType(precision=10, scale=3)).alias('year_exceeding_kwh'),
                        sum(col('month_finalconsumption_kwh')).cast(DecimalType(precision=10, scale=3)).alias('year_finalconsumption_kwh'),
                        sum(col('scoring')).cast(DecimalType(precision=10, scale=3)).alias('scoring'),
                    )

                scoring = float(calibration_y_sdf.select('scoring').first()['scoring'])

                self.calibrations[self.calibration_date_scope][attribute].append({
                    'id': data_id,
                    'params': params,
                    'dataframe_m': utils.df_to_json(calibration_m_sdf),
                    'dataframe_y': utils.df_to_json(calibration_y_sdf),
                    'scoring': scoring
                })

                self.calibrations[self.calibration_date_scope][attribute].sort(key=lambda c: c['scoring'], reverse=True)

                self.calibration_done = True

                print(f'[DEBUG] calibrated {attribute} ({json.dumps(params)}) with a score {scoring}')
                calibration_y_sdf.show()

        return [tuple([c['params'][a] for a in attribute_parts]) if len(attribute_parts) > 0 else c['params'][attribute] for c in self.calibrations[self.calibration_date_scope][attribute][0:best_count]]

    def _calibrate_angle(self) -> List[int]:
        to_combine = [[1.0], self._get_range_values(25, 50, 1), [0]]

        return self._calibrate_attribute('angle', to_combine)

    def _calibrate_aspect(self) -> List[int]:
        to_combine = [[1.0], [30], self._get_range_values(-15, 15, 1)]

        return self._calibrate_attribute('aspect', to_combine)

    def _calibrate_angle_and_aspect(self, best_angles: List[int], best_aspects: List[int]) -> List[int]:
        to_combine = [[1.0], best_angles, best_aspects]

        return self._calibrate_attribute('angle+aspect', to_combine, best_count=10)

    def _persist_calibrations(self, force: bool = False) -> None:
        if force or self.calibration_done:
            with open(self.calibrations_file, 'w', encoding='utf-8') as f:
                json.dump(self.calibrations, f, ensure_ascii=False)

    def _calibrate(self) -> None:
        if self.pvgis_config['calibrate_angle_and_aspect']:
            best_angles = self._calibrate_angle()

            self._persist_calibrations()

            best_aspects = self._calibrate_aspect()

            self._persist_calibrations()

            best_angle_aspect_list = self._calibrate_angle_and_aspect(best_angles, best_aspects)

            self.pvgis_config['calibrate_angle_and_aspect'] = False
            self.pvgis_config['params']['angle'] = best_angle_aspect_list[0][0]
            self.pvgis_config['params']['aspect'] = best_angle_aspect_list[0][1]

            if self.calibration_done:
                for c in self.calibrations.values():
                    c['is_last'] = False

                self.calibrations[self.calibration_date_scope]['is_last'] = True

            self._persist_calibrations()

    def has_calibrations(self) -> bool:
        return True if self.calibrations else False

    def _get_current_calibration(self) -> Tuple[str, Dict[str, any]]:
        return next((k, v) for k, v in self.calibrations.items() if v['is_last'])

    def _get_calibrations_yearly(self, attribute: str) -> DataFrame:
        calibration_cache_key = f'{attribute}_y'

        if calibration_cache_key not in self.calibrations_cache:
            attribute_parts = attribute.split('+')

            _, calibration = self._get_current_calibration()

            y_schema = self.calibration_y_schemas[attribute]
            y_columns = None
            y_data = []

            for i, c in enumerate(calibration[attribute]):
                if i == 0:
                    y_columns = c['dataframe_y']['columns']

                y_data += c['dataframe_y']['data']

            attribute_cols = []

            for a in attribute_parts:
                attribute_cols.append(col(a))

            y_sdf = utils.json_to_df({'data': y_data, 'columns': y_columns}, self.spark, y_schema)

            utils.df_to_json_file(y_sdf, os.path.join('docs', 'data', 'selfsupply', f'{attribute}_calibrations_y_data.json'))

            self.calibrations_cache[calibration_cache_key] = y_sdf.orderBy(*attribute_cols).cache()

        return self.calibrations_cache[calibration_cache_key]

    def _get_calibrations_monthly(self, attribute: str) -> DataFrame:
        calibration_cache_key = f'{attribute}_m'

        if calibration_cache_key not in self.calibrations_cache:
            attribute_parts = attribute.split('+')

            _, calibration = self._get_current_calibration()

            m_schema = self.calibration_m_schemas[attribute]
            m_columns = None
            m_data = []

            for i, c in enumerate(calibration[attribute]):
                if i == 0:
                    m_columns = c['dataframe_m']['columns']

                m_data += c['dataframe_m']['data']

            attribute_cols = []

            for a in attribute_parts:
                attribute_cols.append(col(a))

            m_sdf = utils.json_to_df({'data': m_data, 'columns': m_columns}, self.spark, m_schema)

            self.calibrations_cache[calibration_cache_key] = m_sdf.orderBy(*attribute_cols, col('year'), col('month')).cache()

        return self.calibrations_cache[calibration_cache_key]

    def get_yearly_calibration_figure_html(self, attribute: str) -> str:
        attribute_parts = attribute.split('+')

        calibrations_y_sdf = self._get_calibrations_yearly(attribute)

        for a in attribute_parts:
            calibrations_y_sdf = calibrations_y_sdf\
                .withColumn(a, col(a).cast(StringType()))

        calibration_y_fig = make_subplots(rows=2, cols=1, vertical_spacing=0.1, row_titles=['Autoconsumo', 'Exceso'])

        calibrations_y_df = calibrations_y_sdf.toPandas()

        if len(attribute_parts) == 1:
            calibration_y_fig.add_trace(go.Scatter(
                name='Autoconsumo',
                legendgroup=attribute_parts[0],
                x=list(calibrations_y_df[attribute_parts[0]]),
                y=list(calibrations_y_df['year_selfsupply_kwh']),
                mode='lines',
                showlegend=False,
                hovertemplate=f'{attribute_parts[0].capitalize()}=%{{x}}º<br>Autoconsumido=%{{y}} kWh'
            ), row=1, col=1)

            calibration_y_fig.add_trace(go.Scatter(
                name='Exceso',
                legendgroup=attribute_parts[0],
                x=list(calibrations_y_df[attribute_parts[0]]),
                y=list(calibrations_y_df['year_exceeding_kwh']),
                mode='lines',
                showlegend=False,
                hovertemplate=f'{attribute_parts[0].capitalize()}=%{{x}}º<br>Exceso=%{{y}} kWh'
            ), row=2, col=1)
        else:
            for i, a in enumerate(calibrations_y_df[attribute_parts[0]].unique()):
                filtered_df = calibrations_y_df[calibrations_y_df[attribute_parts[0]] == a]
                name = f'{a}'

                calibration_y_fig.add_trace(go.Scatter(
                    name=name,
                    legendgroup=name,
                    x=list(filtered_df[attribute_parts[-1]]),
                    y=list(filtered_df['year_selfsupply_kwh']),
                    line=dict(color=plotly.colors.DEFAULT_PLOTLY_COLORS[i]),
                    mode='lines',
                    showlegend=True,
                    hovertemplate=f'{attribute_parts[-1].capitalize()}=%{{x}}º<br>Autoconsumido=%{{y}} kWh'
                ), row=1, col=1)

                calibration_y_fig.add_trace(go.Scatter(
                    name=name,
                    legendgroup=name,
                    x=list(filtered_df[attribute_parts[-1]]),
                    y=list(filtered_df['year_exceeding_kwh']),
                    line=dict(color=calibration_y_fig.data[i * 2].line.color),
                    mode='lines',
                    showlegend=False,
                    hovertemplate=f'{attribute_parts[-1].capitalize()}=%{{x}}º<br>Exceso=%{{y}} kWh'
                ), row=2, col=1)

        calibration_y_fig.update_xaxes(matches='x', row=2, col=1)
        calibration_y_fig.update_layout(**{'xaxis2': {'title': self.figure_y_titles[attribute]['xaxis']}, 'yaxis2': {'title': 'kWh'}})

        calibration_y_fig.update_layout(xaxis={'title': None}, yaxis={'title': 'kWh'}, legend=dict(title=self.figure_y_titles[attribute]['legend']))

        return calibration_y_fig.to_html(include_plotlyjs=False, full_html=False, div_id=f'{attribute.replace("+", "_")}_calibration_y_figure', default_height='600px')

    def get_monthly_calibration_figure_html(self, attribute: str) -> str:
        calibrations_m_sdf = self._get_calibrations_monthly(attribute)

        month_unpivot_expr = "stack(2, 'Autoconsumo', month_selfsupply_kwh, 'Exceso', month_exceeding_kwh) as (energy_type, energy_qty_kwh)"

        calibrations_m_sdf = calibrations_m_sdf\
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
                        .when(col('month') == 12, 'Diciembre')) \
            .drop('month')\
            .withColumnRenamed('month_text', 'month')\
            .select(attribute, 'month', 'month_consumption_kwh', expr(month_unpivot_expr))\
            .withColumn('energy_pct', ps_round(col('energy_qty_kwh') / col('month_consumption_kwh') * 100, 2).cast(DecimalType(precision=10, scale=2)))\
            .withColumn(attribute, col(attribute).cast(StringType()))

        calibration_m_fig = px.line(calibrations_m_sdf.toPandas(),
                                    x=attribute,
                                    y='energy_qty_kwh',
                                    facet_col='month',
                                    facet_col_wrap=3,
                                    facet_col_spacing=0.02,
                                    facet_row_spacing=0.04,
                                    color='energy_type',
                                    category_orders={'month': constants.MONTHS_ES_ORDER},
                                    hover_data=['energy_pct'],
                                    labels=dict(angle='Inclinación', aspect='Azimuth', energy_qty_kwh='kWh',
                                                energy_type='Energía', energy_pct='% Consumo', month='Mes'))
        calibration_m_fig.update_xaxes(tickangle=90)
        calibration_m_fig.for_each_annotation(lambda a: a.update(text=a.text.replace("Mes=", "")))

        for axis in calibration_m_fig.layout:
            if type(calibration_m_fig.layout[axis]) == go.layout.XAxis:
                calibration_m_fig.layout[axis].title.text = ''

        calibration_m_fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))

        return calibration_m_fig.to_html(include_plotlyjs=False, full_html=False, div_id=f'{attribute}_calibration_m_figure', default_height='1200px')
