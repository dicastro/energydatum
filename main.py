import datetime as dt
import glob
import math
import os
from decimal import Decimal

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import toml as toml
from dateutil import rrule
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from jinja2 import Environment, select_autoescape, FileSystemLoader
from plotly.subplots import make_subplots
from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import col, sum, avg, min as ps_min, max as ps_max, to_date, regexp_replace, month, year, \
    dayofmonth, dayofweek, countDistinct, when, round as ps_round, lpad, concat, lit, stddev as std
from pyspark.sql.types import DecimalType, StringType, StructType, DateType, IntegerType, StructField

import constants
from services.bank_days import BankDays
from services.esios.esios_indicator import EsiosIndicator
from services.esios.esios_price import EsiosPrice
from services.pvgis.pvgis import Pvgis
from services.rates.rate_20td_info import Rate20TDInfo
from services.rates.rate_fix_info import RateFixInfo
from services.rates.rate_wk_info import RateWKInfo
from utils import df_to_json_file

load_dotenv()

config = toml.load('config.toml')

jinja_env = Environment(loader=FileSystemLoader('templates'), autoescape=select_autoescape(['html']))

today = dt.date.today()

TABLE_CLASSES = ('ui', 'celled', 'table', 'dt')

spark = SparkSession.builder.master('local[*]')\
    .appName('energy-calc')\
    .config("spark.executor.cores", 4)\
    .config('spark.executor.memory', '16G')\
    .config('spark.driver.memory', '16G')\
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')\
    .config('spark.default.parallelism', '4')\
    .getOrCreate()

consumption_schema = StructType([
    StructField('date', DateType(), False),
    StructField('method', StringType(), False),
    StructField('year', IntegerType(), False),
    StructField('month', IntegerType(), False),
    StructField('month_text', StringType(), False),
    StructField('dom', IntegerType(), False),
    StructField('dow', IntegerType(), False),
    StructField('dow_text', StringType(), False),
    StructField('dow_order', IntegerType(), False),
    StructField('hour', IntegerType(), False),
    StructField('hour_consumption_kwh', DecimalType(precision=10, scale=3), False),
])


def read_consumptions() -> DataFrame:
    consumption_files = glob.glob(os.path.join('docs', 'data', 'consumption', 'raw', 'y', 'consumption_raw_*.json'))

    sdf = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=consumption_schema)

    if len(consumption_files) > 0:
        for consumption_file in consumption_files:
            consumption_file_df = pd.read_json(consumption_file, orient='split', convert_dates=False)

            consumption_file_sdf = spark.createDataFrame(consumption_file_df) \
                .withColumn('date', to_date(col('date'), 'yyyy-MM-dd'))\
                .withColumn('year', col('year').cast(IntegerType()))\
                .withColumn('month', col('month').cast(IntegerType()))\
                .withColumn('dom', col('dom').cast(IntegerType()))\
                .withColumn('dow', col('dow').cast(IntegerType()))\
                .withColumn('dow_order', col('dow_order').cast(IntegerType()))\
                .withColumn('hour', col('hour').cast(IntegerType()))\
                .withColumn('hour_consumption_kwh', col('hour_consumption_kwh').cast(DecimalType(precision=10, scale=3)))

            # new spark DataFrame is created in order to ensure that it has the same schema as the previous ones
            sdf = sdf\
                .union(spark.createDataFrame(consumption_file_sdf.toPandas(), schema=consumption_schema)) \
                .cache()

    print('Existing consumption rows: {rows}'.format(rows=sdf.count()))

    return sdf


consumption_sdf = read_consumptions()

new_consumption_files = glob.glob(os.path.join('import', 'consumptions', '*.csv'))

if len(new_consumption_files) == 0:
    new_consumption_sdf = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=consumption_schema)
else:
    new_consumption_sdf = spark.read.options(inferSchema=True, header=True, delimiter=';')\
        .csv(new_consumption_files)\
        .withColumnRenamed('CUPS', 'cups')\
        .drop('cups')\
        .withColumnRenamed('Hora', 'hour')\
        .withColumn('date', to_date(col('Fecha'), "dd/MM/yyyy"))\
        .drop('Fecha')\
        .withColumn('hour_consumption_kwh', regexp_replace('Consumo_kWh', ',', '.').cast(DecimalType(scale=3)))\
        .drop('Consumo_kWh')\
        .withColumnRenamed('Metodo_obtencion', 'method')\
        .withColumn('year', year('date'))\
        .withColumn('month', month('date'))\
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
                    .when(col('dow') == 7, 6))

    # in days with 25 hours (when there was an hour change from 03:00 to 02:00)
    # the extra hour (#4) is discarded from dataset
    # in days with 23 hours (when there was an hour change from 02:00 to 03:00)
    # hours from #3 (included) are incremented by 1
    window_date = Window.partitionBy(col('date'))

    new_consumption_sdf = new_consumption_sdf\
        .withColumn('hour_changed', when(ps_max('hour').over(window_date) == 25, -1)
                    .when(ps_max('hour').over(window_date) == 23, 1)
                    .otherwise(0))\
        .withColumn('keep_row', when(col('hour_changed') == 0, 1)
                    .when((col('hour_changed') == 1), 1)
                    .when((col('hour_changed') == -1) & (col('hour') != 4), 1)
                    .otherwise(0))\
        .withColumn('hour', when((col('hour_changed') == -1) & (col('hour') > 4), col('hour') - 1)
                    .when((col('hour_changed') == 1) & (col('hour') > 2), col('hour') + 1)
                    .otherwise(col('hour')))

    new_consumption_sdf = new_consumption_sdf\
        .filter(col('keep_row') == 1)\
        .drop('hour_changed', 'keep_row')\
        .select(
            col('date'), col('method'),
            col('year'), col('month'), col('month_text'), col('dom'), col('dow'), col('dow_text'), col('dow_order'),
            col('hour'), col('hour_consumption_kwh')
        ).cache()

new_consumption_rows = new_consumption_sdf.count()

print('New consumption rows: {rows}'.format(rows=new_consumption_rows))

if new_consumption_rows > 0:
    date_hour_window_spec = Window.partitionBy([col('date'), col('hour')])

    consumption_sdf = consumption_sdf \
        .withColumn('priority', lit(1))\
        .union(new_consumption_sdf.withColumn('priority', lit(2)))\
        .withColumn('max_priority', ps_max(col('priority')).over(date_hour_window_spec))\
        .filter(col('priority') == col('max_priority'))\
        .drop('max_priority', 'priority')\
        .cache()

    print('Consumption rows after removing duplicates: {rows}'.format(rows=consumption_sdf.count()))

    consumption_years = list(consumption_sdf.select(col('year')).distinct().orderBy('year').toPandas()['year'])

    for consumption_year in consumption_years:
        print('DEBUG: persisting consumptions of year {year}'.format(year=consumption_year))
        df_to_json_file(consumption_sdf
                        .filter(year('date') == consumption_year)
                        .orderBy(col('date'), col('hour')),
                        os.path.join('docs', 'data', 'consumption', 'raw', 'y', f'consumption_raw_{consumption_year}.json'), (0,))

    consumption_sdf = read_consumptions()
    consumption_yearmonths = list(consumption_sdf.select(concat(col('year'), lpad(col('month'), 2, '0')).alias('yearmonth')).distinct().orderBy('yearmonth').toPandas()['yearmonth'])

    for consumption_yearmonth in consumption_yearmonths:
        print('DEBUG: persisting consumptions of yearmonth {yearmonth}'.format(yearmonth=consumption_yearmonth))
        df_to_json_file(consumption_sdf
                        .withColumn('yearmonth', concat(col('year'), lpad(col('month'), 2, '0')))
                        .filter(year('yearmonth') == consumption_yearmonth)
                        .orderBy(col('date'), col('hour'))
                        .drop('yearmonth'),
                        os.path.join('docs', 'data', 'consumption', 'raw', 'moy', f'consumption_raw_{consumption_yearmonth}.json'), (0,))

    for new_consumption_file in new_consumption_files:
        os.replace(new_consumption_file, new_consumption_file.replace(os.path.join('import', 'consumptions', 'consumption_'), os.path.join('import', 'consumptions', 'processed', 'consumption_')))

consumption_sdf.show(24)

# TODO: this line is duplicated
consumption_years = list(consumption_sdf.select(col('year')).distinct().orderBy('year').toPandas()['year'])

consumption_date_min = consumption_sdf.select(ps_min('date').alias('date_min')).first()['date_min']
consumption_date_max = consumption_sdf.select(ps_max('date').alias('date_max')).first()['date_max']

TABLE_CLASSES = ('ui', 'celled', 'table', 'dt')

MONTHS_ES_ORDER = [
    'Enero', 'Febrero', 'Marzo',
    'Abril', 'Mayo', 'Junio',
    'Julio', 'Agosto', 'Septiembre',
    'Octubre', 'Noviembre', 'Diciembre'
]

DOW_TEXT_ES_ORDER = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']

jinja_common_context = {
    'version': constants.VERSION,
    'contextpath': constants.CONTEXT_PATH,
    'today': today.strftime('%d/%m/%Y'),
    'date_min': consumption_date_min.strftime('%d/%m/%Y'),
    'date_max': consumption_date_max.strftime('%d/%m/%Y'),
    'table_classes': TABLE_CLASSES,
}

bank_days = BankDays(spark)
bank_days_sdf = bank_days.get_bank_days(consumption_years)
bank_days_sdf.show(5)

esios_price = EsiosPrice(jinja_env, jinja_common_context, spark)
price_sdf = esios_price.get_prices(consumption_date_max)
price_sdf.show(24)

price_date_min, price_date_max = esios_price.get_price_date_range()

# -------------------------------
# Yearly consumption
# -------------------------------
print('DEBUG: yearly consumption')

consumption_y = consumption_sdf\
    .groupBy(col('year')) \
    .agg(
        ps_round(sum(col('hour_consumption_kwh')).alias('y_sum_kwh'), 2).alias('y_sum_kwh'),
        countDistinct(col('month')).alias('y_months_count')
    )\
    .orderBy(col('year'))\
    .withColumn('year', col('year').cast(StringType()))\

consumption_y_fig = px.bar(consumption_y.toPandas(),
                           x='year',
                           y='y_sum_kwh',
                           orientation='v',
                           custom_data=['y_months_count'],
                           labels=dict(year='Año', y_sum_kwh='Consumo (kWh)', y_months_count='Meses'))
consumption_y_fig.update_traces(hovertemplate='Año=%{x}<br>Consumo (kWh)=%{y}<br>%{customdata[0]} meses')
consumption_y_fig.update_layout(xaxis={'title': None})

consumption_y_fig_html = consumption_y_fig.to_html(include_plotlyjs=False, full_html=False, div_id='consumption_y_fig')

df_to_json_file(consumption_y, os.path.join('docs', 'data', 'consumption', 'consumption_y.json'))

# -------------------------------
# Monthly consumption
# -------------------------------
print('DEBUG: monthly consumption')

consumption_moy = consumption_sdf\
    .groupBy(col('year'), col('month'))\
    .agg(
        sum(col('hour_consumption_kwh')).alias('moy_sum_kwh'),
        ps_min(col('month_text')).alias('month_text')
    )\

consumption_moy_evol_full = consumption_moy\
    .orderBy(col('year'), col('month'))\
    .withColumn('month_year', concat(lpad(col('month'), 2, '0'), lit('-'), col('year')))\
    .drop('month', 'year')\
    .select('month_year', 'moy_sum_kwh')

consumption_moy_evol_full_fig = px.line(consumption_moy_evol_full.toPandas(),
                                        x='month_year',
                                        y='moy_sum_kwh',
                                        labels=dict(month_year='Mes/Año', moy_sum_kwh='Consumo (kWh)'))
consumption_moy_evol_full_fig.update_layout(xaxis={'title': None})
consumption_moy_evol_full_fig.update_xaxes(tickangle=90)


consumption_moy_evol_comp = consumption_moy\
    .orderBy(col('year'), col('month'))\
    .drop('month')\
    .withColumnRenamed('month_text', 'month')\
    .withColumn('year', col('year').cast(StringType()))\
    .select('year', 'month', 'moy_sum_kwh')

consumption_moy_comp_fig = px.bar(consumption_moy_evol_comp.toPandas(),
                                  x='year',
                                  y='moy_sum_kwh',
                                  facet_col='month',
                                  facet_col_wrap=3,
                                  facet_col_spacing=0.02,
                                  facet_row_spacing=0.10,
                                  color='year',
                                  category_orders={'month': constants.MONTHS_ES_ORDER},
                                  labels=dict(year='Año', moy_sum_kwh='Consumo (kWh)', month='Mes'))
consumption_moy_comp_fig.for_each_annotation(lambda a: a.update(text=a.text.replace("Mes=", "")))

for axis in consumption_moy_comp_fig.layout:
    if type(consumption_moy_comp_fig.layout[axis]) == go.layout.XAxis:
        consumption_moy_comp_fig.layout[axis].title.text = ''

consumption_moy_evol_year_fig = px.line(consumption_moy_evol_comp.toPandas(),
                                        x='month',
                                        y='moy_sum_kwh',
                                        color='year',
                                        category_orders={'month': constants.MONTHS_ES_ORDER},
                                        labels=dict(month='Mes', moy_sum_kwh='Consumo (kWh)', year='Año'))
consumption_moy_evol_year_fig.update_layout(xaxis={'title': None})
consumption_moy_evol_year_fig.update_xaxes(tickangle=90)

consumption_moy_evol_full_fig_html = consumption_moy_evol_full_fig.to_html(include_plotlyjs=False, full_html=False, div_id='consumption_moy_evolution_full_figure')
consumption_moy_evol_year_fig_html = consumption_moy_evol_year_fig.to_html(include_plotlyjs=False, full_html=False, div_id='consumption_moy_evolution_per_year_figure')
consumption_moy_comp_fig_html = consumption_moy_comp_fig.to_html(include_plotlyjs=False, full_html=False, div_id='consumption_moy_comparison_per_month_year_figure', default_height='800px')

df_to_json_file(consumption_moy_evol_comp, os.path.join('docs', 'data', 'consumption', 'consumption_moy_evol_comp.json'))
df_to_json_file(consumption_moy_evol_full, os.path.join('docs', 'data', 'consumption', 'consumption_moy_evol_full.json'))

# -------------------------------
# Day of week consumption
# -------------------------------
print('DEBUG: day of week consumption')

consumption_dom_sum_base = consumption_sdf\
    .groupBy(col('year'), col('month'), col('dom'))\
    .agg(
        sum(col('hour_consumption_kwh')).alias('dom_consumption_kwh'),
        ps_min(col('month_text')).alias('month_text'),
        ps_min(col('dow_text')).alias('dow_text'),
        ps_min(col('dow_order')).alias('dow_order'),
    )\
    .orderBy(col('year'), col('month'), col('dom'))\

consumption_dow_sum = consumption_dom_sum_base \
    .groupBy(col('year'), col('month'), col('dow_text'))\
    .agg(
        ps_round(sum(col('dom_consumption_kwh')), 3).alias('dow_sum_kwh'),
        ps_min(col('month_text')).alias('month_text'),
        ps_min(col('dow_order')).alias('dow_order'),
    )\
    .orderBy(col('year'), col('month'), col('dow_order'))\
    .drop('month')\
    .withColumnRenamed('month_text', 'month')\
    .withColumn('year', col('year').cast(StringType()))\
    .drop('dow')\
    .withColumnRenamed('dow_text', 'dow')

consumption_dow_sum_fig = px.line(consumption_dow_sum.toPandas(),
                                  x='dow',
                                  y='dow_sum_kwh',
                                  color='year',
                                  facet_col='month',
                                  facet_col_wrap=3,
                                  facet_col_spacing=0.02,
                                  facet_row_spacing=0.10,
                                  category_orders={'month': constants.MONTHS_ES_ORDER, 'dow': constants.DOW_TEXT_ES_ORDER},
                                  labels=dict(dow='Día', dow_sum_kwh='Consumo (kWh)', year='Año', month='Mes'))
consumption_dow_sum_fig.for_each_annotation(lambda a: a.update(text=a.text.replace("Mes=", "")))
consumption_dow_sum_fig.update_xaxes(tickangle=90)

for axis in consumption_dow_sum_fig.layout:
    if type(consumption_dow_sum_fig.layout[axis]) == go.layout.XAxis:
        consumption_dow_sum_fig.layout[axis].title.text = ''

consumption_dow_sum_fig_html = consumption_dow_sum_fig.to_html(include_plotlyjs=False, full_html=False, div_id='consumption_dow_sum_figure', default_height='800px')


consumption_dow_avg = consumption_dom_sum_base\
    .groupBy(col('year'), col('month'), col('dow_text')) \
    .agg(
        ps_round(avg(col('dom_consumption_kwh')), 3).alias('dow_avg_kwh'),
        ps_min(col('month_text')).alias('month_text'),
        ps_min(col('dow_order')).alias('dow_order'),
    )\
    .orderBy(col('year'), col('month'), col('dow_order'))\
    .drop('month')\
    .withColumnRenamed('month_text', 'month')\
    .withColumn('year', col('year').cast(StringType()))\
    .drop('dow')\
    .withColumnRenamed('dow_text', 'dow')

consumption_dow_avg_fig = px.line(consumption_dow_avg.toPandas(),
                                  x='dow',
                                  y='dow_avg_kwh',
                                  color='year',
                                  facet_col='month',
                                  facet_col_wrap=3,
                                  facet_col_spacing=0.02,
                                  facet_row_spacing=0.10,
                                  category_orders={'month': constants.MONTHS_ES_ORDER, 'dow': constants.DOW_TEXT_ES_ORDER},
                                  labels=dict(dow='Día', dow_avg_kwh='Consumo medio (kWh)', year='Año', month='Mes'))
consumption_dow_avg_fig.for_each_annotation(lambda a: a.update(text=a.text.replace("Mes=", "")))
consumption_dow_avg_fig.update_xaxes(tickangle=90)

for axis in consumption_dow_avg_fig.layout:
    if type(consumption_dow_avg_fig.layout[axis]) == go.layout.XAxis:
        consumption_dow_avg_fig.layout[axis].title.text = ''

consumption_dow_avg_fig_html = consumption_dow_avg_fig.to_html(include_plotlyjs=False, full_html=False, div_id='consumption_dow_avg_figure', default_height='900px')

df_to_json_file(consumption_dow_sum.select('year', 'month', 'dow', 'dow_sum_kwh'), os.path.join('docs', 'data', 'consumption', 'consumption_dow_sum.json'))
df_to_json_file(consumption_dow_avg.select('year', 'month', 'dow', 'dow_avg_kwh'), os.path.join('docs', 'data', 'consumption', 'consumption_dow_avg.json'))

# -------------------------------
# Hour of day consumption
# -------------------------------
print('DEBUG: hour of day consumption')

consumption_hod_avg = consumption_sdf\
    .groupBy(col('year'), col('month'), col('hour'))\
    .agg(
        ps_round(avg(col('hour_consumption_kwh')), 3).alias('hod_avg_kwh'),
        ps_min(col('month_text')).alias('month_text')
    )\
    .orderBy(col('year'), col('month'), col('hour'))\
    .drop('month')\
    .withColumnRenamed('month_text', 'month')\
    .withColumn('year', col('year').cast(StringType()))\
    .withColumn('hour', col('hour').cast(StringType()))

consumption_hod_avg_fig = px.line(consumption_hod_avg.toPandas(),
                                  x='hour',
                                  y='hod_avg_kwh',
                                  facet_col='month',
                                  facet_col_wrap=3,
                                  facet_col_spacing=0.02,
                                  facet_row_spacing=0.05,
                                  color='year',
                                  category_orders={'month': constants.MONTHS_ES_ORDER},
                                  labels=dict(hour='Hora', hod_avg_kwh='Consumo medio (kWh)', year='Año', month='Mes'))
consumption_hod_avg_fig.for_each_annotation(lambda a: a.update(text=a.text.replace("Mes=", "")))
consumption_hod_avg_fig.update_xaxes(tickangle=90)

for axis in consumption_hod_avg_fig.layout:
    if type(consumption_hod_avg_fig.layout[axis]) == go.layout.XAxis:
        consumption_hod_avg_fig.layout[axis].title.text = ''

consumption_hod_avg_fig_html = consumption_hod_avg_fig.to_html(include_plotlyjs=False, full_html=False, div_id='consumption_hod_avg_figure', default_height='900px')


consumption_hod_sum = consumption_sdf\
    .groupBy(col('year'), col('month'), col('hour'))\
    .agg(
        ps_round(sum(col('hour_consumption_kwh')), 3).alias('hod_sum_kwh'),
        ps_min(col('month_text')).alias('month_text')
    )\
    .orderBy(col('year'), col('month'), col('hour'))\
    .drop('month')\
    .withColumnRenamed('month_text', 'month')\
    .withColumn('year', col('year').cast(StringType()))\
    .withColumn('hour', col('hour').cast(StringType()))

consumption_hod_sum_fig = px.line(consumption_hod_sum.toPandas(),
                                  x='hour',
                                  y='hod_sum_kwh',
                                  facet_col='month',
                                  facet_col_wrap=3,
                                  facet_col_spacing=0.02,
                                  facet_row_spacing=0.05,
                                  color='year',
                                  category_orders={'month': constants.MONTHS_ES_ORDER},
                                  labels=dict(hour='Hora', hod_sum_kwh='Consumo (kWh)', year='Año', month='Mes'))
consumption_hod_sum_fig.for_each_annotation(lambda a: a.update(text=a.text.replace("Mes=", "")))
consumption_hod_sum_fig.update_xaxes(tickangle=90)

for axis in consumption_hod_sum_fig.layout:
    if type(consumption_hod_sum_fig.layout[axis]) == go.layout.XAxis:
        consumption_hod_sum_fig.layout[axis].title.text = ''

consumption_hod_sum_fig_html = consumption_hod_sum_fig.to_html(include_plotlyjs=False, full_html=False, div_id='consumption_dow_sum_figure', default_height='900px')

df_to_json_file(consumption_hod_avg.select('year', 'month', 'hour', 'hod_avg_kwh'), os.path.join('docs', 'data', 'consumption', 'consumption_hod_avg.json'))
df_to_json_file(consumption_hod_sum.select('year', 'month', 'hour', 'hod_sum_kwh'), os.path.join('docs', 'data', 'consumption', 'consumption_hod_sum.json'))

# -------------------------------
# Rate periods monthly
# -------------------------------
print('DEBUG: rate periods monthly consumption')

rate_period_from = consumption_date_max - relativedelta(years=1)

rate_period_from = max(price_date_min, consumption_date_min, rate_period_from)

# 2.0TD
rate_20td_info = Rate20TDInfo()

year_month_window_spec = Window.partitionBy([col('year'), col('month')])

rate_20td_m_periods_data = consumption_sdf\
    .filter(col('date') > rate_period_from)\
    .join(bank_days_sdf
          .select(col('year'), col('month'), col('dom'), lit(True).alias('is_bank_day')),
          on=['year', 'month', 'dom'], how='left')\
    .fillna(False, subset=['is_bank_day'])\
    .withColumn('period', rate_20td_info.get_period('hour', 'dow', 'is_bank_day'))\
    .groupBy(col('year'), col('month'), col('period'))\
    .agg(
        sum(col('hour_consumption_kwh')).alias('period_kwh')
    )\
    .orderBy(col('year').desc(), col('month').desc(), col('period'))\
    .withColumn('month_kwh', sum(col('period_kwh')).over(year_month_window_spec))\
    .withColumn('period_pct', ps_round(col('period_kwh') / col('month_kwh') * 100, 2))\
    .drop('month_kwh')\
    .orderBy(col('year'), col('month'), col('period'))\
    .withColumn('month_year', concat(lpad(col('month'), 2, '0'), lit('-'), col('year')))\

rate_20td_m_periods = rate_20td_m_periods_data\
    .drop('month', 'year') \
    .select('month_year', 'period', 'period_kwh', 'period_pct')

rate_20td_m_periods_fig = px.line(rate_20td_m_periods.toPandas(),
                                  x='month_year',
                                  y='period_kwh',
                                  color='period',
                                  color_discrete_sequence=rate_20td_info.get_color_sequence(),
                                  hover_data=['period_pct'],
                                  labels=dict(month_year='Mes/Año',
                                              period_kwh='Consumo (kWh)',
                                              period='Periodo',
                                              period_pct='% Periodo'))
rate_20td_m_periods_fig.update_layout(xaxis={'title': None})
rate_20td_m_periods_fig.update_xaxes(tickangle=90)

rate_20td_m_periods_fig_html = rate_20td_m_periods_fig.to_html(include_plotlyjs=False, full_html=False, div_id='rate_20td_m_periods_figure')

rate_20td_m_periods_pct_fig = px.line(rate_20td_m_periods.toPandas(),
                                      x='month_year',
                                      y='period_pct',
                                      color='period',
                                      color_discrete_sequence=rate_20td_info.get_color_sequence(),
                                      hover_data=['period_kwh'],
                                      labels=dict(month_year='Mes/Año',
                                                  period_kwh='Consumo (kWh)',
                                                  period='Periodo',
                                                  period_pct='% Periodo'))
rate_20td_m_periods_pct_fig.update_layout(xaxis={'title': None})
rate_20td_m_periods_pct_fig.update_xaxes(tickangle=90)

rate_20td_m_periods_pct_fig_html = rate_20td_m_periods_pct_fig.to_html(include_plotlyjs=False, full_html=False, div_id='rate_20td_m_periods_pct_figure')


# Weekend
rate_wk_info = RateWKInfo()

rate_wk_m_periods_data = consumption_sdf \
    .filter(col('date') > rate_period_from) \
    .join(bank_days_sdf
          .select(col('year'), col('month'), col('dom'), lit(True).alias('is_bank_day')),
          on=['year', 'month', 'dom'], how='left')\
    .fillna(False, subset=['is_bank_day'])\
    .withColumn('period', rate_wk_info.get_period('hour', 'dow', 'is_bank_day'))\
    .groupBy(col('year'), col('month'), col('period'))\
    .agg(
        sum(col('hour_consumption_kwh')).alias('period_kwh')
    )\
    .orderBy(col('year').desc(), col('month').desc(), col('period'))\
    .withColumn('month_kwh', sum(col('period_kwh')).over(year_month_window_spec))\
    .withColumn('period_pct', ps_round(col('period_kwh') / col('month_kwh') * 100, 2))\
    .drop('month_kwh')\
    .orderBy(col('year'), col('month'), col('period'))\
    .withColumn('month_year', concat(lpad(col('month'), 2, '0'), lit('-'), col('year')))\

rate_wk_m_periods = rate_wk_m_periods_data\
    .drop('month', 'year')\
    .select('month_year', 'period', 'period_kwh', 'period_pct')

rate_wk_m_periods_fig = px.line(rate_wk_m_periods.toPandas(),
                                x='month_year',
                                y='period_kwh',
                                color='period',
                                color_discrete_sequence=rate_wk_info.get_color_sequence(),
                                hover_data=['period_pct'],
                                labels=dict(month_year='Mes/Año',
                                            period_kwh='Consumo (kWh)',
                                            period='Periodo',
                                            period_pct='% Periodo'))
rate_wk_m_periods_fig.update_layout(xaxis={'title': None})
rate_wk_m_periods_fig.update_xaxes(tickangle=90)

rate_wk_m_periods_fig_html = rate_wk_m_periods_fig.to_html(include_plotlyjs=False, full_html=False, div_id='rate_wk_m_periods_figure')

rate_wk_m_periods_pct_fig = px.line(rate_wk_m_periods.toPandas(),
                                    x='month_year',
                                    y='period_pct',
                                    color='period',
                                    color_discrete_sequence=rate_wk_info.get_color_sequence(),
                                    hover_data=['period_kwh'],
                                    labels=dict(month_year='Mes/Año',
                                                period_kwh='Consumo (kWh)',
                                                period='Periodo',
                                                period_pct='% Periodo'))
rate_wk_m_periods_pct_fig.update_layout(xaxis={'title': None})
rate_wk_m_periods_pct_fig.update_xaxes(tickangle=90)

rate_wk_m_periods_pct_fig_html = rate_wk_m_periods_pct_fig.to_html(include_plotlyjs=False, full_html=False, div_id='rate_wk_m_periods_pct_figure')

# Fix
rate_fix_info = RateFixInfo()

rate_fix_m_periods_data = consumption_sdf \
    .filter(col('date') > rate_period_from) \
    .join(bank_days_sdf
          .select(col('year'), col('month'), col('dom'), lit(True).alias('is_bank_day')),
          on=['year', 'month', 'dom'], how='left')\
    .fillna(False, subset=['is_bank_day'])\
    .withColumn('period', rate_fix_info.get_period('hour', 'dow', 'is_bank_day'))\
    .groupBy(col('year'), col('month'), col('period'))\
    .agg(
        sum(col('hour_consumption_kwh')).alias('period_kwh')
    )\
    .orderBy(col('year').desc(), col('month').desc(), col('period'))\
    .orderBy(col('year'), col('month'), col('period'))\
    .withColumn('month_year', concat(lpad(col('month'), 2, '0'), lit('-'), col('year')))\

rate_fix_m_periods = rate_fix_m_periods_data\
    .drop('month', 'year')\
    .select('month_year', 'period', 'period_kwh')

rate_fix_m_periods_fig = px.line(rate_fix_m_periods.toPandas(),
                                 x='month_year',
                                 y='period_kwh',
                                 color='period',
                                 color_discrete_sequence=rate_fix_info.get_color_sequence(),
                                 labels=dict(month_year='Mes/Año',
                                             period_kwh='Consumo (kWh)',
                                             period='Periodo'))
rate_fix_m_periods_fig.update_layout(xaxis={'title': None})
rate_fix_m_periods_fig.update_xaxes(tickangle=90)

rate_fix_m_periods_fig_html = rate_fix_m_periods_fig.to_html(include_plotlyjs=False, full_html=False, div_id='rate_fix_m_periods_figure')


df_to_json_file(rate_20td_m_periods, os.path.join('docs', 'data', 'consumption', 'rate_20td_m_periods.json'))
df_to_json_file(rate_wk_m_periods, os.path.join('docs', 'data', 'consumption', 'rate_wk_m_periods.json'))
df_to_json_file(rate_fix_m_periods, os.path.join('docs', 'data', 'consumption', 'rate_fix_m_periods.json'))

# -------------------------------
# Day of month consumption
# -------------------------------
print('DEBUG: day of month consumption')

# -------------------------------
# Costs
# -------------------------------
print('DEBUG: costs')

print(f"DEBUG:   consumption rows: {consumption_sdf.filter(col('date') > rate_period_from).count()}")

consumption_with_price_sdf = consumption_sdf\
    .filter(col('date') > rate_period_from)\
    .join(price_sdf.select(col('year'), col('month'), col('dom'), col('hour'),
                           col('hour_pricebuy_ekwh'), col('hour_pricesell_ekwh'),
                           col('month_avg_pricebuy_ekwh'), col('month_avg_pricesell_ekwh')),
          on=['year', 'month', 'dom', 'hour'], how='left')\
    .cache()

print(f'DEBUG:   consumption rows (including price): {consumption_with_price_sdf.count()}')

rate_20td_pvpc_cost = consumption_with_price_sdf\
    .withColumn('hour_cost', ps_round(col('hour_consumption_kwh') * col('hour_pricebuy_ekwh'), 5))\
    .groupBy(col('year'), col('month'))\
    .agg(sum(col('hour_cost')).alias('month_cost'))\
    .orderBy(col('year'), col('month'))\
    .select('month_cost')

rate_20td_pvpc_cost_data = {
    'title': 'PVPC',
    'values': [float(n) for n in list(rate_20td_pvpc_cost.toPandas()['month_cost'])]
}

month_year_list = [dt_i.strftime('%m-%Y') for dt_i in rrule.rrule(rrule.MONTHLY, dtstart=rate_period_from, until=consumption_date_max)]

# -------------------------------
# self-supply
# -------------------------------

print('DEBUG: self-supply')

consumption_with_price_and_bankdays_sdf = consumption_with_price_sdf\
    .join(bank_days_sdf
          .select(col('year'), col('month'), col('dom'), lit(True).alias('is_bank_day')),
          on=['year', 'month', 'dom'], how='left')\
    .fillna(False, subset=['is_bank_day'])

pvgis = Pvgis(config['pvgis'], spark, consumption_with_price_and_bankdays_sdf, consumption_date_min, consumption_date_max, [rate_20td_info, rate_wk_info, rate_fix_info])

jinja_common_context['has_calibrations'] = pvgis.has_calibrations()

# angle calibration
print('DEBUG:   angle calibration')

angle_calibration_m_fig_html = pvgis.get_monthly_calibration_figure_html('angle')
angle_calibration_y_fig_html = pvgis.get_yearly_calibration_figure_html('angle')

# aspect calibration
print('DEBUG:   aspect calibration')

aspect_calibration_m_fig_html = pvgis.get_monthly_calibration_figure_html('aspect')
aspect_calibration_y_fig_html = pvgis.get_yearly_calibration_figure_html('aspect')

# angle+aspect calibration
print('DEBUG:   angle+aspect calibration')

angle_aspect_calibration_y_fig_html = pvgis.get_yearly_calibration_figure_html('angle+aspect')

with open('config.toml', 'w', encoding='utf-8') as config_file:
    toml.dump(config, config_file)


# -------------------------------
# production estimation
# -------------------------------
print('DEBUG: production estimation')

production_estimation_configurations = pvgis.get_production_estimation_configurations()

# TODO: delete this
# best_production_estimation_m_sdf = pvgis.get_best_production_estimation()
#
# best_production_estimation_m_df = best_production_estimation_m_sdf.toPandas()
#
# x = [
#     list(best_production_estimation_m_df['month']),
#     list(best_production_estimation_m_df['name']),
# ]
#
# production_estimation_m_fig = go.Figure()
# production_estimation_m_fig.add_trace(go.Bar(x=x, y=list(best_production_estimation_m_df['month_selfsupply_kwh']), name='Autoconsumo'))
# production_estimation_m_fig.add_trace(go.Bar(x=x, y=list(best_production_estimation_m_df['month_finalconsumption_kwh']), name='Consumo'))
# production_estimation_m_fig.add_trace(go.Bar(x=x, y=list(best_production_estimation_m_df['month_exceedingsold_simplified_kwh']), name='Exceso Vendido'))
# production_estimation_m_fig.add_trace(go.Bar(x=x, y=list(best_production_estimation_m_df['month_exceedingwasted_simplified_kwh']), name='Exceso Regalado'))
#
# production_estimation_m_fig.update_xaxes(tickangle=90)
# production_estimation_m_fig.update_layout(barmode='relative', legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
#
# production_estimation_m_fig_html = production_estimation_m_fig.to_html(include_plotlyjs=False, full_html=False, div_id='production_estimation_m_figure', default_height='800px')

# ----------------------------------------------------------------------------------------------------------------------

# -------------------------------
# Configuration
# -------------------------------
print('DEBUG: configuration')

# -------------------------------
# Data
# -------------------------------

# Esios Indicators
print('DEBUG: esios indicators')

EsiosIndicator(jinja_env, jinja_common_context).refresh_esios_indicators()

# Esios price
print('DEBUG: esios price')

price_period_from = price_date_max - relativedelta(years=1)

price_period_from = max(price_date_min, price_period_from)

esios_price_years = list(price_sdf.select(col('year')).distinct().orderBy('year').toPandas()['year'])

price_pvpc_evol = price_sdf\
    .filter(col('date') >= price_period_from)\
    .orderBy(col('date'))\
    .cache()

# Esios Buy Price
print('DEBUG: esios buy price')

price_rate_figure_ids = {}

price_dates_sdf = price_pvpc_evol\
    .select(col('date'))\
    .distinct()\
    .orderBy(col('date'))\
    .cache()

price_buy_pvpc_evol_fig = px.line(price_pvpc_evol.toPandas(),
                                  x='datetime',
                                  y='hour_pricebuy_ekwh',
                                  labels=dict(date='Fecha',
                                          hour_pricebuy_ekwh='Precio (€/kWh)'))
price_buy_pvpc_evol_fig.update_layout(xaxis={'title': None})
price_buy_pvpc_evol_fig.update_xaxes(
    tickangle=90,
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=3, label="3m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=9, label="9m", step="month", stepmode="backward"),
            dict(label="12m", step="all")
        ])
    ))

price_buy_pvpc_evol_fig_html = price_buy_pvpc_evol_fig.to_html(include_plotlyjs=False, full_html=False, div_id='price_pvpc_evol_figure')


def missing_value(x):
    return not x or math.isnan(x)


rate_info_list = [rate_20td_info, rate_wk_info, rate_fix_info]

html_template_data = {}

price_evol_with_bank_days_sdf = price_pvpc_evol\
    .join(bank_days_sdf
          .select(col('year'), col('month'), col('dom'), lit(True).alias('is_bank_day')),
          on=['year', 'month', 'dom'], how='left')\
    .fillna(False, subset=['is_bank_day'])\
    .cache()

for rate_info in rate_info_list:
    print('DEBUG:     {rate_type}'.format(rate_type=rate_info.get_rate_type()))

    price_pvpc_rate_evol = price_evol_with_bank_days_sdf\
        .withColumn('period', rate_info.get_period('hour', 'dow', 'is_bank_day'))\
        .groupBy(col('date'), col('period'))\
        .agg(
            ps_round(avg(col('hour_pricebuy_ekwh')), 3).alias('avg_period_kwh'),
            ps_round(std(col('hour_pricebuy_ekwh')), 3).alias('std_period_kwh')
        )\
        .withColumn('avg_period_kwh', col('avg_period_kwh').cast(DecimalType(10, 3)))\
        .withColumn('std_period_kwh', col('std_period_kwh').cast(DecimalType(10, 3)))\
        .withColumn('upp_period_kwh', col('avg_period_kwh') + col('std_period_kwh'))\
        .withColumn('low_period_kwh', col('avg_period_kwh') - col('std_period_kwh'))\
        .orderBy('date', 'period')\
        .cache()

    rate_period_list = rate_info.get_periods()

    price_pvpc_rate_evol_fig = make_subplots(rows=len(rate_period_list), cols=1,
                                             vertical_spacing=0.2,
                                             row_titles=rate_info.get_periods())

    for (rate_i, rate_period) in enumerate(rate_period_list):
        row = rate_i + 1

        price_pvpc_rate_period_evol = price_dates_sdf\
            .join(price_pvpc_rate_evol.filter(col('period') == rate_period), on='date', how='left')\
            .fillna(rate_period, subset=['period'])\
            .toPandas()

        price_pvpc_rate_period_x = list(price_pvpc_rate_period_evol['date'])
        price_pvpc_rate_period_y = list(price_pvpc_rate_period_evol['avg_period_kwh'])
        price_pvpc_rate_period_y_upper = list(price_pvpc_rate_period_evol['upp_period_kwh'])
        price_pvpc_rate_period_y_lower = list(price_pvpc_rate_period_evol['low_period_kwh'])

        price_pvpc_rate_evol_fig.add_trace(go.Scatter(
            name=rate_period,
            x=price_pvpc_rate_period_x,
            y=price_pvpc_rate_period_y,
            mode='lines',
            line=dict(color=rate_info.get_period_color_rgba(rate_period)),
            showlegend=True,
            connectgaps=False,
            customdata=np.dstack((price_pvpc_rate_period_y_lower, price_pvpc_rate_period_y_upper))[0],
            hovertemplate='Fecha=%{x}<br>Precio (\u20ac/kWh)=[%{customdata[0]} > %{y} < %{customdata[1]}]'
        ), row=row, col=1)

        filling_x = price_pvpc_rate_period_x + price_pvpc_rate_period_x[::-1]
        filling_y = price_pvpc_rate_period_y_upper + price_pvpc_rate_period_y_lower[::-1]

        gaps = []

        for (prev_pos, curr) in enumerate(filling_y[1:]):
            prev = filling_y[prev_pos]
            pos = prev_pos + 1

            if not missing_value(prev) and missing_value(curr):
                gaps.append(dict(pos=pos, date=filling_x[prev_pos]))
            elif missing_value(prev) and not missing_value(curr):
                gaps.append(dict(pos=pos, date=filling_x[pos]))

        for (gap_i, gap) in enumerate(gaps):
            filling_x.insert(gap['pos'] + gap_i, gap['date'])
            filling_y.insert(gap['pos'] + gap_i, Decimal('0.0'))

        filling_y = [y if y and not math.isnan(y) else Decimal('0.0') for y in filling_y]

        price_pvpc_rate_evol_fig.add_trace(go.Scatter(
            name=f'{rate_period} (±SD)',
            x=filling_x,
            y=filling_y,
            fill='toself',
            fillcolor=rate_info.get_period_color_rgba(rate_period, 0.2),
            line=dict(width=0),
            showlegend=True,
            hoverinfo='skip',
            connectgaps=True
        ), row=row, col=1)

        if row > 1:
            price_pvpc_rate_evol_fig.update_yaxes(matches='y', row=row, col=1)
            price_pvpc_rate_evol_fig.update_xaxes(matches='x', row=row, col=1)
            price_pvpc_rate_evol_fig.update_layout(**{f'yaxis{row}': {'title': 'Precio medio (€/kWh)'}})

    price_pvpc_rate_evol_fig.update_layout(xaxis={'title': None}, yaxis={'title': 'Precio medio (€/kWh)'}, legend=dict(title='Periodo'), hovermode='x')
    price_pvpc_rate_evol_fig.update_xaxes(
        tickangle=90,
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=3, label="3m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(count=9, label="9m", step="month", stepmode="backward"),
                dict(label="12m", step="all")
            ])
        )
    )

    figure_id = f'price_pvpc_{rate_info.get_rate_type()}_evol_figure'
    price_rate_figure_ids[rate_info.get_rate_type()] = figure_id

    price_pvpc_rate_evol_fig_html = price_pvpc_rate_evol_fig.to_html(include_plotlyjs=False,
                                                                     full_html=False,
                                                                     div_id=figure_id,
                                                                     default_height=f'{len(rate_period_list) * 400}px')

    html_template_data[f'price_pvpc_{rate_info.get_rate_type()}_evol_fig'] = price_pvpc_rate_evol_fig_html
    html_template_data[f'rate_{rate_info.get_rate_type()}_info_series'] = rate_wk_info.get_series()
    html_template_data[f'rate_{rate_info.get_rate_type()}_info_periods'] = rate_wk_info.get_periods()

    price_pvpc_rate_evol.unpersist()

price_evol_with_bank_days_sdf.unpersist()
price_dates_sdf.unpersist()

# Esios sell price
print('DEBUG: esios sell price')

price_sell_pvpc_evol_fig = px.line(price_pvpc_evol.toPandas(),
                                   x='datetime',
                                   y='hour_pricesell_ekwh',
                                   labels=dict(date='Fecha',
                                               hour_pricesell_ekwh='Precio (€/kWh)'))
price_sell_pvpc_evol_fig.update_layout(xaxis={'title': None})
price_sell_pvpc_evol_fig.update_xaxes(
    tickangle=90,
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=3, label="3m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=9, label="9m", step="month", stepmode="backward"),
            dict(label="12m", step="all")
        ])
    ))

price_sell_pvpc_evol_fig_html = price_sell_pvpc_evol_fig.to_html(include_plotlyjs=False, full_html=False, div_id='price_pvpc_evol_figure')


price_pvpc_evol_m_sdf = price_pvpc_evol\
    .groupBy(col('year'), col('month'))\
    .agg(
        ps_round(avg(col('hour_pricesell_ekwh')).alias('avg_price_sell_kwh'), 3).alias('avg_price_sell_kwh'),
        ps_round(std(col('hour_pricesell_ekwh')).alias('std_price_sell_kwh'), 3).alias('std_price_sell_kwh'),
    )\
    .withColumn('avg_price_sell_kwh', col('avg_price_sell_kwh').cast(DecimalType(10, 3)))\
    .withColumn('std_price_sell_kwh', col('std_price_sell_kwh').cast(DecimalType(10, 3)))\
    .withColumn('upp_price_sell_kwh', col('avg_price_sell_kwh') + col('std_price_sell_kwh'))\
    .withColumn('low_price_sell_kwh', col('avg_price_sell_kwh') - col('std_price_sell_kwh'))\
    .orderBy(col('year'), col('month'))\
    .withColumn('month_year', concat(lpad(col('month'), 2, '0'), lit('-'), col('year')))\
    .drop('month', 'year')\

price_pvpc_evol_m_df = price_pvpc_evol_m_sdf.toPandas()

price_sell_pvpc_evol_m_x = list(price_pvpc_evol_m_df['month_year'])
price_sell_pvpc_evol_m_y = list(price_pvpc_evol_m_df['avg_price_sell_kwh'])
price_sell_pvpc_evol_m_y_upper = list(price_pvpc_evol_m_df['upp_price_sell_kwh'])
price_sell_pvpc_evol_m_y_lower = list(price_pvpc_evol_m_df['low_price_sell_kwh'])

price_sell_pvpc_evol_m_fig = px.line(price_pvpc_evol_m_df,
                                     x='month_year',
                                     y='avg_price_sell_kwh',
                                     custom_data=['low_price_sell_kwh', 'upp_price_sell_kwh'],
                                     labels=dict(month_year='Mes/Año',
                                                 avg_price_sell_kwh='Precio (€/kWh)'))
price_sell_pvpc_evol_m_fig.update_traces(hovertemplate='Fecha=%{x}<br>Precio (\u20ac/kWh)=[%{customdata[0]} > %{y} < %{customdata[1]}]')
price_sell_pvpc_evol_m_fig.update_layout(xaxis={'title': None})
price_sell_pvpc_evol_m_fig.update_xaxes(tickangle=90)

price_sell_pvpc_evol_m_filling_x = price_sell_pvpc_evol_m_x + price_sell_pvpc_evol_m_x[::-1]
price_sell_pvpc_evol_m_filling_y = price_sell_pvpc_evol_m_y_upper + price_sell_pvpc_evol_m_y_lower[::-1]

price_sell_pvpc_evol_m_fig.add_trace(go.Scatter(
            name=f'Precio (±SD)',
            x=price_sell_pvpc_evol_m_filling_x,
            y=price_sell_pvpc_evol_m_filling_y,
            fill='toself',
            fillcolor='rgba(99,110,250,0.2)',
            line=dict(width=0),
            showlegend=False,
            hoverinfo='skip',
            connectgaps=True
        ))

price_sell_pvpc_evol_m_fig_html = price_sell_pvpc_evol_m_fig.to_html(include_plotlyjs=False, full_html=False, div_id='price_pvpc_evol_m_figure')

# bank days
print('DEBUG: bank days')

# weather
print('DEBUG: weather')


# htmls
print('DEBUG: render all htmls')

jinja_env.get_template('consumption/consumption_y.html')\
    .stream(
        consumption_y_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
        consumption_y_fig=consumption_y_fig_html,
        **jinja_common_context
    ).dump(os.path.join('docs', 'consumption', 'consumption_y.html'))

jinja_env.get_template('consumption/consumption_moy.html')\
    .stream(
        consumption_moy_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
        consumption_moy_comp_fig=consumption_moy_comp_fig_html,
        consumption_moy_evol_year_fig=consumption_moy_evol_year_fig_html,
        consumption_moy_evol_full_fig=consumption_moy_evol_full_fig_html,
        **jinja_common_context
    ).dump(os.path.join('docs', 'consumption', 'consumption_moy.html'))

jinja_env.get_template('consumption/consumption_dow.html')\
    .stream(
        consumption_dow_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
        consumption_dow_sum_fig=consumption_dow_sum_fig_html,
        consumption_dow_avg_fig=consumption_dow_avg_fig_html,
        **jinja_common_context
    ).dump(os.path.join('docs', 'consumption', 'consumption_dow.html'))

jinja_env.get_template('consumption/consumption_hod.html')\
    .stream(
        consumption_hod_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
        consumption_hod_sum_fig=consumption_hod_sum_fig_html,
        consumption_hod_avg_fig=consumption_hod_avg_fig_html,
        **jinja_common_context
    ).dump(os.path.join('docs', 'consumption', 'consumption_hod.html'))

jinja_env.get_template('consumption/period_m.html')\
    .stream(
        period_m_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
        rate_20td_m_periods_fig=rate_20td_m_periods_fig_html,
        rate_20td_m_periods_pct_fig=rate_20td_m_periods_pct_fig_html,
        rate_20td_info_series=rate_20td_info.get_series(),
        rate_20td_info_periods=rate_20td_info.get_periods(),
        rate_wk_m_periods_fig=rate_wk_m_periods_fig_html,
        rate_wk_m_periods_pct_fig=rate_wk_m_periods_pct_fig_html,
        rate_wk_info_series=rate_wk_info.get_series(),
        rate_wk_info_periods=rate_wk_info.get_periods(),
        rate_fix_m_periods_fig=rate_fix_m_periods_fig_html,
        rate_fix_info_series=rate_fix_info.get_series(),
        rate_fix_info_periods=rate_fix_info.get_periods(),
        **jinja_common_context
    ).dump(os.path.join('docs', 'consumption', 'period_m.html'))

jinja_env.get_template('consumption/consumption_dom.html')\
    .stream(
        consumption_dom_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
        max_date_year=consumption_date_max.year,
        max_date_month_js=(consumption_date_max.month - 1),
        max_date_day=consumption_date_max.day,
        min_date_year=consumption_date_min.year,
        min_date_month_js=(consumption_date_min.month - 1),
        min_date_day=consumption_date_min.day,
        **jinja_common_context
    ).dump(os.path.join('docs', 'consumption', 'consumption_dom.html'))

jinja_env.get_template('cost.html')\
    .stream(
        cost_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
        month_year_list=month_year_list,
        months_count=len(month_year_list),
        pvpc_data=rate_20td_pvpc_cost_data,
        **jinja_common_context
    ).dump(os.path.join('docs', 'cost.html'))

jinja_env.get_template('selfsupply/calibrations.html')\
    .stream(
        calibrations_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
        angle_calibration_m_fig=angle_calibration_m_fig_html,
        angle_calibration_y_fig=angle_calibration_y_fig_html,
        aspect_calibration_m_fig=aspect_calibration_m_fig_html,
        aspect_calibration_y_fig=aspect_calibration_y_fig_html,
        angle_aspect_calibration_y_fig=angle_aspect_calibration_y_fig_html,
        **jinja_common_context
    ).dump(os.path.join('docs', 'selfsupply', 'calibrations.html'))

jinja_env.get_template('selfsupply/calculator.html')\
    .stream(
        production_estimation_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
        production_estimation_configurations=production_estimation_configurations,
        production_estimation_date_scope=pvgis.get_date_scope(),
        **jinja_common_context
    ).dump(os.path.join('docs', 'selfsupply', 'calculator.html'))

jinja_env.get_template('configuration.html')\
    .stream(
        configuration_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
        rate_20td_info_series=rate_20td_info.get_series(),
        rate_20td_info_periods=rate_20td_info.get_periods(),
        rate_wk_info_series=rate_wk_info.get_series(),
        rate_wk_info_periods=rate_wk_info.get_periods(),
        rate_fix_info_series=rate_fix_info.get_series(),
        rate_fix_info_periods=rate_fix_info.get_periods(),
        github_username=config['github']['username'],
        configuration_backup_filename=config['github']['configuration_backup_filename'],
        **jinja_common_context
    ).dump(os.path.join('docs', 'configuration.html'))

jinja_env.get_template('esios/esios_price_buy_20td.html')\
    .stream(
        price_buy_pvpc_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
        esios_price_years=esios_price_years,
        price_pvpc_evol_fig=price_buy_pvpc_evol_fig_html,
        price_rate_figure_ids=price_rate_figure_ids,
        **(jinja_common_context | html_template_data)
    ).dump(os.path.join('docs', 'esios', 'esios_price_buy_20td.html'))

jinja_env.get_template('esios/esios_price_sell_20td.html')\
    .stream(
        price_sell_pvpc_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
        esios_price_years=esios_price_years,
        price_pvpc_evol_fig=price_sell_pvpc_evol_fig_html,
        price_pvpc_evol_m_fig=price_sell_pvpc_evol_m_fig_html,
        **jinja_common_context
    ).dump(os.path.join('docs', 'esios', 'esios_price_sell_20td.html'))

jinja_env.get_template('bank_days.html')\
    .stream(
        bank_days_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
        bank_days_file_name=bank_days.get_bank_days_file_name(),
        **jinja_common_context
    ).dump(os.path.join('docs', 'bank_days.html'))

jinja_env.get_template('weather.html')\
    .stream(
        weather_menu_item_active=constants.MENU_ITEM_ACTIVE_CLASS,
        **jinja_common_context
    ).dump(os.path.join('docs', 'weather.html'))
