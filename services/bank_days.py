import glob
import io
import os.path

import pandas as pd
import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, lower, min as ps_min, max as ps_max, to_date, initcap
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

import utils


class BankDays:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bank_days_list_files_pattern = 'docs/data/bank_days_*.json'
        self.csv_separator = ';'
        self.url = 'https://datos.madrid.es/egob/catalogo/300082-0-calendario_laboral.csv'
        self.headers = {
            'Accept': 'text/csv'
        }
        self.national_bank_day_types = ['Festivo nacional', 'Fiesta nacional']
        self.sdf = None
        self.year_min = None
        self.year_max = None
        self.file_name = None

        self.schema = StructType([
            StructField('year', IntegerType(), False),
            StructField('month', IntegerType(), False),
            StructField('dom', IntegerType(), False),
            StructField('bank_day_name', StringType(), False),
        ])

        self._load_bank_days()
        self._calculate_year_range()

    def _load_bank_days(self) -> None:
        bank_days_files = glob.glob(self.bank_days_list_files_pattern)

        if len(bank_days_files) == 1:
            self.file_name = os.path.basename(bank_days_files[0])
            self.sdf = utils.json_file_to_df(bank_days_files[0], self.spark, self.schema)

    def _calculate_year_range(self) -> None:
        if self.sdf is not None:
            self.year_min = self.sdf.select(ps_min('year').alias('year_min')).first()['year_min']
            self.year_max = self.sdf.select(ps_max('year').alias('year_max')).first()['year_max']

    def _update_required(self, years: list[int]) -> bool:
        update_required = True

        if self.year_max is not None and self.year_min is not None:
            existing_years = [y for y in range(self.year_min, self.year_max + 1)]

            # check if all years in years are in existing_years
            if set(years).issubset(existing_years):
                update_required = False

        return update_required

    def _update_bank_days(self, years: list[int]) -> None:
        response = requests.get(self.url, headers=self.headers)

        if response.status_code == 200:
            response.encoding = response.apparent_encoding
            response_csv = response.text

            csv_data = io.StringIO(response_csv)

            df = pd.read_csv(csv_data,
                             sep=self.csv_separator, parse_dates=False, keep_default_na=False, na_values=['_'])

            sdf = self.spark.createDataFrame(df)\
                .withColumnRenamed('Tipo de Festivo', 'bank_day_type')\
                .withColumnRenamed('Día', 'bank_day')\
                .withColumnRenamed('Festividad', 'bank_day_name')\
                .filter(col('bank_day_type').isin(self.national_bank_day_types))\
                .withColumn('bank_day', to_date(col('bank_day'), 'dd/MM/yyyy'))\
                .withColumn('year', year('bank_day'))\
                .filter(col('year').isin(years))\
                .withColumn('month', month('bank_day'))\
                .withColumn('dom', dayofmonth('bank_day'))\
                .withColumn('bank_day_name', lower('bank_day_name'))\
                .select('year', 'month', 'dom', 'bank_day_name')\
                .orderBy('year', 'month', 'dom')

            year_min = sdf.select(ps_min('year').alias('year_min')).first()['year_min']
            year_max = sdf.select(ps_max('year').alias('year_max')).first()['year_max']

            self.file_name = f'docs/data/bank_days_{year_min}_{year_max}.json'
            utils.df_to_json_file(sdf, self.file_name)

            self.sdf = sdf
            self.year_min = year_min
            self.year_max = year_max
        else:
            raise Exception(response.status_code)

    def get_bank_days(self, years: list[int]) -> DataFrame:
        if self._update_required(years):
            self._update_bank_days(years=years)

        return self.sdf

    def get_bank_days_file_name(self) -> str:
        return self.file_name
