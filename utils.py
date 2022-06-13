import glob
import json
import os
from decimal import Decimal
from typing import Callable

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def df_to_json_file(sdf, destination_path, date_cols_indexes=()) -> None:
    if len(date_cols_indexes) > 0:
        destination_temp = f'{destination_path}.tmp'

        sdf.toPandas()\
            .to_json(destination_temp, orient='split', index=False, date_format='iso', date_unit='s')

        with open(destination_temp, 'r') as temp_file:
            json_data = json.load(temp_file)

            for data_elem in json_data['data']:
                for date_col_index in date_cols_indexes:
                    data_elem[date_col_index] = data_elem[date_col_index][0:10]

            with open(destination_path, 'w', encoding='utf-8') as file:
                json.dump(json_data, file, ensure_ascii=False)

        os.remove(destination_temp)
    else:
        sdf.toPandas()\
            .to_json(destination_path, orient='split', index=False, date_format='iso', date_unit='s', force_ascii=False)


def df_to_json(df, date_cols_indexes=()):
    json_string = df.toPandas() \
        .to_json(orient='split', index=False, date_format='iso', date_unit='s')

    json_data = json.loads(json_string)

    if len(date_cols_indexes) > 0:
        for data_elem in json_data['data']:
            for date_col_index in date_cols_indexes:
                data_elem[date_col_index] = data_elem[date_col_index][0:10]

    return json_data


def json_file_to_df(json_path: str, spark: SparkSession, sp_schema: StructType,
                    column_transformer: Callable[[DataFrame], DataFrame] = lambda df: df) -> DataFrame:
    json_file_list = glob.glob(json_path)

    sdf = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=sp_schema)

    if len(json_file_list) > 0:
        for json_file in json_file_list:
            json_file_df = pd.read_json(json_file, orient='split', convert_dates=False)

            for column in json_file_df:
                if json_file_df[column].dtype == 'float64':
                    json_file_df[column] = json_file_df[column].apply(lambda v: Decimal(v))

            consumption_file_sdf = column_transformer(spark.createDataFrame(json_file_df))

            # new spark DataFrame is created in order to ensure that it has the same schema as the previous ones
            sdf = sdf\
                .union(spark.createDataFrame(consumption_file_sdf.toPandas(), schema=sp_schema)) \
                .cache()

    print('DEBUG: read rows: {rows}'.format(rows=sdf.count()))

    return sdf


def json_to_df(json_data, spark: SparkSession, sp_schema: StructType) -> DataFrame:
    df = pd.DataFrame(json_data['data'], columns=json_data['columns'])

    for column in df:
        if df[column].dtype == 'float64':
            df[column] = df[column].apply(lambda v: Decimal(v))

    return spark.createDataFrame(df, schema=sp_schema)\
        .cache()
