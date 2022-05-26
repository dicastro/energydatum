import glob
import json
import os
from typing import Callable, Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def df_to_json(df, destination_path, date_cols_indexes=()) -> None:
    if len(date_cols_indexes) > 0:
        destination_temp = f'{destination_path}.tmp'

        df.toPandas()\
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
        df.toPandas()\
            .to_json(destination_path, orient='split', index=False, date_format='iso', date_unit='s', force_ascii=False)


def json_to_df(json_path: str, spark: SparkSession, schema: StructType,
               column_transformer: Callable[[DataFrame], DataFrame] = lambda df: df) -> DataFrame:
    json_file_list = glob.glob(json_path)

    sdf = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=schema)

    if len(json_file_list) > 0:
        for json_file in json_file_list:
            json_file_df = pd.read_json(json_file, orient='split', convert_dates=False)

            consumption_file_sdf = column_transformer(spark.createDataFrame(json_file_df))

            # new spark DataFrame is created in order to ensure that it has the same schema as the previous ones
            sdf = sdf\
                .union(spark.createDataFrame(consumption_file_sdf.toPandas(), schema=schema)) \
                .cache()

    print('DEBUG: read rows: {rows}'.format(rows=sdf.count()))

    return sdf
