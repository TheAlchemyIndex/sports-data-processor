from pyspark.sql import functions as fn
from pyspark.sql import Window

DECIMAL_POINTS = 9


def last_n_rows(df, partition_col_1, partition_col_2, num_of_rows):
    window = Window.partitionBy(partition_col_1, partition_col_2).orderBy(fn.desc("date"))

    result_df = df.withColumn("row_num", fn.row_number().over(window))
    filtered_df = result_df.filter(fn.col("row_num") <= num_of_rows).drop("row_num")

    return filtered_df


def calculate_partitioned_avg(partition_col_1, partition_col_2, target_col):
    window = Window.partitionBy(partition_col_1, partition_col_2)
    return fn.round(fn.avg(target_col).over(window), DECIMAL_POINTS)
