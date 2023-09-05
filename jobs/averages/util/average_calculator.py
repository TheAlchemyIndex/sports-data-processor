from pyspark.sql import functions as fn
from pyspark.sql import Window

_decimal_points = 9


def last_n_rows(df, partition_col_1, partition_col_2, num_of_rows):
    window = Window.partitionBy(partition_col_1, partition_col_2).orderBy(fn.desc("date"))

    result_df = df.withColumn("row_num", fn.row_number().over(window))
    filtered_df = result_df.filter(fn.col("row_num") <= num_of_rows).drop("row_num")

    return filtered_df


# TODO Make more efficient and have one method
def calculate_partitioned_avg_single(partition_col, target_col):
    window = Window.partitionBy(partition_col)
    return fn.round(fn.avg(target_col).over(window), _decimal_points)


def calculate_partitioned_avg(partition_col_1, partition_col_2, target_col):
    window = Window.partitionBy(partition_col_1, partition_col_2)
    return fn.round(fn.avg(target_col).over(window), _decimal_points)


def last_value_in_col(df, partition_col, target_col, new_col):
    window = Window.partitionBy(partition_col).orderBy("date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    last_value_df = df.withColumn(new_col, fn.last(target_col, ignorenulls=True).over(window))

    return last_value_df.withColumn(target_col, fn.coalesce(target_col, new_col))
