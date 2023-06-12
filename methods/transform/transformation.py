from typing import Optional
import pyspark.sql.functions as f
from pyspark import Row
from pyspark.sql.types import DoubleType, DateType
import methods.constants.constants as c
import methods.utils.utils as u
from pyspark.sql import DataFrame, Window


class Transformation:
    def __init__(self):
        pass

    @staticmethod
    def select_column(df: DataFrame) -> DataFrame:
        return df.select(c.SEASON,
                         c.TITLE,
                         c.NSEASON,
                         f.col(c.ORIGINALD).cast(DateType()),
                         c.ORIGINALY,
                         f.col(c.RATING).cast(DoubleType()),
                         c.VOTES,
                         f.col(c.VIEWERM).cast(DoubleType())
                         )

    @staticmethod
    def clean_df(df: DataFrame) -> DataFrame:
        return df.where(f.col(c.RATING).isNotNull() &
                        f.col(c.VOTES).isNotNull() &
                        f.col(c.VIEWERM).isNotNull())

    @staticmethod
    def best_season(df: DataFrame) -> DataFrame:
        return df.groupBy(f.col(c.SEASON)).agg(f.sum(c.RATING).alias(c.TOTAL_RATING))

    @staticmethod
    def best_year(df: DataFrame) -> DataFrame:
        return df.groupBy(f.col(c.ORIGINALY)) \
            .agg(f.sum(f.col(c.VIEWERM)).alias(c.VIEWERM)) \
            .orderBy(f.col(c.VIEWERM).desc())

    @staticmethod
    def best_chapter_df(df: DataFrame) -> DataFrame:
        return df.select(*df.columns, (f.round(f.col(c.RATING) * f.col(c.VIEWERM), 2)).alias(c.SCORE)) \
            .orderBy(f.col(c.SCORE).desc())

    @staticmethod
    def best_chapter(df: DataFrame) -> DataFrame:
        return df.select(f.col(c.TITLE),
                         (f.col(c.RATING) * f.col(c.VIEWERM)).alias(c.SCORE)) \
            .orderBy(f.col(c.SCORE).desc())

    @staticmethod
    def top_episodies(df: DataFrame) -> DataFrame:
        """window = Window.partitionBy(f.col(c.SEASON)).orderBy(f.col(c.SCORE).desc())
        return df.select(*df.columns,
                         f.rank().over(window).alias(c.TOP))"""
        window = Window.partitionBy(c.SEASON).orderBy(f.desc(c.SCORE))
        ranked_df = df.withColumn(c.RANK, f.row_number().over(window))
        filtered_df = ranked_df.filter(f.col(c.RANK) <= 3)
        return filtered_df

