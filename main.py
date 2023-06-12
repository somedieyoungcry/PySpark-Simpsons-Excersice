from pyspark.sql import SparkSession
import methods.constants.constants as c
import pyspark.sql.functions as f
import methods.utils.utils as u
from methods.transform.transformation import Transformation


def main():
    spark = SparkSession.builder.appName(c.APP_NAME).master(c.MODE).getOrCreate()

    t = Transformation()
    #simpson_df = spark.read.option("delimiter", "|").csv(c.INPUT_PATH)
    #simpson_df.show()

    simpson_df = spark.read.option(c.HEADER, c.TRUE_STRING).option("delimiter", "|").csv(c.INPUT_PATH_2)
    #simpson_df.show()

    select_columns_df = t.select_column(simpson_df)
    #select_columns_df.show()

    cleanDF_df = t.clean_df(select_columns_df)
    #cleanDF_df.show()
    # cleanDF_df.explain()

    best_season_df = t.best_season(cleanDF_df)
    best_season_df.show(1)
    # best_season_df.explain()

    best_year_df = t.best_year(cleanDF_df)
    best_year_df.show(1)
    # best_year_df.explain()

    best_chapter_df = t.best_chapter_df(cleanDF_df)
    #best_chapter_df.show(1)

    best_chapter_df_1 = t.best_chapter(cleanDF_df)
    best_chapter_df_1.show(1)

    top_episodies_df = t.top_episodies(best_chapter_df)
    top_episodies_df.show()

    top_episodies_df.write.mode(c.MODE_OVERWRITE).parquet(c.OUTPUT)


if __name__ == '__main__':
    main()
