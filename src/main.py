from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("datamarca-pipeline").master("local[*]").getOrCreate()
    df = spark.createDataFrame(
        [(1, "tienda_a", 25000.0), (2, "tienda_b", 18000.0)],
        ["id_transaccion", "tienda", "monto"],
    )
    df.show()
    spark.stop()


if __name__ == "__main__":
    main()