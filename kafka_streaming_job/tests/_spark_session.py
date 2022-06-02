import pathlib

from pyspark.sql import SparkSession

jars = [filepath.absolute().__str__() for filepath in pathlib.Path("docker/jars/").glob('**/*')]

jar_dependencies = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2"
]

packages = ",".join(jar_dependencies)
print("packages: {}".format(packages))


def get_spark_session():
    spark = SparkSession.builder \
        .appName("Kafka streaming") \
        .master("local") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "lakehouse") \
        .config("spark.jars.packages", packages) \
        .config("spark.jars", ",".join(jars)) \
        .config("spark.sql.legacy.createHiveTableByDefault", "false") \
        .config("spark.sql.sources.default", "iceberg") \
        .getOrCreate()

    return spark
