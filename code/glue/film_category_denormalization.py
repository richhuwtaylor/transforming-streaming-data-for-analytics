import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node s3 - Film-Category
s3FilmCategory_node1692626246895 = glueContext.create_dynamic_frame.from_catalog(
    database="sakila",
    table_name="film_category",
    transformation_ctx="s3FilmCategory_node1692626246895",
)

# Script generated for node S3 - Film
S3Film_node1692626327887 = glueContext.create_dynamic_frame.from_catalog(
    database="sakila", table_name="film", transformation_ctx="S3Film_node1692626327887"
)

# Script generated for node s3 - Category
s3Category_node1692626739629 = glueContext.create_dynamic_frame.from_catalog(
    database="sakila",
    table_name="category",
    transformation_ctx="s3Category_node1692626739629",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1692626454914 = ApplyMapping.apply(
    frame=s3FilmCategory_node1692626246895,
    mappings=[
        ("film_id", "long", "right_film_id", "long"),
        ("category_id", "long", "right_category_id", "long"),
        ("last_update", "string", "right_last_update", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1692626454914",
)

# Script generated for node Join - Film-Category_ID
S3Film_node1692626327887DF = S3Film_node1692626327887.toDF()
RenamedkeysforJoin_node1692626454914DF = RenamedkeysforJoin_node1692626454914.toDF()
JoinFilmCategory_ID_node1692626413305 = DynamicFrame.fromDF(
    S3Film_node1692626327887DF.join(
        RenamedkeysforJoin_node1692626454914DF,
        (
            S3Film_node1692626327887DF["film_id"]
            == RenamedkeysforJoin_node1692626454914DF["right_film_id"]
        ),
        "left",
    ),
    glueContext,
    "JoinFilmCategory_ID_node1692626413305",
)

# Script generated for node Change Schema
ChangeSchema_node1692626591199 = ApplyMapping.apply(
    frame=JoinFilmCategory_ID_node1692626413305,
    mappings=[
        ("film_id", "long", "film_id", "long"),
        ("title", "string", "title", "string"),
        ("description", "string", "description", "string"),
        ("release_year", "long", "release_year", "long"),
        ("language_id", "long", "language_id", "long"),
        ("original_language_id", "double", "original_language_id", "double"),
        ("length", "long", "length", "long"),
        ("rating", "string", "rating", "string"),
        ("special_features", "string", "special_features", "string"),
        ("right_category_id", "long", "right_category_id", "long"),
    ],
    transformation_ctx="ChangeSchema_node1692626591199",
)

# Script generated for node Join
ChangeSchema_node1692626591199DF = ChangeSchema_node1692626591199.toDF()
s3Category_node1692626739629DF = s3Category_node1692626739629.toDF()
Join_node1692626831163 = DynamicFrame.fromDF(
    ChangeSchema_node1692626591199DF.join(
        s3Category_node1692626739629DF,
        (
            ChangeSchema_node1692626591199DF["right_category_id"]
            == s3Category_node1692626739629DF["category_id"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1692626831163",
)

# Script generated for node Change Schema
ChangeSchema_node1692626977532 = ApplyMapping.apply(
    frame=Join_node1692626831163,
    mappings=[
        ("film_id", "long", "film_id", "long"),
        ("title", "string", "title", "string"),
        ("description", "string", "description", "string"),
        ("release_year", "long", "release_year", "long"),
        ("language_id", "long", "language_id", "long"),
        ("original_language_id", "double", "original_language_id", "double"),
        ("length", "long", "length", "long"),
        ("rating", "string", "rating", "string"),
        ("special_features", "string", "special_features", "string"),
        ("category_id", "long", "category_id", "long"),
        ("name", "string", "category_name", "string"),
    ],
    transformation_ctx="ChangeSchema_node1692626977532",
)

# Script generated for node Amazon S3
AmazonS3_node1692627120850 = glueContext.getSink(
    path="s3://dataeng-curated-zone-<SUFFIX>/filmdb/film_category/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1692627120850",
)
AmazonS3_node1692627120850.setCatalogInfo(
    catalogDatabase="curatedzonedb", catalogTableName="film_category"
)
AmazonS3_node1692627120850.setFormat("glueparquet")
AmazonS3_node1692627120850.writeFrame(ChangeSchema_node1692626977532)
job.commit()