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

# Script generated for node S3 - Film_Category
S3Film_Category_node1692632280602 = glueContext.create_dynamic_frame.from_catalog(
    database="curatedzonedb",
    table_name="film_category",
    transformation_ctx="S3Film_Category_node1692632280602",
)

# Script generated for node S3 - Streaming
S3Streaming_node1692632321110 = glueContext.create_dynamic_frame.from_catalog(
    database="streaming-db",
    table_name="streaming",
    transformation_ctx="S3Streaming_node1692632321110",
)

# Script generated for node ApplyMapping - Streaming
ApplyMappingStreaming_node1692632349277 = ApplyMapping.apply(
    frame=S3Streaming_node1692632321110,
    mappings=[
        ("timestamp", "string", "timestamp", "string"),
        ("eventtype", "string", "eventtype", "string"),
        ("film_id", "int", "film_id_streaming", "int"),
        ("distributor", "string", "distributor", "string"),
        ("platform", "string", "platform", "string"),
        ("state", "string", "state", "string"),
        ("partition_0", "string", "partition_0", "string"),
        ("partition_1", "string", "partition_1", "string"),
    ],
    transformation_ctx="ApplyMappingStreaming_node1692632349277",
)

# Script generated for node Join
ApplyMappingStreaming_node1692632349277DF = (
    ApplyMappingStreaming_node1692632349277.toDF()
)
S3Film_Category_node1692632280602DF = S3Film_Category_node1692632280602.toDF()
Join_node1692632415260 = DynamicFrame.fromDF(
    ApplyMappingStreaming_node1692632349277DF.join(
        S3Film_Category_node1692632280602DF,
        (
            ApplyMappingStreaming_node1692632349277DF["film_id_streaming"]
            == S3Film_Category_node1692632280602DF["film_id"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1692632415260",
)

# Script generated for node Amazon S3
AmazonS3_node1692632463082 = glueContext.getSink(
    path="s3://dataeng-curated-zone-<SUFFIX>/streaming/streaming-films/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1692632463082",
)
AmazonS3_node1692632463082.setCatalogInfo(
    catalogDatabase="curatedzonedb", catalogTableName="streaming_films"
)
AmazonS3_node1692632463082.setFormat("glueparquet")
AmazonS3_node1692632463082.writeFrame(Join_node1692632415260)
job.commit()
