import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1701833206713 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://finalproinput/heart_disease_health_indicators_BRFSS2015.csv"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1701833206713",
)

# Script generated for node Change Schema
ChangeSchema_node1701830099043 = ApplyMapping.apply(
    frame=AmazonS3_node1701833206713,
    mappings=[
        ("heartdiseaseorattack", "string", "heartdiseaseorattack", "string"),
        ("highbp", "string", "highbp", "string"),
        ("highchol", "string", "highchol", "string"),
        ("cholcheck", "string", "cholcheck", "string"),
        ("bmi", "string", "bmi", "string"),
        ("smoker", "string", "smoker", "string"),
        ("stroke", "string", "stroke", "string"),
        ("diabetes", "string", "diabetes", "string"),
        ("physactivity", "string", "physactivity", "string"),
        ("fruits", "string", "fruits", "string"),
        ("veggies", "string", "veggies", "string"),
        ("hvyalcoholconsump", "string", "hvyalcoholconsump", "string"),
        ("anyhealthcare", "string", "anyhealthcare", "string"),
        ("nodocbccost", "string", "nodocbccost", "string"),
        ("genhlth", "string", "genhlth", "string"),
        ("menthlth", "string", "menthlth", "string"),
        ("physhlth", "string", "physhlth", "string"),
        ("diffwalk", "string", "diffwalk", "string"),
        ("sex", "string", "sex", "string"),
        ("age", "string", "age", "string"),
        ("education", "string", "education", "string"),
        ("income", "string", "income", "string"),
    ],
    transformation_ctx="ChangeSchema_node1701830099043",
)

# Script generated for node Amazon S3
AmazonS3_node1701830140540 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1701830099043,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://querylocationnn/Unsaved/",
        "compression": "gzip",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1701830140540",
)

job.commit()
