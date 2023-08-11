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

# Script generated for node Customer trusted
Customertrusted_node1691702200940 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-nanodegree/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Customertrusted_node1691702200940",
)

# Script generated for node Accelerometer landing
Accelerometerlanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerometerlanding_node1",
)

# Script generated for node Join Customer
JoinCustomer_node1691702325417 = Join.apply(
    frame1=Accelerometerlanding_node1,
    frame2=Customertrusted_node1691702200940,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1691702325417",
)

# Script generated for node Drop Fields
DropFields_node1691702608366 = DropFields.apply(
    frame=JoinCustomer_node1691702325417,
    paths=["x", "y", "z", "user", "timestamp"],
    transformation_ctx="DropFields_node1691702608366",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1691702439841 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1691702608366,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-nanodegree/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1691702439841",
)

job.commit()
