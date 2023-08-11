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

# Script generated for node Step_trainer Records
Step_trainerRecords_node1691777946556 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-nanodegree/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Step_trainerRecords_node1691777946556",
)

# Script generated for node Customer curated
Customercurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-nanodegree/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="Customercurated_node1",
)

# Script generated for node Join
Join_node1691778078023 = Join.apply(
    frame1=Customercurated_node1,
    frame2=Step_trainerRecords_node1691777946556,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1691778078023",
)

# Script generated for node Drop Customer Fields
DropCustomerFields_node1691778750399 = DropFields.apply(
    frame=Join_node1691778078023,
    paths=[
        "serialNumber",
        "timeStamp",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
    ],
    transformation_ctx="DropCustomerFields_node1691778750399",
)

# Script generated for node Step trainer trusted
StepTrainerTrusted_node1676578616395 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropCustomerFields_node1691778750399,
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1676578616395",
)

job.commit()
