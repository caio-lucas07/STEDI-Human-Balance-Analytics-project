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

# Script generated for node Step trainer trusted
Steptrainertrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="Steptrainertrusted_node1",
)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1691789608063 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometertrusted_node1691789608063",
)

# Script generated for node Join
Join_node1691789613633 = Join.apply(
    frame1=Steptrainertrusted_node1,
    frame2=Accelerometertrusted_node1691789608063,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1691789613633",
)

# Script generated for node Machine learning curated
StepTrainerTrusted_node1676578616395 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1676402768067,
    database="stedi",
    table_name="machine_learning_curated",
    transformation_ctx="StepTrainerTrusted_node1676578616395",
)

job.commit()
