import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col 

    dyf=dfc.select(list(dfc.keys())[0])

    df=dyf.toDF()

    df=df.withColumn("Total_Value",
                     col("Price") * col("Quantity"))
                     
                     
    result_dyf=DynamicFrame.fromDF(df,glueContext,"result_dyf")

    return DynamicFrameCollection({"result":result_dyf},glueContext)
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1794294332265 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://anshpractice/raw/"], "recurse": True}, transformation_ctx="AmazonS3_node1794294332265")

# Script generated for node Drop Duplicates
DropDuplicates_node1794294594101 =  DynamicFrame.fromDF(AmazonS3_node1794294332265.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1794294594101")

# Script generated for node Change Schema
ChangeSchema_node1794294612655 = ApplyMapping.apply(frame=DropDuplicates_node1794294594101, mappings=[("orderid", "string", "orderid", "string"), ("customer", "string", "customer", "string"), ("item", "string", "item", "string"), ("quantity", "string", "quantity", "bigint"), ("price", "string", "price", "double"), ("orderdate", "string", "orderdate", "string"), ("cancelled_flag", "string", "cancelled_flag", "string")], transformation_ctx="ChangeSchema_node1794294612655")

# Script generated for node Custom Transform
CustomTransform_node1794294650799 = MyTransform(glueContext, DynamicFrameCollection({"ChangeSchema_node1794294612655": ChangeSchema_node1794294612655}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1794295717642 = SelectFromCollection.apply(dfc=CustomTransform_node1794294650799, key=list(CustomTransform_node1794294650799.keys())[0], transformation_ctx="SelectFromCollection_node1794295717642")

# Script generated for node SQL Query
SqlQuery745 = '''
SELECT * FROM myDataSource
WHERE Total_Value > 1000;
'''
SQLQuery_node1794295424781 = sparkSqlQuery(glueContext, query = SqlQuery745, mapping = {"myDataSource":SelectFromCollection_node1794295717642}, transformation_ctx = "SQLQuery_node1794295424781")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1794426568434_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [ColumnCount = 8,ColumnValues "Total_Value" > 1000
    ]
"""

EvaluateDataQuality_node1794426568434 = EvaluateDataQuality().process_rows(frame=SQLQuery_node1794295424781, ruleset=EvaluateDataQuality_node1794426568434_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794426568434", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node Aggregate
Aggregate_node1794422920122 = sparkAggregate(glueContext, parentFrame = SQLQuery_node1794295424781, groups = ["item"], aggs = [["Total_Value", "sum"]], transformation_ctx = "Aggregate_node1794422920122")

# Script generated for node Select From Collection
SelectFromCollection_node1794427015801 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1794426568434, key="Select From Collection", transformation_ctx="SelectFromCollection_node1794427015801")

# Script generated for node Rename Field
RenameField_node1794423057220 = RenameField.apply(frame=Aggregate_node1794422920122, old_name="`sum(Total_Value)`", new_name="Total_value", transformation_ctx="RenameField_node1794423057220")

# Script generated for node Amazon S3
AmazonS3_node1794422678803 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1794427015801, connection_type="s3", format="glueparquet", connection_options={"path": "s3://anshpractice/sink/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1794422678803")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RenameField_node1794423057220, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1794422608764", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
additional_options = {"path": "s3://anshpractice/sinkagg/", "write.parquet.compression-codec": "snappy"}
AmazonS3_node1794422933414_df = RenameField_node1794423057220.toDF()
AmazonS3_node1794422933414_df.write.format("delta").options(**additional_options).mode("append").save()

job.commit()