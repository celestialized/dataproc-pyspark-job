#!/usr/bin/env python

"""BigQuery I/O PySpark example."""

from pyspark.sql import SparkSession
from pyspark.sql.functions  import col,current_date,date_sub,lit,datediff,avg,count_distinct,count
from datetime import datetime, timedelta


VDPLIST = [90,91,93,94]
VFSCLIST = [7095,4867,3699]
VGCCLIST = [6907,6956,6974,9173]
VWEEK =  52
ref_date = datetime.today() - timedelta(weeks=VWEEK)
print(ref_date)
print(datetime.today())
spark = SparkSession \
    .builder \
    .appName('spark-bigquery-demo') \
    .getOrCreate()
def read_bq(project,dataset,table):
    data_read = spark.read.format('bigquery') \
        .option("viewsEnabled", "true") \
        .option("parentProject",project) \
        .option("materializationDataset",dataset ) \
        .option("viewMaterializationProject",project) \
        .option("table", table).load()
    return data_read

def calender():
    CALENDAR_DIM = read_bq('wmt-net-strat-and-sim-dev','Perishables_fineline_faas','wmt-edw-prod.US_CORE_DIM_VM.CALENDAR_DIM').select("WM_YR_WK_ID","WM_WEEK_NBR","WM_MONTH_NBR","WM_MONTH_NAME","WM_QTR_NBR","WM_QTR_NAME","CALENDAR_DATE").\
        filter(col("CALENDAR_DATE") >= ref_date).filter(col("CALENDAR_DATE") <= datetime.today())
    CALENDAR_DIM.limit(10).show()
    return CALENDAR_DIM

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "wmt-net-strat-and-sim-dev-export-bucket/reverse_logistics_dev"
spark.conf.set('temporaryGcsBucket', bucket)


#Load data from BigQuery.

INVL  = read_bq('wmt-net-strat-and-sim-dev','Perishables_fineline_faas','wmt-edw-prod.US_WM_VM.DC_INVOICE_LINE')\
    .select("SHIP_TO_STORE_NBR","INVOICE_DATE","INVOICE_NBR","ITEM_NBR","EACH_SHIP_QTY","VNPK_EACH_QTY").\
    withColumn("GDC_OUTBOUND",(col("EACH_SHIP_QTY")/col("VNPK_EACH_QTY"))).filter(col("INVOICE_DATE") >= ref_date)\
    .filter(col("INVOICE_DATE") <= datetime.today())
INV  = read_bq('wmt-net-strat-and-sim-dev','Perishables_fineline_faas','wmt-edw-prod.US_WM_VM.DC_INVOICE').select("SHIP_FROM_DC_NBR","INVOICE_DATE","INVOICE_NBR").filter(~(col("SHIP_FROM_DC_NBR").isin(VFSCLIST))).filter(col("INVOICE_DATE") >= ref_date).filter(col("INVOICE_DATE") <= datetime.today())
MASTER_ITEMS = read_bq('wmt-net-strat-and-sim-dev','Perishables_fineline_faas','wmt-net-strat-and-sim-dev.Perishables_fineline_faas.master_items').select("MDS_FAM_ID","FINELINE_NBR","DEPT_NBR")

CALENDAR_DIM = calender()


joined_data = INVL.join(INV,["INVOICE_NBR","INVOICE_DATE"])\
    .join(CALENDAR_DIM,CALENDAR_DIM.CALENDAR_DATE == INVL.INVOICE_DATE).\
    join(MASTER_ITEMS,MASTER_ITEMS.MDS_FAM_ID  == INVL.ITEM_NBR)

grouped_data = joined_data.groupBy("SHIP_FROM_DC_NBR","SHIP_TO_STORE_NBR","FINELINE_NBR","DEPT_NBR","WM_YR_WK_ID","WM_WEEK_NBR","WM_MONTH_NBR","WM_MONTH_NAME","WM_QTR_NBR","WM_QTR_NAME")\
    .agg(count(col("GDC_OUTBOUND")).alias("GDC_OUTBOUND_QTY"),count_distinct(col("INVOICE_DATE")).alias("GDC_STORE_REPLENISHMENT_FREQUENCY"))

grouped_data.write.format('bigquery') \
    .mode("overwrite") \
    .option('table', 'wmt-net-strat-and-sim-dev.Perishables_fineline_faas.GDC_STR') \
    .save()

