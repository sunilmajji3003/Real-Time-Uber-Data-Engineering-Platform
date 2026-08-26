# Databricks notebook source
import pandas as pd
files=[
    {"file":"map_cities"},
    {"file":"map_cancellation_reasons"},
    {"file":"bulk_rides"},
    {"file":"map_payment_methods"},
    {"file":"map_ride_statuses"},
    {"file":"map_vehicle_makes"},
    {"file":"map_vehicle_types"}
    ]

for file in files:
   url= f"https://storageuberproject.blob.core.windows.net/raw/sink/{file['file']}.json?sp=r&st=2026-08-25T11:24:42Z&se=2026-08-25T19:39:42Z&spr=https&sv=2026-02-06&sr=c&sig=6uMz5pzhbMHMONUWcBn1l7kYEz8HIXq4S%2B6s%2FKn3TVE%3D"

   df=pd.read_json(url)
   df_spark=spark.createDataFrame(df)
   df_spark.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(f"uberproject.bronze.{file['file']}")