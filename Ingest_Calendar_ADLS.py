# Databricks notebook source
cal_df = spark.read.format("csv").option("header",True).load("abfss://inbounddatacontainer@airbnbstorageaccountjv.dfs.core.windows.net/boston/03242024/calendar.csv.gz (1)")
display(cal_df)