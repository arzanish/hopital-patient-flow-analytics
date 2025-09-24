from pyspark.sql.functions import *

# Azure Event Hub Configuration
event_hub_namespace = "hospital-analytics-namespace-ayn.servicebus.windows.net"
event_hub_name = "hospital-analytics-eh"  
event_hub_conn_str = dbutils.secrets.get(scope= 'hospitalanalyticsvaultscope' , key='eventhub-connection')

kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

# Read from Event Hub
raw_df = (spark.readStream
          .format('kafka')
          .options(**kafka_options)
          .load()
          )

# Casting data to JSON 
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

# ADLS configuration (no JVM call)
storage_account_name = "hospitalstorage"
storage_key = dbutils.secrets.get(scope="hospitalanalyticsvaultscope", key="storage-connection")

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_key)

bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/patient_flow"

# Write Stream to Storage (bronze)
(
    json_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation","dbfs:/mnt/bronze/_checkpoints/patient_flow")
    .start(bronze_path)
)
