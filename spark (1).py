from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import boto3
from io import BytesIO

# Create a Spark session
spark = SparkSession.builder.appName("HeartDiseaseVisualization").getOrCreate()

# Specify your S3 path
s3_path = "s3://outputfinall/finalcsv/run-1701832521370-part-r-00000 (1).csv"

# Read the CSV file from S3 into a PySpark DataFrame
df = spark.read.option("header", "true").csv(s3_path)

# Convert PySpark DataFrame to Pandas DataFrame for visualization
df_pandas = df.toPandas()

# Convert 'heartdiseaseorattack' column to numeric
df_pandas['heartdiseaseorattack'] = pd.to_numeric(df_pandas['heartdiseaseorattack'], errors='coerce')

# Set up the figure and axes dynamically based on the number of columns
num_cols = len(df_pandas.columns[1:])
num_rows = (num_cols - 1) // 3 + 1
fig, axes = plt.subplots(nrows=num_rows, ncols=3, figsize=(15, 5 * num_rows))
fig.suptitle('Relationships between Features and Heart Disease (Count Plots)', fontsize=16)

# Create detailed subplots for count plots
for i, column in enumerate(df_pandas.columns[1:]):
    row = i // 3
    col = i % 3

    if df_pandas[column].dtype == 'int64':
        sns.countplot(x=column, hue='heartdiseaseorattack', data=df_pandas, ax=axes[row, col])
    else:
        sns.countplot(x=column, hue='heartdiseaseorattack', data=df_pandas, ax=axes[row, col])
    
    axes[row, col].set_title(column, fontsize=14)

# Adjust layout
plt.tight_layout(rect=[0, 0, 1, 0.96])

# Save the count plots to a BytesIO buffer
buffer_count_plots = BytesIO()
plt.savefig(buffer_count_plots, format='png')
buffer_count_plots.seek(0)

# Corrected S3 bucket name (without "s3://" prefix and trailing slash)
s3_bucket = "cretee"

# Specify the folder structure within the bucket for the key
s3_count_plots_key = "store/image1.png"

# Create an S3 client
s3 = boto3.client("s3")

# Upload the count plot image to S3
s3.upload_fileobj(buffer_count_plots, s3_bucket, s3_count_plots_key)

# Set up a new figure for box plots
fig_box, axes_box = plt.subplots(nrows=num_rows, ncols=3, figsize=(15, 5 * num_rows))
fig_box.suptitle('Relationships between Features and Heart Disease (Box Plots)', fontsize=16)

# Create detailed subplots for box plots
for i, column in enumerate(df_pandas.columns[1:]):
    row = i // 3
    col = i % 3

    if df_pandas[column].dtype == 'int64':
        sns.boxplot(x='heartdiseaseorattack', y=column, data=df_pandas, ax=axes_box[row, col])
    else:
        sns.boxplot(x='heartdiseaseorattack', y=column, data=df_pandas, ax=axes_box[row, col])
    
    axes_box[row, col].set_title(column, fontsize=14)

# Adjust layout for box plots
plt.tight_layout(rect=[0, 0, 1, 0.96])

# Save the box plots to a BytesIO buffer
buffer_box_plots = BytesIO()
plt.savefig(buffer_box_plots, format='png')
buffer_box_plots.seek(0)

# Specify the folder structure within the bucket for the key
s3_box_plots_key = "store/image2.png"

# Upload the box plot image to S3
s3.upload_fileobj(buffer_box_plots, s3_bucket, s3_box_plots_key)

# Stop the Spark session
spark.stop()
