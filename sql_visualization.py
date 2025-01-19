from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import boto3
from io import BytesIO

# Initialize Spark session
spark = SparkSession.builder.appName("SQLQueryVisualization").getOrCreate()

# Replace with your actual S3 path
s3_path = "s3://outputfinall/finalcsv/run-1701832521370-part-r-00000 (1).csv"

# Define the S3 client object
s3 = boto3.client("s3")

# Define the S3 bucket name
s3_bucket = "cretee"

# Read CSV into Spark DataFrame
df = spark.read.option("header", "true").csv(s3_path)
df.createOrReplaceTempView("my_table")

# ---------------------
# Visualization 1: High-Risk Combinations
# ---------------------
query_combination1 = """
SELECT
    CASE WHEN bmi > 30 AND age < 4 AND diabetes = 2 AND highchol = 1 THEN 'HighRiskCombination1'
         WHEN bmi > 30 AND age BETWEEN 4 AND 6 AND (diabetes = 1 OR diabetes = 2) AND highchol = 1 THEN 'HighRiskCombination2'
         WHEN bmi > 30 AND age > 7 AND diabetes = 0 AND highchol = 0 THEN 'LowRiskCombination'
         ELSE 'Other'
    END AS Combination,
    COUNT(*) AS HeartDiseaseCount
FROM
    my_table
WHERE
    HeartDiseaseorAttack = 1
GROUP BY
    Combination
"""

combination_df1 = spark.sql(query_combination1)
combination_pandas1 = combination_df1.toPandas()

plt.figure(figsize=(10, 6))
ax1 = sns.barplot(x="Combination", y="HeartDiseaseCount", data=combination_pandas1)
plt.title('High-Risk Combinations vs. Heart Disease')
plt.xlabel('Combination')
plt.ylabel('Heart Disease Count')

# Add count numbers on each bar
for p in ax1.patches:
    ax1.annotate(f'{int(p.get_height())}', (p.get_x() + p.get_width() / 2., p.get_height()), ha='center', va='center', xytext=(0, 10), textcoords='offset points')

# Save and upload the visualization
buffer_visualization1 = BytesIO()
plt.savefig(buffer_visualization1, format='png')
buffer_visualization1.seek(0)
s3.upload_fileobj(buffer_visualization1, s3_bucket, "store/high_risk_combinations.png")

# ---------------------
# Visualization 2: Healthy Lifestyle
# ---------------------
query_combination2 = """
SELECT
    CASE WHEN income > 7 AND (fruits = 1 OR veggies = 1) AND anyhealthcare = 1 THEN 'HighIncome&HealthyDiet&Healthcare'
         ELSE 'Other'
    END AS Combination,
    COUNT(*) AS HeartDiseaseCount
FROM
    my_table
WHERE
    HeartDiseaseorAttack = 1
GROUP BY
    Combination
"""

combination_df2 = spark.sql(query_combination2)
combination_pandas2 = combination_df2.toPandas()

plt.figure(figsize=(10, 6))
ax2 = sns.barplot(x="Combination", y="HeartDiseaseCount", data=combination_pandas2)
plt.title('Healthy Lifestyle vs. Heart Disease')
plt.xlabel('Combination')
plt.ylabel('Heart Disease Count')

# Add count numbers on each bar
for p in ax2.patches:
    ax2.annotate(f'{int(p.get_height())}', (p.get_x() + p.get_width() / 2., p.get_height()), ha='center', va='center', xytext=(0, 10), textcoords='offset points')

# Save and upload the visualization
buffer_visualization2 = BytesIO()
plt.savefig(buffer_visualization2, format='png')
buffer_visualization2.seek(0)
s3.upload_fileobj(buffer_visualization2, s3_bucket, "store/healthy_lifestyle.png")

# ---------------------
# Visualization 3: Gender Combo
# ---------------------
query_combination3 = """
SELECT
    CASE WHEN age > 8 AND sex = 1 THEN 'Age>8 && Sex=1'
         WHEN age > 8 AND sex = 0 THEN 'Age>8 && Sex=0'
    END AS Combination,
    SUM(heartdiseaseorattack) AS HeartDiseaseCount
FROM
    my_table
WHERE
    (age > 8 AND sex = 1) OR
    (age > 8 AND sex = 0 AND (highchol = 1 OR highbp = 1 OR smoker = 1 OR hvyalcoholconsump = 1))
GROUP BY
    Combination
"""

combination_df3 = spark.sql(query_combination3)
combination_pandas3 = combination_df3.toPandas()

plt.figure(figsize=(10, 6))
ax3 = sns.barplot(x="Combination", y="HeartDiseaseCount", data=combination_pandas3)
plt.title('Gender Combo vs. Heart Disease')
plt.xlabel('Combinations')
plt.ylabel('Heart Disease Count')

# Add count numbers on each bar
for p in ax3.patches:
    ax3.annotate(f'{int(p.get_height())}', (p.get_x() + p.get_width() / 2., p.get_height()), ha='center', va='center', xytext=(0, 10), textcoords='offset points')

# Save and upload the visualization
buffer_visualization3 = BytesIO()
plt.savefig(buffer_visualization3, format='png')
buffer_visualization3.seek(0)
s3.upload_fileobj(buffer_visualization3, s3_bucket, "store/gender_combo.png")

# Stop Spark session
spark.stop()
