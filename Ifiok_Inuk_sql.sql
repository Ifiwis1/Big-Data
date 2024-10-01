-- Databricks notebook source
-- MAGIC %python
-- MAGIC #Define function to create RDD from csv files
-- MAGIC def createRDD(file_csv):
-- MAGIC     #Use the sc.textFile() function to create a RDD from the file
-- MAGIC     RDD = sc.textFile(f"/FileStore/tables/{file_csv}.csv")
-- MAGIC     return RDD
-- MAGIC #Execute function to create the RDDs for the clinicaltrial_2023.csv file
-- MAGIC RDD1 = createRDD("clinicaltrial_2023")
-- MAGIC #Output the result
-- MAGIC RDD1.take(4)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Execute function to create the RDD for the pharma.csv file
-- MAGIC RDD2 = createRDD("pharma")
-- MAGIC #Output the result
-- MAGIC RDD2.take(4)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Clean the clinical trial file
-- MAGIC # Function to clean file
-- MAGIC def cleanFile(row):
-- MAGIC     # strip()method is used to remove leading and trailing commas and double quotes
-- MAGIC     cleanFile = row.strip(',').strip('"')
-- MAGIC     # For this file, the delimiter is \t, so each record was splitted using the split() method
-- MAGIC     cleanFile = cleanFile.split('\t')
-- MAGIC     return cleanFile
-- MAGIC RDD1 = RDD1.map(cleanFile)
-- MAGIC RDD1.take(10)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Data Cleaning
-- MAGIC #Fill up empty cells with empty strings in the required cells
-- MAGIC def fillUpCell(cell):
-- MAGIC     newCell = 14 - len(cell)
-- MAGIC     if len(cell) < 14:
-- MAGIC         cell += [''] * newCell
-- MAGIC     return cell
-- MAGIC
-- MAGIC #Format date to have the same format-YYYY-MM-DD,while filling up incomplete dates with '-01
-- MAGIC def FormatDate(date):
-- MAGIC     if len(date) != 10:
-- MAGIC         date = date + '-01'
-- MAGIC     return date
-- MAGIC
-- MAGIC # Execute the functions and map them to the RDD1 to clean the RDD
-- MAGIC clinicaltrial_RDD = RDD1.map(lambda row: [FormatDate(row[index]) if index in [12, 13] else row[index] for index in range(len(row))])\
-- MAGIC                         .map(fillUpCell)
-- MAGIC #Return the cleaned RDD
-- MAGIC
-- MAGIC clinicaltrial_RDD.take(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Cleaning the pharma data 
-- MAGIC def cleanPharma(row):
-- MAGIC     # strip()method is used to remove leading and trailing commas and double quotes
-- MAGIC     # For this file, the delimiter is ',', so each record was splitted using the split() method
-- MAGIC     pharmaClean = row.replace('"', '')\
-- MAGIC                      .split(',')
-- MAGIC     return pharmaClean
-- MAGIC
-- MAGIC #Execute the function on RDD2
-- MAGIC pharma_RDD = RDD2.map(cleanPharma)
-- MAGIC
-- MAGIC # Extract the Parent_Company column
-- MAGIC pharma_RDD = pharma_RDD.map(lambda row: [row[1]])
-- MAGIC
-- MAGIC # Show the first 10 rows after removing extra double quotes
-- MAGIC pharma_RDD.take(20)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##Convert the Clinical trial RDD to DF
-- MAGIC from pyspark.sql.types import *
-- MAGIC #Extract the first row of the RDD as the header
-- MAGIC header = clinicaltrial_RDD.first()
-- MAGIC
-- MAGIC # Remove header row from the RDD
-- MAGIC clinicaltrial_RDD = clinicaltrial_RDD.filter(lambda row: row != header)
-- MAGIC #Convert the RDD to DF
-- MAGIC Clinicaltrial_DF = clinicaltrial_RDD.toDF(header)
-- MAGIC Clinicaltrial_DF.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##Convert the Pharma RDD to DF
-- MAGIC #Extract the first row of the RDD as the header
-- MAGIC header2 = pharma_RDD.first()
-- MAGIC
-- MAGIC # Remove header row from the RDD
-- MAGIC pharma_RDD = pharma_RDD.filter(lambda row: row != header2)
-- MAGIC #Convert the RDD to DF
-- MAGIC pharma_DF = pharma_RDD.toDF(header2)
-- MAGIC pharma_DF.show(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Create a temporary view for the pharmaDF
-- MAGIC pharma_DF.createOrReplaceTempView("pharmaView")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Create a temporary view for the clinicaltrialDF
-- MAGIC Clinicaltrial_DF.createOrReplaceTempView("clinicaltrialView")

-- COMMAND ----------

--Problem Statement 1 --Number of distinct studies in the dataset
SELECT DISTINCT COUNT(*) AS Distinct_Studies
FROM clinicaltrialView;

-- COMMAND ----------

--Problem Statement 2---List all the types of studies with their frequencies
--Select and rename the columns
SELECT Type AS Types_of_Study , COUNT(Type) AS Frequency
FROM clinicaltrialView
--Filter for results that the Type column is not empty
WHERE Type != ''
--Group by the Type of study
GROUP BY Type
--Order the resulting table by the frequency in descending order
ORDER BY Frequency DESC;

-- COMMAND ----------

--Problem Statement 3---Top 5 conditions with their frequencies
--Select and rename the columns
SELECT c.Condition AS SplitConditions, COUNT(*) AS Frequency
--Subquery to get the table of splitted conditions (split on |)
FROM (SELECT explode(split(Conditions, "\\|")) AS Condition
      FROM clinicaltrialView) AS c
--Group by the condition
GROUP BY c.Condition
--Order the resulting table by the frequency in descending order
ORDER BY Frequency DESC
--Limit result to top 5
LIMIT 5;

-- COMMAND ----------

--Problem Statement 4--- Top 10 most common non-pharma sponsors with the number of clinical trials they have sponsored
--Select and rename the columns
SELECT Sponsor AS Non_pharma_Sponsor, COUNT(*) AS No_of_Trials
FROM clinicaltrialView
--Filter using NOT IN and a subquery of the list of parent companies to get the non-pharma sponsors
WHERE Sponsor NOT IN 
      (SELECT DISTINCT Parent_Company FROM pharmaView)
--Group by the sponsor
GROUP BY Sponsor
--Order the resulting table by the trials in descending order
ORDER BY No_of_Trials DESC
--Limit result to top 10
LIMIT 10;

-- COMMAND ----------

--Problem Statement 5 ---- Number of Completed Studies for each month in 2023
--Select and rename the columns.Format the date to get the completion month
SELECT DATE_FORMAT(TO_DATE(Completion, 'yyyy-MM-dd'), 'MMM') AS Month_Completed, COUNT(*) AS No_of_Completed_Studies
FROM clinicaltrialView
--Filter the result for rows with Completion year as 2023 and Completed Status
WHERE Completion LIKE '2023%' AND Status = 'COMPLETED'
--Group the result by the Month_Completed
GROUP BY Month_Completed
--Order the resulting table by the month using CASE..WHEN..THEN
ORDER BY
  CASE WHEN Month_Completed LIKE 'Jan%' THEN 1
      WHEN Month_Completed LIKE 'Feb%' THEN 2
      WHEN Month_Completed LIKE 'Mar%' THEN 3
      WHEN Month_Completed LIKE 'Apr%' THEN 4
      WHEN Month_Completed LIKE 'May%' THEN 5
      WHEN Month_Completed LIKE 'Jun%' THEN 6
      WHEN Month_Completed LIKE 'Jul%' THEN 7
      WHEN Month_Completed LIKE 'Aug%' THEN 8
      WHEN Month_Completed LIKE 'Sep%' THEN 9
      WHEN Month_Completed LIKE 'Oct%' THEN 10
      WHEN Month_Completed LIKE 'Nov%' THEN 11
      WHEN Month_Completed LIKE 'Dec%' THEN 12
  END;


-- COMMAND ----------

--Further Analysis #3----Number of clinical trials terminated by Pharmaceutical sponsors, starting with the highest, limit to Top 10
--Select and rename the columns.
SELECT Sponsor AS Pharmaceutical_Sponsors, COUNT(*) AS Number_of_terminated_trials
FROM clinicaltrialView
--Filter the result for rows sponsor in subquery that return the pharma sponsors and terminated Status
WHERE Sponsor IN (SELECT * FROM pharmaView) AND Status = "TERMINATED"
--Group the result by the Sponsor
GROUP BY Sponsor
--Order the resulting table by the Number_of_terminated_trials in descending order
ORDER BY Number_of_terminated_trials DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Visualization for Further Analysis 3
-- MAGIC pharma_sponsor_terminated_trials_df = spark.sql("""
-- MAGIC     SELECT Sponsor AS Pharmaceutical_Sponsors, COUNT(*) AS Number_of_terminated_trials
-- MAGIC     FROM clinicaltrialView
-- MAGIC     WHERE Sponsor IN (SELECT * FROM pharmaView) AND Status = "TERMINATED"
-- MAGIC     GROUP BY Sponsor
-- MAGIC     ORDER BY Number_of_terminated_trials DESC
-- MAGIC     LIMIT 10
-- MAGIC """)
-- MAGIC #Save the DF in csv format
-- MAGIC pharma_sponsor_terminated_trials_df.write.csv("/FileStore/tables/pharma_sponsor_terminated_trials_df.csv", header=True, mode="overwrite")
-- MAGIC pharma_sponsor_terminated_trials_df.show()

-- COMMAND ----------

--Create the table for the export to PowerBi
drop table if exists Sponsor_trial;
create table Sponsor_trial
--Save csv as the teable created
using csv
options (path "dbfs:/FileStore/tables/pharma_sponsor_terminated_trials_df.csv", header="False", inferSchema = "True")
