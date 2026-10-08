# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Data Reading Json

# COMMAND ----------

# DBTITLE 1,Data Reading Json
df_json = spark.read.table("workspace.default.drivers")

# COMMAND ----------

df_json.display()

# COMMAND ----------

dbutils.fs.ls("/")
# It gives all files present in that path

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading 

# COMMAND ----------

# DBTITLE 1,Data Reading
df = spark.read.table("workspace.default.big_mart_sales")

# COMMAND ----------

# Reading delta path in Scala
var df=spark.read.format("delta").load("/FileStore/tables/delta")

# Reading delta path in sql
select * from delta.`/FileStore/tables/delta`

# COMMAND ----------

# Reading parquet path in Scala
var df=spark.read.format("delta").load("/FileStore/tables/delta")

# Reading parquet path in sql
select * from parquet.`/FileStore/tables/delta`

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Defination

# COMMAND ----------

# DBTITLE 1,Schema Defination
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading CSV - .option methods

# COMMAND ----------

# DBTITLE 1,Loading CSV - .option methods


# COMMAND ----------

# MAGIC %md
# MAGIC # Common Spark `.option()` Variations
# MAGIC
# MAGIC | Option                   | Example                                            | Purpose                         |
# MAGIC | ------------------------ | -------------------------------------------------- | ------------------------------- |
# MAGIC | header                   | `.option("header","true")`                         | First row contains column names |
# MAGIC | inferSchema              | `.option("inferSchema","true")`                    | Automatically detect data types |
# MAGIC | delimiter / sep          | `.option("delimiter", ",")`                        | Column separator                |
# MAGIC | quote                    | `.option("quote", "\"")`                           | Quote character                 |
# MAGIC | escape                   | `.option("escape", "\"")`                          | Escape special characters       |
# MAGIC | nullValue                | `.option("nullValue","NULL")`                      | Treat value as NULL             |
# MAGIC | mode                     | `.option("mode","PERMISSIVE")`                     | Handle corrupt records          |
# MAGIC | encoding                 | `.option("encoding","UTF-8")`                      | File encoding                   |
# MAGIC | multiline                | `.option("multiline","true")`                      | Read multiline records          |
# MAGIC | dateFormat               | `.option("dateFormat","yyyy-MM-dd")`               | Date format                     |
# MAGIC | timestampFormat          | `.option("timestampFormat","yyyy-MM-dd HH:mm:ss")` | Timestamp format                |
# MAGIC | ignoreLeadingWhiteSpace  | `.option("ignoreLeadingWhiteSpace","true")`        | Ignore leading spaces           |
# MAGIC | ignoreTrailingWhiteSpace | `.option("ignoreTrailingWhiteSpace","true")`       | Ignore trailing spaces          |
# MAGIC | maxColumns               | `.option("maxColumns","5000")`                     | Maximum columns allowed         |
# MAGIC | maxCharsPerColumn        | `.option("maxCharsPerColumn","100000")`            | Maximum characters per column   |
# MAGIC | badRecordsPath           | `.option("badRecordsPath","/tmp/badrecords")`      | Store malformed records         |
# MAGIC | pathGlobFilter           | `.option("pathGlobFilter","*.csv")`                | Read matching files only        |
# MAGIC | recursiveFileLookup      | `.option("recursiveFileLookup","true")`            | Read subdirectories recursively |
# MAGIC
# MAGIC ## Example Usage
# MAGIC
# MAGIC ```scala
# MAGIC val df = spark.read
# MAGIC   .option("header", "true")
# MAGIC   .option("inferSchema", "true")
# MAGIC   .option("delimiter", ",")
# MAGIC   .option("nullValue", "NULL")
# MAGIC   .option("mode", "PERMISSIVE")
# MAGIC   .csv("/FileStore/data.csv")
# MAGIC ```
# MAGIC

# COMMAND ----------

# DBTITLE 1,DBUTILS COMMANDS


# COMMAND ----------

# MAGIC %md
# MAGIC # Complete Databricks `dbutils` Cheat Sheet
# MAGIC
# MAGIC `dbutils` is a utility package provided by Databricks for interacting with the file system, notebooks, widgets, secrets, jobs, libraries, and credentials.
# MAGIC
# MAGIC ## 1. dbutils.fs (File System Utilities)
# MAGIC
# MAGIC | Command                      | Purpose                      |
# MAGIC | ---------------------------- | ---------------------------- |
# MAGIC | `dbutils.fs.ls(path)`        | List files and directories   |
# MAGIC | `dbutils.fs.cp(src, dst)`    | Copy files                   |
# MAGIC | `dbutils.fs.mv(src, dst)`    | Move/Rename files            |
# MAGIC | `dbutils.fs.rm(path)`        | Delete file                  |
# MAGIC | `dbutils.fs.rm(path, true)`  | Delete directory recursively |
# MAGIC | `dbutils.fs.mkdirs(path)`    | Create directory             |
# MAGIC | `dbutils.fs.head(path)`      | Read beginning of file       |
# MAGIC | `dbutils.fs.put(path, data)` | Write text file              |
# MAGIC | `dbutils.fs.mounts()`        | List mounts                  |
# MAGIC | `dbutils.fs.refreshMounts()` | Refresh mount cache          |
# MAGIC | `dbutils.fs.mount()`         | Create mount point           |
# MAGIC | `dbutils.fs.unmount()`       | Remove mount point           |
# MAGIC | `dbutils.fs.help()`          | Show help                    |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. dbutils.widgets (Notebook Parameters)
# MAGIC
# MAGIC | Command                         | Purpose                    |
# MAGIC | ------------------------------- | -------------------------- |
# MAGIC | `dbutils.widgets.text()`        | Create text widget         |
# MAGIC | `dbutils.widgets.dropdown()`    | Create dropdown            |
# MAGIC | `dbutils.widgets.combobox()`    | Create combo box           |
# MAGIC | `dbutils.widgets.multiselect()` | Create multi-select widget |
# MAGIC | `dbutils.widgets.get()`         | Read widget value          |
# MAGIC | `dbutils.widgets.getAll()`      | Get all widget values      |
# MAGIC | `dbutils.widgets.remove()`      | Remove widget              |
# MAGIC | `dbutils.widgets.removeAll()`   | Remove all widgets         |
# MAGIC
# MAGIC ### Example
# MAGIC
# MAGIC ```scala id="4f9s2a"
# MAGIC dbutils.widgets.text("env","dev","Environment")
# MAGIC
# MAGIC val env = dbutils.widgets.get("env")
# MAGIC println(env)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. dbutils.secrets (Secret Management)
# MAGIC
# MAGIC | Command                        | Purpose               |
# MAGIC | ------------------------------ | --------------------- |
# MAGIC | `dbutils.secrets.get()`        | Retrieve secret value |
# MAGIC | `dbutils.secrets.list()`       | List secrets in scope |
# MAGIC | `dbutils.secrets.listScopes()` | List secret scopes    |
# MAGIC | `dbutils.secrets.help()`       | Show help             |
# MAGIC
# MAGIC ### Example
# MAGIC
# MAGIC ```scala id="x8q3p1"
# MAGIC val pwd = dbutils.secrets.get(
# MAGIC   scope="jdbc-scope",
# MAGIC   key="password"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. dbutils.notebook (Notebook Utilities)
# MAGIC
# MAGIC | Command                   | Purpose                        |
# MAGIC | ------------------------- | ------------------------------ |
# MAGIC | `dbutils.notebook.run()`  | Execute another notebook       |
# MAGIC | `dbutils.notebook.exit()` | Return value and exit notebook |
# MAGIC | `dbutils.notebook.help()` | Show help                      |
# MAGIC
# MAGIC ### Example
# MAGIC
# MAGIC ```scala id="7q4mke"
# MAGIC val result = dbutils.notebook.run(
# MAGIC   "/Shared/ChildNotebook",
# MAGIC   300
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. dbutils.jobs (Job Utilities)
# MAGIC
# MAGIC | Command                         | Purpose         |
# MAGIC | ------------------------------- | --------------- |
# MAGIC | `dbutils.jobs.taskValues.set()` | Set task value  |
# MAGIC | `dbutils.jobs.taskValues.get()` | Read task value |
# MAGIC | `dbutils.jobs.help()`           | Show help       |
# MAGIC
# MAGIC ### Example
# MAGIC
# MAGIC ```scala id="p4e7zt"
# MAGIC dbutils.jobs.taskValues.set(
# MAGIC   key="recordCount",
# MAGIC   value="1000"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6. dbutils.library (Library Utilities)
# MAGIC
# MAGIC > Mostly used in older Databricks runtimes.
# MAGIC
# MAGIC | Command                           | Purpose                |
# MAGIC | --------------------------------- | ---------------------- |
# MAGIC | `dbutils.library.install()`       | Install library        |
# MAGIC | `dbutils.library.uninstall()`     | Uninstall library      |
# MAGIC | `dbutils.library.restartPython()` | Restart Python session |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7. dbutils.credentials (Cloud Credentials)
# MAGIC
# MAGIC Available on some cloud environments.
# MAGIC
# MAGIC | Command                                 | Purpose           |
# MAGIC | --------------------------------------- | ----------------- |
# MAGIC | `dbutils.credentials.showCurrentRole()` | Show current role |
# MAGIC | `dbutils.credentials.assumeRole()`      | Assume IAM role   |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Discover Available Commands
# MAGIC
# MAGIC ```scala id="9z7g6b"
# MAGIC dbutils.help()
# MAGIC
# MAGIC dbutils.fs.help()
# MAGIC dbutils.widgets.help()
# MAGIC dbutils.secrets.help()
# MAGIC dbutils.notebook.help()
# MAGIC dbutils.jobs.help()
# MAGIC dbutils.library.help(
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Most Frequently Used in Real Projects
# MAGIC
# MAGIC ### File Handling
# MAGIC
# MAGIC ```scala id="k3d1hf"
# MAGIC dbutils.fs.ls()
# MAGIC dbutils.fs.cp()
# MAGIC dbutils.fs.mv()
# MAGIC dbutils.fs.rm()
# MAGIC dbutils.fs.mkdirs()
# MAGIC ```
# MAGIC
# MAGIC ### Parameterization
# MAGIC
# MAGIC ```scala id="n5u2xs"
# MAGIC dbutils.widgets.text()
# MAGIC dbutils.widgets.get()
# MAGIC ```
# MAGIC
# MAGIC ### Secure Credentials
# MAGIC
# MAGIC ```scala id="v8c6am"
# MAGIC dbutils.secrets.get()
# MAGIC ```
# MAGIC
# MAGIC ### Workflow Orchestration
# MAGIC
# MAGIC ```scala id="r2t9jw"
# MAGIC dbutils.notebook.run()
# MAGIC dbutils.notebook.exit()
# MAGIC ```
# MAGIC
# MAGIC ### Job Communication
# MAGIC
# MAGIC ```scala id="m1p4qr"
# MAGIC dbutils.jobs.taskValues.set()
# MAGIC dbutils.jobs.taskValues.get()
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL Schema 

# COMMAND ----------

# DBTITLE 1,DDL Schema
df = spark.sql("""
    SELECT 
        Item_Identifier,
        CAST(Item_Weight AS STRING) AS Item_Weight,
        Item_Fat_Content,
        Item_Visibility,
        Item_Type,
        Item_MRP,
        Outlet_Identifier,
        Outlet_Establishment_Year,
        Outlet_Size,
        Outlet_Location_Type,
        Outlet_Type,
        Item_Outlet_Sales
    FROM workspace.default.big_mart_sales
""")

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

my_strct_schema = StructType([ StructField('Item_Identifier',StringType(),True), 
                              StructField('Item_Weight',StringType(),True), 
                              StructField('Item_Fat_Content',StringType(),True), 
                              StructField('Item_Visibility',StringType(),True), 
                              StructField('Item_MRP',StringType(),True), 
                              StructField('Outlet_Identifier',StringType(),True), 
                              StructField('Outlet_Establishment_Year',StringType(),True), 
                              StructField('Outlet_Size',StringType(),True), 
                              StructField('Outlet_Location_Type',StringType(),True), 
                              StructField('Outlet_Type',StringType(),True),
                               StructField('Item_Outlet_Sales',StringType(),True)])

# COMMAND ----------

df = spark.read.format('csv')
.schema(my_strct_schema)
.option('header',True)
.load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Defination
# MAGIC
# MAGIC DDL schema is a string-based schema definition, while StructType is Spark's object-based schema definition that provides more flexibility and supports complex nested structures.
# MAGIC # DDL vs StructType
# MAGIC
# MAGIC | Feature                   | DDL                    | StructType               |
# MAGIC | ------------------------- | ---------------------- | ------------------------ |
# MAGIC | Syntax                    | String                 | Scala object             |
# MAGIC | Easy to write             | ✅                      | ❌                        |
# MAGIC | Readability               | High for small schemas | Better for large schemas |
# MAGIC | Nested fields             | Limited                | ✅ Excellent              |
# MAGIC | Production usage          | Sometimes              | ✅ Common                 |
# MAGIC | Dynamic schema generation | Difficult              | Easy                     |
# MAGIC
# MAGIC ## DDL Example
# MAGIC
# MAGIC ```scala
# MAGIC val ddlSchema =
# MAGIC   "id INT, name STRING, salary DOUBLE"
# MAGIC
# MAGIC val df = spark.read
# MAGIC   .schema(ddlSchema)
# MAGIC   .csv("/FileStore/data.csv")
# MAGIC ```
# MAGIC
# MAGIC ## StructType Example
# MAGIC
# MAGIC ```scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC
# MAGIC val schema = StructType(Array(
# MAGIC   StructField("id", IntegerType, true),
# MAGIC   StructField("name", StringType, true),
# MAGIC   StructField("salary", DoubleType, true)
# MAGIC ))
# MAGIC
# MAGIC val df = spark.read
# MAGIC   .schema(schema)
# MAGIC   .csv("/FileStore/data.csv")
# MAGIC ```
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT

# COMMAND ----------

# DBTITLE 1,SELECT
df = spark.read.table("workspace.default.big_mart_sales")
display(df)

# COMMAND ----------

df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Visibility')).display()

# In Scala
# df.selectExpr("Item_Identifier")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

# DBTITLE 1,ALIAS
df.select(col('Item_Identifier').alias('Item_Id')).display()

# In Scala
# df.selectExpr("Item_Identifier AS Item_Id")
# or df.selectExpr("Item_Identifier").alias("Item_Id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILTER
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1

# COMMAND ----------

# DBTITLE 1,FILTER
df = spark.read.table("workspace.default.big_mart_sales")
display(df)

# COMMAND ----------

df.filter(col("Item_Fat_Content")=='Regular').display()

#In scala
# df.filter("Item_Fat_Content = 'Regular'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2

# COMMAND ----------

df.filter((col("Item_Fat_Content")=='Regular') & (col('Item_Weight')>10)).display()

# In scala
# df.filter("Item_Fat_Content = 'Regular' AND Item_Weight > 10").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3
# MAGIC

# COMMAND ----------

df.filter((col('outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# In Scala
# df.filter(
#  (col("Outlet_Size").isNull) &&
#  (col("Outlet_Location_Type").isin("Tier 1", "Tier 2"))
# ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WithColumnRenamed

# COMMAND ----------

# MAGIC %md
# MAGIC It is used to change the name of the column , Inside function first it takes existing column name after it the name you want to give to that column .

# COMMAND ----------

# DBTITLE 1,WithColumnRenamed
df.withColumnRenamed("Item_Weight","Item_Wt").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WithColumn 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 1

# COMMAND ----------

# DBTITLE 1,WithColumn
df=df.withColumn("flag",lit("new"))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2

# COMMAND ----------

df.withColumn("multiply",col("Item_Weight")*col("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3

# COMMAND ----------

df.withColumn("Item_Fat_Content",regexp_replace(col("Item_Fat_Content"),"Regular","Reg"))\
.withColumn("Item_Fat_Content",regexp_replace(col("Item_Fat_Content"),"Low Fat","Lf")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type Casting 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Type Casting
from pyspark.sql.functions import *
from pyspark.sql.types import *
df=df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))

# In sql
# CAST(Item_Weight AS STRING) AS Item_Weight


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sort/Orderby

# COMMAND ----------

# DBTITLE 1,Sort/Orderby
# Descending
df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# Ascending 
df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# Sorting both Asc and Desc
df.sort(["Item_Weight","Item_Visibility"],ascending=[0,0]).display()

# In SQL 
# SELECT *
# FROM df
# ORDER BY Item_Weight DESC,
#          Item_Visibility DESC;

# COMMAND ----------

df.sort(["Item_Weight","Item_Visibility"],ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIMIT

# COMMAND ----------

# DBTITLE 1,LIMIT
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP - 
# MAGIC In PySpark, drop() is used to remove one or more columns from a DataFrame.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,DROP 
# Drop one column
df.drop('Item_Visibility').display()

# COMMAND ----------

# Drop multiple columns
df.drop("Item_Visibility","Item_Weight").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Duplicates
# MAGIC

# COMMAND ----------

# DBTITLE 1,Drop Duplicates
# Scenario 1
df.dropDuplicates().display()

# COMMAND ----------

# Scenario 2 - It removes duplicate rows based only on the Item_Type column. For each unique Item_Type, Spark keeps one row and removes the   rest.
df.drop_duplicates(["Item_Type"]).display()

# COMMAND ----------

# OR just do
df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION and UNION BYNAME
# MAGIC
# MAGIC  Preaparing Dataframes

# COMMAND ----------

# DBTITLE 1,UNION and UNION BYNAME
data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION -
# MAGIC
# MAGIC Union is used to combine rows from two DataFrames having the same schema (same number of columns and compatible data types).
# MAGIC It works like stacking one DataFrame below another.

# COMMAND ----------

# DBTITLE 1,UNION
df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION BY NAME

# COMMAND ----------

# DBTITLE 1,UNION BY NAME
# unionByName() combines two DataFrames by matching columns using column names, not their positions.

# Why it is needed

# union() matches columns based on position.

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### String Function

# COMMAND ----------

# DBTITLE 1,String Function
# 1) INITCAP - initcap() converts the first letter of each word to uppercase and the remaining letters to lowercase.
df.select(initcap('Item_Type').alias("Item_Type")).display()

# COMMAND ----------

# 2) LOWER
df.select(lower('Item_Type').alias("Item_Type")).display()



# COMMAND ----------

# 3) UPPER
df.select(upper('Item_Type').alias("Item_Type")).display()


# COMMAND ----------

# 4) CONCAT
df.select(concat('Item_Type',lit(' '),'Item_Identifier').alias("Item")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE FUNCTIONS

# COMMAND ----------

# DBTITLE 1,DATE FUNCTIONS
df = spark.read.table("workspace.default.big_mart_sales")
# 1) Current_date
df=df.withColumn('current_date',current_date())
df.display()

# COMMAND ----------

# 2) date_add
df=df.withColumn('week_after',date_add('current_date',7))
df.display()

# COMMAND ----------

# 3) date_sub()
df=df.withColumn('week_before',date_sub('current_date',7))
df.display()


# COMMAND ----------

# 4) datediff()
df=df.withColumn('datediff',date_diff('week_after','current_date'))
df.display()

# COMMAND ----------

# 5) Date Format()
df=df.withColumn('week_before',date_format('week_before','dd-MM-yyyy'))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Null Values

# COMMAND ----------

# MAGIC %md
# MAGIC Droping Null Values

# COMMAND ----------

# DBTITLE 1,Handling Null Values
# 1) dropna - any
# dropna(how="any") removes rows that contain at least one NULL value in any column.
df.dropna('any').display()

# COMMAND ----------

# DBTITLE 1,dropna
# 2) dropna - all
# dropna(how="all") removes a row only when all selected columns are NULL.
df.dropna('all').display()

# COMMAND ----------

#3) dropna - subset
#dropna(subset=...) removes rows that have NULL values in specific column(s) only.
#Instead of checking all columns, Spark checks only the columns mentioned in subset.

df.dropna(subset=['Outlet_Type']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Filling Null Values

# COMMAND ----------

# DBTITLE 1,fillna
df.fillna('NA').display()

# COMMAND ----------

df.fillna('NA',subset=['Item_Weight']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split and Indexing
# MAGIC In PySpark, split() is used to divide a string into an array based on a delimiter, and indexing is used to access a specific element from that array.
# MAGIC
# MAGIC Function : split('column_name','delimiter')[index]
# MAGIC
# MAGIC ![image_1782629546289.png](./image_1782629546289.png "image_1782629546289.png")

# COMMAND ----------

df = spark.read.table("workspace.default.big_mart_sales")
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPLIT

# COMMAND ----------

# DBTITLE 1,split
df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC INDEXING

# COMMAND ----------

# DBTITLE 1,Indexing
df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explode
# MAGIC
# MAGIC explode() is used to convert each element of an array or each key-value pair of a map into separate rows.
# MAGIC
# MAGIC In simple words:
# MAGIC One row containing multiple values → Multiple rows, one value per row
# MAGIC
# MAGIC ![image_1782639250189.png](./image_1782639250189.png "image_1782639250189.png")

# COMMAND ----------

# DBTITLE 1,Explode
df_exp=df.withColumn('Outlet_Type',split('Outlet_Type',' '))
df_exp.display()

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Array_Contains
# MAGIC
# MAGIC Definition :
# MAGIC array_contains() checks whether a specific value exists in an array column.
# MAGIC
# MAGIC If the value is present → True
# MAGIC
# MAGIC If the value is not present → False
# MAGIC
# MAGIC If the array is NULL → NULL
# MAGIC
# MAGIC ![image_1782639694545.png](./image_1782639694545.png "image_1782639694545.png")

# COMMAND ----------

# DBTITLE 1,Array_Contains
df_exp.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### GroupBy

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,GroupBy
# Scenario 1
df.groupBy("Item_Type").agg(sum("Item_MRP")).display()

# COMMAND ----------

# Scenario 2
df.groupBy("Item_Type").agg(avg("Item_MRP")).display()

# COMMAND ----------

# Scenario 3
df.groupBy("Item_Type","Outlet_Size").agg(sum("Item_MRP").cast("int").alias("Total_MRP")).display()

# COMMAND ----------

# Scenario 4
df.groupBy("Item_Type","Outlet_Size").agg(sum("Item_MRP"),avg("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Collect_List
# MAGIC
# MAGIC collect_list() is an aggregate function that collects all values from a column into an array (list) for each group.
# MAGIC
# MAGIC ![image_1782646242618.png](./image_1782646242618.png "image_1782646242618.png")

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()

# COMMAND ----------

# DBTITLE 1,Collect_List
df_book.groupBy("user").agg(collect_list("book").alias("books")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PIVOT
# MAGIC
# MAGIC pivot() is used to transform unique values from one column into multiple columns.
# MAGIC In simple words:
# MAGIC
# MAGIC Rows ➜ Columns
# MAGIC It is similar to creating a Pivot Table in Excel.
# MAGIC
# MAGIC Syntax : \
# MAGIC  df.groupBy(group_column) \
# MAGIC   .pivot(pivot_column) \
# MAGIC   .agg(aggregate_function)

# COMMAND ----------

df = spark.read.table("workspace.default.big_mart_sales")
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,PIVOT
df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WHEN-OTHERWISE

# COMMAND ----------

# DBTITLE 1,When-Otherwise
df = spark.read.table("workspace.default.big_mart_sales")
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Scenario 1
from pyspark.sql.functions import when, col
df=df.withColumn('Veg_flag',when(col('Item_Type')=='Meat','Non-Veg').otherwise('Veg'))
df.display()

# COMMAND ----------

df.display()

# COMMAND ----------

# Scenario 2
df.withColumn('veg_exp_flag',
              when(
                  (col('Veg_flag')=='Veg') & (col('Item_MRP')<100),'Veg_Inexpensive'
                    ).when((col('Veg_flag')=='Veg') & (col('Item_MRP')>100),'Veg_Expensive'
                         ) .otherwise('Non-Veg')
                    ) .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### JOINS
# MAGIC

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1) INNER JOIN

# COMMAND ----------

# DBTITLE 1,INNER JOIN
df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2) LEFT JOIN

# COMMAND ----------

# DBTITLE 1,LEFT JOIN
df1.join(df2,df1['dept_id']==df2['dept_id'],"left").display()

# OR LEFT OUTER

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3) RIGHT JOIN 

# COMMAND ----------

# DBTITLE 1,RIGHT JOIN
df1.join(df2,df1['dept_id']==df2['dept_id'],"right").display()

# OR RIGHT OUTER

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4) FULL JOIN

# COMMAND ----------

# DBTITLE 1,FULL JOIN
df1.join(df2,df1['dept_id']==df2['dept_id'],"full").display()

# OR outer OR full_outer

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5) LEFT ANTI JOIN 
# MAGIC
# MAGIC An Anti Join returns only the rows from the left DataFrame that do NOT have a matching row in the right DataFrame.

# COMMAND ----------

# DBTITLE 1,LEFT ANTI JOIN
df1.join(df2,df1['dept_id']==df2['dept_id'],"anti").display()

# OR

df1.join(df2,df1['dept_id']==df2['dept_id'],"left_anti").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6) Left Semi Join
# MAGIC
# MAGIC Only matching rows from the left DataFrame (returns only left columns)

# COMMAND ----------

# DBTITLE 1,Left Semi Join
df1.join(df2,df1['dept_id']==df2['dept_id'],"left_semi").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7) Cross Join 

# COMMAND ----------

# DBTITLE 1,Cross Join
df1.join(df2,df1['dept_id']==df2['dept_id'],"cross").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Functions

# COMMAND ----------

df = spark.read.table("workspace.default.big_mart_sales")
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1) ROW NUMBER
# MAGIC
# MAGIC row_number() is a window function that assigns a unique sequential number to each row within a window (partition).

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,Row_Number
from pyspark.sql.window import Window
# Scenario 1
df.withColumn('Row_num',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# Scenario 2
df.withColumn('Row_num',row_number().over(Window.partitionBy('Outlet_Identifier').orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### RANK And DENSE_RANK
# MAGIC
# MAGIC ![image_1782652223008.png](./image_1782652223008.png "image_1782652223008.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2) RANK
# MAGIC
# MAGIC row_number() assigns a unique sequential number to each row within a window, starting from 1. Even if two rows have the same value, they receive different row numbers.

# COMMAND ----------

# DBTITLE 1,RANK
df.withColumn('Rank',rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3) DENSE_RANK
# MAGIC
# MAGIC rank() assigns the same rank to rows with equal values. After a tie, it skips the next rank(s).

# COMMAND ----------

# DBTITLE 1,DENSE_RANK
df.withColumn('Rank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### RANK VS DENSE_RANK
# MAGIC
# MAGIC Both are window functions used to rank rows within a partition. The main difference is how they handle ties (duplicate values).

# COMMAND ----------


df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
        .withColumn('denseRank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4) Cumulative Sum
# MAGIC
# MAGIC ![image_1782654721771.png](./image_1782654721771.png "image_1782654721771.png")

# COMMAND ----------

# DBTITLE 1,Cumulative Sum
from pyspark.sql.window import Window
# Scenario 1
df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

# Scenario 2
df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5) LAG
# MAGIC
# MAGIC Definition :
# MAGIC Returns the previous row's value.

# COMMAND ----------

# DBTITLE 1,LAG
df.withColumn(
    "Previous_MRP",
    lag("Item_MRP", 1).over(Window.orderBy("Item_MRP"))
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6) LEAD
# MAGIC
# MAGIC Definition :
# MAGIC Returns the next row's value.

# COMMAND ----------

# DBTITLE 1,LEAD
df.withColumn(
    "Previous_MRP",
    lead("Item_MRP", 1).over(Window.orderBy("Item_MRP"))
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7) FIRST
# MAGIC
# MAGIC Definition :
# MAGIC Returns the first value in the window.

# COMMAND ----------

# DBTITLE 1,FIRST
df.withColumn(
    "First_MRP",
    first("Item_MRP").over(Window.orderBy("Item_MRP"))
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LAST
# MAGIC
# MAGIC For last() to return the last value of the entire window, use an unbounded window frame.

# COMMAND ----------

# DBTITLE 1,LAST
df.withColumn(
    "Last_MRP",
    last("Item_MRP").over(Window.orderBy("Item_MRP") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### USER DEFINED FUNCTION (UDF)
# MAGIC
# MAGIC Definition : \
# MAGIC A User Defined Function (UDF) is a custom function created by the user when the required logic cannot be achieved using PySpark's built-in functions.\
# MAGIC In simple words:\
# MAGIC A UDF lets you write your own Python function and apply it to DataFrame columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1

# COMMAND ----------

# DBTITLE 1,UDF
def my_func(x):
    return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2

# COMMAND ----------

my_udf=udf(my_func)

# COMMAND ----------

df.withColumn('myNewCol',my_udf(col('Item_MRP'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA WRITING

# COMMAND ----------

df.write.format('csv').save('dbfs:/FileStore/tables/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA WRITING MODES
# MAGIC
# MAGIC 1) APPEND
# MAGIC 2) OVERWRITE
# MAGIC 3) IGNORE
# MAGIC 4) ERROR

# COMMAND ----------

# MAGIC %md
# MAGIC ### A) Writing in CSV

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. APPEND
# MAGIC
# MAGIC Adds new data to the existing table or files without deleting the existing data.

# COMMAND ----------

# DBTITLE 1,APPEND
df.write.format('csv')\
    .mode('append')\
        .save('dbfs:/FileStore/tables/')

# OR

df.write.format('csv')\
    .mode('append')\
        .option('path','dbfs:/FileStore/tables/')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2) OVERWRITE
# MAGIC
# MAGIC Deletes the existing data and writes the new data.

# COMMAND ----------

# DBTITLE 1,OVERWRITE
df.write.format('csv')\
    .mode('overwrite')\
        .option('path','dbfs:/FileStore/tables/')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3) ERROR OR errorifexists
# MAGIC
# MAGIC Throws an error if the destination already exists.\
# MAGIC This is the default write mode if you don't specify one.

# COMMAND ----------

# DBTITLE 1,ERROR
df.write.format('csv')\
    .mode('error')\
        .option('path','dbfs:/FileStore/tables/')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4) IGNORE
# MAGIC
# MAGIC If the destination already exists, Spark does nothing. No error is thrown.
# MAGIC

# COMMAND ----------

# DBTITLE 1,IGNORE
df.write.format('csv')\
    .mode('ignore')\
        .option('path','dbfs:/FileStore/tables/')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### B) Writing in PARQUET

# COMMAND ----------

df.write.format('parquet')\
    .mode('ignore')\
        .option('path','dbfs:/FileStore/tables/')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### C) Writing As Table

# COMMAND ----------

df.write.format('csv')\
    .mode('ignore')\
        .option('path','dbfs:/FileStore/tables/')\
            .saveAsTable()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPARK SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1

# COMMAND ----------

# DBTITLE 1,SPARK SQL
df.createOrReplaceTempView('temp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp where Item_Fat_Content='Low Fat'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2

# COMMAND ----------

 res=spark.sql("select * from temp")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managed vs External Tables
# MAGIC
# MAGIC ![image_1782746429730.png](./image_1782746429730.png "image_1782746429730.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ------------------------------------------- COMPLETED ------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ### 