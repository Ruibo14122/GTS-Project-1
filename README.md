# GTS-Project-1

    from pyspark.sql.session import *
    from pyspark.sql.functions import *
    from pyspark.sql import *
    from pyspark.sql.types import StringType, MapType
    import re

# Create SparkSession
    spark = SparkSession.builder.appName("JsonToDataFrame").getOrCreate()

# Read delta type files and transform into DataFrame --- Business/Checkin/Tips/User1/User2/Review1/Review2/Review3
    delta_business_path = "dbfs:/user/hive/warehouse/yelp_academic_dataset_business"
    business_df = spark.read.format("delta").load(delta_business_path)

    delta_checkin_path = "dbfs:/user/hive/warehouse/yelp_academic_dataset_checkin"
    checkin_df = spark.read.format("delta").load(delta_checkin_path)

    delta_tip_path = "dbfs:/user/hive/warehouse/yelp_academic_dataset_tip"
    tip_df = spark.read.format("delta").load(delta_tip_path)

    delta_user1_path = "dbfs:/user/hive/warehouse/yelp_academic_dataset_user_part_1"
    user1_df = spark.read.format("delta").load(delta_user1_path)

    delta_user2_path = "dbfs:/user/hive/warehouse/yelp_academic_dataset_user_part_2"
    user2_df = spark.read.format("delta").load(delta_user2_path)

    delta_review1_path = "dbfs:/user/hive/warehouse/yelp_academic_dataset_review_part_1"
    review1_df = spark.read.format("delta").load(delta_review1_path)

    delta_review2_path = "dbfs:/user/hive/warehouse/yelp_academic_dataset_review_part_2"
    review2_df = spark.read.format("delta").load(delta_review2_path)

    delta_review3_path = "dbfs:/user/hive/warehouse/yelp_academic_dataset_review_part_3"
    review3_df = spark.read.format("delta").load(delta_review3_path)

# Combine User1 2, Review 1 2 3 respectively

    user_df = user1_df.union(user2_df)

    review_df = review1_df.union(review2_df).union(review3_df)

# Create flatten data function

    def flatten_business(df_raw):

      attribute_field_names = [field.name for field in df_raw.schema["attributes"].dataType.fields]
    
      * Create a list of key-value pairs for column names and column references
      attribute_columns = []
      for field_name in attribute_field_names:
          attribute_columns.append(lit(field_name))
          attribute_columns.append(col("attributes." + field_name))
        
      # Transform struct into map type
      df_raw = df_raw.withColumn("attributes_map", create_map(*attribute_columns))
    
      # Explode df_raw
      df_raw = df_raw.select("*", explode("attributes_map").alias("AttributeKey", "AttributeValue"))
    
      # Delete old attributes and attributes_map columns
      df_raw = df_raw.drop("attributes", "attributes_map")
      return df_raw

# Flatten business_df

business_df = flatten_business(business_df)

# Create raw_to_bronze function
    def raw_to_bronze(df, drop_columns=None, if_rating = True):
      # Delete unwanted column
      if drop_columns:
          df = df.drop(*drop_columns)
        
      # Delete duplicates
      df = df.dropDuplicates()
    
      # Delete null values
      df = df.dropna()

      return df 

# Use raw_to_bronze function to exsisting dfs

business_df = raw_to_bronze(business_df)

checkin_df = raw_to_bronze(checkin_df)

tip_df = raw_to_bronze(tip_df)

user_df = raw_to_bronze(user_df)

review_df = raw_to_bronze(review_df)

# Question A. How many reviews are there for each business?

review_counts = review_df.groupBy("business_id").agg(count("*").alias("review_count"))

review_counts = review_counts.orderBy(desc("review_count"))


# Question B. How many businesses take place in each state, In each city? What kind of business do they have the most in each state, in each city ?

# Question B(1). Each state business counts

business_state_counts = business_df.groupBy("state").agg(count("*").alias("state_count"))

business_state_counts = business_state_counts.orderBy(desc("state_count"))

# Question B(2). Each city business counts 

business_city_counts = business_df.groupBy("state","city").agg(count("*").alias("city_count"))

business_city_counts = business_city_counts.orderBy(desc("city_count"))

# Question B(3) Each state  most frequent business type 

business_state_category_counts = (
    business_df
    .groupBy("state", "categories")
    .agg(count("*").alias("category_count"))
    .orderBy("state", desc("category_count"))
)

    # Create a window partitioned by state and ordered by category count in descending order
window = Window.partitionBy("state").orderBy(desc("category_count"))

    # Assign row numbers within each state partition
business_state_category_counts = business_state_category_counts.withColumn("row_number", row_number().over(window))

        # Filter to get the top category for each state
most_frequent_business_category = business_state_category_counts.filter(col("row_number") == 1).drop("row_number")

#Question B(4) Each city within each state most frequent business type
business_city_state_category_counts = (
    business_df
    .groupBy("state", "city", "categories")
    .agg(count("*").alias("category_count"))
    .orderBy("state", "city", desc("category_count"))
)
        # Create a window partitioned by state and city and ordered by category count in descending order
window = Window.partitionBy("state", "city").orderBy(desc("category_count"))

        # Assign row numbers within each state and city partition
business_city_state_category_counts = business_city_state_category_counts.withColumn("row_number", row_number().over(window))

        # Filter to get the top category for each city within each state
most_frequent_business_category_by_city = business_city_state_category_counts.filter(col("row_number") == 1).drop("row_number")

#Question C. What time do people usually write reviews? 
review_df = review_df.withColumn("timestamp", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))

    # Extract the hour from the timestamp
review_df = review_df.withColumn("hour", hour(col("timestamp")))

    # Filter out rows where 'hour' is null
review_not_null_df = review_df.filter(col("hour").isNotNull())

    # Count the number of reviews per hour and order by count in descending order

review_hour_rank = review_not_null_df.groupBy("hour").count().orderBy(col("count").desc())

    # Display the result to find the most common hours for writing reviews
    
review_hour_rank.show()

    # Reviews are most frequently written in the evening hours, particularly between 6 PM and 11 PM. 
    # The peak activity occurs at 6 PM and 7 PM
    # Suggesting that many users tend to write reviews after typical work hours
    # Possibly reflecting on their dining experiences during dinner time.

#Create bronze_to_silver functionss(type2 SCD, combining multiple bronze tables into 1)

def bronze_to_silver(business_df, checkin_df, tip_df, user_df, review_df, existing_silver_df=None):
    # First, combine the new datasets
    silver_df = business_df.join(checkin_df, "business_id", "outer") \
                           .join(tip_df, "business_id", "outer") \
                           .join(user_df, "user_id", "outer") \
                           .join(review_df, "business_id", "outer")
    
    # Add SCD Type 2 columns: effective_date, end_date, is_current, and version
    silver_df = silver_df.withColumn("effective_date", current_timestamp()) \
                         .withColumn("end_date", lit(None).cast("timestamp")) \
                         .withColumn("is_current", lit(True)) \
                         .withColumn("version", lit(1))
    
    # If an existing silver table is provided, merge it with the new data to apply SCD Type 2 logic
    if existing_silver_df:
        # Determine the changed records in the new data
        # You may need to define a more complex condition to compare the differences in records
        changed_records = silver_df.join(existing_silver_df, "business_id", "inner") \
                                   .filter(silver_df["column_to_compare"] != existing_silver_df["column_to_compare"]) \
                                   .select(silver_df["*"])
        
        # Update the existing records: set end_date and is_current for changed records
        windowSpec = Window.partitionBy("business_id").orderBy(desc("effective_date"))
        existing_silver_df = existing_silver_df.withColumn("row_number", row_number().over(windowSpec)) \
                                               .withColumn("end_date", when(col("row_number") == 1, current_timestamp()).otherwise(col("end_date"))) \
                                               .withColumn("is_current", when(col("row_number") == 1, lit(False)).otherwise(col("is_current"))) \
                                               .drop("row_number")
        
        # Add the changed records to the existing silver table
        silver_df = existing_silver_df.unionByName(changed_records)
    
    return silver_df

# Apply the transformation function to the datasets
silver_table = bronze_to_silver(business_df, checkin_df, tip_df, user_df, review_df)


#Save all the work to DBFS

# Define the base path for Delta tables
base_path = "dbfs:/user/hive/warehouse"

# Save the 'raw to bronze' DataFrames
business_df.write.format("delta").mode("overwrite").save(f"{base_path}/business_bronze")
checkin_df.write.format("delta").mode("overwrite").save(f"{base_path}/checkin_bronze")
tip_df.write.format("delta").mode("overwrite").save(f"{base_path}/tip_bronze")
user_df.write.format("delta").mode("overwrite").save(f"{base_path}/user_bronze")
review_df.write.format("delta").mode("overwrite").save(f"{base_path}/review_bronze")

# Save the 'bronze to silver' DataFrame
silver_table.write.format("delta").mode("overwrite").save(f"{base_path}/silver")

