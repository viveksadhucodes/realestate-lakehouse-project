from pyspark.sql.functions import current_timestamp, lit, col, sum

def process_bronze_table(spark, base_path, table_name, primary_key=None):

    # 🔹 Section header for readability in logs
    print("\n" + "=" * 50)
    print(f"🔄 Processing: {table_name}")
    print("=" * 50)

    # 1️⃣ Read raw CSV from source path
    # Using inferSchema for flexibility (okay for project, avoid in prod for large pipelines)
    df = spark.read.option("header", True).option("inferSchema", True) \
        .csv(base_path + f"{table_name}.csv")

    # Capture raw record count for validation later
    raw_count = df.count()

    # 2️⃣ Print schema for quick sanity check
    # Helps verify data types early (especially dates, numerics)
    print("\n📌 Schema:")
    df.printSchema()

    # 3️⃣ Null analysis (only show columns that actually have nulls)
    # Keeps output clean instead of printing useless 0s
    null_counts = df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ])

    # Convert Row → dict for easier filtering
    null_dict = null_counts.collect()[0].asDict()

    # Filter only columns where null count > 0
    filtered_nulls = {k: v for k, v in null_dict.items() if v > 0}

    if filtered_nulls:
        print("\n⚠️ Columns with NULL values:")
        for k, v in filtered_nulls.items():
            print(f"{k:<25} → {v}")
    else:
        print("\n✅ No NULL values found")

    # 4️⃣ Duplicate check based on primary key (if provided)
    # Important for ensuring uniqueness at Bronze itself
    if primary_key:
        dup_count = df.groupBy(primary_key).count().filter("count > 1").count()

        if dup_count > 0:
            print(f"\n⚠️ Duplicates found on {primary_key}: {dup_count}")
        else:
            print(f"\n✅ No duplicates found on {primary_key}")

    # 5️⃣ Add audit columns
    # These are essential for tracking lineage and debugging downstream
    df = df.withColumn("ingestion_time", current_timestamp()) \
           .withColumn("source_file", lit(f"{table_name}.csv")) \
           .withColumn("layer", lit("bronze"))

    # 6️⃣ Show only audit columns (keep preview lightweight)
    # No need to display full dataset in Bronze
    print("\n📌 Audit Columns Preview:")
    display(df.select("ingestion_time", "source_file", "layer").limit(5))

    # 7️⃣ Write data to Delta table
    # Overwrite mode used here for simplicity (fine for batch reloads)
    table_full_name = f"bronze_{table_name}"

    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_full_name)

    # 8️⃣ Post-write validation
    # Compare raw vs stored count to ensure no data loss
    delta_count = spark.table(table_full_name).count()
    status = "OK" if raw_count == delta_count else "MISMATCH"

    print(f"\n✅ Validation → Raw: {raw_count} | Delta: {delta_count} | Status: {status}")

    # 9️⃣ Final log summary (useful for pipeline logs)
    print(f"✔ {table_name} → Loaded successfully | Null Columns: {len(filtered_nulls)}")

    # Return the created Delta table as DataFrame for chaining if needed
    return spark.table(table_full_name)