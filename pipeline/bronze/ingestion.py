from pyspark.sql.functions import current_timestamp, lit, col, sum

def process_bronze_table(spark, base_path, table_name, primary_key=None):

    print("\n" + "=" * 50)
    print(f"🔄 Processing: {table_name}")
    print("=" * 50)

    # 1️⃣ Read CSV
    df = spark.read.option("header", True).option("inferSchema", True) \
        .csv(base_path + f"{table_name}.csv")

    raw_count = df.count()

    # 2️⃣ Schema (keep this)
    print("\n📌 Schema:")
    df.printSchema()

    # 3️⃣ Null check (ONLY non-zero columns)
    null_counts = df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ])

    null_dict = null_counts.collect()[0].asDict()
    filtered_nulls = {k: v for k, v in null_dict.items() if v > 0}

    if filtered_nulls:
        print("\n⚠️ Columns with NULL values:")
        for k, v in filtered_nulls.items():
            print(f"{k:<25} → {v}")
    else:
        print("\n✅ No NULL values found")

    # 4️⃣ Duplicate check
    if primary_key:
        dup_count = df.groupBy(primary_key).count().filter("count > 1").count()

        if dup_count > 0:
            print(f"\n⚠️ Duplicates found on {primary_key}: {dup_count}")
        else:
            print(f"\n✅ No duplicates found on {primary_key}")

    # 5️⃣ Add audit columns
    df = df.withColumn("ingestion_time", current_timestamp()) \
           .withColumn("source_file", lit(f"{table_name}.csv")) \
           .withColumn("layer", lit("bronze"))

    # 6️⃣ Show audit columns ONLY (this is enough)
    print("\n📌 Audit Columns Preview:")
    display(df.select("ingestion_time", "source_file", "layer").limit(5))

    # 7️⃣ Write Delta table
    table_full_name = f"bronze_{table_name}"

    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_full_name)

    # 8️⃣ Validation
    delta_count = spark.table(table_full_name).count()
    status = "OK" if raw_count == delta_count else "MISMATCH"

    print(f"\n✅ Validation → Raw: {raw_count} | Delta: {delta_count} | Status: {status}")

    # 9️⃣ Final interpretation (important)
    print(f"✔ {table_name} → Loaded successfully | Null Columns: {len(filtered_nulls)}")

    return spark.table(table_full_name)