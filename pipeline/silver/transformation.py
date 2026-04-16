from pyspark.sql.functions import col, lower, trim, date_format, lit

def clean_transactions(df):

    print("\n🔧 Starting cleaning for transactions...")

    # 1️⃣ Drop invalid rows (combine filters → more efficient)
    df = df.filter(
        (col("deal_price").isNotNull()) & (col("deal_price") > 0) &
        (col("commission_amount").isNotNull()) & (col("commission_amount") > 0) &
        (col("deal_date").isNotNull()) &
        (col("transaction_id").isNotNull()) &
        (col("customer_id").isNotNull()) &
        (col("property_id").isNotNull()) &
        (col("agent_id").isNotNull())
    )

    # 2️⃣ Remove duplicates
    df = df.dropDuplicates(["transaction_id"])

    # 3️⃣ Handle nulls
    df = df.fillna({
        "payment_mode": "unknown"
    })

    # 4️⃣ Standardize strings
    df = df.withColumn("payment_mode", lower(trim(col("payment_mode")))) \
           .withColumn("city", lower(trim(col("city")))) \
           .withColumn("deal_status", lower(trim(col("deal_status"))))

    # 5️⃣ Derived column
    df = df.withColumn("year_month", date_format(col("deal_date"), "yyyy-MM"))

    # 6️⃣ Update layer
    df = df.withColumn("layer", lit("silver"))

    return df