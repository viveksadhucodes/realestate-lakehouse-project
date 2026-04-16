from pyspark.sql.functions import (
    col, lower, trim, date_format, lit,
    when, year,current_date,datediff
)

def clean_transactions(df):

    # 1️⃣ Filter invalid rows (null + negative + key checks)
    df = df.filter(
        (col("deal_price").isNotNull()) & (col("deal_price") > 0) &
        (col("commission_amount").isNotNull()) & (col("commission_amount") > 0) &
        (col("commission_amount") < col("deal_price")) &
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

    # 5️⃣ Fix categorical consistency
    df = df.withColumn(
        "deal_status",
        when(col("deal_status").isin("completed", "complete"), "completed")
        .when(col("deal_status").isin("cancelled", "canceled"), "cancelled")
        .otherwise("unknown")
    )

    # 6️⃣ Validate ID formats (optional but strong)
    df = df.filter(
        col("customer_id").rlike("^C[0-9]+$") &
        col("property_id").rlike("^P[0-9]+$") &
        col("agent_id").rlike("^A[0-9]+$")
    )

    # 7️⃣ Derived columns
    df = df.withColumn("year_month", date_format(col("deal_date"), "yyyy-MM")) \
           .withColumn("deal_year", year(col("deal_date")))

    # 8️⃣ Price category
    df = df.withColumn(
        "price_category",
        when(col("deal_price") < 5000000, "low")
        .when(col("deal_price") < 15000000, "mid")
        .when(col("deal_price") < 30000000, "high")
        .otherwise("luxury")
    )

    # 9️⃣ Layer tracking
    df = df.withColumn("layer", lit("silver"))

    return df
def clean_customers(df):

    # 1️⃣ Remove null primary key
    df = df.filter(col("customer_id").isNotNull())

    # 2️⃣ Remove duplicates
    df = df.dropDuplicates(["customer_id"])

    # 3️⃣ Remove invalid values
    df = df.filter(col("age") > 0)

    # 4️⃣ Handle nulls
    df = df.fillna({
        "segment": "unknown",
        "home_city": "unknown",
        "income_band": "unknown",
        "acquisition_channel": "unknown"
    })

    # 5️⃣ Standardize strings
    df = df.withColumn("home_city", lower(trim(col("home_city")))) \
           .withColumn("segment", lower(trim(col("segment")))) \
           .withColumn("income_band", lower(trim(col("income_band")))) \
           .withColumn("acquisition_channel", lower(trim(col("acquisition_channel"))))

    # 6️⃣ Fix segment consistency
    df = df.withColumn(
    "segment",
    when(col("segment").isNull(), "unknown")
    .otherwise(lower(trim(col("segment"))))
)

    # 7️⃣ Derived column
    df = df.withColumn("signup_year", year(col("signup_date")))

    # 8️⃣ Layer tracking
    df = df.withColumn("layer", lit("silver"))

    return df
def clean_properties(df):

    # 1️⃣ Remove null primary key
    df = df.filter(col("property_id").isNotNull())

    # 2️⃣ Remove duplicates
    df = df.dropDuplicates(["property_id"])

    # 3️⃣ Remove invalid numeric values
    df = df.filter(
        (col("size_sqm") > 0) &
        (col("bedrooms") >= 0) &
        (col("bathrooms") >= 0) &
        (col("list_price") > 0) &
        (col("location_score") >= 0) &
        (col("amenities_count") >= 0)
    )

    # 4️⃣ Handle nulls
    df = df.fillna({
        "property_type": "unknown",
        "city": "unknown",
        "neighborhood_code": "unknown"
    })

    # 5️⃣ Standardize strings
    df = df.withColumn("property_type", lower(trim(col("property_type")))) \
           .withColumn("city", lower(trim(col("city")))) \
           .withColumn("neighborhood_code", lower(trim(col("neighborhood_code"))))

    # 6️⃣ Validate ID format
    df = df.filter(col("property_id").rlike("^P[0-9]+$"))

    # 7️⃣ Derived column → price per sqm
    df = df.withColumn(
        "price_per_sqm",
        col("list_price") / col("size_sqm")
    )

    # 8️⃣ Size category
    df = df.withColumn(
        "size_category",
        when(col("size_sqm") < 75, "small")
        .when(col("size_sqm") < 200, "medium")
        .otherwise("large")
    )

    # 9️⃣ Age of property
    df = df.withColumn(
        "property_age",
        year(current_date()) - col("year_built")
    )

    # 🔟 Layer tracking
    df = df.withColumn("layer", lit("silver"))

    return df
def clean_listings(df):

    # 1️⃣ Remove null primary key
    df = df.filter(col("listing_id").isNotNull())

    # 2️⃣ Remove duplicates
    df = df.dropDuplicates(["listing_id"])

    # 3️⃣ Remove invalid values
    df = df.filter(
        (col("listed_price") > 0) &
        (col("property_id").isNotNull()) &
        (col("agent_id").isNotNull())
    )

    # 4️⃣ Handle nulls
    df = df.fillna({
        "listing_channel": "unknown",
        "days_on_market": 0
    })

    # 5️⃣ Standardize strings
    df = df.withColumn("listing_channel", lower(trim(col("listing_channel"))))

    # 6️⃣ Validate IDs
    df = df.filter(
        col("listing_id").rlike("^L[0-9]+$") &
        col("property_id").rlike("^P[0-9]+$") &
        col("agent_id").rlike("^A[0-9]+$")
    )

    # 7️⃣ Create status from sold_flag
    df = df.withColumn(
        "status",
        when(col("sold_flag") == True, "sold")
        .otherwise("active")
    )

    # 8️⃣ Derived columns
    df = df.withColumn(
        "listing_month",
        date_format(col("listing_date"), "yyyy-MM")
    )

    # Optional (very good for viva)
    df = df.withColumn(
        "time_to_sell",
        when(col("sold_flag") == True,
             datediff(col("close_date"), col("listing_date")))
    )

    # 9️⃣ Layer tracking
    df = df.withColumn("layer", lit("silver"))

    return df
def clean_agents(df):

    # 1️⃣ Remove null primary key
    df = df.filter(col("agent_id").isNotNull())

    # 2️⃣ Remove duplicates
    df = df.dropDuplicates(["agent_id"])

    # 3️⃣ Remove invalid numeric values
    df = df.filter(
    (col("experience_years") >= 0) &
    (col("agent_rating") >= 0) &
    (col("agent_rating") <= 5) &
    (col("base_commission_rate") >= 0)
)

    # 4️⃣ Handle nulls
    df = df.fillna({
        "agent_city": "unknown",
        "base_commission_rate": 0.0
    })

    # 5️⃣ Standardize strings
    df = df.withColumn("agent_city", lower(trim(col("agent_city"))))

    # 6️⃣ Validate ID format
    df = df.filter(col("agent_id").rlike("^A[0-9]+$"))

    # 7️⃣ Derived column → experience level
    df = df.withColumn(
    "experience_level",
    when(col("experience_years") < 3, "junior")
    .when(col("experience_years") < 8, "mid")
    .otherwise("senior")
)

    # 8️⃣ Commission category (nice addition)
    df = df.withColumn(
        "commission_category",
        when(col("base_commission_rate") < 0.02, "low")
        .when(col("base_commission_rate") < 0.05, "medium")
        .otherwise("high")
    )

    # 9️⃣ Layer tracking
    df = df.withColumn("layer", lit("silver"))

    return df
def clean_interactions(df):

    # 1️⃣ Remove null primary key
    df = df.filter(col("interaction_id").isNotNull())

    # 2️⃣ Remove duplicates
    df = df.dropDuplicates(["interaction_id"])

    # 3️⃣ Remove invalid rows
    df = df.filter(
        col("customer_id").isNotNull() &
        col("property_id").isNotNull()
    )

    # 4️⃣ Handle nulls
    df = df.fillna({
        "interaction_type": "unknown",
        "device": "unknown",
        "source": "unknown"
    })

    # 5️⃣ Standardize strings
    df = df.withColumn("interaction_type", lower(trim(col("interaction_type")))) \
           .withColumn("device", lower(trim(col("device")))) \
           .withColumn("source", lower(trim(col("source"))))

    # 6️⃣ Validate IDs
    df = df.filter(
        col("interaction_id").rlike("^I[0-9]+$") &
        col("customer_id").rlike("^C[0-9]+$") &
        col("property_id").rlike("^P[0-9]+$")
    )

    # 7️⃣ Derived time features
    df = df.withColumn("interaction_month",
                       date_format(col("event_ts"), "yyyy-MM"))

    df = df.withColumn("interaction_year",
                       year(col("event_ts")))

    # 8️⃣ Session bucket
    df = df.withColumn(
        "session_bucket",
        when(col("session_minutes") < 2, "short")
        .when(col("session_minutes") < 10, "medium")
        .otherwise("long")
    )

    # 9️⃣ Sentiment category
    df = df.withColumn(
        "sentiment_category",
        when(col("intent_sentiment") < 0.4, "low")
        .when(col("intent_sentiment") < 0.7, "medium")
        .otherwise("high")
    )

    # 🔟 Layer tracking
    df = df.withColumn("layer", lit("silver"))

    return df