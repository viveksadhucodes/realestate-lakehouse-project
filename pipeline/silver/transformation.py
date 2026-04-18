from pyspark.sql.functions import (
    col, lower, trim, date_format,
    when, year, current_date
)

# =========================
# TRANSACTIONS
# =========================
def clean_transactions(df):

    df = df.filter(
        (col("deal_price") > 0) &
        (col("commission_amount") > 0) &
        (col("commission_amount") < col("deal_price")) &
        col("deal_date").isNotNull() &
        col("transaction_id").isNotNull()
    ).dropDuplicates(["transaction_id"])

    df = df.fillna({"payment_mode": "unknown"})

    df = df.withColumn("payment_mode", lower(trim(col("payment_mode")))) \
           .withColumn("city", lower(trim(col("city")))) \
           .withColumn("deal_status", lower(trim(col("deal_status"))))

    df = df.withColumn(
        "deal_status",
        when(col("deal_status").isin("completed", "complete"), "completed")
        .when(col("deal_status").isin("cancelled", "canceled"), "cancelled")
        .otherwise("unknown")
    )

    df = df.withColumn("year_month", date_format(col("deal_date"), "yyyy-MM")) \
           .withColumn("deal_year", year(col("deal_date")))

    df = df.withColumn(
        "price_category",
        when(col("deal_price") < 5_000_000, "low")
        .when(col("deal_price") < 15_000_000, "mid")
        .when(col("deal_price") < 30_000_000, "high")
        .otherwise("luxury")
    )

    return df


# =========================
# CUSTOMERS (FIXED PROPERLY)
# =========================
def clean_customers(df):

    df = df.filter(col("customer_id").isNotNull()) \
           .dropDuplicates(["customer_id"]) \
           .filter(col("age") > 0)

    df = df.fillna({
        "segment": "unknown",
        "home_city": "unknown",
        "income_band": "unknown",
        "acquisition_channel": "unknown"
    })

    df = df.withColumn("home_city", lower(trim(col("home_city")))) \
           .withColumn("segment", lower(trim(col("segment")))) \
           .withColumn("acquisition_channel", lower(trim(col("acquisition_channel")))) \
           .withColumn("signup_year", year(col("signup_date")))

    # =========================
    # 🔥 INCOME BAND CLEANING (REAL FIX)
    # =========================
    df = df.withColumn(
        "income_band",
        when(col("income_band").isNull(), "unknown")
        .when(trim(col("income_band")) == "", "unknown")
        .when(lower(trim(col("income_band"))).isin("na", "n/a", "unk", "null"), "unknown")
        .otherwise(lower(trim(col("income_band"))))
    )

    # =========================
    # 🔥 STANDARDIZE VALUES
    # =========================
    df = df.withColumn(
        "income_band",
        when(col("income_band").isin("low", "lower"), "low")
        .when(col("income_band").isin("mid", "middle"), "mid")
        .when(col("income_band").isin("high", "upper"), "high")
        .when(col("income_band").isin("upper-mid", "upper_mid"), "upper-mid")
        .when(col("income_band").isin("lower-mid", "lower_mid"), "lower-mid")
        .otherwise(col("income_band"))
    )

    return df


# =========================
# PROPERTIES
# =========================
def clean_properties(df):

    df = df.filter(col("property_id").isNotNull()) \
           .dropDuplicates(["property_id"]) \
           .filter(
               (col("size_sqm") > 0) &
               (col("list_price") > 0)
           )

    df = df.fillna({
        "property_type": "unknown",
        "city": "unknown"
    })

    df = df.withColumn("property_type", lower(trim(col("property_type")))) \
           .withColumn("city", lower(trim(col("city")))) \
           .withColumn("price_per_sqm", col("list_price") / col("size_sqm")) \
           .withColumn("property_age", year(current_date()) - col("year_built")) \
           .withColumn(
               "property_category",
               when(col("list_price") < 5_000_000, "low")
               .when(col("list_price") < 15_000_000, "mid")
               .when(col("list_price") < 30_000_000, "high")
               .otherwise("luxury")
           )

    return df


# =========================
# AGENTS
# =========================
def clean_agents(df):

    df = df.filter(col("agent_id").isNotNull()) \
           .dropDuplicates(["agent_id"]) \
           .filter(col("agent_rating").between(0, 5))

    df = df.fillna({"agent_city": "unknown"})

    df = df.withColumn("agent_city", lower(trim(col("agent_city")))) \
           .withColumn(
               "experience_level",
               when(col("experience_years") < 3, "junior")
               .when(col("experience_years") < 8, "mid")
               .otherwise("senior")
           ) \
           .withColumn(
               "commission_category",
               when(col("base_commission_rate") < 0.02, "low")
               .when(col("base_commission_rate") < 0.05, "medium")
               .otherwise("high")
           )

    return df


# =========================
# LISTINGS
# =========================
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def clean_listings(df):

    df = df.filter(
        col("listing_id").isNotNull() &
        col("property_id").isNotNull() &
        (col("listed_price") > 0) &
        (col("days_on_market") >= 0)
    )

    window = Window.partitionBy("property_id").orderBy(col("listed_price").desc())

    df = df.withColumn("rn", row_number().over(window)) \
           .filter(col("rn") == 1) \
           .drop("rn")

    df = df.fillna({
        "listing_channel": "unknown"
    })

    df = df.withColumn("listing_channel", lower(trim(col("listing_channel"))))

    return df


# =========================
# FINAL FACT TABLE
# =========================
def build_silver_fact(trans, cust, prop, agent, listings):

    trans = trans.dropDuplicates(["transaction_id"])
    cust = cust.dropDuplicates(["customer_id"])
    prop = prop.dropDuplicates(["property_id"])
    agent = agent.dropDuplicates(["agent_id"])
    listings = listings.dropDuplicates(["property_id"])

    trans = trans.select(
        "transaction_id",
        "customer_id",
        "property_id",
        "agent_id",
        "city",
        "deal_date",
        "deal_price",
        "commission_amount",
        "payment_mode",
        "deal_status",
        "year_month",
        "price_category"
    )

    cust = cust.select(
        "customer_id",
        "home_city",
        "segment",
        "income_band"
    )

    prop = prop.select(
        "property_id",
        "property_type",
        col("city").alias("property_city"),
        "price_per_sqm",
        "property_age",
        "property_category"
    )

    agent = agent.select(
        "agent_id",
        "agent_city",
        "experience_level",
        "commission_category"
    )

    listings = listings.select(
        "property_id",
        "listing_channel",
        "listed_price",
        "days_on_market",
        "sold_flag"
    )

    df = trans \
        .join(cust, "customer_id", "left") \
        .join(prop, "property_id", "left") \
        .join(agent, "agent_id", "left") \
        .join(listings, "property_id", "left")

    df = df.withColumn(
        "city_variation_flag",
        when(
            (col("property_city").isNull()) |
            (col("city").isNull()) |
            (col("property_city") != col("city")),
            1
        ).otherwise(0)
    )
    df = df.withColumn(
        "income_band",
        when(col("income_band").isNull(), "unknown")
        .otherwise(col("income_band"))
    )
    df = df.dropDuplicates(["transaction_id"])
    df=df.fillna({
        "agent_city":"unknown"
    })
    

    return df