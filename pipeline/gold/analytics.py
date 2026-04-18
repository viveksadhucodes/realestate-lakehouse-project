# pipeline/gold/analytics.py

from pyspark.sql.functions import sum, count, avg, col, when, round
from pyspark.sql.window import Window
from pyspark.sql.functions import rank


# ============================================================
# CORE KPIs
# ============================================================

def revenue_by_city(df):
    return df.groupBy("city") \
        .agg(sum("deal_price").alias("total_revenue")) \
        .orderBy(col("total_revenue").desc())


def monthly_sales(df):
    return df.groupBy("year_month") \
        .agg(count("*").alias("total_sales")) \
        .orderBy("year_month")


def top_agents(df):
    agent_df = df.groupBy("agent_id") \
        .agg(sum("commission_amount").alias("total_commission"))

    window = Window.orderBy(col("total_commission").desc())

    return agent_df.withColumn("rank", rank().over(window))


def top_expensive_properties(df):
    return df.orderBy(col("deal_price").desc()).limit(10)


def avg_price_by_property_type(df):
    return df.groupBy("property_type") \
        .agg(avg("deal_price").alias("avg_price")) \
        .orderBy(col("avg_price").desc())


def customer_purchase_frequency(df):
    return df.groupBy("customer_id") \
        .agg(count("*").alias("purchase_count"))


def property_demand(df):
    return df.groupBy("city") \
        .agg(
            count("*").alias("total_transactions"),
            avg("days_on_market").alias("avg_days_on_market"),
            sum(col("sold_flag").cast("int")).alias("sold_properties")
        )


def buyer_type(df):
    cust_freq = df.groupBy("customer_id") \
        .agg(count("*").alias("purchase_count"))

    return cust_freq.withColumn(
        "buyer_type",
        when(col("purchase_count") > 1, "Repeat").otherwise("New")
    )


def running_revenue(df):
    window = Window.orderBy("deal_date")

    return df.withColumn(
        "running_revenue",
        sum("deal_price").over(window)
    )


def agent_ranking(df):

    df = df.fillna({
        "agent_city": "unknown"
    })

    agent_df = df.groupBy("agent_id", "agent_city") \
        .agg(sum("deal_price").alias("revenue"))

    window = Window.partitionBy("agent_city") \
        .orderBy(col("revenue").desc())

    return agent_df.withColumn("rank", rank().over(window))

# ============================================================
# 🔥 ADVANCED KPIs
# ============================================================

def revenue_by_property_category(df):
    return df.groupBy("property_category") \
        .agg(sum("deal_price").alias("total_revenue")) \
        .orderBy(col("total_revenue").desc())


def sales_by_income_band(df):
    return df.fillna({"income_band": "unknown"}) \
        .groupBy("income_band") \
        .agg(count("*").alias("total_sales")) \
        .orderBy(col("total_sales").desc())


def commission_efficiency(df):
    return df.withColumn(
        "commission_ratio",
        round(col("commission_amount") / col("deal_price"), 4)
    ).groupBy("agent_id") \
     .agg(avg("commission_ratio").alias("avg_commission_ratio")) \
     .filter(col("avg_commission_ratio").isNotNull()) \
     .orderBy(col("avg_commission_ratio").desc()) \
     .limit(10)


def listing_conversion_rate(df):
    return df.groupBy("listing_channel") \
        .agg(
            count("*").alias("total_listings"),
            sum(col("sold_flag").cast("int")).alias("sold_count")
        ).withColumn(
            "conversion_rate",
            round(col("sold_count") / col("total_listings"), 4)
        ).orderBy(col("conversion_rate").desc())


def city_variation_impact(df):
    return df.groupBy("city_variation_flag", "city") \
        .agg(
            count("*").alias("total_transactions"),
            avg("deal_price").alias("avg_price")
        )


def market_speed_analysis(df):
    return df.groupBy("property_type") \
        .agg(
            avg("days_on_market").alias("avg_days"),
            count("*").alias("transactions")
        ).orderBy(col("avg_days").asc())


# ============================================================
# 🧠 BEHAVIORAL KPIs
# ============================================================

def fastest_selling_category(df):
    return df.groupBy("property_category") \
        .agg(
            avg("days_on_market").alias("avg_days_on_market"),
            count("*").alias("total_sales")
        ) \
        .orderBy(col("avg_days_on_market").asc())


def high_value_buyers(df):
    return df.fillna({"income_band": "unknown"}) \
        .groupBy("income_band") \
        .agg(
            avg("deal_price").alias("avg_purchase_value"),
            count("*").alias("total_transactions")
        ) \
        .orderBy(col("avg_purchase_value").desc())


def premium_buyers(df):
    return df.fillna({"income_band": "unknown"}) \
        .filter(col("property_category") == "high") \
        .groupBy("income_band") \
        .agg(
            count("*").alias("premium_purchases"),
            avg("deal_price").alias("avg_price")
        ) \
        .orderBy(col("premium_purchases").desc())
