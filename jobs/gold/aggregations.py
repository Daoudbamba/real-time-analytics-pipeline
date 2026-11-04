"""Business aggregates, written as pure functions shared by streaming and batch jobs.

Revenue only counts `purchase` events: a `payment` settles an order that was
already counted, so adding both would count the sale twice.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

PURCHASE = "purchase"


def _is_purchase() -> Column:
    return F.col("event_type") == PURCHASE


def _with_time_bounds(window_column: Column) -> list[Column]:
    return [
        window_column.getField("start").alias("window_start"),
        window_column.getField("end").alias("window_end"),
    ]


def sales_per_window(silver: DataFrame, window_duration: str) -> DataFrame:
    """Compute revenue, transactions, units and average basket for each window."""
    return (
        silver.filter(_is_purchase())
        .groupBy(F.window("event_ts", window_duration).alias("time_window"))
        .agg(
            F.sum("amount").alias("revenue"),
            F.count("*").alias("transactions"),
            F.sum("quantity").alias("units_sold"),
        )
        .select(
            *_with_time_bounds(F.col("time_window")),
            "revenue",
            "transactions",
            "units_sold",
            F.round(F.col("revenue") / F.col("transactions"), 2).alias("average_basket"),
        )
    )


def country_activity_per_window(silver: DataFrame, window_duration: str) -> DataFrame:
    """Compute events, active users, transactions and revenue by country and window."""
    return (
        silver.groupBy(F.window("event_ts", window_duration).alias("time_window"), "country")
        .agg(
            F.count("*").alias("events"),
            F.approx_count_distinct("user_id").alias("active_users"),
            F.count(F.when(_is_purchase(), 1)).alias("transactions"),
            F.coalesce(F.sum(F.when(_is_purchase(), F.col("amount"))), F.lit(0)).alias("revenue"),
        )
        .select(
            *_with_time_bounds(F.col("time_window")),
            "country",
            "events",
            "active_users",
            "transactions",
            F.col("revenue").cast("decimal(22,2)").alias("revenue"),
        )
    )


def top_products(silver: DataFrame, limit: int) -> DataFrame:
    """Rank the best-selling products of each day by revenue."""
    daily = (
        silver.filter(_is_purchase())
        .groupBy("event_date", "product_id", "category")
        .agg(
            F.sum("amount").alias("revenue"),
            F.sum("quantity").alias("units_sold"),
            F.count("*").alias("transactions"),
        )
    )
    ranking = Window.partitionBy("event_date").orderBy(F.col("revenue").desc(), F.col("product_id"))
    return daily.withColumn("rank", F.row_number().over(ranking)).filter(F.col("rank") <= limit)


def sales_trend(silver: DataFrame) -> DataFrame:
    """Compute hourly revenue and its change from the previous hour with sales."""
    hourly = (
        silver.filter(_is_purchase())
        .groupBy(F.date_trunc("hour", "event_ts").alias("hour"))
        .agg(
            F.sum("amount").alias("revenue"),
            F.count("*").alias("transactions"),
        )
    )
    previous = F.lag("revenue").over(Window.orderBy("hour"))
    return (
        hourly.withColumn("previous_revenue", previous)
        .withColumn(
            "revenue_change_pct",
            F.round(
                (F.col("revenue") - F.col("previous_revenue")) / F.col("previous_revenue") * 100, 2
            ),
        )
        .orderBy("hour")
    )
