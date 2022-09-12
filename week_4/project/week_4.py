from typing import List

from dagster import asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    group_name="corise",
    description="Get a list of stocks from an S3 file",
    op_tags={"kind": "s3"},
)
def get_s3_data(context):
    stocks = list()
    key = context.op_config["s3_key"]
    for row in context.resources.s3.get_data(key):
        stock = Stock.from_list(row)
        stocks.append(stock)
    return stocks


@asset(
    description="Find the largest stock value",
    group_name="corise",
)
def process_data(get_s3_data):
    big_stock = max(get_s3_data, key=lambda x:x.high)
    return Aggregation(date=big_stock.date, high=big_stock.high)


@asset(
    required_resource_keys={"redis"},
    description="Loads aggregations into redis cache",
    group_name="corise",
    op_tags={"kind":"redis"},
)
def put_redis_data(context, process_data):
    context.resources.redis.put_data(process_data.date.strftime("%m/%d/%Y"), str(process_data.high))


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
        definitions=[get_s3_data, process_data, put_redis_data],
        resource_defs={"s3": s3_resource, "redis": redis_resource},
        resource_config_by_key={
            "s3": {
                "config": {
                    "bucket": "dagster",
                    "access_key": "test",
                    "secret_key": "test",
                    "endpoint_url": "http://localhost:4566",
                }
            },
            "redis": {
                "config": {
                    "host": "redis",
                    "port": 6379,
                }
            },
        }
    )
