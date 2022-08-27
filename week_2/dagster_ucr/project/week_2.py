from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    out={"stocks": Out(dagster_type=List[Stock])},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    stocks = list()
    key = context.op_config["s3_key"]
    for row in context.resources.s3.get_data(key):
        stock = Stock.from_list(row)
        stocks.append(stock)
    return stocks


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    tags={"kind": "transformation"},
    description="Find the largest stock value",
)
def process_data(stocks):
    big_stock = max(stocks, key=lambda x:x.high)
    return Aggregation(date=big_stock.date, high=big_stock.high)


@op(
    ins={"aggregations": In(dagster_type=Aggregation)},
    required_resource_keys={"redis"},
    tags={"kind":"redis"},
    description="Placeholder for loading data",
)
def put_redis_data(context, aggregations):
    context.resources.redis.put_data(aggregations.date, str(aggregations.high))


@graph
def week_2_pipeline():
    s3_data = get_s3_data()
    transformed_data = process_data(s3_data)
    put_redis_data(transformed_data)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://host.docker.internal:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

docker_week_2_pipeline = week_2_pipeline.to_job(
    name="docker_week_2_pipeline",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
