from typing import List

from dagster import (
    In,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


@op(
    out={"stocks": Out(dagster_type=List[Stock])},
    required_resource_keys={"s3"},
    config_schema={"s3_key": str},
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
    description="Find the largest stock value",
)
def process_data(stocks):
    big_stock = max(stocks, key=lambda x:x.high)
    return Aggregation(date=big_stock.date, high=big_stock.high)


@op(
    ins={"aggregations": In(dagster_type=Aggregation)},
    required_resource_keys={"redis"},
    tags={"kind":"redis"},
    description="Loads aggregations into redis cache",
)
def put_redis_data(context, aggregations):
    context.resources.redis.put_data(aggregations.date.strftime("%m/%d/%Y"), str(aggregations.high))


@graph
def week_3_pipeline():
    s3_data = get_s3_data()
    transformed_data = process_data(s3_data)
    put_redis_data(transformed_data)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(partition_keys=list((map(str, range(1,11)))))
def docker_config(partition_key: str):
    return {
        "resources": {
            "s3": {
                "config": {
                    "bucket": "dagster",
                    "access_key": "test",
                    "secret_key": "test",
                    "endpoint_url": "http://localstack:4566",
                }
            },
            "redis": {
                "config": {
                    "host": "redis",
                    "port": 6379,
                }
            },
        },
        "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}},
    }


local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource
        },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")
docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")


@sensor(job=docker_week_3_pipeline)
def docker_week_3_sensor(context):
    new_keys = get_s3_keys(
        bucket="dagster",
        prefix="prefix",
        endpoint_url="http://localstack:4566"
    )
    if not new_keys:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for key in new_keys:
        yield RunRequest(
            run_key=key,
            run_config={
                "resources": {
                    "s3": {
                        "config": {
                            "bucket": "dagster",
                            "access_key": "test",
                            "secret_key": "test",
                            "endpoint_url": "http://localstack:4566",
                        }
                    },
                    "redis": {
                        "config": {
                            "host": "redis",
                            "port": 6379,
                        }
                    },
                },
                "ops": {
                    "get_s3_data": {"config": {"s3_key": key}},
                },
            }
        )
