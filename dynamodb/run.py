# %%
import inspect
import os
import time

from dotenv import load_dotenv

import yaml
import boto3
from boto3.dynamodb.conditions import Key, Attr

import pandas as pd

load_dotenv()


def log_func(func):
    def wrapper(*args, **kwargs):
        func_args = inspect.signature(func).bind(*args, **kwargs).arguments
        func_args_str = ", ".join(map("{0[0]} = {0[1]!r}".format, func_args.items()))
        print(f"\n{'-'*40}")
        print(f"[Func]\t{func.__qualname__}")
        print(f"[Args]:\t{func_args_str}")
        return func(*args, **kwargs)

    return wrapper


def time_func(func):
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"\nFunction {func.__name__}{args} Took {total_time:.4f} seconds")
        return result

    return timeit_wrapper


dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url="http://localhost:8100",
    region_name=os.environ.get("REGION_NAME"),
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
)


with open("env.yaml", "r", encoding="utf-8") as fp:
    params = yaml.safe_load(fp)["DynamoDB"]
with open("item.yaml", "r", encoding="utf-8") as fp:
    all_items = yaml.safe_load(fp)["TestItem"]


@log_func
def create_table(dynamodb_resource: boto3.resource, params: dict):
    try:
        table = dynamodb_resource.create_table(**params)
        table.wait_until_exists()
        print(f"Table ({params['TableName']}) created.")
    except Exception as e:
        raise e


@log_func
def deleteTable(dynamodb_resource: boto3.resource, table_name: str):
    try:
        table = dynamodb_resource.Table(table_name)
        table.delete()
        table.wait_until_not_exists()
        print(f"Table ({table_name}) deleted.")

    except Exception as e:
        raise e


@log_func
def put_item(dynamodb_resource: boto3.resource, table_name: str, items: dict):
    try:
        dynamodb_table = dynamodb_resource.Table(table_name)
    except Exception as e:
        raise e
    with dynamodb_table.batch_writer() as batch:
        for counter, item in enumerate(items):
            batch.put_item(Item=item)
    print(f"Items put: {counter+1}")


@log_func
def truncate_table(dynamodb_resource: boto3.resource, table_name: str):
    try:
        table = dynamodb_resource.Table(table_name)
    except Exception as e:
        raise e

    table_key_name = [key.get("AttributeName") for key in table.key_schema]

    projectionExpression = ", ".join("#" + key for key in table_key_name)
    expressionAttrNames = {"#" + key: key for key in table_key_name}

    counter = 0
    page = table.scan(
        ProjectionExpression=projectionExpression,
        ExpressionAttributeNames=expressionAttrNames,
    )
    with table.batch_writer() as batch:
        while page["Count"] > 0:
            counter += page["Count"]
            for itemKeys in page["Items"]:
                batch.delete_item(Key=itemKeys)
            if "LastEvaluatedKey" in page:
                page = table.scan(
                    ProjectionExpression=projectionExpression,
                    ExpressionAttributeNames=expressionAttrNames,
                    ExclusiveStartKey=page["LastEvaluatedKey"],
                )
            else:
                break
    print(f"Items deleted: {counter}")


# @time_func
@log_func
def scan_table(dynamodb_resource: boto3.resource, table_name: str):
    try:
        table = dynamodb_resource.Table(table_name).scan()
    except Exception as e:
        raise e

    res_df = pd.DataFrame(table["Items"])
    print(f"Scanned table size: {res_df.shape}")

    return res_df


@log_func
def query_table(
    dynamodb_resource: boto3.resource,
    table_name: str,
    options: dict,
):
    table = dynamodb_resource.Table(table_name)
    query_res = table.query(**options)

    # query_res = query_res["Items"]

    print(query_res)


deleteTable(dynamodb, params["TableName"])
create_table(dynamodb, params)
put_item(dynamodb, params["TableName"], all_items)
scan_table(dynamodb, params["TableName"])
truncate_table(dynamodb, params["TableName"])
scan_table(dynamodb, params["TableName"])

options = {
    # "Select": "COUNT",
    "KeyConditionExpression": Key("user_id").eq("test_user_3") & Key("count").eq(40),
    # "FilterExpression": Attr("status").eq(1),
}


query_table(dynamodb_resource=dynamodb, table_name=params["TableName"], options=options)

# %%
