import inspect
import os
import time
from pprint import pprint

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


@log_func
def list_tables(dynamodb_resource: boto3.resource):
    tables = dynamodb_resource.tables.all()
    for table in tables:
        print(table)


@log_func
def get_table_schema(dynamodb_resource: boto3.resource, table_name: str):
    try:
        dynamodb_table = dynamodb_resource.Table(table_name)
    except Exception as e:
        raise e
    attrs = dynamodb_table.attribute_definitions
    pprint(attrs)
    schema = dynamodb_table.key_schema
    pprint(schema)


@log_func
def create_table(dynamodb_resource: boto3.resource, params: dict):
    try:
        table = dynamodb_resource.create_table(**params)
        table.wait_until_exists()
        print(f"Table ({params['TableName']}) created.")
    except Exception as e:
        raise e


@log_func
def delete_table(dynamodb_resource: boto3.resource, table_name: str):
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
def update_item(dynamodb_resource: boto3.resource, table_name: str, option: dict):
    try:
        dynamodb_table = dynamodb_resource.Table(table_name)
    except Exception as e:
        raise e
    dynamodb_table.update_item(**option)


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


@log_func
def scan_table(dynamodb_resource: boto3.resource, table_name: str):
    try:
        table = dynamodb_resource.Table(table_name)
    except Exception as e:
        raise e

    response = table.scan()

    data = response["Items"]
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        data.extend(response["Items"])

    print(data)

    return data


@log_func
def query_table(
    dynamodb_resource: boto3.resource,
    table_name: str,
    options: dict,
):
    table = dynamodb_resource.Table(table_name)
    query_res = table.query(**options)

    data = query_res["Items"]

    print(data)


@log_func
def copy_table(
    src_dynamodb: boto3.client,
    src_table_name: str,
    dst_dynamodb: boto3.client,
    dst_table_name: str,
):
    dynamo_paginator = src_dynamodb.get_paginator("scan")

    dynamodb_response = dynamo_paginator.paginate(
        TableName=src_table_name,
        Select="ALL_ATTRIBUTES",
        ReturnConsumedCapacity="NONE",
        ConsistentRead=True,
    )

    for page in dynamodb_response:
        if page["Items"]:
            for count, item in enumerate(page["Items"]):
                dst_dynamodb.put_item(TableName=dst_table_name, Item=item)
            print(f"Items transfered: {count+1}")
        else:
            print("Original Table is empty.")


if __name__ == "__main__":

    dynamodb = boto3.resource(
        "dynamodb",
        endpoint_url="http://localhost:8100",
        region_name=os.environ.get("REGION_NAME"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )

    with open("env.yaml", "r", encoding="utf-8") as f:
        params = yaml.safe_load(f)["DynamoDB"]
    with open("item.yaml", "r", encoding="utf-8") as f:
        items = yaml.safe_load(f)["TestItem"]

    get_table_schema(dynamodb, "TestTable")
    list_tables(dynamodb)
    delete_table(dynamodb, "TestTable")
    create_table(dynamodb, params)
    put_item(dynamodb, "TestTable", items)
    scan_table(dynamodb, "TestTable")
    truncate_table(dynamodb, "TestTable")
    scan_table(dynamodb, "TestTable")

    update_option = {
        "Key": {"user_id": "test_user_2", "count": 100},
        "UpdateExpression": "set #attr1 = :_status",
        "ExpressionAttributeNames": {"#attr1": "status"},
        "ExpressionAttributeValues": {":_status": 404},
        "ReturnValues": "UPDATED_NEW",
    }

    update_item(
        dynamodb_resource=dynamodb, table_name="TestTable", option=update_option
    )

    query_options = {
        # "Select": "COUNT",
        "KeyConditionExpression": Key("user_id").eq("test_user_3")
        & Key("count").eq(40),
        # "FilterExpression": Attr("status").eq(1),
    }

    query_table(
        dynamodb_resource=dynamodb, table_name="TestTable", options=query_options
    )

    src_dynamodb_client = boto3.client(
        "dynamodb",
        endpoint_url="http://localhost:8100",
        region_name=os.environ.get("REGION_NAME"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )
    dst_dynamodb_client = boto3.client(
        "dynamodb",
        endpoint_url="http://localhost:8100",
        region_name=os.environ.get("REGION_NAME"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )

    copy_table(src_dynamodb_client, "TestTable", dst_dynamodb_client, "TestTable2")
