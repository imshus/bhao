import json

import boto3
from boto3.dynamodb.conditions import Key

TABLE_NAME = "gold-rate-tracker"
SOURCES = ["jmd_patil", "mega_bullion", "shri_sai", "shri_ganesh"]

CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def get_table():
    return boto3.resource("dynamodb").Table(TABLE_NAME)


def latest_for(source_id):
    table = get_table()
    resp = table.query(
        KeyConditionExpression=Key("source").eq(source_id),
        ScanIndexForward=False,
        Limit=1,
    )
    items = resp.get("Items", [])
    return items[0] if items else None


def respond(status, body):
    return {"statusCode": status, "headers": CORS_HEADERS, "body": json.dumps(body, default=str)}


def handle_latest():
    results = []
    for source_id in SOURCES:
        item = latest_for(source_id)
        if item:
            item["cash_bhaw"] = item.get("diff1")
            item["rtgs_bhaw"] = item.get("diff2")
            results.append(item)
    return respond(200, results)


def handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")

    if method == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    return handle_latest()
