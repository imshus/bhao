"""Connection handler for the API Gateway WebSocket API (gold-tracker-ws).

API Gateway does not keep a list of who is connected - each connection is just
an id handed to us on $connect. So we store the ids in DynamoDB, and the fetch
Lambda reads that table to know where to push new rates.

Routes:
  $connect    -> remember this connection id (with a TTL, so abandoned rows
                 disappear on their own if a $disconnect is ever missed)
  $disconnect -> forget it
  $default    -> the client asked us something; {"action": "snapshot"} sends
                 the latest stored rates back down the same socket

Deployed by deploy.sh. Nothing here writes rates - it only tracks connections.
"""

import json
import os
import time

import boto3
from boto3.dynamodb.conditions import Key

TABLE_NAME = os.environ.get("TABLE_NAME", "gold-rate-tracker")
CONNECTIONS_TABLE = os.environ.get("CONNECTIONS_TABLE", "gold-rate-tracker-connections")
SOURCES = ["jmd_patil", "mega_bullion", "shri_sai", "shri_ganesh"]

# A browser tab left open all day re-connects on its own if it drops, so a
# couple of hours is plenty; the TTL only exists to sweep up rows whose
# $disconnect never arrived.
CONNECTION_TTL_SECONDS = 2 * 60 * 60


def connections_table():
    return boto3.resource("dynamodb").Table(CONNECTIONS_TABLE)


def rates_table():
    return boto3.resource("dynamodb").Table(TABLE_NAME)


def latest_for(source_id):
    resp = rates_table().query(
        KeyConditionExpression=Key("source").eq(source_id),
        ScanIndexForward=False,
        Limit=1,
    )
    items = resp.get("Items", [])
    return items[0] if items else None


def latest_snapshot():
    results = []
    for source_id in SOURCES:
        item = latest_for(source_id)
        if item:
            item["cash_bhaw"] = item.get("diff1")
            item["rtgs_bhaw"] = item.get("diff2")
            results.append(item)
    return results


def management_client(request_context):
    endpoint = "https://%s/%s" % (request_context["domainName"], request_context["stage"])
    return boto3.client("apigatewaymanagementapi", endpoint_url=endpoint)


def on_connect(connection_id):
    connections_table().put_item(
        Item={
            "connectionId": connection_id,
            "connected_at": int(time.time()),
            "expires_at": int(time.time()) + CONNECTION_TTL_SECONDS,
        }
    )
    return {"statusCode": 200, "body": "connected"}


def on_disconnect(connection_id):
    try:
        connections_table().delete_item(Key={"connectionId": connection_id})
    except Exception:
        pass   # the row may already be gone; disconnect must never fail loudly
    return {"statusCode": 200, "body": "disconnected"}


def on_message(connection_id, request_context, body):
    try:
        message = json.loads(body) if body else {}
    except ValueError:
        message = {}

    action = message.get("action")

    if action == "snapshot":
        payload = {"type": "snapshot", "sources": latest_snapshot()}
        management_client(request_context).post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(payload, default=str).encode("utf-8"),
        )
    elif action == "ping":
        management_client(request_context).post_to_connection(
            ConnectionId=connection_id, Data=json.dumps({"type": "pong"}).encode("utf-8")
        )

    return {"statusCode": 200, "body": "ok"}


def handler(event, context):
    request_context = event.get("requestContext", {})
    route = request_context.get("routeKey")
    connection_id = request_context.get("connectionId")

    if route == "$connect":
        return on_connect(connection_id)
    if route == "$disconnect":
        return on_disconnect(connection_id)
    return on_message(connection_id, request_context, event.get("body"))
