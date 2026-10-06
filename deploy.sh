#!/bin/bash
# Gold Rate Tracker — full AWS deployment script
#
# Provisions everything from scratch in whatever AWS account the given
# profile points to: IAM role, DynamoDB table, 2 Lambda functions,
# EventBridge schedule, API Gateway, S3 + CloudFront dashboard, and a
# monthly budget alert.
#
# Usage:
#   1. Configure an AWS CLI profile with your (company) account credentials:
#        aws configure --profile gold-tracker
#   2. Edit the CONFIG block below (at minimum: BUDGET_EMAIL).
#   3. Run:  bash deploy.sh
#
# Safe to re-run: steps that already exist are skipped or updated in place.

set -e
cd "$(dirname "$0")"

# ============ CONFIG — edit before running ============
AWS_PROFILE="${AWS_PROFILE:-gold-tracker}"
REGION="ap-south-1"
BUDGET_REGION="us-east-1"          # Budgets API is only available here
BUDGET_EMAIL="CHANGE-ME@example.com"
BUDGET_LIMIT_USD="5"
# =======================================================

echo "== Using AWS profile: $AWS_PROFILE =="
ACCOUNT_ID=$(aws sts get-caller-identity --profile "$AWS_PROFILE" --query Account --output text)
echo "Account: $ACCOUNT_ID | Region: $REGION"

if [ "$BUDGET_EMAIL" = "CHANGE-ME@example.com" ]; then
  echo "!! Edit BUDGET_EMAIL at the top of this script before running. Aborting."
  exit 1
fi

ROLE_NAME="gold-tracker-lambda-role"
TABLE_NAME="gold-rate-tracker"
FETCH_FN="gold-tracker-fetch"
API_FN="gold-tracker-api"
RULE_NAME="gold-tracker-schedule"
STREAM_RULE_NAME="gold-tracker-stream"
WS_FN="gold-tracker-ws"
WS_API_NAME="gold-tracker-ws"
WS_STAGE="prod"
CONNECTIONS_TABLE="gold-rate-tracker-connections"
BUCKET_NAME="gold-tracker-dashboard-${ACCOUNT_ID}"

# ---------- 1. Budget alert (safety net, do this first) ----------
echo "== 1. Budget alert =="
cat > /tmp/budget.json <<EOF
{
  "BudgetName": "gold-tracker-monthly",
  "BudgetLimit": { "Amount": "$BUDGET_LIMIT_USD", "Unit": "USD" },
  "TimeUnit": "MONTHLY",
  "BudgetType": "COST"
}
EOF
cat > /tmp/notifications.json <<EOF
[
  { "Notification": {"NotificationType":"ACTUAL","ComparisonOperator":"GREATER_THAN","Threshold":80,"ThresholdType":"PERCENTAGE"},
    "Subscribers": [{"SubscriptionType":"EMAIL","Address":"$BUDGET_EMAIL"}] },
  { "Notification": {"NotificationType":"FORECASTED","ComparisonOperator":"GREATER_THAN","Threshold":100,"ThresholdType":"PERCENTAGE"},
    "Subscribers": [{"SubscriptionType":"EMAIL","Address":"$BUDGET_EMAIL"}] }
]
EOF
aws budgets create-budget --account-id "$ACCOUNT_ID" \
  --budget file:///tmp/budget.json \
  --notifications-with-subscribers file:///tmp/notifications.json \
  --profile "$AWS_PROFILE" --region "$BUDGET_REGION" 2>/dev/null || echo "  (budget already exists, skipping)"

# ---------- 2. IAM role for Lambda ----------
echo "== 2. IAM role =="
cat > /tmp/trust-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{ "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole" }]
}
EOF
aws iam create-role --role-name "$ROLE_NAME" \
  --assume-role-policy-document file:///tmp/trust-policy.json \
  --profile "$AWS_PROFILE" >/dev/null 2>&1 || echo "  (role already exists, skipping)"
aws iam attach-role-policy --role-name "$ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole \
  --profile "$AWS_PROFILE" 2>/dev/null || true
aws iam attach-role-policy --role-name "$ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess \
  --profile "$AWS_PROFILE" 2>/dev/null || true
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
echo "  waiting for IAM role to propagate..."
sleep 10

# ---------- 3. DynamoDB table ----------
echo "== 3. DynamoDB table =="
aws dynamodb create-table --table-name "$TABLE_NAME" \
  --attribute-definitions AttributeName=source,AttributeType=S AttributeName=timestamp,AttributeType=S \
  --key-schema AttributeName=source,KeyType=HASH AttributeName=timestamp,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null 2>&1 || echo "  (table already exists, skipping)"

# ---------- 3b. DynamoDB table: live WebSocket connections ----------
echo "== 3b. Connections table =="
aws dynamodb create-table --table-name "$CONNECTIONS_TABLE" \
  --attribute-definitions AttributeName=connectionId,AttributeType=S \
  --key-schema AttributeName=connectionId,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null 2>&1 || echo "  (table already exists, skipping)"
# TTL sweeps up rows whose disconnect event never arrived.
aws dynamodb update-time-to-live --table-name "$CONNECTIONS_TABLE" \
  --time-to-live-specification "Enabled=true,AttributeName=expires_at" \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null 2>&1 || true

# ---------- 4. Lambda: fetch function ----------
echo "== 4. Lambda fetch function =="
(cd lambda_fetch && rm -f function.zip && zip -q function.zip lambda_function.py)
if aws lambda get-function --function-name "$FETCH_FN" --profile "$AWS_PROFILE" --region "$REGION" >/dev/null 2>&1; then
  aws lambda update-function-code --function-name "$FETCH_FN" \
    --zip-file "fileb://lambda_fetch/function.zip" \
    --profile "$AWS_PROFILE" --region "$REGION" >/dev/null
else
  aws lambda create-function --function-name "$FETCH_FN" \
    --runtime python3.12 --role "$ROLE_ARN" --handler lambda_function.handler \
    --timeout 900 --memory-size 128 \
    --zip-file "fileb://lambda_fetch/function.zip" \
    --profile "$AWS_PROFILE" --region "$REGION" >/dev/null
fi
FETCH_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FETCH_FN}"

# ---------- 5. Lambda: api function ----------
echo "== 5. Lambda api function =="
(cd lambda_api && rm -f function.zip && zip -q function.zip lambda_function.py)
if aws lambda get-function --function-name "$API_FN" --profile "$AWS_PROFILE" --region "$REGION" >/dev/null 2>&1; then
  aws lambda update-function-code --function-name "$API_FN" \
    --zip-file "fileb://lambda_api/function.zip" \
    --profile "$AWS_PROFILE" --region "$REGION" >/dev/null
else
  aws lambda create-function --function-name "$API_FN" \
    --runtime python3.12 --role "$ROLE_ARN" --handler lambda_function.handler \
    --timeout 10 --memory-size 128 \
    --zip-file "fileb://lambda_api/function.zip" \
    --profile "$AWS_PROFILE" --region "$REGION" >/dev/null
fi
API_FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${API_FN}"

# ---------- 5b. Lambda: websocket connection handler ----------
echo "== 5b. Lambda websocket function =="
(cd lambda_ws && rm -f function.zip && zip -q function.zip lambda_function.py)
if aws lambda get-function --function-name "$WS_FN" --profile "$AWS_PROFILE" --region "$REGION" >/dev/null 2>&1; then
  aws lambda update-function-code --function-name "$WS_FN" \
    --zip-file "fileb://lambda_ws/function.zip" \
    --profile "$AWS_PROFILE" --region "$REGION" >/dev/null
else
  aws lambda create-function --function-name "$WS_FN" \
    --runtime python3.12 --role "$ROLE_ARN" --handler lambda_function.handler \
    --timeout 10 --memory-size 128 \
    --zip-file "fileb://lambda_ws/function.zip" \
    --profile "$AWS_PROFILE" --region "$REGION" >/dev/null
fi
WS_FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${WS_FN}"

# An existing fetch function keeps its old 30s timeout, so raise it in place.
aws lambda update-function-configuration --function-name "$FETCH_FN" \
  --timeout 900 --profile "$AWS_PROFILE" --region "$REGION" >/dev/null 2>&1 || true

echo "  waiting for functions to become active..."
sleep 8

# ---------- 6. EventBridge schedule (every 30 min in production) ----------
echo "== 6. EventBridge schedule =="
aws events put-rule --name "$RULE_NAME" \
  --schedule-expression "rate(30 minutes)" --state ENABLED \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null
aws lambda add-permission --function-name "$FETCH_FN" \
  --statement-id eventbridge-invoke --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null 2>&1 || true
aws events put-targets --rule "$RULE_NAME" \
  --targets "Id=1,Arn=${FETCH_ARN}" \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null

# ---------- 7. API Gateway ----------
echo "== 7. API Gateway =="
API_ID=$(aws apigatewayv2 get-apis --profile "$AWS_PROFILE" --region "$REGION" \
  --query "Items[?Name=='gold-tracker-api'].ApiId" --output text)
if [ -z "$API_ID" ]; then
  API_RESULT=$(aws apigatewayv2 create-api --name gold-tracker-api --protocol-type HTTP \
    --target "$API_FN_ARN" \
    --cors-configuration AllowOrigins="*",AllowMethods="GET,OPTIONS" \
    --profile "$AWS_PROFILE" --region "$REGION")
  API_ID=$(echo "$API_RESULT" | python3 -c "import json,sys; print(json.load(sys.stdin)['ApiId'])")
fi
API_ENDPOINT="https://${API_ID}.execute-api.${REGION}.amazonaws.com"
aws lambda add-permission --function-name "$API_FN" \
  --statement-id apigateway-invoke-all --action lambda:InvokeFunction \
  --principal apigateway.amazonaws.com \
  --source-arn "arn:aws:execute-api:${REGION}:${ACCOUNT_ID}:${API_ID}/*" \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null 2>&1 || true

# ---------- 7b. API Gateway WebSocket API (live push) ----------
echo "== 7b. WebSocket API =="
WS_API_ID=$(aws apigatewayv2 get-apis --profile "$AWS_PROFILE" --region "$REGION" \
  --query "Items[?Name=='${WS_API_NAME}'].ApiId" --output text)
if [ -z "$WS_API_ID" ] || [ "$WS_API_ID" = "None" ]; then
  WS_API_ID=$(aws apigatewayv2 create-api --name "$WS_API_NAME" --protocol-type WEBSOCKET \
    --route-selection-expression '$request.body.action' \
    --profile "$AWS_PROFILE" --region "$REGION" --query ApiId --output text)
  echo "  created WebSocket API $WS_API_ID"
else
  echo "  reusing WebSocket API $WS_API_ID"
fi

# One AWS_PROXY integration, shared by all three routes.
WS_INTEGRATION=$(aws apigatewayv2 get-integrations --api-id "$WS_API_ID" \
  --profile "$AWS_PROFILE" --region "$REGION" --query "Items[0].IntegrationId" --output text)
if [ -z "$WS_INTEGRATION" ] || [ "$WS_INTEGRATION" = "None" ]; then
  WS_INTEGRATION=$(aws apigatewayv2 create-integration --api-id "$WS_API_ID" \
    --integration-type AWS_PROXY --integration-method POST \
    --integration-uri "arn:aws:apigateway:${REGION}:lambda:path/2015-03-31/functions/${WS_FN_ARN}/invocations" \
    --profile "$AWS_PROFILE" --region "$REGION" --query IntegrationId --output text)
fi

for ROUTE in '$connect' '$disconnect' '$default'; do
  EXISTING=$(aws apigatewayv2 get-routes --api-id "$WS_API_ID" \
    --profile "$AWS_PROFILE" --region "$REGION" \
    --query "Items[?RouteKey=='${ROUTE}'].RouteId" --output text)
  if [ -z "$EXISTING" ] || [ "$EXISTING" = "None" ]; then
    aws apigatewayv2 create-route --api-id "$WS_API_ID" --route-key "$ROUTE" \
      --target "integrations/${WS_INTEGRATION}" \
      --profile "$AWS_PROFILE" --region "$REGION" >/dev/null
    echo "  route $ROUTE created"
  fi
done

aws apigatewayv2 create-stage --api-id "$WS_API_ID" --stage-name "$WS_STAGE" --auto-deploy \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null 2>&1 || echo "  (stage already exists, skipping)"
aws apigatewayv2 create-deployment --api-id "$WS_API_ID" --stage-name "$WS_STAGE" \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null 2>&1 || true

aws lambda add-permission --function-name "$WS_FN" \
  --statement-id apigateway-ws-invoke --action lambda:InvokeFunction \
  --principal apigateway.amazonaws.com \
  --source-arn "arn:aws:execute-api:${REGION}:${ACCOUNT_ID}:${WS_API_ID}/*" \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null 2>&1 || true

WS_ENDPOINT="https://${WS_API_ID}.execute-api.${REGION}.amazonaws.com/${WS_STAGE}"
WS_WSS_URL="wss://${WS_API_ID}.execute-api.${REGION}.amazonaws.com/${WS_STAGE}"

# post_to_connection needs execute-api:ManageConnections - the managed
# DynamoDB/basic-execution policies don't cover it.
cat > /tmp/ws-push-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{ "Effect": "Allow", "Action": ["execute-api:ManageConnections"],
    "Resource": "arn:aws:execute-api:${REGION}:${ACCOUNT_ID}:${WS_API_ID}/*" }]
}
EOF
aws iam put-role-policy --role-name "$ROLE_NAME" --policy-name gold-tracker-ws-push \
  --policy-document file:///tmp/ws-push-policy.json --profile "$AWS_PROFILE" >/dev/null

# Tell both functions where to push and where to look up connections.
aws lambda wait function-updated --function-name "$FETCH_FN" --profile "$AWS_PROFILE" --region "$REGION" 2>/dev/null || true
aws lambda update-function-configuration --function-name "$FETCH_FN" \
  --environment "Variables={CONNECTIONS_TABLE=${CONNECTIONS_TABLE},WS_ENDPOINT=${WS_ENDPOINT},STREAM_SECONDS=840,POLL_SECONDS=2}" \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null
aws lambda wait function-updated --function-name "$WS_FN" --profile "$AWS_PROFILE" --region "$REGION" 2>/dev/null || true
aws lambda update-function-configuration --function-name "$WS_FN" \
  --environment "Variables={CONNECTIONS_TABLE=${CONNECTIONS_TABLE},TABLE_NAME=${TABLE_NAME}}" \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null

# ---------- 7c. EventBridge: keep a stream alive while viewers are connected ----------
echo "== 7c. Streaming schedule =="
# Fires every 15 min; each run polls and pushes for up to 14 min, then exits.
# If nobody is connected the run returns immediately, so an idle dashboard
# costs ~4 short invocations an hour and nothing else.
aws events put-rule --name "$STREAM_RULE_NAME" \
  --schedule-expression "rate(15 minutes)" --state ENABLED \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null
aws lambda add-permission --function-name "$FETCH_FN" \
  --statement-id eventbridge-stream-invoke --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${STREAM_RULE_NAME}" \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null 2>&1 || true
cat > /tmp/stream-target.json <<EOF
[{"Id":"1","Arn":"${FETCH_ARN}","Input":"{\"stream\":true}"}]
EOF
aws events put-targets --rule "$STREAM_RULE_NAME" --targets file:///tmp/stream-target.json \
  --profile "$AWS_PROFILE" --region "$REGION" >/dev/null

# ---------- 8. S3 bucket for dashboard ----------
echo "== 8. S3 + dashboard =="
aws s3api create-bucket --bucket "$BUCKET_NAME" --region "$REGION" \
  --create-bucket-configuration LocationConstraint="$REGION" \
  --profile "$AWS_PROFILE" >/dev/null 2>&1 || echo "  (bucket already exists, skipping)"
aws s3 website "s3://${BUCKET_NAME}/" --index-document index.html \
  --region "$REGION" --profile "$AWS_PROFILE"
aws s3api put-public-access-block --bucket "$BUCKET_NAME" \
  --public-access-block-configuration "BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=false" \
  --region "$REGION" --profile "$AWS_PROFILE"
cat > /tmp/bucket-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{ "Sid": "PublicReadGetObject", "Effect": "Allow", "Principal": "*",
    "Action": "s3:GetObject", "Resource": "arn:aws:s3:::${BUCKET_NAME}/*" }]
}
EOF
aws s3api put-bucket-policy --bucket "$BUCKET_NAME" --policy file:///tmp/bucket-policy.json \
  --region "$REGION" --profile "$AWS_PROFILE"

# Inject the real API + WebSocket URLs into the dashboard before uploading
sed -e "s#https://17gdivfex7.execute-api.ap-south-1.amazonaws.com/#${API_ENDPOINT}/#g" \
    -e "s#wss://WEBSOCKET_ENDPOINT_PLACEHOLDER#${WS_WSS_URL}#g" \
  site/index.html > /tmp/index.html

aws s3 cp /tmp/index.html "s3://${BUCKET_NAME}/index.html" --content-type "text/html" \
  --region "$REGION" --profile "$AWS_PROFILE"
aws s3 cp site/style.css "s3://${BUCKET_NAME}/style.css" --content-type "text/css" \
  --region "$REGION" --profile "$AWS_PROFILE"

S3_WEBSITE="http://${BUCKET_NAME}.s3-website.${REGION}.amazonaws.com"

# ---------- 9. CloudFront (HTTPS) ----------
echo "== 9. CloudFront =="
CF_ID=$(aws cloudfront list-distributions --profile "$AWS_PROFILE" \
  --query "DistributionList.Items[?Comment=='Gold Rate Tracker dashboard'].Id" --output text 2>/dev/null)
if [ -z "$CF_ID" ]; then
  cat > /tmp/cloudfront-config.json <<EOF
{
  "CallerReference": "gold-tracker-$(date +%s)",
  "Comment": "Gold Rate Tracker dashboard",
  "Enabled": true,
  "DefaultRootObject": "index.html",
  "Origins": { "Quantity": 1, "Items": [{
    "Id": "s3-website-origin",
    "DomainName": "${BUCKET_NAME}.s3-website.${REGION}.amazonaws.com",
    "CustomOriginConfig": { "HTTPPort": 80, "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
      "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]} }
  }]},
  "DefaultCacheBehavior": {
    "TargetOriginId": "s3-website-origin",
    "ViewerProtocolPolicy": "redirect-to-https",
    "AllowedMethods": {"Quantity": 2, "Items": ["GET","HEAD"], "CachedMethods": {"Quantity": 2, "Items": ["GET","HEAD"]}},
    "CachePolicyId": "658327ea-f89d-4fab-a63d-7e88639e58f6",
    "Compress": true
  },
  "PriceClass": "PriceClass_100"
}
EOF
  CF_RESULT=$(aws cloudfront create-distribution --distribution-config file:///tmp/cloudfront-config.json --profile "$AWS_PROFILE")
  CF_DOMAIN=$(echo "$CF_RESULT" | python3 -c "import json,sys; print(json.load(sys.stdin)['Distribution']['DomainName'])")
else
  CF_DOMAIN=$(aws cloudfront get-distribution --id "$CF_ID" --profile "$AWS_PROFILE" --query "Distribution.DomainName" --output text)
fi

echo ""
echo "================================================================"
echo " DEPLOYMENT COMPLETE"
echo "================================================================"
echo " Dashboard (S3, works now):    $S3_WEBSITE/index.html"
echo " Dashboard (HTTPS, ~10 min):   https://${CF_DOMAIN}/index.html"
echo " API endpoint:                 ${API_ENDPOINT}/"
echo " WebSocket endpoint:           ${WS_WSS_URL}"
echo " DynamoDB table:                $TABLE_NAME"
echo " Fetch schedule:                every 30 minutes (history)"
echo " Live stream:                   every 15 min, only while viewers connected"
echo "================================================================"
echo ""
echo "Next: give the API endpoint above to whoever consumes this data (e.g. MRPscan backend)."
