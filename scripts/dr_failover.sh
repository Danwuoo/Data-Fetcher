#!/usr/bin/env bash
# 示範跨區域故障切換

PRIMARY="ap-southeast-1"
SECONDARY="ap-northeast-1"
BUCKET="zxq-data"

function switch_primary() {
  echo "將主要區域切換至 $SECONDARY"
  aws s3api put-bucket-replication --bucket "$BUCKET" --replication-configuration file://replication.json
  aws route53 change-resource-record-sets --hosted-zone-id ZONEID --change-batch file://switch.json
  echo "切換完成"
}

switch_primary
