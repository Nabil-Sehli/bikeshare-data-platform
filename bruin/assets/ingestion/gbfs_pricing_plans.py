"""@bruin
name: bruin_raw.gbfs_pricing_plans
type: python
image: python:3.12
connection: warehouse
description: Current Citi Bike pricing plans from the GBFS system_pricing_plans feed.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: plan_id
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: currency
    type: varchar
    checks:
      - name: accepted_values
        value: ["USD"]
  - name: unlock_price
    type: float
    checks:
      - name: not_null
      - name: min
        value: 0
  - name: per_minute_rate
    type: float
    checks:
      - name: min
        value: 0
@bruin"""

from datetime import datetime, timezone

import pandas as pd
import requests

FEED = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/system_pricing_plans.json"


def materialize(**kwargs):
    feed = requests.get(FEED, timeout=30).json()
    loaded_at = datetime.now(timezone.utc)
    rows = []
    for plan in feed["data"]["plans"]:
        per_min = plan.get("per_min_pricing") or [{}]
        rows.append(
            {
                "plan_id": plan["plan_id"],
                "name": plan.get("name"),
                "currency": plan.get("currency"),
                "unlock_price": float(plan.get("price") or 0),
                "per_minute_rate": float(per_min[0].get("rate") or 0),
                "is_taxable": bool(plan.get("is_taxable")),
                "description": plan.get("description"),
                "loaded_at": loaded_at,
            }
        )
    return pd.DataFrame(rows)
