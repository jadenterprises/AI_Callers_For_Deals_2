"""
Template handler for new client campaigns.

This handler is designed to be used for new clients created through
the New_Deal_Creation process. It dynamically receives its storage
configuration (bucket, CSV path, etc.) from the main router,
based on the `agent_config.json` file.

The constants defined below (BUCKET_NAME, etc.) serve only as
fallbacks and should not need to be edited for new deployments.
The router provides the correct values at runtime.
"""

import csv
import io
import json
import os
from datetime import datetime
from typing import Any, Dict

import pandas as pd
from google.cloud import bigquery, storage

PROJECT_ID = os.getenv("GCP_PROJECT", "retell-calling")
BQ_DATASET_ID = "lead_warehouse"
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID", "retell_call_history")

# Fallback constants if not provided by the router config.
# The `New_Deal_Creation` process and `agent_config.json` will override these.
BUCKET_NAME = "REPLACE_ME_BUCKET"
CSV_PATH = "raw_leads/inbound_webhook.csv"
KEY_COLUMN = "Phone"

try:
    bq_client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)
except Exception as exc:  # pragma: no cover
    bq_client = storage_client = None
    print(f"client_template: failed to init cloud clients: {exc}")

HEADERS = [
    "Date",
    "Phone",
    "Call Time",
    "First Name",
    "Last Name",
    "Address",
    "City",
    "Input State",
    "State Given",
    "Zip",
    "Input Email",
    "Email Given",
    "Accredited",
    "Correct Name",
    "New Investments",
    "Sectors",
    "DNC",
    "Summary",
    "Quality",
    "Disconnection Reason",
    "Interested",
    "Liquid To Invest",
    "Job",
    "Follow Up",
    "Past Experience",
    "Recording",
]

VAR_ALIASES = {
    "First Name": ("first_name", "First Name", "firstName"),
    "Last Name": ("last_name", "Last Name", "lastName"),
    "Address": ("address", "Address"),
    "City": ("city", "City"),
    "State": ("state", "State", "Input State"),
    "Zip": ("zip", "Zip", "zip_code", "zipCode"),
    "Email": ("email", "Email", "Input Email"),
}


def get_var(vars_dict: dict, *keys, default: str = "") -> str:
    if not isinstance(vars_dict, dict):
        return default
    for k in keys:
        if k in vars_dict:
            return vars_dict[k]
    return default


def normalize_phone(raw: str) -> str:
    if not raw or not isinstance(raw, str):
        return ""
    digits = "".join(ch for ch in raw if ch.isdigit())
    return digits[1:] if len(digits) == 11 and digits.startswith("1") else digits


def get_valid_datetime(ts):
    try:
        if ts and isinstance(ts, (int, float)):
            if ts > 10 ** 12:
                ts /= 1000
        return datetime.fromtimestamp(ts)
    except Exception as exc:
        print(f"client_template warning: bad timestamp {ts}: {exc}")
    return None


def build_row(call: dict, vars_: dict, analysis: dict, cost: dict) -> Dict[str, Any]:
    def a_field(*keys, default: str = "") -> str:
        if not isinstance(analysis, dict):
            return default
        norm = {str(k).strip(): v for k, v in analysis.items()}
        for k in keys:
            if k in norm:
                return norm[k]
        return default

    end_dt = get_valid_datetime(call.get("end_timestamp"))
    return {
        "Date": end_dt.strftime("%Y-%m-%d %H:%M:%S") if end_dt else "",
        "Phone": normalize_phone(call.get("to_number", "")),
        "Call Time": cost.get("total_duration_seconds", ""),
        "First Name": get_var(vars_, *VAR_ALIASES["First Name"]),
        "Last Name": get_var(vars_, *VAR_ALIASES["Last Name"]),
        "Address": get_var(vars_, *VAR_ALIASES["Address"]),
        "City": get_var(vars_, *VAR_ALIASES["City"]),
        "Input State": get_var(vars_, *VAR_ALIASES["State"]),
        "State Given": a_field("_state"),
        "Zip": get_var(vars_, *VAR_ALIASES["Zip"]),
        "Input Email": get_var(vars_, *VAR_ALIASES["Email"]),
        "Email Given": a_field("_email"),
        "Accredited": str(a_field("_accredited_investor", "_accredited _investor")).lower(),
        "Correct Name": str(a_field("_correct_name", "_correct _name")).lower(),
        "New Investments": str(a_field("_new_investments", "_new _investments")).lower(),
        "Sectors": a_field("_investment_sectors", "_investment _sectors"),
        "DNC": str(a_field("_dnc", "_d_n_c")).lower(),
        "Summary": a_field("_summary", "_call_summary", "_call _summary"),
        "Quality": a_field("_quality"),
        "Disconnection Reason": call.get("disconnection_reason", ""),
        "Interested": str(a_field("_interested")).lower(),
        "Liquid To Invest": str(a_field("_liquid_to_invest", "_liquid _to _invest")).lower(),
        "Job": a_field("_job"),
        "Follow Up": a_field("_follow_up", "_follow _up"),
        "Past Experience": str(a_field("_past_experience", "_past _experience")).lower(),
        "Recording": analysis.get("recording_url", ""),
    }


def append_to_gcs_csv(
    bucket_name: str, path: str, new_df: pd.DataFrame, key_column: str
):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(path)

    try:
        existing_bytes = blob.download_as_bytes()
        existing_df = pd.read_csv(io.BytesIO(existing_bytes))
    except Exception:
        existing_df = pd.DataFrame(columns=HEADERS)

    if not existing_df.columns.equals(new_df.columns):
        existing_df = existing_df.reindex(columns=HEADERS)

    combined_df = pd.concat([existing_df, new_df], ignore_index=True)

    if key_column in combined_df.columns:
        combined_df[key_column] = combined_df[key_column].astype(str)
        combined_df.drop_duplicates(subset=[key_column], keep="last", inplace=True)

    blob.upload_from_string(
        combined_df.to_csv(index=False, quoting=csv.QUOTE_ALL),
        content_type="text/csv",
        if_generation_match=blob.generation or 0,
    )


def log_to_bigquery(payload: dict, call: dict):
    if not (bq_client and BQ_TABLE_ID):
        return
    table_ref = f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
    try:
        analysis = call.get("call_analysis", {}).get("custom_analysis_data", {})
        row = {
            "ingestion_timestamp": datetime.utcnow().isoformat(),
            "call_id": call.get("call_id"),
            "to_number": call.get("to_number"),
            "from_number": call.get("from_number"),
            "disposition": str(
                analysis.get("_correct_name", analysis.get("_correct _name", ""))
            ),
            "retell_agent_id": call.get("agent_id"),
            "call_duration_ms": call.get("call_cost", {}).get("total_duration_seconds", 0) * 1000,
            "transcript": json.dumps(call.get("transcript")),
            "full_webhook_payload": json.dumps(payload),
        }
        errors = bq_client.insert_rows_json(table_ref, [row])
        if errors:
            print(f"client_template BigQuery insert errors: {errors}")
    except Exception as exc:
        print(f"client_template BigQuery logging failed: {exc}")


def handle(payload: dict, call: dict, config: dict):
    if not storage_client:
        print("client_template: storage client missing – aborting.")
        return

    log_to_bigquery(payload, call)

    vars_ = call.get("retell_llm_dynamic_variables", {}) or {}
    analysis = call.get("call_analysis", {}).get("custom_analysis_data", {}) or {}
    analysis["recording_url"] = call.get("recording_url", "")
    cost = call.get("call_cost", {}) or {}

    row = build_row(call, vars_, analysis, cost)
    if not row["Date"]:
        print("client_template: invalid/missing end_timestamp, skipping.")
        return

    try:
        # Get storage config from the routed agent_config, with fallbacks to defaults
        bucket_name = config.get("bucket_name") or config.get("bucket", BUCKET_NAME)
        csv_path = config.get("csv_path", CSV_PATH)
        key_column = config.get("key_column", KEY_COLUMN)

        df = pd.DataFrame([row], columns=HEADERS)
        append_to_gcs_csv(bucket_name, csv_path, df, key_column)
    except Exception as exc:
        print(f"client_template: unable to update CSV – {exc}")