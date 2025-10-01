import base64
import json
import os
import io
import csv
from datetime import datetime
from typing import Dict, Any, Tuple

import pandas as pd
from google.cloud import firestore, bigquery, storage
import functions_framework

# ───────────────────────── Configuration ──────────────────────────
PROJECT_ID = os.getenv("GCP_PROJECT", "retell-calling")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "retell-calling-reference-data")
# Optional: GCS URI "bucket/path/to/agent_config.json" containing agent config
AGENT_CONFIG_URI = os.getenv("AGENT_CONFIG_URI", "")

# A single, ever‑growing CSV: rows are appended each run
GCS_CSV_PATH = "raw_leads/inbound_webhook.csv"

BQ_DATASET_ID = "lead_warehouse"
# Set to "" to disable BigQuery logging
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID", "retell_call_history")

LEADS_COLLECTION = "leads"

# ───────────────────── Approved Retell agent IDs ──────────────────
AGENT_CONFIG = {
    # ── CORE agents (unchanged) ──
    "agent_4f2c32a8c2ca2ba51b4424e79b": {
        "bucket": "retell-calling-reference-data",
        "csv_path": "raw_leads/inbound_webhook.csv",
        "use_firestore": True,
        "key_column": "Firestore_ID",
    },
    "agent_ac2199cdcb5af27a4e0684035e": {
        "bucket": "retell-calling-reference-data",
        "csv_path": "raw_leads/inbound_webhook.csv",
        "use_firestore": True,
        "key_column": "Firestore_ID",
    },
    "agent_75254a5a68eaf2de1c6b108e38": {
        "bucket": "retell-calling-reference-data",
        "csv_path": "raw_leads/inbound_webhook.csv",
        "use_firestore": True,
        "key_column": "Firestore_ID",
    },
    "agent_011ed302f36f8b8f67b3828546": {
        "bucket": "retell-calling-reference-data",
        "csv_path": "raw_leads/inbound_webhook.csv",
        "use_firestore": True,
        "key_column": "Firestore_ID",
    },

    # ── Football agent ──
    "agent_e1931906c2d794eaf3ec30a296": {
        "bucket": "ial-football-retell-calling-reference-data-lztx9c",
        "csv_path": "raw_leads/inbound_webhook.csv",
        "use_firestore": False,
        "key_column": "Phone",
    },

}

# Will be replaced by contents of AGENT_CONFIG_URI if present
ALLOWED_AGENT_IDS = set(AGENT_CONFIG)

# ───────────────────── Initialise Cloud clients ───────────────────
try:
    db = firestore.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)
    print("Google‑Cloud clients initialised successfully.")
except Exception as e:
    print(f"CRITICAL: failed to initialise clients – {e}")
    db = bq_client = storage_client = None


def _load_agent_config_from_gcs(uri: str):
    """Load agent configuration mapping from a JSON file in GCS.

    The file must have the structure:

    {
      "agents": {
        "agent_id": {
          "bucket": "...",
          "csv_path": "...",
          "use_firestore": false,
          "key_column": "Phone"
        }
      }
    }
    """

    if not (uri and storage_client):
        return None
    try:
        bucket, path = uri.split("/", 1)
        blob = storage_client.bucket(bucket).blob(path)
        data = json.loads(blob.download_as_text())
        return data.get("agents", {})
    except Exception as exc:  # pragma: no cover - best effort only
        print(f"Failed to load agent config from {uri}: {exc}")
        return None


_dynamic_agents = _load_agent_config_from_gcs(AGENT_CONFIG_URI)
if _dynamic_agents:
    AGENT_CONFIG = _dynamic_agents
    ALLOWED_AGENT_IDS = set(AGENT_CONFIG)

# ──────────────────────── CSV schema / headers ─────────────────────
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
    "Summery",
    "Quality", 
    "Disconnection Reason",
    "Recording",
    # NEW COLUMN – filled with Firestore document ID
    "Firestore_ID",
    # Flags for downstream processing
    "Processed",
    "Sector Processed",
    # ─── NEW Vista‑only columns ───
    "Interested", "Liquid To Invest", "Job",
    "Follow Up", "Past Experience","summary"
]

# ─────────────────── Helper: flexible key lookup ───────────────────
VAR_ALIASES = {
    "First Name": ("first_name", "First Name", "firstName"),
    "Last Name": ("last_name", "Last Name", "lastName"),
    "Address": ("address", "Address"),
    "City": ("city", "City"),
    "State": ("state", "State", "Input State"),
    "Zip": ("zip", "Zip", "zip_code", "zipCode"),
    "Email": ("email", "Email", "Input Email"),
    "Recording": ("recording_url", " recording_url", "recording_url ", "recording",)
}

def get_var(vars_dict: dict, *keys, default="") -> str:
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
            if ts > 10 ** 12:  # ms → s
                ts /= 1000
        return datetime.fromtimestamp(ts)
    except Exception as e:
        print(f"Warning: bad timestamp {ts}: {e}")
    return None


# ──────────────── Build a one‑row dict for CSV ─────────────────────

def build_row(call: dict, vars_: dict, analysis: dict, cost: dict) -> Dict[str, Any]:
    def a_field(*keys, default=""):
        if not isinstance(analysis, dict):
            return default
        # normalise keys by stripping whitespace
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
        "Summery": a_field("_summary", "_call_summary", "_call _summery", "_Summary", "Summary","summary"),
        "Quality": a_field("_quality"),
        "Disconnection Reason": call.get("disconnection_reason", ""),
        "Firestore_ID": vars_.get("firestore_doc_id", ""),
        "Processed": "",
        "Sector Processed": "",
        # ─── Vista additions ───
        "Interested": str(a_field("_interested")).lower(),
        "Liquid To Invest": str(a_field("_liquid_to_invest", "_liquid _to _invest")).lower(),
        "Job": a_field("_job"),
        "Follow Up": a_field("_follow_up", "_follow _up"),
        "Past Experience": str(a_field("_past_oil", "_past _oil", "_past_experience", "past_experience")).lower(),
        "Recording": get_var(vars_, *VAR_ALIASES["Recording"]),
    }


# ─────────────── Helper: append to a single CSV file ───────────────

def append_to_gcs_csv(bucket_name: str, path: str, new_df: pd.DataFrame,
                      key_column: str = None):
    """Atomic read‑append‑write with optimistic locking."""
    bucket = storage_client.bucket(bucket_name)
    blob   = bucket.blob(path)

    # Load existing data if the file exists
    try:
        existing_bytes = blob.download_as_bytes()
        existing_df    = pd.read_csv(io.BytesIO(existing_bytes))
    except Exception:
        existing_df    = pd.DataFrame(columns=HEADERS)

    # Align columns in case of new headers
    if not existing_df.columns.equals(new_df.columns):
        existing_df = existing_df.reindex(columns=HEADERS)

    combined_df = pd.concat([existing_df, new_df], ignore_index=True)

    # Deduplicate on the chosen key
    if key_column and key_column in combined_df.columns:
        combined_df[key_column] = combined_df[key_column].astype(str)
        combined_df.drop_duplicates(subset=[key_column],
                                     keep="last", inplace=True)

    # ---- write back (all cells quoted) ----
    blob.upload_from_string(
        combined_df.to_csv(index=False, quoting=csv.QUOTE_ALL),  # always‑quote
        content_type="text/csv",
        if_generation_match=blob.generation or 0                # fail if blob changed mid‑flight
    )

    print(
        f"Appended {len(new_df)} row(s) to gs://{bucket_name}/{path}. "
        f"Total rows: {len(combined_df)}"
    )


# ─────────────── Firestore + BigQuery helpers ─────────────────────

def get_lead_by_id(doc_id: str):
    if not (db and doc_id):
        return None, None
    try:
        ref = db.collection(LEADS_COLLECTION).document(doc_id)
        doc = ref.get()
        return (ref, doc.to_dict()) if doc.exists else (None, None)
    except Exception as e:
        print(f"Error loading lead {doc_id}: {e}")
        return None, None


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
            "call_duration_ms": call.get("call_cost", {}).get("total_duration_seconds", 0)
            * 1000,
            "transcript": json.dumps(call.get("transcript")),
            "full_webhook_payload": json.dumps(payload),
        }
        errors = bq_client.insert_rows_json(table_ref, [row])
        if errors:
            print(f"BigQuery insert errors: {errors}")
    except Exception as e:
        print(f"BigQuery logging failed: {e}")


# ───────────────── Cloud Function entry‑point ──────────────────────
@functions_framework.http
def retell_webhook_endpoint(request):
    if not all([db, bq_client, storage_client]):
        return ("Internal server error: clients not configured.", 500)

    if not request.is_json:
        return ("Invalid payload: content‑type must be application/json.", 400)

    payload = request.get_json(silent=True)
    if not payload:
        return ("Empty or invalid JSON payload.", 400)

    if payload.get("event") != "call_analyzed":
        return ("Skipping non‑analysis event.", 200)

    call = payload.get("data") or payload.get("call", {})
    if not call:
        return ("Invalid payload structure.", 400)

    agent_id = call.get("agent_id")
    cfg = AGENT_CONFIG.get(agent_id)
    if not cfg:
        print(f"Ignoring call from unapproved agent {agent_id}")
        return ("Call from unapproved agent ignored.", 200)

    # swap bucket/path dynamically
    KEY_COL = (
        cfg.get("key_column")
        or cfg.get("keyColumn")
        or cfg.get("key")
        or "Firestore_ID"
    )
    USE_FIRESTORE = cfg.get("use_firestore")
    if USE_FIRESTORE is None:
        USE_FIRESTORE = cfg.get("firestore")
    if USE_FIRESTORE is None:
        # Legacy behaviour defaults to Firestore sync enabled
        USE_FIRESTORE = True

    # Log raw payload to BigQuery
    log_to_bigquery(payload, call)

    vars_ = call.get("retell_llm_dynamic_variables", {}) or {}
    analysis = call.get("call_analysis", {}).get("custom_analysis_data", {}) or {}
    cost = call.get("call_cost", {}) or {}

    row = build_row(call, vars_, analysis, cost)
    if not row["Date"]:
        return ("Webhook logged, but missing/invalid end_timestamp. Skipping CSV.", 200)

    try:
        df = pd.DataFrame([row], columns=HEADERS)
        bucket_name = (
            cfg.get("bucket")
            or cfg.get("bucket_name")
            or cfg.get("bucketName")
            or BUCKET_NAME
        )
        csv_path = cfg.get("csv_path") or cfg.get("path") or GCS_CSV_PATH
        append_to_gcs_csv(bucket_name, csv_path, df, key_column=KEY_COL)
    except Exception as e:
        print(f"CRITICAL: unable to update inbound_webhook.csv – {e}")

    # ─────── Firestore side‑effects (unchanged) ────────
    if not USE_FIRESTORE:
        return ("CSV written (Vista – no Firestore sync).", 200)

    firestore_doc_id = vars_.get("firestore_doc_id")
    if not firestore_doc_id:
        return ("CSV written, but payload lacks firestore_doc_id.", 200)

    lead_ref, lead_data = get_lead_by_id(firestore_doc_id)
    if not lead_ref:
        return (f"CSV written, but lead {firestore_doc_id} not found.", 200)

    call_id = call.get("call_id")
    if lead_data and any(
        d.get("call_id") == call_id for d in lead_data.get("disposition_history", [])
    ):
        return (f"CSV written, duplicate call_id {call_id} ignored.", 200)

    try:
        @firestore.transactional
        def txn(t, ref, r):
            entry = {
                "call_id": call_id,
                "timestamp": datetime.utcnow(),
                "disposition": r.get("Correct Name", "").strip(),
            }
            t.update(
                ref,
                {
                    "call_attempts": firestore.Increment(1),
                    "last_call_timestamp": datetime.utcnow(),
                    "disposition_history": firestore.ArrayUnion([entry]),
                    "disposition": r.get("Correct Name", "").strip(),
                    "Status": "Called",
                    "analysis_email": r.get("Email Given"),
                    "analysis_state": r.get("State Given"),
                    "analysis_accredited": r.get("Accredited"),
                    "analysis_new_investments": r.get("New Investments"),
                    "analysis_sectors": r.get("Sectors"),
                    "analysis_dnc": r.get("DNC"),
                    "analysis_summary": r.get("Summery"),
                    "analysis_quality": r.get("Quality"), 
                    "call_duration_seconds": r.get("Call Time"),
                    "disconnection_reason": r.get("Disconnection Reason"),
                    "processed": False,
                    "sector_processed": False,
                },
            )

        txn(db.transaction(), lead_ref, row)
        print(f"Lead {lead_ref.id} updated with call results.")
    except Exception as e:
        print(f"Firestore transaction failed for {lead_ref.id}: {e}")
        return ("Error updating Firestore.", 500)

    return ("Webhook processed successfully.", 200)