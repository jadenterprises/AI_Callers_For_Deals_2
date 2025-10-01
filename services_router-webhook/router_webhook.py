"""
router_webhook.py
─────────────────────────────────────────────────────────────
Single Cloud‑Function entry point for all Retell webhooks.

* Validates the inbound JSON.
* Looks up `agent_id` in AGENT_CONFIGS.
* Dynamically imports the designated module
  and calls module.handle(payload, call, config).
* Each handler module owns its own HEADERS, build_row(),
  Firestore / CSV logic, etc., but can be overridden by the
  per-agent config.

Deploy as a 1st‑gen or 2nd‑gen Cloud Function with:
  gcloud functions deploy retell-webhook \
     --runtime python312 \
     --entry-point retell_webhook_router \
     --trigger-http \
     --region us-central1
"""

import importlib
import json
import os
import traceback
from typing import Callable, Dict

import functions_framework
from google.cloud import logging  # optional but recommended
from google.cloud import storage

# ────────────────────────────────────────────────────────────
# 1)  ROUTING TABLE  – add / remove lines as campaigns change
# ────────────────────────────────────────────────────────────
# Optional: GCS URI "bucket/path/to/agent_config.json"
AGENT_CONFIG_URI = os.getenv("AGENT_CONFIG_URI", "")

# Default fallback configs; will be replaced if AGENT_CONFIG_URI is provided
DEFAULT_AGENT_CONFIGS: Dict[str, Dict] = {
    # ------- Core agents (Firestore + CSV) -------
    "agent_ac2199cdcb5af27a4e0684035e": {
        "handler": "handlers.core",
        "bucket_name": "retell-calling-reference-data",
        "csv_path": "raw_leads/inbound_webhook.csv",
        "key_column": "Phone",
    },
    "agent_75254a5a68eaf2de1c6b108e38": {
        "handler": "handlers.core",
        "bucket_name": "retell-calling-reference-data",
        "csv_path": "raw_leads/inbound_webhook.csv",
        "key_column": "Phone",
    },
    "agent_011ed302f36f8b8f67b3828546": {
        "handler": "handlers.core",
        "bucket_name": "retell-calling-reference-data",
        "csv_path": "raw_leads/inbound_webhook.csv",
        "key_column": "Phone",
    },
    "agent_4f2c32a8c2ca2ba51b4424e79b": {
        "handler": "handlers.core",
        "bucket_name": "retell-calling-reference-data",
        "csv_path": "raw_leads/inbound_webhook.csv",
        "key_column": "Phone",
    },
    # ------- Football agent (CSV-only) -------
    "agent_e1931906c2d794eaf3ec30a296": {
        "handler": "handlers.football",
        "bucket_name": "ial-football-retell-calling-reference-data-lztx9c",
        "csv_path": "raw_leads/inbound_webhook.csv",
        "key_column": "Phone",
    },
}

AGENT_CONFIGS: Dict[str, Dict] = DEFAULT_AGENT_CONFIGS.copy()


# ────────────────────────────────────────────────────────────
# 2)  OPTIONAL – Cloud Logging for better observability
#     Comment these three lines out if you don't need them.
# ────────────────────────────────────────────────────────────
_log_client = logging.Client()
_log = _log_client.logger("retell-router")

# Initialise a storage client for optional dynamic config
try:
    _storage_client = storage.Client(project=os.getenv("GCP_PROJECT", "retell-calling"))
except Exception as exc:  # pragma: no cover
    _storage_client = None
    print(f"router-webhook: storage client init failed: {exc}")


def _load_handlers_from_gcs(uri: str) -> Dict[str, Dict]:
    """Return {agent_id: full_config_dict} loaded from GCS JSON."""
    if not (uri and _storage_client):
        return None
    try:
        bucket, path = uri.split("/", 1)
        blob = _storage_client.bucket(bucket).blob(path)
        data = json.loads(blob.download_as_text())
        # Return the entire config for each agent that has a handler defined
        return {
            aid: cfg for aid, cfg in data.get("agents", {}).items() if "handler" in cfg
        }
    except Exception as exc:  # pragma: no cover - best effort
        print(f"router-webhook: failed to load handler config from {uri}: {exc}")
        return None


_dynamic_configs = _load_handlers_from_gcs(AGENT_CONFIG_URI)
if _dynamic_configs:
    AGENT_CONFIGS.update(_dynamic_configs)


# ────────────────────────────────────────────────────────────
# 3)  INTERNAL HELPERS
# ────────────────────────────────────────────────────────────
def _import_handle(modpath: str) -> Callable[[dict, dict, dict], None]:
    """
    Dynamically import `modpath` and return its `handle` function.
    Raises AttributeError if the function is missing.
    """
    module = importlib.import_module(modpath)
    try:
        return getattr(module, "handle")
    except AttributeError as exc:  # pragma: no cover
        raise AttributeError(
            f"Module {modpath!r} must expose a top‑level `handle(payload, call, config)`"
        ) from exc


def _log_struct(severity: str, message: str, **kwargs) -> None:
    """Helper for structured logging that appears in Cloud Logging."""
    _log.log_struct(
        {"message": message, **kwargs},
        severity=severity,
    )


# ────────────────────────────────────────────────────────────
# 4)  CLOUD‑FUNCTION ENTRY POINT
# ────────────────────────────────────────────────────────────
@functions_framework.http
def retell_webhook_router(request):
    """
    Cloud Functions (Python) HTTP handler.
    """

    # -------- Basic HTTP / JSON validation --------
    if request.method != "POST":
        return "method not allowed – use POST", 405
    if not request.is_json:
        return "content‑type must be application/json", 400

    payload: dict = request.get_json(silent=True) or {}
    if payload.get("event") != "call_analyzed":
        return "event ignored", 200

    call: dict = payload.get("data") or payload.get("call", {})
    agent_id: str = call.get("agent_id", "")

    if not agent_id:
        return "missing agent_id", 400

    # -------- Routing --------
    agent_config = AGENT_CONFIGS.get(agent_id)
    if agent_config is None:
        _log_struct(
            "WARNING",
            "Unmapped agent – dropping payload",
            agent_id=agent_id,
            call_id=call.get("call_id"),
        )
        return "agent not routed", 200

    # -------- Dynamic dispatch to handler --------
    try:
        modpath = agent_config["handler"]
        handle = _import_handle(modpath)
        handle(payload, call, agent_config)  # <-- your per‑agent logic
        return "ok", 200

    except Exception as exc:  # pragma: no cover
        # Ensure stack trace is visible in Cloud Logging
        _log_struct(
            "ERROR",
            "handler threw exception",
            agent_id=agent_id,
            call_id=call.get("call_id"),
            error=str(exc),
            traceback=traceback.format_exc(),
        )
        # Returning 500 allows Retell to retry the webhook.
        return "handler failed", 500