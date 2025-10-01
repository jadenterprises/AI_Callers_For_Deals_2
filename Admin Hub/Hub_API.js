// Hub_API.js - Main API handler (called from Bootstrap.js)

function json_(o) { 
  return ContentService.createTextOutput(JSON.stringify(o))
    .setMimeType(ContentService.MimeType.JSON); 
}

function cooldownRemainingSeconds_(key, minutes) {
  const ts = Number(PropertiesService.getScriptProperties().getProperty('COOLDOWN_' + key) || 0);
  if (!ts) return 0;
  const until = ts + minutes * 60 * 1000;
  return Math.max(0, Math.ceil((until - Date.now()) / 1000));
}

function markCooldownNow_(key) {
  PropertiesService.getScriptProperties().setProperty('COOLDOWN_' + key, String(Date.now()));
}

// Main POST handler for Hub API (called from Bootstrap.js doPost)
function doPostHub(e) {
  let params;
  try {
    params = JSON.parse(e.postData.contents);
  } catch (err) {
    return json_({ ok: false, error: 'INVALID_REQUEST_BODY' });
  }

  const token = String(params.token || '');
  
  if (token !== CFG().CLIENT_TOKEN) {
    return json_({ ok: false, error: 'UNAUTHORIZED' });
  }

  const action = String(params.action || '').toLowerCase();
  console.log(`Hub API: Received action '${action}'.`);
  
  if (action === 'balance') {
    return json_({ ok: true, balance: getBalance_() });
  }

  if (action === 'send_run') {
    const run = parseInt(params.run || '1', 10);
    const count = parseInt(params.count || '0', 10);
    if (run < 1 || run > 8) {
      return json_({ ok: false, error: 'Invalid run number.' });
    }
    
    // Check if function exists
    if (typeof startHubSendRun !== 'function') {
      return json_({ ok: false, error: 'Send function not available' });
    }
    
    markCooldownNow_('send');
    const TAB_OUTBOUND = 'Outbound Leads'; // Define here if not imported
    const result = startHubSendRun(TAB_OUTBOUND, run, count);
    return json_(result);
  }

  if (action === 'cooldown_status') {
    const COOLDOWN_MINUTES_CLIENT = 21; // Default value
    const C = CFG();
    const sendCooldown = COOLDOWN_MINUTES_CLIENT;
    const ingestCooldown = C.INGEST_COOLDOWN_MIN || 20;
    return json_({
      ok: true,
      send_seconds: cooldownRemainingSeconds_('send', sendCooldown),
      ingest_seconds: cooldownRemainingSeconds_('ingest', ingestCooldown)
    });
  }

  if (action === 'ingest') {
    // Check if function exists
    if (typeof hubIngestWebhooks_ !== 'function') {
      return json_({ ok: false, error: 'Ingest function not available' });
    }
    
    const ingestCooldown = CFG().INGEST_COOLDOWN_MIN || 20;
    const remaining = cooldownRemainingSeconds_('ingest', ingestCooldown);
    if (remaining > 0) {
      return json_({ ok: false, error: `Please wait ${Math.ceil(remaining/60)} min to ingest.` });
    }
    markCooldownNow_('ingest');
    const result = hubIngestWebhooks_();
    return json_(result);
  }

  if (action === 'portal_auth') {
    const pass = String(params.password || '');
    const valid = (pass && CFG().PORTAL_PASSWORD && pass === CFG().PORTAL_PASSWORD);
    return json_({ ok: valid });
  }

  return json_({ ok: false, error: 'UNKNOWN_ACTION' });
}

/**
 * Deletes any existing webhook polling triggers and creates a new one
 * that runs every 5 minutes. Stores the trigger's start time and ID.
 */
function startWebhookIngestPolling_() {
  const handlerFunction = 'hubIngestWebhooks_';

  // 1. Delete any existing triggers for this handler to prevent duplicates.
  const allTriggers = ScriptApp.getProjectTriggers();
  for (const trigger of allTriggers) {
    if (trigger.getHandlerFunction() === handlerFunction) {
      ScriptApp.deleteTrigger(trigger);
      console.log(`Deleted existing polling trigger: ${trigger.getUniqueId()}`);
    }
  }

  // 2. Create a new trigger to run every 5 minutes.
  const newTrigger = ScriptApp.newTrigger(handlerFunction)
    .timeBased()
    .everyMinutes(5)
    .create();
  console.log(`Created new 5-minute polling trigger: ${newTrigger.getUniqueId()}`);

  // 3. Store the creation time and ID to manage its 90-minute lifespan.
  const P = PropertiesService.getScriptProperties();
  P.setProperties({
    'WEBHOOK_POLL_START_TIME': Date.now(),
    'WEBHOOK_POLL_TRIGGER_ID': newTrigger.getUniqueId()
  });
}