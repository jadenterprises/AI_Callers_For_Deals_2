/** Results Sheet — Webhook Ingestion Menu
 * Adds a menu to ingest the latest webhook CSV via your Hub, and (optionally) sweep recalls.
 * Script properties required:
 *   HUB_URL       (string)  – Hub Web App URL
 *   CLIENT_TOKEN  (string)  – client token the Hub validates
 */
function _getPhoneSetFromSheet_(ss, tabName) {
  const sheet = ss.getSheetByName(tabName);
  if (!sheet) return new Set();

  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  const phoneCol = headers.indexOf("Phone");
  if (phoneCol === -1) return new Set();

  const set = new Set();
  for (let i = 1; i < data.length; i++) {
    const phone = String(data[i][phoneCol]).replace(/\D/g, '');
    if (phone.length >= 10) set.add(phone.slice(-10)); // Normalize to last 10 digits
  }
  return set;
}

function _listsSpreadsheet_() {
  const id = PropertiesService.getScriptProperties().getProperty('RESULTS_SS_ID');
  return SpreadsheetApp.openById(id);
}

function CFG_() {
  const P = PropertiesService.getScriptProperties();
  return {
    HUB_URL: P.getProperty('HUB_URL') || '',
    CLIENT_TOKEN: P.getProperty('CLIENT_TOKEN') || ''
  };
}

function onOpen() {
  const ui = SpreadsheetApp.getUi();

  // Original Menu
  ui.createMenu('Get Call Results')
    .addItem('Load Call Results', 'menuIngestWebhooks')
    .addToUi();

  // New Config Menu
  ui.createMenu('🛠️ Config')
    .addItem('Load Config from Record', 'manualConfigFromProvisionRecord')
    .addToUi();
}

/** 1) Ingest webhook CSV via Hub (Archive + Results updates happen server-side) */
function menuIngestWebhooks() {
  const ui = SpreadsheetApp.getUi();
  const { HUB_URL, CLIENT_TOKEN } = CFG_();
  if (!HUB_URL || !CLIENT_TOKEN) {
    ui.alert('Missing HUB_URL or CLIENT_TOKEN in Script properties.');
    return;
  }

  const lock = LockService.getDocumentLock();
  if (!lock.tryLock(10 * 1000)) {
    ui.alert('An operation is in progress. Try again in a few seconds.');
    return;
  }

  try {
    SpreadsheetApp.getActive().toast('Ingesting webhook data…', 'Webhooks', 5);
    const res = UrlFetchApp.fetch(HUB_URL, {
      method: 'post',
      contentType: 'application/json',
      muteHttpExceptions: true,
      payload: JSON.stringify({ action: 'ingest', token: CLIENT_TOKEN })
    });
    const code = res.getResponseCode();
    let body = {};
    try { body = JSON.parse(res.getContentText() || '{}'); } catch (_) {}
    if (code >= 200 && code < 300 && body.ok) {
      ui.alert('Webhook ingest complete.\nArchive + Results updated.');
    } else {
      ui.alert('Ingest error: ' + (body.error || `HTTP ${code}`));
    }
  } catch (e) {
    ui.alert('Unexpected error: ' + e);
  } finally {
    lock.releaseLock();
  }
}

/** 2) (Optional) Sweep due recalls from Archive → Outbound:Recycle via Hub */
function menuSweepRecalls() {
  const ui = SpreadsheetApp.getUi();
  const { HUB_URL, CLIENT_TOKEN } = CFG_();
  if (!HUB_URL || !CLIENT_TOKEN) {
    ui.alert('Missing HUB_URL or CLIENT_TOKEN in Script properties.');
    return;
  }
  try {
    SpreadsheetApp.getActive().toast('Sweeping due recalls…', 'Webhooks', 5);
    const res = UrlFetchApp.fetch(HUB_URL, {
      method: 'post',
      contentType: 'application/json',
      muteHttpExceptions: true,
      payload: JSON.stringify({ action: 'sweep', token: CLIENT_TOKEN })
    });
    const code = res.getResponseCode();
    let body = {};
    try { body = JSON.parse(res.getContentText() || '{}'); } catch (_) {}
    if (code >= 200 && code < 300 && body.ok) {
      ui.alert('Recall sweep finished. Check Outbound → Recycle.');
    } else {
      ui.alert('Sweep error: ' + (body.error || `HTTP ${code}`));
    }
  } catch (e) {
    ui.alert('Network error: ' + e);
  }
}

/** 3) (Optional) Create a 5-minute time-driven trigger to auto-ingest */
function menuCreateIngestTrigger() {
  // Remove any existing copies of same trigger to avoid duplicates
  const thisFunc = 'autoIngestTick';
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === thisFunc)
    .forEach(t => ScriptApp.deleteTrigger(t));

  ScriptApp.newTrigger(thisFunc)
    .timeBased()
    .everyMinutes(5)
    .create();

  SpreadsheetApp.getActive().toast('Created 5-minute ingest trigger.', 'Webhooks', 5);
}

/** Trigger target — calls the same Hub ingest action */
function autoIngestTick() {
  const { HUB_URL, CLIENT_TOKEN } = CFG_();
  if (!HUB_URL || !CLIENT_TOKEN) return;
  try {
    UrlFetchApp.fetch(HUB_URL, {
      method: 'post',
      contentType: 'application/json',
      muteHttpExceptions: true,
      payload: JSON.stringify({ action: 'ingest', token: CLIENT_TOKEN })
    });
  } catch (_) { /* best-effort */ }
}
