/**
 * Config Loader for BOUND sheets (Outbound Console & Results)
 * - Load from PROVISION_RECORD_ID/RECORD_ID if present
 * - Otherwise auto-detect retell-provision.json:
 *     1) in any parent folder(s) of this spreadsheet
 *     2) Drive-wide (most recent), via Drive v3 (UrlFetch)
 * - Accepts a raw File ID or a full Drive URL (we extract the id)
 * - Safe merge of properties (append) and persists the record id for future runs
 *
 * Requires scopes:
 *   - https://www.googleapis.com/auth/drive.readonly
 *   - https://www.googleapis.com/auth/script.external_request
 */

/** ───────────────────────── Helpers ───────────────────────── **/

/** Extract a Drive File ID from a raw id or a full Drive URL. */
function _parseRecordArg_(arg) {
  var a = String(arg || '').trim();
  if (!a) return '';
  if (/^[A-Za-z0-9_-]{20,}$/.test(a)) return a;            // looks like an id already
  var m = a.match(/[-\w]{25,}/g);                           // first long-ish token
  return (m && m[0]) ? m[0] : '';
}

/** Try to find retell-provision.json in any parent folder(s) of the active spreadsheet. */
function _findInParents_() {
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (!ss) return '';
    var file = DriveApp.getFileById(ss.getId());
    var parents = file.getParents();
    while (parents.hasNext()) {
      var folder = parents.next();
      var iter = folder.getFilesByName('retell-provision.json');
      // prefer most recently modified — do a tiny pass
      var latest = { id: '', when: 0 };
      while (iter.hasNext()) {
        var f = iter.next();
        var when = f.getLastUpdated().getTime ? f.getLastUpdated().getTime() : 0;
        if (when >= latest.when) { latest.id = f.getId(); latest.when = when; }
      }
      if (latest.id) return latest.id;
    }
  } catch (_) {}
  return '';
}

/** Drive-wide fallback (most recent): search via Drive v3 Files.list (UrlFetch). */
function _findAnywhere_() {
  try {
    var token = ScriptApp.getOAuthToken();
    var url = "https://www.googleapis.com/drive/v3/files"
            + "?q=" + encodeURIComponent("name='retell-provision.json' and trashed=false")
            + "&fields=files(id,name,modifiedTime)"
            + "&orderBy=modifiedTime desc"
            + "&pageSize=1"
            + "&includeItemsFromAllDrives=true&supportsAllDrives=true";
    var r = UrlFetchApp.fetch(url, { headers: { Authorization: "Bearer " + token }, muteHttpExceptions: true });
    if (r.getResponseCode() >= 300) return '';
    var j = JSON.parse(r.getContentText() || "{}");
    var arr = j.files || [];
    return arr.length ? arr[0].id : '';
  } catch (_) { return ''; }
}

/** Resolve a record file id:
 *   1) pre-set Script Properties (PROVISION_RECORD_ID/RECORD_ID)
 *   2) parent folders
 *   3) Drive-wide
 */
function _resolveRecordId_() {
  try {
    var P = PropertiesService.getScriptProperties();
    var preset = P.getProperty('PROVISION_RECORD_ID') || P.getProperty('RECORD_ID');
    if (preset) return preset;
  } catch (_) {}
  var inParents = _findInParents_();
  if (inParents) return inParents;
  return _findAnywhere_();
}

/** Get the JSON record via Drive v3 media endpoint. */
function _fetchRecord_(fileId) {
  var token = ScriptApp.getOAuthToken();
  var url = "https://www.googleapis.com/drive/v3/files/" + encodeURIComponent(fileId) + "?alt=media&supportsAllDrives=true";
  var r = UrlFetchApp.fetch(url, { headers: { Authorization: "Bearer " + token }, muteHttpExceptions: true });
  if (r.getResponseCode() >= 300) {
    throw new Error("Failed to download provision record: HTTP " + r.getResponseCode() + " " + r.getContentText());
  }
  return JSON.parse(r.getContentText() || "{}");
}

/** ─────────────────── Main: load & apply ─────────────────── **/

/**
 * Preferred one-click entry from menu: tries everything with no parameter.
 */
function manualConfigAuto() {
  return manualConfigFromProvisionRecord('');
}

/**
 * Load from a specific File ID or full Drive URL; if omitted, auto-detect.
 * @param {string=} recordFileIdOrUrl
 */
function manualConfigFromProvisionRecord(recordFileIdOrUrl) {
  // 1) Choose the record id
  var recordId = _parseRecordArg_(recordFileIdOrUrl);
  if (!recordId) recordId = _resolveRecordId_();

  // 2) Prompt as a last resort (accept full URL or ID)
  if (!recordId && typeof SpreadsheetApp !== 'undefined' && SpreadsheetApp.getUi) {
    try {
      var ui = SpreadsheetApp.getUi();
      var res = ui.prompt(
        'Paste the Provision Record link (or ID)',
        'Tip: you can paste the full Drive URL – no need to extract the ID.',
        ui.ButtonSet.OK_CANCEL
      );
      if (res.getSelectedButton() === ui.Button.OK) {
        recordId = _parseRecordArg_(res.getResponseText());
      }
    } catch (_) {}
  }

  if (!recordId) {
    var msg = "Could not locate 'retell-provision.json'. Ensure it exists and you have access.";
    Logger.log(msg);
    try { SpreadsheetApp.getUi().alert(msg); } catch(_) {}
    return { ok:false, error: msg };
  }

  // 3) Read the record and decide role
  var rec = _fetchRecord_(recordId);
  if (!rec.scripts || !rec.props) {
    var e2 = "Provision record missing 'scripts' or 'props'.";
    Logger.log(e2);
    try { SpreadsheetApp.getUi().alert(e2); } catch(_) {}
    return { ok:false, error: e2 };
  }

  var myScriptId = ScriptApp.getScriptId();
  var role = "Unknown", props = null;

  // ScriptId match first
  if (rec.scripts.outboundScriptId === myScriptId) { role = "Outbound"; props = rec.props.outboundProps; }
  else if (rec.scripts.resultsScriptId === myScriptId) { role = "Results";  props = rec.props.resultsProps; }

  // Fallback: match container spreadsheet id (bound projects)
  if (!props) {
    try {
      var ssId = SpreadsheetApp.getActiveSpreadsheet().getId();
      if (rec.sheets && rec.sheets.outboundId === ssId) { role = "Outbound (ID Match)"; props = rec.props.outboundProps; }
      else if (rec.sheets && rec.sheets.resultsId === ssId) { role = "Results (ID Match)";  props = rec.props.resultsProps; }
    } catch(_) {}
  }

  if (!props) {
    var e3 = "This script/sheet was not found in the provision record.";
    Logger.log(e3);
    try { SpreadsheetApp.getUi().alert(e3); } catch(_) {}
    return { ok:false, error: e3 };
  }

  // 4) Apply (merge) and remember record id
  try {
    var P = PropertiesService.getScriptProperties();
    P.setProperties(props, false);          // merge (append) keys safely
    P.setProperty('BOOTSTRAPPED','1');
    P.setProperty('PROVISION_RECORD_ID', recordId);
    P.setProperty('RECORD_ID', recordId);

    var count = Object.keys(props).length;
    var okMsg = "Applied " + count + " properties for role: " + role + ".";
    Logger.log(okMsg);
    try { SpreadsheetApp.getUi().alert(okMsg); } catch(_) {}
    return { ok:true, message: okMsg, count: count, role: role, recordId: recordId };
  } catch (e) {
    var e4 = "Failed to apply Script Properties: " + e;
    Logger.log(e4);
    try { SpreadsheetApp.getUi().alert(e4); } catch(_) {}
    return { ok:false, error: e4 };
  }
}

/** ─────────────────── Convenience menu handlers ─────────────────── **/

/** One-click: set/change the record id property manually (accepts URL or ID). */
function menuSetRecordId() {
  var ui = SpreadsheetApp.getUi();
  var res = ui.prompt('Set/Change Provision Record', 'Paste the Drive URL or File ID of retell-provision.json:', ui.ButtonSet.OK_CANCEL);
  if (res.getSelectedButton() !== ui.Button.OK) return;
  var id = _parseRecordArg_(res.getResponseText());
  if (!id) { ui.alert('That did not look like a valid URL or File ID.'); return; }
  PropertiesService.getScriptProperties().setProperty('PROVISION_RECORD_ID', id);
  PropertiesService.getScriptProperties().setProperty('RECORD_ID', id);
  ui.alert('Stored record id: ' + id);
}

/** Show the current record id we will use. */
function menuShowRecordId() {
  var P = PropertiesService.getScriptProperties();
  var cur = P.getProperty('PROVISION_RECORD_ID') || P.getProperty('RECORD_ID') || '(none)';
  SpreadsheetApp.getUi().alert('Current provision record id:\n' + cur);
}
