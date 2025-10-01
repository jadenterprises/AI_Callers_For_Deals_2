
/**
 * Config Loader (top-level; works in stand-alone editors and bound sheets)
 * Usage:
 *   • Run manualConfigAuto()                     — auto-detects the matching retell-provision.json
 *   • Or run manualConfigFromProvisionRecord('<Drive URL or File ID>')
 * Behavior:
 *   • Merges Script Properties (does not wipe others)
 *   • Writes PROVISION_RECORD_ID and RECORD_ID for future zero‑arg runs
 *
 * Requires scopes (manifest):
 *   https://www.googleapis.com/auth/drive.readonly
 *   https://www.googleapis.com/auth/script.external_request
 *   (optional) https://www.googleapis.com/auth/userinfo.email
 */

/** ───────────────────────── Helpers ───────────────────────── **/

/** Extract a Drive File ID from a raw id or a full Drive URL. */
function __cfg_parseRecordArg(arg) {
  var a = String(arg || '').trim();
  if (!a) return '';
  if (/^[A-Za-z0-9_-]{20,}$/.test(a)) return a;         // already looks like an id
  var m = a.match(/[-\w]{25,}/g);                       // first long-ish token from URL
  return (m && m[0]) ? m[0] : '';
}

/**
 * Drive-wide scan (most recent first) to find a provision file that references *this* script id.
 * Tight bounds for speed; adequate for typical libraries.
 */
function __cfg_autoFindRecordId() {
  var token = ScriptApp.getOAuthToken();
  var q = "name='retell-provision.json' and trashed=false";
  var pageToken = null, examined = 0;

  for (var page = 0; page < 5; page++) {
    var url = "https://www.googleapis.com/drive/v3/files"
            + "?q=" + encodeURIComponent(q)
            + "&fields=files(id,name,modifiedTime),nextPageToken"
            + "&orderBy=modifiedTime desc"
            + "&pageSize=100"
            + (pageToken ? "&pageToken="+encodeURIComponent(pageToken) : "")
            + "&includeItemsFromAllDrives=true&supportsAllDrives=true";

    var r = UrlFetchApp.fetch(url, { headers:{ Authorization:"Bearer " + token }, muteHttpExceptions:true });
    if (r.getResponseCode() >= 300) break;

    var j = JSON.parse(r.getContentText() || "{}");
    var arr = j.files || [];
    for (var i=0; i<arr.length && examined<400; i++) {
      examined++;
      var fid = arr[i].id;
      try {
        var res = UrlFetchApp.fetch(
          "https://www.googleapis.com/drive/v3/files/" + encodeURIComponent(fid) + "?alt=media&supportsAllDrives=true",
          { headers:{ Authorization:"Bearer " + token }, muteHttpExceptions:true }
        );
        if (res.getResponseCode() >= 300) continue;

        var rec = JSON.parse(res.getContentText() || "{}");
        var myId = ScriptApp.getScriptId();
        var s = (rec && rec.scripts) || {};
        if (s.hubScriptId===myId || s.portalScriptId===myId || s.outboundScriptId===myId || s.resultsScriptId===myId) {
          return fid;
        }
      } catch (_) {}
    }
    pageToken = j.nextPageToken;
    if (!pageToken) break;
  }
  return "";
}

/** Download the provision record JSON via Drive v3 media endpoint. */
function __cfg_fetchRecord(fileId) {
  var token = ScriptApp.getOAuthToken();
  var resp = UrlFetchApp.fetch(
    "https://www.googleapis.com/drive/v3/files/" + encodeURIComponent(fileId) + "?alt=media&supportsAllDrives=true",
    { headers:{ Authorization:"Bearer " + token }, muteHttpExceptions:true }
  );
  if (resp.getResponseCode() >= 300) {
    throw new Error("Failed to download provision record: HTTP " + resp.getResponseCode() + " " + resp.getContentText());
  }
  return JSON.parse(resp.getContentText() || "{}");
}

/** ─────────────────── Main: load & apply ─────────────────── **/

/** Zero-argument convenience: select this in the editor and click Run ▶︎ */
function manualConfigAuto() {
  return manualConfigFromProvisionRecord('');
}

/**
 * Load from a specific File ID or full Drive URL; if omitted, auto-detect.
 * @param {string=} recordFileIdOrUrl
 */
function manualConfigFromProvisionRecord(recordFileIdOrUrl) {
  // 1) Resolve the record id
  var id = __cfg_parseRecordArg(recordFileIdOrUrl);

  // Prefer an id already stored on this script
  if (!id) {
    try {
      var P0 = PropertiesService.getScriptProperties();
      id = P0.getProperty('PROVISION_RECORD_ID') || P0.getProperty('RECORD_ID') || '';
    } catch (_) {}
  }

  // Auto-detect as next best
  if (!id) id = __cfg_autoFindRecordId();

  // In a bound Sheet, give the user a prompt as a last resort
  if (!id && typeof SpreadsheetApp!=='undefined' && SpreadsheetApp.getUi) {
    try {
      var ui = SpreadsheetApp.getUi();
      var res = ui.prompt(
        'Paste the Provision Record link (or ID)',
        'Tip: you can paste the full Drive URL – no need to extract the ID.',
        ui.ButtonSet.OK_CANCEL
      );
      if (res.getSelectedButton() === ui.Button.OK) {
        id = __cfg_parseRecordArg(res.getResponseText());
      }
    } catch(_) {}
  }

  if (!id) {
    var msg = "Could not locate a matching 'retell-provision.json'. Ensure it exists and you have access.";
    Logger.log(msg);
    if (typeof SpreadsheetApp!=='undefined' && SpreadsheetApp.getUi) { try{ SpreadsheetApp.getUi().alert(msg); }catch(_){} }
    return { ok:false, error: msg };
  }

  // 2) Fetch record & determine role for this script
  var rec = __cfg_fetchRecord(id);
  if (!rec.scripts || !rec.props) {
    var e2 = "Provision record missing 'scripts' or 'props'.";
    Logger.log(e2);
    return { ok:false, error: e2 };
  }

  var myScriptId = ScriptApp.getScriptId();
  var role = "Unknown", props = null;

  if (rec.scripts.hubScriptId === myScriptId)           { role="Hub";     props=rec.props.hubProps; }
  else if (rec.scripts.portalScriptId === myScriptId)   { role="Portal";  props=rec.props.portalProps; }
  else if (rec.scripts.outboundScriptId === myScriptId) { role="Outbound"; props=rec.props.outboundProps; }
  else if (rec.scripts.resultsScriptId === myScriptId)  { role="Results";  props=rec.props.resultsProps; }

  // Fallback for bound Sheets (match active spreadsheet id)
  if (!props && typeof SpreadsheetApp!=='undefined' && SpreadsheetApp.getActiveSpreadsheet) {
    try {
      var ssId = SpreadsheetApp.getActiveSpreadsheet().getId();
      if (rec.sheets && rec.sheets.outboundId === ssId) { role="Outbound (ID Match)"; props=rec.props.outboundProps; }
      else if (rec.sheets && rec.sheets.resultsId === ssId) { role="Results (ID Match)"; props=rec.props.resultsProps; }
    } catch(_) {}
  }

  if (!props) {
    var e3 = "This script ID (" + myScriptId + ") did not match any script in the provision record.";
    Logger.log(e3);
    if (typeof SpreadsheetApp!=='undefined' && SpreadsheetApp.getUi) { try{ SpreadsheetApp.getUi().alert(e3); }catch(_){} }
    return { ok:false, error: e3 };
  }

  // 3) Apply properties (merge) and remember the record id
  try {
    var P = PropertiesService.getScriptProperties();
    P.setProperties(props, false);            // merge (append) keys safely
    P.setProperty('BOOTSTRAPPED','1');
    P.setProperty('PROVISION_RECORD_ID', id); // remember for zero‑arg runs
    P.setProperty('RECORD_ID', id);

    var count = Object.keys(props).length;
    var msgOk = "Applied " + count + " properties for role " + role + " (record " + id + ").";
    Logger.log(msgOk);
    if (typeof SpreadsheetApp!=='undefined' && SpreadsheetApp.getUi) { try{ SpreadsheetApp.getUi().alert(msgOk); }catch(_){} }
    return { ok:true, message: msgOk, count: count, role: role, recordId: id };
  } catch (e) {
    var e4 = "Failed to apply Script Properties: " + e;
    Logger.log(e4);
    return { ok:false, error: e4 };
  }
}
