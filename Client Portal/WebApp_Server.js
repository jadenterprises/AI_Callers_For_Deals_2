/** ================== CONFIG & UTILITIES ================== **/

function ssById_(id){ return SpreadsheetApp.openById(id); }


/** Call Hub with JSON payload; returns parsed body or throws with a clear message */
function callHub_(payload) {
  const C = WEB_CFG_();
  const url = sanitizeHubUrl_(C.HUB_URL);
  if (!url || !C.CLIENT_TOKEN) throw new Error('Hub URL or CLIENT_TOKEN not set');

  const options = {
    method: 'post',
    contentType: 'application/json',
    headers: {
      Authorization: 'Bearer ' + ScriptApp.getOAuthToken()
    },
    muteHttpExceptions: true,
    followRedirects: false, // Set to false to handle redirects manually
    payload: JSON.stringify(Object.assign({ token: C.CLIENT_TOKEN }, payload || {}))
  };

  console.log("Portal LOG: Attempting to call Hub with the following details:");
  console.log(`--> URL Called: ${url}`);
  console.log(`--> Method Used: ${options.method}`);

  let res = UrlFetchApp.fetch(url, options);
  let code = res.getResponseCode();

  // Manually handle HTTP 302 redirects, which Google Apps Script uses for long-running executions.
  if (code >= 300 && code < 400) {
    const headers = res.getHeaders();
    const redirectUrl = headers['Location'] || headers['location'];
    if (redirectUrl) {
      console.log(`--> Detected redirect (HTTP ${code}). Following to: ${redirectUrl}`);

      // The redirected URL still needs the original POST data and token.
      const redirectOptions = {

        muteHttpExceptions: true,
        followRedirects: false
      };

      res = UrlFetchApp.fetch(redirectUrl, redirectOptions);
      code = res.getResponseCode();
    }
  }

  const text = res.getContentText() || '';
  const ctype = String(res.getHeaders()['Content-Type'] || res.getHeaders()['content-type'] || '').toLowerCase();

  // Optional: Log final status for easier debugging.
  console.log(`--> Final Hub Response Code: ${code}`);
  console.log(`--> Final Hub Content-Type: ${ctype}`);

  if (code < 200 || code >= 300) {
    try {
      // If the body is JSON, it might contain a structured error from the Hub.
      const errorPayload = JSON.parse(text);
      if (errorPayload && errorPayload.error) {
        throw new Error('HUB_ERROR: ' + errorPayload.error);
      }
    } catch (_) { /* fall through to generic error */ }
    // If not JSON or no .error field, throw a generic HTTP error.
    throw new Error('HUB_HTTP_' + code + ': ' + text.slice(0, 120));
  }

  if (!ctype.includes('application/json')) {
    throw new Error('HUB_NOT_JSON: Check HUB_URL/token/authorization. Got ' +
      (ctype || 'unknown content type') + ' … ' + text.slice(0, 120));
  }

  return JSON.parse(text);
}



/** ================== WEB APP ENTRY ================== **/

function doGetHandler() {
  return HtmlService.createHtmlOutputFromFile('ClientPortal')
    .setTitle('AI Calling Dashboard')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

/** -------- Password login (Hub-validated; optional local fallback) -------- */
function portalAuth(password) {
  const pass = String(password || '');
  try {
    const r = callHub_({ action: 'portal_auth', password: pass });
    if (r && r.ok) return true;
  } catch (_) { /* fall back below if configured */ }
  const C = WEB_CFG_();
  if (C.PORTAL_PASSWORD && pass && pass === C.PORTAL_PASSWORD) return true;
  return false;
}

/** ================== DASHBOARD STATE ================== **/

function webGetState(){
  const C  = WEB_CFG_();
  const st = {};

  // 1) Credits (direct from sheet)
  st.credits = _fastGetCredits_(C.OUTBOUND_SS_ID, C.CREDIT_TAB) ?? '—';

  // 2) Run availability (1..8) + Calls Made
// NEW: try fast counters, fallback to scan
const fast = _fastRunCountsFromCredit_(C.OUTBOUND_SS_ID, C.CREDIT_TAB);
const runs = fast || _countRunAvailability_(C.OUTBOUND_SS_ID, C.CT_TZ, C.OUTBOUND_TAB);
  st.run = runs.counts;      // r1..r8
  st.callsMade = runs.calls; // number with Last Call value

  // 3) Results counters (Good/Later/Bad/Not Interested)
  st.results = _countResults_(C.RESULTS_SS_ID, C);

  // 4) Cooldowns (send + ingest) from Hub
  try {
    const cd = callHub_({ action: 'cooldown_status' });
    st.cooldownSec       = (cd && cd.ok) ? Number(cd.send_seconds   || 0) : 0;
    st.ingestCooldownSec = (cd && cd.ok) ? Number(cd.ingest_seconds || 0) : 0;
  } catch(_) { st.cooldownSec = 0; st.ingestCooldownSec = 0; }

  // 5) Recall days and run amount
  st.recall    = _getRecallDays_(C.OUTBOUND_SS_ID, C.CREDIT_TAB, C.CT_TZ);
  st.runAmount = Math.max(100, Math.min(1000, C.DEFAULT_RUN_LIMIT));

  // (We keep downloads server-side via exportResultsXlsx which transforms Phone)
  // 6) Credit ledger
  st.ledger    = _getLedger_(C.OUTBOUND_SS_ID, C.CREDIT_TAB, C.CT_TZ);

  return st;
}

/** ================== UI ACTIONS ================== **/

// WebApp_Server.gs in the Client Portal
function webSetRecallDays(noAnswer, answered){
  const C  = WEB_CFG_();
  const ss = ssById_(C.OUTBOUND_SS_ID);
  const sh = ss.getSheetByName(C.CREDIT_TAB);
  if (!sh) return { ok:false, error:'Credit sheet not found' };

  const setIfExists = (a1,val)=>{ try{ sh.getRange(a1).setValue(Number(val||0)); }catch(_){ } };

  // NEW order: write I4/I6, fallback to J4/J6, then legacy I3/I5
  try { setIfExists('I4', noAnswer); setIfExists('I6', answered); }
  catch(_) { try { setIfExists('J4', noAnswer); setIfExists('J6', answered); }
             catch(__){ setIfExists('I3', noAnswer); setIfExists('I5', answered); } }

  return { ok:true };
}

/** Save per-run limit (100–1000) to DEFAULT_RUN_LIMIT property */
function webSetRunAmount(n){
  n = Math.max(100, Math.min(1000, parseInt(n||'1000',10)));
  PropertiesService.getScriptProperties().setProperty('DEFAULT_RUN_LIMIT', String(n));
  return { ok:true, value:n };
}

/** Send Run (accept 1..8; Hub enforces windows/cooldown/debits) */
function webSendRun(runNumber) {
  console.log(`Portal: webSendRun triggered for Run ${runNumber}. Attempting to call Hub.`);

  // Get the configured number of calls to send per run
  const C = WEB_CFG_();
  const run = Math.max(1, Math.min(8, parseInt(runNumber || '1', 10)));
  const count = C.DEFAULT_RUN_LIMIT;

  // This is the correct, simple payload the Hub script expects
  const payload = {
    action: 'send_run',
    run: run,
    count: count 
  };

  try {
    // Use the existing 'callHub_' helper to correctly send the request to the Hub
    const hubResponse = callHub_(payload);
    
    // Check if the Hub processed the request successfully and pass its response back to the browser
    if (hubResponse && hubResponse.ok) {
      return hubResponse; // The client-side code is already set up to show the message from this response
    } else {
      // If the Hub returns an error, throw it so the client-side failure handler can display it
      throw new Error(hubResponse.error || 'The Hub returned an unspecified error.');
    }
  } catch (e) {
    // This will be caught by the .withFailureHandler on the client side
    throw new Error('Failed to send run: ' + e.message);
  }
}



/** Get call results (Hub enforces ~20‑min cooldown) */
function webGetCallResults(){
  const res = callHub_({ action:'ingest' });
  if (res && res.ok) return { ok:true, message:'Results ingested (if any were available).' };
  return { ok:false, message: (res && res.error) ? res.error : 'Unable to ingest now.' };
}

/** Export Results tab to Excel (.xlsx), stripping any +1 (or '+1) and forcing plain 10 digits in Phone */
function exportResultsXlsx(which){
  const C = WEB_CFG_();
  const ss = ssById_(C.RESULTS_SS_ID);
  if (!ss) throw new Error('Results workbook not found.');

  // Support 'good' and 'later' (add more keys if you want Bad/NI too)
  const map = {
    good:  C.GOOD_TAB || 'Good Leads',
    later: C.LATER_TAB || 'Good Leads For Later',
    bad:   C.BAD_TAB   || 'Bad Leads',              // optional
    ni:    C.NI_TAB    || 'Not Interested Leads'    // optional
  };
  const key = String(which||'').toLowerCase();
  const tab = map[key];
  if (!tab) throw new Error('Unsupported export: ' + which);

  const sh = ss.getSheetByName(tab);
  if (!sh) throw new Error('Tab not found: ' + tab);

  // Copy tab into a temp file so we can transform without touching the source
  const tempName = `${tab} Export ${Utilities.formatDate(new Date(), C.CT_TZ, 'yyyy-MM-dd')}`;
  const temp = SpreadsheetApp.create(tempName);
  const blank = temp.getSheets()[0];
  sh.copyTo(temp).setName(tab);
  temp.deleteSheet(blank);
  const t = temp.getSheetByName(tab);

  // Clean Phone column in the temp sheet
  const lastRow = t.getLastRow(), lastCol = t.getLastColumn();
  if (lastRow > 1) {
    const headers = t.getRange(1,1,1,lastCol).getValues()[0];
    const phoneCol = (function findPhoneCol_(hdrs){
      // 1) Prefer exact "Phone" or "Phone Number"
      for (let i=0;i<hdrs.length;i++){
        const h = String(hdrs[i]||'').trim().toLowerCase().replace(/\s+/g,'');
        if (h === 'phone' || h === 'phonenumber') return i;
      }
      // 2) Otherwise, first header that contains 'phone'
      for (let i=0;i<hdrs.length;i++){
        const h = String(hdrs[i]||'').toLowerCase();
        if (h.indexOf('phone') !== -1) return i;
      }
      return -1;
    })(headers);

    if (phoneCol >= 0) {
      const rng  = t.getRange(2, phoneCol+1, lastRow-1, 1);
      // (a) Force plain text so no custom "+1" format survives into XLSX
      rng.setNumberFormat('@');

      // (b) Rewrite values as pure 10-digit strings (strip apostrophes, +1, punctuation)
      const vals = rng.getValues();
      const out  = new Array(vals.length);

      for (let r=0; r<vals.length; r++){
        let s = String(vals[r][0] || '');

        // Strip leading apostrophes and whitespace
        s = s.replace(/^'+/, '').trim();

        // Quick strip of a visible "+1 " prefix if present
        if (/^\+1\b/.test(s)) s = s.replace(/^\+1[\s\-]*/, '');

        // Extract digits and normalize NANP
        let d = s.replace(/\D/g,'');
        if (d.length === 11 && d[0] === '1') d = d.slice(1);
        if (d.length > 10) d = d.slice(-10);

        // Write clean 10 digits if available, else write best-effort (without +1)
        out[r] = [ (d.length === 10) ? d : s ];
      }
      rng.setValues(out);
    }
  }

  // Ensure updates are committed before exporting
  SpreadsheetApp.flush();
  Utilities.sleep(200);

  // Export the temp spreadsheet to .xlsx
  const blob = UrlFetchApp.fetch(
    'https://www.googleapis.com/drive/v3/files/' + temp.getId() +
    '/export?mimeType=application%2Fvnd.openxmlformats-officedocument.spreadsheetml.sheet',
    { headers: { Authorization: 'Bearer ' + ScriptApp.getOAuthToken() }, muteHttpExceptions:true }
  ).getBlob().setName(`${tab}-${Utilities.formatDate(new Date(), C.CT_TZ, 'yyyy-MM-dd')}.xlsx`);

  const file = DriveApp.createFile(blob);

  // Clean up temp
  try { DriveApp.getFileById(temp.getId()).setTrashed(true); } catch(_){}

  // Send a direct download URL back to the UI
  return { url: 'https://drive.google.com/uc?export=download&id=' + file.getId() };
}
/**
 * STAGE 1 of upload: Client-side JS sends the file data here to be placed in a temporary
 * file in Drive. This avoids payload size limits of google.script.run.
 * @param {object} payload - An object with {name: string, dataUrl: string}
 * @returns {object} An object like { fileId: '...' }
 */
function webStageFile(payload) {
  try {
    if (!payload || !payload.dataUrl || !payload.name) {
      throw new Error('Missing file payload for staging.');
    }
    const C = WEB_CFG_();

    // 1) Decode data URL -> Blob
    const m = String(payload.dataUrl).match(/^data:([^;]+);base64,(.*)$/);
    if (!m) throw new Error('Invalid data URL.');
    const mime = m[1];
    const bytes = Utilities.base64Decode(m[2]);
    const blob = Utilities.newBlob(bytes, mime, String(payload.name));

    // 2) Find or create a dedicated "Temporary Uploads" folder inside the main outbound sheet's folder.
    const outboundSs = DriveApp.getFileById(C.OUTBOUND_SS_ID);
    const parents = outboundSs.getParents();
    const parentFolder = parents.hasNext() ? parents.next() : DriveApp.getRootFolder();

    let uploadFolder = parentFolder.getFoldersByName('Temporary Uploads').hasNext()
      ? parentFolder.getFoldersByName('Temporary Uploads').next()
      : parentFolder.createFolder('Temporary Uploads');

    // 3) Create the file and return its ID.
    const file = uploadFolder.createFile(blob);
    return { ok: true, fileId: file.getId() };

  } catch (err) {
    console.error('Error during file staging: ' + err.toString());
    throw new Error('Failed to stage file on server: ' + (err && err.message ? err.message : err));
  }
}


/**
 * STAGE 2 of upload: Client-side JS calls this with the fileId from webStageFile.
 * This function now fetches the file from Drive and processes it.
 * @param {object} payload - An object with { fileId: string }
 */
function webUploadLeads(payload) {
  let fileId;
  if (typeof payload === 'string') {
    fileId = payload;
  } else if (Array.isArray(payload) && payload.length > 0) {
    const firstEl = payload[0];
    if (typeof firstEl === 'string') {
      fileId = firstEl;
    } else if (firstEl !== null && typeof firstEl === 'object') {
      fileId = firstEl.fileId || firstEl.id;
    }
  } else if (payload !== null && typeof payload === 'object') {
    fileId = payload.fileId || payload.id;
  }

  if (!fileId) {
    throw new Error('Missing fileId for processing.');
  }

  let file;
  try {
    file = DriveApp.getFileById(fileId);
    const blob = file.getBlob();
    const C = WEB_CFG_();

    // 1) Convert CSV/XLSX -> rows
    const rows = _blobToRows_(blob);
    if (!rows || rows.length < 2) {
      throw new Error('No data rows detected. Check header row and file format.');
    }
    if (rows.length > 60000) {
      throw new Error('File too large. Please split into ≤60k rows.');
    }

    // 2) Clean/dedupe/append to Outbound Leads
    const added = _commitUploadRowsToOutbound_(C.OUTBOUND_SS_ID, rows);

    // 3) Return message
    return added
      ? `Upload complete. Added ${added.toLocaleString()} new lead(s).`
      : 'Upload complete. 0 new rows (duplicates or no valid phone numbers).';

  } catch (err) {
    throw new Error('Upload failed: ' + (err && err.message ? err.message : err));
  } finally {
    // 4) Clean up: always trash the temporary file
    if (file) {
      try {
        file.setTrashed(true);
      } catch (e) {
        console.error(`Failed to trash temporary file ${fileId}: ${e.toString()}`);
      }
    }
  }
}

/** ================== INTERNAL HELPERS ================== **/

/* Run availability (1..8) + Calls Made. Honors Next Call (blank or <= today) and skips terminal labels. */
function _countRunAvailability_(outboundId, tz, tabName){
  const out = { r1:0, r2:0, r3:0, r4:0, r5:0, r6:0, r7:0, r8:0 }; // counts
  let calls = 0; // Last Call non-empty
  try {
    const ss = ssById_(outboundId);
    const sh = ss.getSheetByName(tabName || 'Outbound Leads');
    if (!sh) return { counts: out, calls };

    const lastRow = sh.getLastRow();
    if (lastRow < 2) return { counts: out, calls };

    // Find columns
    const headers = sh.getRange(1,1,1, sh.getLastColumn()).getValues()[0];
    const idx = {};
    headers.forEach((h,i)=> idx[String(h||'').toLowerCase().replace(/\s+/g,'')] = i);
    let runCol  = idx['run'];
    let nextCol = idx['nextcall'];
    let lastCallCol = idx['lastcall'];
    if (runCol == null) {
      headers.forEach((h,i)=>{
        const s = String(h||'').toLowerCase();
        if (runCol == null  && /run/.test(s))          runCol = i;
        if (nextCol == null && /next\s*call/.test(s))  nextCol = i;
        if (lastCallCol == null && /last\s*call/.test(s)) lastCallCol = i;
      });
    }
    if (lastCallCol != null) {
      const lc = sh.getRange(2, lastCallCol+1, lastRow-1, 1).getValues();
      for (let i=0;i<lc.length;i++){
        const v = lc[i][0];
        if (v !== '' && v != null) calls++;
      }
    }

    if (runCol == null) return { counts: out, calls };

    const runs = sh.getRange(2, runCol+1, lastRow-1, 1).getValues();
    const next = (nextCol != null) ? sh.getRange(2, nextCol+1, lastRow-1, 1).getValues() : null;
    const today = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd');

    for (let i=0;i<runs.length;i++){
      const raw = String(runs[i][0]||'').toLowerCase();
      if (!raw) continue;

      // Skip freeze states
      if (raw.includes('good leads for later') || raw.includes('good leads') || raw.includes('bad leads') || raw.includes('not interested')) continue;

      // Next Call gating
      if (next && next[i] && next[i][0]){
        const ymd = String(next[i][0]).substring(0,10);
        if (ymd && ymd > today) continue;
      }
      const m = raw.match(/\d+/);
      const n = m ? parseInt(m[0],10) : 0;
      if      (n===1) out.r1++;
      else if (n===2) out.r2++;
      else if (n===3) out.r3++;
      else if (n===4) out.r4++;
      else if (n===5) out.r5++;
      else if (n===6) out.r6++;
      else if (n===7) out.r7++;
      else if (n===8) out.r8++;
    }
  } catch(_){}
  return { counts: out, calls };
}

/* Results totals from Results workbook (Good/Later/Bad/Not Interested) */
function _countResults_(resultsId, C){
  const out = { good:0, later:0, bad:0, notInterested:0 };
  try {
    const ss = ssById_(resultsId);
    const tabs = [
      [C.GOOD_TAB, 'good'],
      [C.LATER_TAB,'later'],
      [C.BAD_TAB,  'bad'],
      [C.NI_TAB,   'notInterested']
    ];
    tabs.forEach(([t,k])=>{
      const sh = ss.getSheetByName(t);
      out[k] = sh ? Math.max(0, sh.getLastRow()-1) : 0;
    });
  } catch(_){}
  return out;
}

/* Recall days (Credit!J4/J6, fallback I3/I5). Defaults 5/30. */
function _getRecallDays_(outboundId, creditTab, tz){
  try{
    const ss = ssById_(outboundId);
    const sh = ss.getSheetByName(creditTab || 'Credit');
    if (!sh) return { noAnswer:5, answered:30 };
    const read = a1 => { try{ return Number(sh.getRange(a1).getValue()||0); }catch(_){ return 0; } };
    // NEW order matching your intent/sheet
    const n = read('I4') || read('J4') || read('I3') || 5;
    const a = read('I6') || read('J6') || read('I5') || 30;
    return { noAnswer:n, answered:a };
  } catch(_){ return { noAnswer:5, answered:30 }; }
}

/* Credit ledger (last 15 entries) */
function _getLedger_(outboundId, creditTab, tz){
  const out = [];
  try{
    const ss = ssById_(outboundId);
    const sh = ss.getSheetByName(creditTab || 'Credit');
    if (!sh) return out;
    const last = sh.getLastRow();
    if (last < 4) return out;
    const rows = sh.getRange(4,1,last-3,3).getValues(); // Date | Calls Sent | Credits Added
    rows.slice(-15).forEach(r=>{
      const d = r[0] ? Utilities.formatDate(new Date(r[0]), tz, 'MM/dd/yyyy') : '';
      out.push({ date:d, calls:Number(r[1]||0), added:Number(r[2]||0) });
    });
  } catch(_){}
  return out.reverse();
}

/* CSV/XLSX → 2D rows */
function _blobToRows_(blob){
  const name = (blob.getName() || '').toLowerCase();
  const mt   = (blob.getContentType() || '').toLowerCase();
  if (name.endsWith('.csv') || mt.indexOf('csv') !== -1) {
    const txt = blob.getDataAsString('UTF-8');
    return Utilities.parseCsv(txt);
  }
  return _readXlsxIntoRows_(blob);
}

/* Convert XLSX via Drive (v2 Advanced Service preferred) → rows */
function _readXlsxIntoRows_(blob){
  if (typeof Drive !== 'undefined' && Drive && Drive.Files && Drive.Files.insert) {
    const file = Drive.Files.insert(
      { title: 'Upload '+new Date().toISOString(), mimeType: 'application/vnd.google-apps.spreadsheet' },
      blob,
      { convert: true }
    );
    try {
      const ss = SpreadsheetApp.openById(file.id);
      return ss.getSheets()[0].getDataRange().getValues();
    } finally {
      try{ Drive.Files.remove(file.id); }catch(_){}
    }
  }
  // Fallback to raw Drive v3 upload
  const token = ScriptApp.getOAuthToken();
  const metadata = { name: (blob.getName()||'Upload'), mimeType: 'application/vnd.google-apps.spreadsheet' };
  const boundary = '----GASFORM'+Date.now();
  const delimiter = `\r\n--${boundary}\r\n`;
  const closeDelim= `\r\n--${boundary}--`;
  const body = delimiter +
    "Content-Type: application/json; charset=UTF-8\r\n\r\n" +
    JSON.stringify(metadata) + delimiter +
    "Content-Type: "+(blob.getContentType()||'application/octet-stream')+"\r\n\r\n" +
    Utilities.newBlob(blob.getBytes()).getDataAsString() +
    closeDelim;
  const resp = UrlFetchApp.fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart', {
    method:'post',
    contentType:'multipart/related; boundary=' + boundary,
    headers: { Authorization: 'Bearer ' + token },
    payload: body,
    muteHttpExceptions:true
  });
  if (resp.getResponseCode() >= 300) throw new Error('XLSX conversion failed. Try CSV.');
  const id = JSON.parse(resp.getContentText()).id;
  const ss = SpreadsheetApp.openById(id);
  try { return ss.getSheets()[0].getDataRange().getValues(); }
  finally {
    try {
      UrlFetchApp.fetch("https://www.googleapis.com/drive/v3/files/"+id, {
        method:"delete", headers:{ Authorization: "Bearer "+token }, muteHttpExceptions:true
      });
    }catch(_){}
  }
}

/* ===== Upload mapping, phone priority, cleaning, dedupe, chunked append ===== */

function _commitUploadRowsToOutbound_(outboundId, rows){
  if (!rows || rows.length < 2) return 0;

  // Detect columns (case/space/punct insensitive).
  const H = _detectColumns_(rows[0]);
  let data = rows.slice(1);

  // NEW: Sanitize the data
  // 1. Trim whitespace from all cells
  data = data.map(row => row.map(cell => (typeof cell === 'string' ? cell.trim() : cell)));
  // 2. Filter out rows that are now completely empty
  data = data.filter(row => row.some(cell => cell !== '' && cell != null));

  // De‑dupe within this upload: keep last occurrence by phone10
  const seen = {}; // phone10 -> outbound row
  data.forEach(r=>{
    const phone10 = _firstValidPhone10_(r, H);
    if (!phone10) return; // Skip rows without a valid phone number

    const e164  = '+1' + phone10;
    const first = _pick_(r, H.first) || '';
    const last  = _pick_(r, H.last)  || '';
    const addr  = _pick_(r, H.addr1) || '';
    const city  = _pick_(r, H.city)  || '';
    const state = _pick_(r, H.state) || '';
    const zip   = _normalizeZip_(_pick_(r, H.zip));
    const email = _emailOk_(_pick_(r, H.email)) ? String(_pick_(r, H.email)) : ''; // Already trimmed

    seen[phone10] = [first,last,e164,addr,city,state,zip,email,'1','',''];  // Run=1
  });

  const prepared = Object.keys(seen).map(k => seen[k]);
  if (!prepared.length) return 0;

  const ss = ssById_(outboundId);
  const sh = ss.getSheetByName('Outbound Leads') || ss.insertSheet('Outbound Leads');
  _ensureOutboundHeaders_(sh);

  // Existing phones (E.164) to avoid duplicates
  const existing = _existingPhoneSet_(sh);
  const toInsert = prepared.filter(r => !existing.has(r[2]));
  if (!toInsert.length) return 0;

  // Chunked write
  const CHUNK = 1000;
  let start = sh.getLastRow()+1;
  for (let i=0;i<toInsert.length;i+=CHUNK){
    const block = toInsert.slice(i,i+CHUNK);
    sh.getRange(start,1,block.length,11).setValues(block);
    start += block.length;
  }
  return toInsert.length;
}

/* Build header index map + lists of candidate phone columns in priority order */
function _detectColumns_(headers){
  const norm = s => String(s||'').toLowerCase().replace(/[^a-z0-9]+/g,'');
  const idx  = {}; (headers||[]).forEach((h,i)=> idx[norm(h)] = i);

  const KW = {
    first: ["firstname","first name","first","fname","primaryfirst","first_name","leadfirstname","leadfirst name"],
    last:  ["lastname","last name","last","lname","surname","last_name","leadlastname","leadlast name"],
    phones: [
      "mobile","mobilephone","cell","cellphone","primarymobilephone1","mobile1","mobilephone1",
      "phone","phonenumber","telephone","primaryphone","dayphone","workphone","homephone","contactnumber","number",
      "phone1","phone2","phone3","phone 1","phone 2","phone 3","secondaryphone","altphone","secondarycontactphone","businessphone"
    ],
    email: ["email","emailaddress","e-mail","primaryemail","workemail","e mail","e.mail"],
    addr1: ["address","address1","streetaddress","addressline1","propertyaddress","address line 1"],
    city:  ["city","propertycity"],
    state: ["state","province","st","propertystate"],
    zip:   ["zip","zipcode","postalcode","postal","zipcode5","zip code","propertyzipcode","5digitzipcode"]
  };

  const first = _findFirst_(idx, KW.first);
  const last  = _findFirst_(idx, KW.last);
  const email = _findFirst_(idx, KW.email);
  const addr1 = _findFirst_(idx, KW.addr1);
  const city  = _findFirst_(idx, KW.city);
  const state = _findFirst_(idx, KW.state);
  const zip   = _findFirst_(idx, KW.zip);
  const phoneCandidates = _findAll_(idx, KW.phones); // array, in priority order

  return { first, last, email, addr1, city, state, zip, phoneCandidates };
}
function _findFirst_(idx, keys){
  for (let k of keys){
    const key = k.toLowerCase().replace(/[^a-z0-9]+/g,'');
    if (idx[key] != null) return idx[key];
    for (let exist in idx) if (exist.indexOf(key) !== -1) return idx[exist];
  }
  return -1;
}
function _findAll_(idx, keys){
  const out = [];
  keys.forEach(k=>{
    const key = k.toLowerCase().replace(/[^a-z0-9]+/g,'');
    if (idx[key] != null && out.indexOf(idx[key]) === -1) out.push(idx[key]);
    else {
      for (let exist in idx) {
        if (exist.indexOf(key) !== -1 && out.indexOf(idx[exist]) === -1) out.push(idx[exist]);
      }
    }
  });
  return out;
}
function _pick_(row, idx){ return (idx!=null && idx>=0) ? row[idx] : ''; }

/* return first valid 10‑digit phone (as 10 digits) from the candidate columns */
function _firstValidPhone10_(row, H){
  if (!H || !H.phoneCandidates || !H.phoneCandidates.length) return '';
  for (let i=0;i<H.phoneCandidates.length;i++){
    const ten = _cleanPhone10_(_pick_(row, H.phoneCandidates[i]));
    if (ten) return ten;
  }
  return '';
}

/* headers for Outbound Leads */
function _ensureOutboundHeaders_(sh){
  const OUT = ['First Name','Last Name','Phone','Address','City','State','Zip','Email','Run','Last Call','Next Call'];
  if (!sh.getLastRow()) { sh.getRange(1,1,1,OUT.length).setValues([OUT]); return; }
  const existing = sh.getRange(1,1,1,Math.max(sh.getLastColumn(), OUT.length)).getValues()[0];
  const missing  = OUT.filter(h => existing.indexOf(h) === -1);
  if (missing.length){
    sh.insertColumnsAfter(existing.length || 1, missing.length);
    sh.getRange(1, existing.length+1, 1, missing.length).setValues([missing]);
  }
}
function _existingPhoneSet_(sh){
  const set = new Set();
  const lastRow = sh.getLastRow(); const lastCol = sh.getLastColumn();
  if (lastRow < 2) return set;
  const headers = sh.getRange(1,1,1,lastCol).getValues()[0];
  // find phone column
  let pCol = -1;
  headers.forEach((h,i)=>{ if (String(h).toLowerCase().replace(/\s+/g,'') === 'phone') pCol = i+1; });
  if (pCol < 1) {
    for (let i=0;i<headers.length;i++){
      if (String(headers[i]||'').toLowerCase().indexOf('phone') !== -1) { pCol = i+1; break; }
    }
  }
  if (pCol < 1) return set;
  const vals = sh.getRange(2, pCol, lastRow-1, 1).getValues();
  vals.forEach(v=>{ const s = String(v[0]||'').trim(); if (s) set.add(s); });
  return set;
}

/* phone/email/zip cleaning */
function _cleanPhone10_(raw){
  if (raw == null) return '';
  let s = String(raw).trim();
  if (s.startsWith('=+')) s = '+' + s.slice(2);
  else if (s.startsWith('=')) s = s.slice(1);
  let d = s.replace(/\D/g,'');
  if (!d) return '';
  if (d.length === 11 && d[0]==='1') d = d.slice(1);
  if (d.length > 10) d = d.slice(-10);
  if (d.length !== 10) return '';
  if (/(\d)\1{4,}/.test(d)) return ''; // e.g., 55555
  if (/[01]/.test(d.charAt(0)) || /[01]/.test(d.charAt(3))) return ''; // NANP
  const toll = ['800','833','844','855','866','877','888'];
  if (toll.indexOf(d.slice(0,3)) !== -1) return '';
  return d;
}
function _normalizeZip_(z){
  if (z==null) return '';
  const m = String(z).match(/\d{5}/);
  return m ? m[0] : '';
}
function _emailOk_(v){
  if (!v) return false;
  return /^[^\s@]+@[^\s@]{2,}\.[^\s@]{2,}$/.test(String(v).trim());
}
/** Fast run counts from Credit (F4:F11 = Run1..Run8; G4 = Calls Made). Falls back if not present. */
function _fastRunCountsFromCredit_(outboundId, creditTab){
  try{
    const ss = ssById_(outboundId);
    const sh = ss.getSheetByName(creditTab || 'Credit');
    if (!sh) return null;
    const r = sh.getRange('F4:F11').getValues(); // 8 cells
    const calls = Number(sh.getRange('G4').getValue() || 0);
    const nums = r.map(a=>Number(a[0]||0));
    if (nums.some(n=>isNaN(n))) return null;
    return { counts: { r1:nums[0], r2:nums[1], r3:nums[2], r4:nums[3], r5:nums[4], r6:nums[5], r7:nums[6], r8:nums[7] }, calls };
  } catch(_){ return null; }
}

/** Fast credit count from Credit tab, cell B1. */
function _fastGetCredits_(outboundId, creditTab) {
  try {
    const ss = ssById_(outboundId);
    const sh = ss.getSheetByName(creditTab || 'Credit');
    if (!sh) return null;
    return Number(sh.getRange('B1').getValue()) || 0;
  } catch (e) {
    console.error("Error in _fastGetCredits_: " + e.toString());
    return null;
  }
}