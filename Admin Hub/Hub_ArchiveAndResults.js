// Hub_ArchiveAndResults.js (Fixed) - Part 1
// This file handles archive management and results processing

function _logPlacementDecision_(rowIndex, row, reason, targetTab) {
  console.log(`Row ${rowIndex + 1}: ${reason} → ${targetTab || 'skipped'}`);
  console.log(JSON.stringify(row));
}

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

function ensureMonthlyArchive() {
  const folder = DriveApp.getFolderById(CFG().FOLDER_ID);
  const name = archiveNameFor_(new Date());

  // Try to find existing
  const files = folder.getFilesByName(name);
  if (files.hasNext()) {
    const id = files.next().getId();
    const ss = ssById_(id);
    const sh = sh_(ss, 'Archive');
    ensureHeaders_(sh, ARCHIVE_HEADERS);
    PropertiesService.getScriptProperties().setProperty('LAST_ARCHIVE_FILE_ID', id);
    return { id, sheet: sh };
  }

  // Create new
  const ss = SpreadsheetApp.create(name);
  DriveApp.getFileById(ss.getId()).moveTo(folder);

  // Fixed: Proper sheet deletion logic
  const sheets = ss.getSheets();
  if (sheets.length > 1) {
    // Keep first sheet, delete others from the end
    for (let i = sheets.length - 1; i > 0; i--) {
      ss.deleteSheet(sheets[i]);
    }
  }
  
  // Rename the remaining sheet to Archive
  const remainingSheet = ss.getSheets()[0];
  remainingSheet.setName('Archive');
  ensureHeaders_(remainingSheet, ARCHIVE_HEADERS);

  PropertiesService.getScriptProperties().setProperty('LAST_ARCHIVE_FILE_ID', ss.getId());
  return { id: ss.getId(), sheet: remainingSheet };
}

function appendArchiveChunked_(sh, rows) {
  const CHUNK = 1000;
  let start = sh.getLastRow() + 1;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const block = rows.slice(i, i + CHUNK);
    sh.getRange(start, 1, block.length, block[0].length).setValues(block);
    start += block.length;
  }
}

/** Read the current CSV object (text + generation) without mutating it. */
function gcsPeekCsvText_() {
  const token = ScriptApp.getOAuthToken();
  const bucket = CFG().GCS_BUCKET;
  const path = CFG().GCS_RESULTS_PATH;
  const base = `https://storage.googleapis.com/storage/v1/b/${bucket}/o`;
  const enc = encodeURIComponent(path);

  // 1) metadata (for generation)
  const meta = UrlFetchApp.fetch(`${base}/${enc}?fields=generation,size`, {
    headers: { Authorization: 'Bearer ' + token },
    muteHttpExceptions: true
  });
  if (meta.getResponseCode() !== 200) return null;
  const j = JSON.parse(meta.getContentText() || '{}');
  const gen = j.generation;

  // 2) media
  const media = UrlFetchApp.fetch(`${base}/${enc}?alt=media`, {
    headers: { Authorization: 'Bearer ' + token },
    muteHttpExceptions: true
  });
  if (media.getResponseCode() !== 200) return null;

  return { text: media.getContentText(), generation: gen };
}

/** Overwrite the live CSV with new content (leftovers) using generation match. */
function gcsOverwriteCsv_(csvText, ifGenerationMatch) {
  const token = ScriptApp.getOAuthToken();
  const bucket = CFG().GCS_BUCKET;
  const path = CFG().GCS_RESULTS_PATH;
  const enc = encodeURIComponent(path);

  const url =
    `https://storage.googleapis.com/upload/storage/v1/b/${bucket}/o` +
    `?uploadType=media&name=${enc}` +
    (ifGenerationMatch ? `&ifGenerationMatch=${encodeURIComponent(ifGenerationMatch)}` : '');

  UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'text/csv',
    headers: { Authorization: 'Bearer ' + token },
    payload: csvText,
    muteHttpExceptions: true
  });
}

/** When no leftovers remain: optionally move the last consumed object to /processed and delete live. */
function gcsFinalizeNoLeftovers_(ifGenerationMatch, moveToProcessed) {
  const token = ScriptApp.getOAuthToken();
  const bucket = CFG().GCS_BUCKET;
  const path = CFG().GCS_RESULTS_PATH;
  const base = `https://storage.googleapis.com/storage/v1/b/${bucket}/o`;
  const enc = encodeURIComponent(path);

  if (moveToProcessed) {
    const ts = Utilities.formatDate(new Date(), CFG().CT_TZ, "yyyy-MM-dd'T'HH-mm-ss");
    const dst = encodeURIComponent(path.replace(/(^|\/)([^\/]+)$/, `$1processed/$2_${ts}`));
    // copy (rewriteTo)
    UrlFetchApp.fetch(`${base}/${enc}/rewriteTo/b/${bucket}/o/${dst}`, {
      method: 'post',
      headers: { Authorization: 'Bearer ' + token },
      muteHttpExceptions: true
    });
  }

  // delete live (guard on generation when provided)
  const delUrl = `${base}/${enc}` + (ifGenerationMatch ? `?ifGenerationMatch=${encodeURIComponent(ifGenerationMatch)}` : '');
  UrlFetchApp.fetch(delUrl, {
    method: 'delete',
    headers: { Authorization: 'Bearer ' + token },
    muteHttpExceptions: true
  });
}

/** Minimal CSV serializer (quotes cells containing comma, quote, CR/LF). */
function csvFromRows_(rows) {
  function esc(s) {
    s = (s === null || s === undefined) ? '' : String(s);
    if (/[",\r\n]/.test(s)) {
      return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
  }
  return rows.map(r => (r || []).map(esc).join(',')).join('\n');
}

/** Download latest webhook CSV (GCS) and rotate it into processed path. */
function gcsDownloadAndRotate_() {
  const token = ScriptApp.getOAuthToken();
  const bucket = CFG().GCS_BUCKET, path = CFG().GCS_RESULTS_PATH;
  const base = `https://storage.googleapis.com/storage/v1/b/${bucket}/o`;
  const enc = encodeURIComponent(path);

  // exist?
  const meta = UrlFetchApp.fetch(`${base}/${enc}?fields=size`, {
    headers: { Authorization: 'Bearer ' + token },
    muteHttpExceptions: true
  });
  if (meta.getResponseCode() !== 200) return null;

  // download
  const media = UrlFetchApp.fetch(`${base}/${enc}?alt=media`, {
    headers: { Authorization: 'Bearer ' + token }
  });
  const blob = media.getBlob();

  // archive copy
  const ts = Utilities.formatDate(new Date(), CFG().CT_TZ, "yyyy-MM-dd'T'HH-mm-ss");
  const arc = encodeURIComponent(path.replace(/(^|\/)([^\/]+)$/, `$1processed/$2_${ts}`));
  UrlFetchApp.fetch(`${base}/${enc}/rewriteTo/b/${bucket}/o/${arc}`, {
    method: 'post',
    headers: { Authorization: 'Bearer ' + token }
  });

  // delete live
  UrlFetchApp.fetch(`${base}/${enc}`, {
    method: 'delete',
    headers: { Authorization: 'Bearer ' + token }
  });

  return blob;
}

/** Next Call Date logic */
function computeNextCallDate_(dateStr, correctName, runTag) {
  if (!dateStr) return '';
  const base = new Date(dateStr);
  const runN = (String(runTag || '').match(/(\d+)/) || [])[1] ? Number(RegExp.$1) : 1;
  let days;
  const cn = (correctName || '').toLowerCase();
  
  const STATUS_GROUP_5D = ['voicemail - no name', 'voicemail - correct name', 'no answer', 
                           'voicemail - company name only', 'voicemail_reached', 'dial_no_answer', ''];
  const STATUS_GROUP_30D = ["prospect reached"];
  
  if (STATUS_GROUP_5D.some(s => cn.includes(s))) days = (runN >= 5 ? 30 : 5);
  else if (STATUS_GROUP_30D.some(s => cn.includes(s))) days = (runN >= 5 ? 60 : 30);
  else days = (runN >= 5 ? 30 : 5); // default behavior

  const next = new Date(base.getTime() + days * 86400000);
  return Utilities.formatDate(next, CFG().CT_TZ, 'yyyy-MM-dd');
}
/** Main webhook ingestion function */
function hubIngestWebhooks_() {
  const P = PropertiesService.getScriptProperties();
  const startTime = Number(P.getProperty('WEBHOOK_POLL_START_TIME') || 0);
  const triggerId = P.getProperty('WEBHOOK_POLL_TRIGGER_ID');

  if (startTime && triggerId) {
    const elapsedMinutes = (Date.now() - startTime) / (1000 * 60);
    if (elapsedMinutes > 90) {
      console.log(`Webhook polling has been active for over 90 minutes. Deleting trigger ID: ${triggerId}`);

      // Find and delete this specific trigger
      const allTriggers = ScriptApp.getProjectTriggers();
      for (const trigger of allTriggers) {
        if (trigger.getUniqueId() === triggerId) {
          ScriptApp.deleteTrigger(trigger);
          break;
        }
      }
      // Clean up properties
      P.deleteProperty('WEBHOOK_POLL_START_TIME');
      P.deleteProperty('WEBHOOK_POLL_TRIGGER_ID');
      return { ok: true, message: 'Polling stopped after 90 minutes.' };
    }
  }
  
  try {
    const blob = gcsDownloadAndRotate_();
    if (!blob) {
      Logger.log('No CSV found to ingest (gcsDownloadAndRotate_ returned null).');
      return { ok: true, message: 'No new results to ingest.' };
    }

    const csv = blob.getDataAsString('UTF-8');
    const rows = Utilities.parseCsv(csv);
    if (!rows || rows.length < 2) {
      Logger.log('CSV had no data rows.');
      return { ok: true, message: 'No new results to ingest.' };
    }

    const srcHeaders = rows[0];
    const dataRows = rows.slice(1);

    // Build phone-to-run map from _Sent Index
    const ssOut = ssById_(CFG().OUTBOUND_SS_ID);
    const sentIdx = sh_(ssOut, TAB_SENT_INDEX);
    ensureHeaders_(sentIdx, ['Phone', 'Run', 'BatchTime']);

    const sentMap = {};
    if (sentIdx.getLastRow() > 1) {
      const si = sentIdx.getRange(2, 1, sentIdx.getLastRow() - 1, 3).getValues();
      si.forEach(r => {
        const p = normalizePhone_(r[0]);
        if (p) sentMap[p] = r[1] || '';
      });
    }

    // Classify rows into Results sheet
    const placedRes = writeResultsMapped_(srcHeaders, dataRows, sentMap) || { placedPhones: [] };
    const placedSet = new Set(placedRes.placedPhones || []);

    // Prepare Archive output
    const archive = ensureMonthlyArchive();
    Logger.log(`Archive target => fileId=${archive.id}, sheet="Archive"`);

    const shA = archive.sheet;
    ensureHeaders_(shA, ARCHIVE_HEADERS);

    const srcMap = _ing_normMap_(srcHeaders);
    const outRows = [];
    const recallMap = {};

    const recallCfg = _ing_getRecallDays_(); // { noAnswer, answered }

    for (let i = 0; i < dataRows.length; i++) {
      const row = dataRows[i] || [];
      const phone = normalizePhone_(_ing_pick_(row, srcMap, ['phone', 'phonenumber', 'to_number', 'tonumber']));
      const date = _ing_pick_(row, srcMap, ['date']);
      const runTag = sentMap[phone] || '';
      const runNum = _ing_runNumberFromTag_(runTag) || '';

      const mapped = ARCHIVE_HEADERS.map(h => {
        const k = _ing_norm(h);
        switch (k) {
          case 'date': return String(date || '');
          case 'phone': return String(phone || '');
          case 'run': return String(runNum || '');
          case 'processed': return '';
          case 'nextcalldate': {
            const ymd = _ing_computeNextCallYMD_(row, srcMap, date, runNum, recallCfg);
            return ymd || '';
          }
          default:
            return String(_ing_pick_(row, srcMap, _ing_aliasesArchive_(k)) || '');
        }
      });

      outRows.push(mapped);

      // Only update "Next Call Date" if not already placed
      if (phone && !placedSet.has(phone)) {
        const ymd = _ing_computeNextCallYMD_(row, srcMap, date, runNum, recallCfg);
        if (ymd) recallMap[phone] = ymd;
      }
    }

    if (outRows.length) {
      const start = shA.getLastRow() + 1;
      appendArchiveChunked_(shA, outRows);
      Logger.log(`Archive appended ${outRows.length} row(s) at row ${start}.`);
    } else {
      Logger.log('Archive had 0 rows to append.');
    }

    if (Object.keys(recallMap).length) {
      _ing_setNextCallForPhones_(recallMap);
    }

    Logger.log('CSV fully consumed; live file removed (archived to /processed).');
    return { ok: true, message: `Successfully ingested ${outRows.length} results.` };

  } catch (e) {
    console.error(`HUB ERROR in hubIngestWebhooks_: ${e.toString()}`);
    return { ok: false, error: 'Ingestion failed: ' + String(e) };
  }
}

/* ================= Ingest helpers (tolerant mapping) ================ */

function _ing_norm(s) { 
  return String(s || '').toLowerCase().replace(/[^a-z0-9]/g, ''); 
}

function _ing_normMap_(headers) { 
  const m = {}; 
  (headers || []).forEach((h, i) => m[_ing_norm(h)] = i); 
  return m; 
}

function _ing_pick_(row, m, cands) {
  for (let i = 0; i < cands.length; i++) {
    const idx = m[_ing_norm(cands[i])];
    if (typeof idx === 'number' && idx >= 0) {
      const v = row[idx];
      if (v !== '' && v !== null && v !== undefined) return v;
    }
  }
  return '';
}

function _ing_aliasesArchive_(targetNorm) {
  const A = {
    'firstname': ['first name', 'firstname', 'first'],
    'lastname': ['last name', 'lastname', 'last'],
    'address': ['address'],
    'city': ['city'],
    'inputstate': ['input state', 'inputstate', 'stateinput'],
    'stategiven': ['state given', 'stategiven', '_state'],
    'zip': ['zip', 'zipcode', 'zip_code'],
    'inputemail': ['input email', 'inputemail', 'emailinput'],
    'emailgiven': ['email given', 'emailgiven', 'emailprovided', '_email'],
    'accredited': ['accredited'],
    'interested': ['interested'],
    'newinvestments': ['new investments', 'newinvestments'],
    'liquidtoinvest': ['liquid to invest', 'liquidtoinvest', 'money', 'liquid'],
    'pastexperience': ['past experience', 'past oil', 'pastoil'],
    'job': ['job'],
    'followup': ['follow up', 'followup', 'follow_up'],
    'summary': ['summary', 'summery'],  // tolerate misspelling
    'quality': ['quality'],
    'recording': ['recording', 'recordingurl', 'recording_url'],
    'calltime': ['call time', 'calltime', 'call_duration', 'duration'],
    'correctname': ['correct name', 'correctname', 'correct_name', 'status'],
    'dnc': ['dnc', 'do_not_call'],
    'disconnectionreason': ['disconnection reason', 'disconnectionreason', 'reason'],
    // pass-throughs:
    'date': ['date'],
    'phone': ['phone', 'phonenumber', 'to_number', 'tonumber'],
    'run': ['run'],
    'nextcalldate': [] // computed
  };
  const list = A[targetNorm] || [];
  if (!list.includes(targetNorm)) list.push(targetNorm);
  return list;
}

function _ing_runNumberFromTag_(tag) {
  if (!tag) return '';
  const m = String(tag).match(/(\d+)/);
  return m ? m[1] : '';
}

function _ing_fmtYMD_(d, tz) { 
  return Utilities.formatDate(d, tz || (CFG().CT_TZ || 'America/Chicago'), 'yyyy-MM-dd'); 
}

/** Read client-configurable recall days */
function _ing_getRecallDays_() {
  try {
    const ss = ssById_(CFG().OUTBOUND_SS_ID);
    const sh = ss.getSheetByName('Credit');
    if (!sh) return { noAnswer: 5, answered: 30 };
    const read = a1 => { 
      try { 
        return Number(sh.getRange(a1).getValue() || 0) 
      } catch (_) { 
        return 0; 
      } 
    };
    // NEW order: prefer I4/I6, then J4/J6, then legacy I3/I5
    const noAns = read('I4') || read('J4') || read('I3') || 5;
    const ans = read('I6') || read('J6') || read('I5') || 30;
    return { noAnswer: noAns, answered: ans };
  } catch (_) { 
    return { noAnswer: 5, answered: 30 }; 
  }
}

/** Decide recall days & produce yyyy-MM-dd (or '' if no recall) */
function _ing_computeNextCallYMD_(row, srcMap, dateStr, runNum, cfg) {
  const tz = CFG().CT_TZ || 'America/Chicago';
  const base = dateStr ? new Date(dateStr) : new Date();
  const dis = String(_ing_pick_(row, srcMap, ['disconnectionreason', 'reason']) || '').toLowerCase();
  const corr = String(_ing_pick_(row, srcMap, ['correctname', 'status']) || '').toLowerCase();
  const q = String(_ing_pick_(row, srcMap, ['quality']) || '').toLowerCase();
  const dnc = String(_ing_pick_(row, srcMap, ['dnc']) || '').toLowerCase() === 'true';
  if (dnc) return ''; // never recall

  // No-answer bucket (5d by default)
  const noAnsKeys = ['dial_no_answer', 'no_answer', 'busy', 'voicemail', 'vm', 'ringout', 'not_available', 'ivr'];
  if (noAnsKeys.some(k => dis.includes(k))) {
    const d = new Date(base.getTime() + (cfg.noAnswer * 86400000));
    return _ing_fmtYMD_(d, tz);
  }

  // Answered but later (30d by default)
  const laterKeys = ['later', 'call back', 'follow up', 'not now', 'vacation', 'busy later', 'check back'];
  if (laterKeys.some(k => corr.includes(k))) {
    const d = new Date(base.getTime() + (cfg.answered * 86400000));
    return _ing_fmtYMD_(d, tz);
  }

  // Fallback: use call time if present
  const ct = Number(_ing_pick_(row, srcMap, ['calltime']) || 0);
  const days = ct > 0 ? cfg.answered : cfg.noAnswer;
  const d = new Date(base.getTime() + (days * 86400000));
  return _ing_fmtYMD_(d, tz);
}

/** Update Outbound Leads → Next Call for a set of phones */
function _ing_setNextCallForPhones_(recallMap) {
  const ss = ssById_(CFG().OUTBOUND_SS_ID);
  const sh = ss.getSheetByName('Outbound Leads');
  if (!sh) return;

  const lastR = sh.getLastRow(), lastC = sh.getLastColumn();
  if (lastR < 2 || lastC < 1) return;
  const vals = sh.getRange(1, 1, lastR, lastC).getValues();
  const headers = vals.shift();
  const H = {}; 
  headers.forEach((h, i) => H[String(h).trim().toLowerCase()] = i);
  const pCol = H['phone'];
  let nCol = H['next call'];
  if (pCol == null) return;

  // create Next Call column if missing
  if (nCol == null) {
    sh.insertColumnAfter(lastC);
    nCol = lastC; // new column index (0-based in our headers map)
    sh.getRange(1, lastC + 1, 1, 1).setValue('Next Call');
  }

  // Build phone -> 1-based row map
  const map = {};
  for (let r = 0; r < vals.length; r++) {
    const norm = normalizePhone_(vals[r][pCol]);
    if (norm) map[norm] = r + 2;
  }

  const updates = [];
  Object.keys(recallMap).forEach(ph => {
    const ri = map[normalizePhone_(ph)];
    if (ri) updates.push({ row: ri, ymd: recallMap[ph] });
  });

  // Write in contiguous blocks
  updates.sort((a, b) => a.row - b.row);
  let i = 0;
  while (i < updates.length) {
    const start = updates[i].row;
    let end = start;
    let j = i + 1;
    const block = [updates[i].ymd];
    while (j < updates.length && updates[j].row === end + 1) { 
      end = updates[j].row; 
      block.push(updates[j].ymd); 
      j++; 
    }
    sh.getRange(start, nCol + 1, block.length, 1).setValues(block.map(v => [v]));
    i = j;
  }
}
/* ======================================================================== */
/* ===================== RESULTS (exact mapping) ========================= */
/* ======================================================================== */

// Exact client Results order (25 columns)
const TARGET_RESULTS_HEADERS = [
  'Date', 'First Name', 'Last Name', 'Phone', 'Address', 'City', 'Input State', 'State Given', 'Zip',
  'Input Email', 'Email Given', 'Accredited', 'Interested', 'New Investments', 'Liquid To Invest',
  'Job', 'Follow Up', 'Summery', 'Quality', 'Recording', 'Call Time', 'Correct Name', 'DNC',
  'Disconnection Reason', 'Run'
];

// Normalize header label → compare (case/whitespace/punct insensitive)
function _normKey_(s) { 
  return String(s || '').toLowerCase().replace(/[^a-z0-9]/g, ''); 
}

// Build src header → index map
function _buildSrcMap_(headers) {
  const m = {};
  headers.forEach((h, i) => { 
    m[_normKey_(h)] = i; 
  });
  return m;
}

// Aliases for tolerant matching (normalized keys)
function _aliases_(targetNorm) {
  const A = {
    'date': ['date'],
    'firstname': ['firstname', 'first'],
    'lastname': ['lastname', 'last'],
    'phone': ['phone', 'phonenumber', 'to_number', 'tonumber'],
    'address': ['address'],
    'city': ['city'],
    'inputstate': ['inputstate', 'stateinput'],
    'stategiven': ['stategiven', '_state'],
    'zip': ['zip', 'zipcode', 'zip_code'],
    'inputemail': ['inputemail', 'emailinput'],
    'emailgiven': ['emailgiven', '_email', 'emailprovided'],
    'accredited': ['accredited'],
    'interested': ['interested'],
    'newinvestments': ['newinvestments'],
    'liquidtoinvest': ['liquidtoinvest', 'liquid', 'liquid_to_invest'],
    'job': ['job'],
    'followup': ['followup', 'follow_up'],
    'summery': ['summery', 'summary'],
    'quality': ['quality'],
    'recording': ['recording', 'recordingurl', 'recording_url'],
    'calltime': ['calltime', 'call_time'],
    'correctname': ['correctname', 'correct_name'],
    'dnc': ['dnc'],
    'disconnectionreason': ['disconnectionreason', 'disconnection_reason'],
    'run': ['run', 'runnumber', 'run_number', 'status']
  };
  const list = A[targetNorm] || [];
  if (!list.includes(targetNorm)) list.push(targetNorm);
  return list;
}

// Pick first non-empty by candidates (stringified)
function _pick_(row, srcMap, cands) {
  for (let i = 0; i < cands.length; i++) {
    const idx = srcMap[cands[i]];
    if (typeof idx === 'number' && idx >= 0) {
      const v = row[idx];
      if (v !== '' && v !== null && v !== undefined) return String(v);
    }
  }
  return '';
}

// Derive run if needed (Status like "Run 3" → "3")
function _deriveRun_(row, srcMap, fallbackRunTag) {
  if (fallbackRunTag) {
    const m = String(fallbackRunTag).match(/(\d+)/);
    if (m) return m[1];
  }
  const v = _pick_(row, srcMap, ['run', 'runnumber', 'run_number']);
  if (v) { 
    const m = String(v).match(/(\d+)/); 
    return m ? m[1] : String(v); 
  }
  const status = _pick_(row, srcMap, ['status']);
  if (status) { 
    const m = String(status).match(/run\s*(\d+)/i); 
    if (m) return m[1]; 
  }
  return '';
}

// Ensure destination Results tab exists with EXACT headers
function _ensureResultsSheet_() {
  const P = PropertiesService.getScriptProperties();
  const resultsId = P.getProperty('RESULTS_SS_ID') || (typeof OUTBOUND_SS_ID !== 'undefined' ? OUTBOUND_SS_ID : '');
  const resultsTab = P.getProperty('RESULTS_TAB_NAME') || 'Results';
  if (!resultsId) throw new Error('RESULTS_SS_ID not set (or OUTBOUND_SS_ID missing)');
  const ss = SpreadsheetApp.openById(resultsId);
  let sh = ss.getSheetByName(resultsTab);
  if (!sh) sh = ss.insertSheet(resultsTab);

  // Make sure header row is exactly the target
  if (sh.getMaxColumns() < TARGET_RESULTS_HEADERS.length) {
    sh.insertColumnsAfter(sh.getMaxColumns(), TARGET_RESULTS_HEADERS.length - sh.getMaxColumns());
  }
  sh.getRange(1, 1, 1, TARGET_RESULTS_HEADERS.length).setValues([TARGET_RESULTS_HEADERS]);
  return sh;
}

function writeResultsMapped_(srcHeaders, dataRows, sentMap) {
  const P = PropertiesService.getScriptProperties();
  const id = P.getProperty('RESULTS_SS_ID');
  if (!id) throw new Error('RESULTS_SS_ID not set');
  const ss = SpreadsheetApp.openById(id);

  // Load the lists from Results (or from LEAD_LISTS_SS_ID if set)
  const listsSS = _listsSpreadsheet_();
  const BAD_LIST_TAB = 'Bad Leads';
  const NI_LIST_TAB = 'Not Interested Leads';

  const badListPhones = _getPhoneSetFromSheet_(listsSS, BAD_LIST_TAB);
  const notIntPhones = _getPhoneSetFromSheet_(listsSS, NI_LIST_TAB);

  const srcMap = _buildSrcMap_(srcHeaders);
  const placed = new Set();

  const buckets = {
    "Good Leads": [],
    "Good Leads For Later": [],
    "Bad Leads": [],
    "Not Interested Leads": []
  };

  for (let i = 0; i < dataRows.length; i++) {
    const row = dataRows[i] || [];

    const phone = normalizePhone_(_pick_(row, srcMap, ['phone', 'to_number', 'tonumber', 'phonenumber']));
    const runTag = sentMap[phone] || '';
    const mapped = TARGET_RESULTS_HEADERS.map(key => {
      const norm = _normKey_(key);
      if (norm === 'run') return _deriveRun_(row, srcMap, runTag);
      if (norm === 'phone') return phone;
      return _pick_(row, srcMap, _aliases_(norm));
    });

    // =============== CLASSIFICATION ===============
    (function() {
      // Priority: Bad List > Not Interested List > Good / Good Later
      if (phone && badListPhones.has(phone)) {
        buckets['Bad Leads'].push(mapped);
        placed.add(phone);
        return;
      }
      if (phone && notIntPhones.has(phone)) {
        buckets['Not Interested Leads'].push(mapped);
        placed.add(phone);
        return;
      }

      // Classification logic
      const normalize = (val) => String(val || '').trim().toLowerCase().replace(/\s+/g, ' ');

      // Get all relevant fields and normalize them once
      const disconnectionReason = normalize(_pick_(row, srcMap, _aliases_('disconnectionreason')));
      const correctName = normalize(_pick_(row, srcMap, _aliases_('correctname')));
      const callTime = Number(_pick_(row, srcMap, _aliases_('calltime')) || 0);
      const quality = normalize(_pick_(row, srcMap, _aliases_('quality')));
      const interested = normalize(_pick_(row, srcMap, _aliases_('interested')));
      const liquidToInvest = normalize(_pick_(row, srcMap, _aliases_('liquidtoinvest')));
      const newInvestments = normalize(_pick_(row, srcMap, _aliases_('newinvestments')));
      const emailGiven = String(_pick_(row, srcMap, _aliases_('emailgiven')) || '').trim();
      const summary = normalize(_pick_(row, srcMap, _aliases_('summery'))); // 'summery' alias handles 'summary'

      let target = null;

      // --- Keyword Sets for Checks ---
      const badLeadDisconnectReasons = new Set(['max_duration_reached', 'dial_failed', 'error_no_audio_received', 'dial_busy', 'invalid_destination']);
      const badLeadCorrectNameKeywords = ['wrong number', 'phone directory / ivr', 'gatekeeper', 'fax line', 'voicemail - wrong name', 'disconnected number'];
      const positiveInterestKeywords = new Set(['yes', 'true', 'y', '1']);
      const liquidInvestKeywords = new Set(['yes', 'true', 'y', '1', 'false']);

      // --- Classification Logic with Priorities ---

      // Priority 1: Bad Leads
      const isBadDisconnect = badLeadDisconnectReasons.has(disconnectionReason);
      const isBadCorrectName = badLeadCorrectNameKeywords.some(keyword => correctName.includes(keyword));

      if (isBadDisconnect || isBadCorrectName) {
        target = 'Bad Leads';
      }
      // Priority 2: Good Leads For Later
      else if (
        correctName.includes('prospect reached') &&
        callTime > 30 &&
        (quality === 'good' || quality === 'unsure') &&
        positiveInterestKeywords.has(interested) &&
        !liquidInvestKeywords.has(liquidToInvest) &&
        (newInvestments.includes('later') || emailGiven !== '')
      ) {
        target = 'Good Leads For Later';
      }
      // Priority 3: Good Leads
      else if (
        correctName.includes('prospect reached') &&
        callTime > 30 &&
        (quality === 'good' || quality === 'unsure') &&
        positiveInterestKeywords.has(interested) &&
        (newInvestments === 'now' || emailGiven !== '')
      ) {
        target = 'Good Leads';
      }
      // Priority 4: Not Interested Leads
      else if (
        callTime > 20 &&
        correctName.includes('prospect reached') &&
        !newInvestments.includes('later') && !newInvestments.includes('now') &&
        emailGiven === '' &&
        (!positiveInterestKeywords.has(interested) || summary.includes('not interested'))
      ) {
        target = 'Not Interested Leads';
      }

      if (target) {
        buckets[target].push(mapped);
        if (phone) placed.add(phone);
      }
      // NOTE: Everything else (not in any bucket) will be handled by recall logic.
    })();
  }

  // Write out tabs (ensuring headers)
  Object.keys(buckets).forEach(tab => {
    if (!buckets[tab].length) return;
    let sh = ss.getSheetByName(tab);
    if (!sh) sh = ss.insertSheet(tab);
    sh.getRange(1, 1, 1, TARGET_RESULTS_HEADERS.length).setValues([TARGET_RESULTS_HEADERS]);
    const startRow = sh.getLastRow() + 1;
    sh.getRange(startRow, 1, buckets[tab].length, TARGET_RESULTS_HEADERS.length).setValues(buckets[tab]);
  });

  return { placedPhones: Array.from(placed) };
}