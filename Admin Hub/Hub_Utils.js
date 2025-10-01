
// Hub_Utils.js - Centralized utility functions
// This file must be loaded first as other files depend on these utilities

/** Get spreadsheet by ID */
function ssById_(id) { 
  return SpreadsheetApp.openById(id); 
}

/** Get or create sheet by name */
function sh_(ss, name) { 
  return ss.getSheetByName(name) || ss.insertSheet(name); 
}

/** Ensure headers exist on sheet */
function ensureHeaders_(sh, headers) {
  const has = sh.getLastRow() ? sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0] : [];
  if (!has.length) { 
    sh.getRange(1, 1, 1, headers.length).setValues([headers]); 
    return; 
  }
  // append missing columns
  const missing = headers.filter(h => !has.includes(h));
  if (missing.length) {
    sh.insertColumnsAfter(has.length || 1, missing.length);
    sh.getRange(1, has.length + 1, 1, missing.length).setValues([missing]);
  }
}

/** Build header index map (lowercase keys) */
function headerMap_(headers) {
  const m = {};
  headers.forEach((h, i) => {
    const key = String(h).trim().toLowerCase();
    m[key] = i;
    // Also add version without spaces
    m[key.replace(/\s+/g, '')] = i;
  });
  return m;
}

/** Get sheet data as headers and rows */
function getSheetData_(sh) {
  const lastR = sh.getLastRow(), lastC = sh.getLastColumn();
  if (lastR < 2 || lastC < 1) return { headers: [], rows: [] };
  const vals = sh.getRange(1, 1, lastR, lastC).getValues();
  const headers = vals.shift();
  return { headers, rows: vals };
}

/** Get current time in Central Time */
function nowCT_() { 
  return new Date(Utilities.formatDate(new Date(), CFG().CT_TZ, "yyyy-MM-dd'T'HH:mm:ss")); 
}

/** Check if within call window */
function withinCallWindow_() {
  const now = new Date();
  const CT_TZ = CFG().CT_TZ || 'America/Chicago';
  const hour = Number(Utilities.formatDate(now, CT_TZ, 'H')); // 0..23
  const CALL_START_HOUR_CT = 7;   // 7 AM CT
  const CALL_END_HOUR_CT = 20;    // 8 PM CT
  return (hour >= CALL_START_HOUR_CT && hour < CALL_END_HOUR_CT);
}

/** Minutes until call window opens */
function minutesUntilOpen_() {
  const CT_TZ = CFG().CT_TZ || 'America/Chicago';
  const CALL_START_HOUR_CT = 7;
  const now = new Date();
  const tzNowStr = Utilities.formatDate(now, CT_TZ, "yyyy-MM-dd'T'HH:mm:ss");
  const base = new Date(tzNowStr);
  const hour = Number(Utilities.formatDate(base, CT_TZ, 'H'));

  if (hour < CALL_START_HOUR_CT) {
    const open = new Date(base);
    open.setHours(CALL_START_HOUR_CT, 0, 0, 0);
    return Math.max(0, Math.ceil((open.getTime() - base.getTime()) / 60000));
  }
  // past END → until tomorrow START
  const openTomorrow = new Date(base);
  openTomorrow.setDate(openTomorrow.getDate() + 1);
  openTomorrow.setHours(CALL_START_HOUR_CT, 0, 0, 0);
  return Math.max(0, Math.ceil((openTomorrow.getTime() - base.getTime()) / 60000));
}

/** Force every non-null property value to a string */
function coerceToStringMap_(obj) {
  const out = {};
  if (!obj || typeof obj !== 'object') return out;
  Object.keys(obj).forEach(k => {
    const v = obj[k];
    if (v !== null && v !== undefined) out[k] = String(v);
  });
  return out;
}

/** Normalize phone to US E.164 (+1XXXXXXXXXX) */
function normalizePhoneE164_(raw) {
  let s = String(raw || '').trim();
  // Handle weird "=+1..." / "=1408..." Sheet cases
  if (s.startsWith('=+')) s = '+' + s.slice(2);
  else if (s.startsWith('=')) s = s.slice(1);
  const digits = s.replace(/\D/g, ''); // keep only numbers
  if (!digits) return '';
  if (digits.length === 10) return '+1' + digits;                 // US 10-digit
  if (digits.length === 11 && digits[0] === '1') return '+' + digits; // 1XXXXXXXXXX
  if (digits.length >= 11 && digits.length <= 15) return '+' + digits; // allow intl
  return ''; // invalid
}

/** Normalize phone to 10-digit string (no country code) */
function normalizePhone_(raw) {
  if (!raw) return '';
  let s = String(raw).trim();
  // Handle Excel formula artifacts
  if (s.startsWith('=+')) s = '+' + s.slice(2);
  else if (s.startsWith('=')) s = s.slice(1);
  // Extract digits only
  const d = s.replace(/\D/g, '');
  if (!d) return '';
  // Handle US numbers
  if (d.length === 11 && d[0] === '1') return d.slice(1);
  if (d.length === 10) return d;
  if (d.length > 10) return d.slice(-10);
  return '';
}

/** Clean up orphaned triggers */
function cleanupOrphanedTriggers() {
  const triggers = ScriptApp.getProjectTriggers();
  const P = PropertiesService.getScriptProperties();
  const validTriggerId = P.getProperty('WEBHOOK_POLL_TRIGGER_ID');
  const sendTriggerId = P.getProperty('SEND_RUN_TRIGGER_ID');
  
  triggers.forEach(trigger => {
    const func = trigger.getHandlerFunction();
    const id = trigger.getUniqueId();
    
    // Clean up webhook polling triggers
    if (func === 'hubIngestWebhooks_' && id !== validTriggerId) {
      ScriptApp.deleteTrigger(trigger);
      console.log(`Cleaned up orphaned webhook trigger: ${id}`);
    }
    
    // Clean up send run triggers
    if (func === 'processSendRunBatch' && id !== sendTriggerId) {
      ScriptApp.deleteTrigger(trigger);
      console.log(`Cleaned up orphaned send trigger: ${id}`);
    }
  });
}

/** Thread-safe credit balance update */
function updateCreditBalance_(delta) {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(10000);
    const current = getBalance_();
    const newBalance = current + delta;
    setBalance_(newBalance);
    return newBalance;
  } catch (e) {
    throw new Error(`Failed to update credit balance: ${e.toString()}`);
  } finally {
    lock.releaseLock();
  }
}