/**
 * Clear Run on Outbound Leads for any phones that appear in Results tabs:
 *  - Not Interested Leads
 *  - Bad Leads
 *  - Good Leads For Later
 *  - Good Leads
 *
 * USAGE:
 *   sweepClassifiedFreezeRuns();  // uses defaults below
 *   // or pass explicit IDs/tabs:
 *   sweepClassifiedFreezeRuns({
 *     resultsId: '1fioSXdZqAUKwNUQb0akwST7vKK7QhwU4thnWM7TcJC0',
 *     outboundId:'1zOKYe4lSZDUA8uHjsGSxDavZQspeUok4GEupm4OCO9c',
 *     tabs: ['Not Interested Leads','Bad Leads','Good Leads For Later','Good Leads']
 *   });
 */
function sweepClassifiedFreezeRuns(opts) {
  opts = opts || {};
  // Defaults from your message
  var RESULTS_ID  = opts.resultsId  || '1fioSXdZqAUKwNUQb0akwST7vKK7QhwU4thnWM7TcJC0';
  var OUTBOUND_ID = opts.outboundId || '1zOKYe4lSZDUA8uHjsGSxDavZQspeUok4GEupm4OCO9c';
  var TABS = opts.tabs || ['Not Interested Leads','Bad Leads','Good Leads For Later','Good Leads'];

  // 1) Collect phones from Results tabs
  var classifiedSet = collectPhonesFromResults_(RESULTS_ID, TABS);

  // Nothing to do
  if (!classifiedSet || classifiedSet.size === 0) {
    return {ok:true, scannedTabs:TABS.length, resultsPhones:0, cleared:0};
  }

  // 2) Open Outbound → Outbound Leads
  var ssOut = SpreadsheetApp.openById(OUTBOUND_ID);
  var shOut = ssOut.getSheetByName('Outbound Leads');
  if (!shOut) throw new Error('Outbound Leads sheet not found in Outbound Console file.');

  var lastRow = shOut.getLastRow();
  var lastCol = shOut.getLastColumn();
  if (lastRow < 2) return {ok:true, scannedTabs:TABS.length, resultsPhones:classifiedSet.size, cleared:0};

  // Find headers (case/space-insensitive)
  var headers = shOut.getRange(1,1,1,lastCol).getValues()[0];
  var phoneCol = findHeaderIndexCi_(headers, 'phone'); // 1-based
  var runCol   = findHeaderIndexCi_(headers,  'run');  // 1-based
  if (!phoneCol || !runCol) throw new Error('Missing Phone or Run column on Outbound Leads.');

  // Read both columns (fast)
  var phones = shOut.getRange(2, phoneCol, lastRow-1, 1).getValues(); // 2..last
  var runs   = shOut.getRange(2, runCol,   lastRow-1, 1).getValues();

  // Determine which rows to clear
  var rowsToClear = [];
  for (var i=0; i<phones.length; i++) {
    var norm = normPhone10_(phones[i][0]);
    if (!norm) continue;
    if (classifiedSet.has(norm)) {
      var runVal = String(runs[i][0]||'').trim();
      if (runVal) rowsToClear.push(i+2); // convert to 1-based sheet row index, add header offset
    }
  }
  if (rowsToClear.length === 0) {
    return {ok:true, scannedTabs:TABS.length, resultsPhones:classifiedSet.size, cleared:0};
  }

  // Batch clear Run in contiguous blocks for speed
  rowsToClear.sort(function(a,b){ return a-b; });
  var cleared = 0;
  var idx = 0;
  while (idx < rowsToClear.length) {
    var start = rowsToClear[idx];
    var end = start;
    var j = idx + 1;
    while (j < rowsToClear.length && rowsToClear[j] === end + 1) { end = rowsToClear[j]; j++; }
    var len = end - start + 1;
    // Either clearContent() on that single column or set blanks; both are fine.
    // Using setValues to be explicit about writing only that column.
    shOut.getRange(start, runCol, len, 1).setValues(new Array(len).fill(['']));
    cleared += len;
    idx = j;
  }

  return {ok:true, scannedTabs:TABS.length, resultsPhones:classifiedSet.size, cleared:cleared};
}

/* ---------- helpers ---------- */

// Read phones from multiple Results tabs into a Set of normalized 10-digit strings
function collectPhonesFromResults_(resultsId, tabNames) {
  var ss = SpreadsheetApp.openById(resultsId);
  var set = new Set();
  for (var t=0; t<tabNames.length; t++) {
    var name = tabNames[t];
    var sh = ss.getSheetByName(name);
    if (!sh) continue;
    var lastRow = sh.getLastRow(), lastCol = sh.getLastColumn();
    if (lastRow < 2) continue;
    var headers = sh.getRange(1,1,1,lastCol).getValues()[0];
    var pCol = findHeaderIndexCi_(headers, 'phone'); // 1-based
    if (!pCol) continue;
    var vals = sh.getRange(2, pCol, lastRow-1, 1).getValues();
    for (var i=0;i<vals.length;i++){
      var norm = normPhone10_(vals[i][0]);
      if (norm) set.add(norm);
    }
  }
  return set;
}

// Case/space-insensitive header finder; returns 1-based column index or 0 if not found
function findHeaderIndexCi_(headers, wanted) {
  var target = String(wanted).toLowerCase().replace(/\s+/g,'');
  for (var i=0;i<headers.length;i++){
    var h = String(headers[i]||'').toLowerCase().replace(/\s+/g,'');
    if (h === target) return i+1;
  }
  // loose contains match as a fallback
  for (var j=0;j<headers.length;j++){
    var hh = String(headers[j]||'').toLowerCase();
    if (hh.indexOf(String(wanted).toLowerCase()) !== -1) return j+1;
  }
  return 0;
}

// Normalize phones to strict 10 digits; tolerate +1, punctuation, spaces
function normPhone10_(v) {
  if (v == null) return '';
  var s = String(v).trim();
  if (s.startsWith('=+')) s = '+' + s.slice(2); // Excel quirk
  else if (s.startsWith('=')) s = s.slice(1);
  var d = s.replace(/\D/g,'');            // keep digits only
  if (!d) return '';
  if (d.length === 11 && d[0] === '1') d = d.slice(1); // drop leading 1
  if (d.length > 10) d = d.slice(-10);                 // keep last 10 if longer
  return (d.length === 10) ? d : '';
}

/**
 * Wrapper: sweep freeze using Hub config values.
 * Reads OUTBOUND_SS_ID and RESULTS_SS_ID from CFG().
 */
function sweepFreezeUsingCfg_() {
  const c = CFG();  // your Hub config function
  return sweepClassifiedFreezeRuns({
    resultsId:  c.RESULTS_SS_ID,
    outboundId: c.OUTBOUND_SS_ID,
    tabs: ['Not Interested Leads','Bad Leads','Good Leads For Later','Good Leads']
  });
}
