/**
 * Unmerge all cells + de-duplicate by Phone.
 * Keeps latest when a Date column exists (sorts Date ↓ then dedupes).
 * Requires Advanced Sheets API enabled (Sheets v4).
 */

/** === Public entry points === */

/** Outbound Console: unmerge + dedupe on "Outbound Leads" */
function cleanOutboundLeads() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  _unmergeSheet_(ss, 'Outbound Leads');
  _dedupeByPhone_(ss, 'Outbound Leads'); // Date desc → dedupe on Phone
  SpreadsheetApp.getActive().toast('Outbound Leads: unmerged and de-duplicated by Phone.', 'Cleanup', 5);
}

/** Results workbook: unmerge + dedupe on the three tabs */
function cleanResultsSheets() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const tabs = ['Good Leads', 'Good Leads For Later', 'Bad Leads'];
  tabs.forEach(name => {
    if (!ss.getSheetByName(name)) return;
    _unmergeSheet_(ss, name);
    _dedupeByPhone_(ss, name);
  });
  SpreadsheetApp.getActive().toast('Results: unmerged and de-duplicated (Good/Later/Bad).', 'Cleanup', 5);
}


/** === Helpers === */

/** Unmerge all merged ranges in a tab */
function _unmergeSheet_(ss, sheetName){
  const sh = ss.getSheetByName(sheetName);
  if (!sh) throw new Error('Sheet not found: ' + sheetName);
  const merged = sh.getDataRange().getMergedRanges();
  merged.forEach(r => r.breakApart());
}

/**
 * Dedupe by Phone using Advanced Sheets API.
 * - If a "Date" column exists, sort that column DESC to keep the latest row for each phone.
 * - Then use deleteDuplicates on the Phone column.
 */
function _dedupeByPhone_(ss, sheetName){
  const sh = ss.getSheetByName(sheetName);
  if (!sh) throw new Error('Sheet not found: ' + sheetName);

  const sheetId = sh.getSheetId();
  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 2 || lastCol < 1) return; // nothing to do

  // Build a header map (case-insensitive, space-insensitive)
  const headers = sh.getRange(1,1,1,lastCol).getValues()[0];
  const H = _headerIndexCI_(headers); // name -> 1-based index

  const phoneCol = H['phone'];
  if (!phoneCol) throw new Error('Phone column not found on "'+sheetName+'"');

  const dateCol  = H['date'] || 0; // optional

  const requests = [];

  // 1) Sort by Date DESC if present (rows 2..last)
  if (dateCol > 0 && lastRow > 2) {
    requests.push({
      sortRange: {
        range: {
          sheetId,
          startRowIndex: 1,             // 0-based; exclude header
          endRowIndex: lastRow,
          startColumnIndex: 0,
          endColumnIndex: lastCol
        },
        sortSpecs: [{
          dimensionIndex: dateCol - 1,  // 0-based column
          sortOrder: 'DESCENDING'
        }]
      }
    });
  }

  // 2) deleteDuplicates on the Phone column (rows 2..last)
  requests.push({
    deleteDuplicates: {
      range: {
        sheetId,
        startRowIndex: 1,
        endRowIndex: lastRow,
        startColumnIndex: 0,
        endColumnIndex: lastCol
      },
      comparisonColumns: [{
        sheetId,
        dimension: 'COLUMNS',
        startIndex: phoneCol - 1,
        endIndex: phoneCol
      }]
    }
  });

  Sheets.Spreadsheets.batchUpdate({ requests }, ss.getId());
}

/** Build a case-insensitive header index lookup: 'phone' => index (1-based) */
function _headerIndexCI_(headers){
  const m = {};
  headers.forEach((h,i)=>{
    const key = String(h||'').toLowerCase().replace(/\s+/g,'');
    if (key) m[key] = i+1; // 1-based
  });
  // Also add a "contains" pass for resiliency (e.g., 'phone number')
  headers.forEach((h,i)=>{
    const low = String(h||'').toLowerCase();
    if (!m['phone'] && /phone/.test(low)) m['phone'] = i+1;
    if (!m['date']  && /date/.test(low))  m['date']  = i+1;
  });
  return m;
}
