/**
 * Refreshes the counters on the "Credit" sheet based on data from the "Outbound Leads" sheet
 * and a separate Leads spreadsheet. This updated version counts Runs 1-8 from "Outbound Leads"
 * and lead status categories from their respective tabs in another spreadsheet.
 */
function refreshOutboundCounters() {
  // --- SETUP ---
  const ss = (typeof CFG === 'function' && CFG().OUTBOUND_SS_ID)
    ? SpreadsheetApp.openById(CFG().OUTBOUND_SS_ID)
    : SpreadsheetApp.getActiveSpreadsheet();

  const outSheet = ss.getSheetByName('Outbound Leads') || ss.getSheetByName((typeof TAB_OUTBOUND !== 'undefined') ? TAB_OUTBOUND : 'Outbound Leads');
  const credit   = ss.getSheetByName('Credit')         || ss.getSheetByName((typeof TAB_CREDIT   !== 'undefined') ? TAB_CREDIT   : 'Credit');

  if (!outSheet) throw new Error('Outbound Leads sheet not found.');
  if (!credit)   throw new Error('Credit sheet not found.');

  // --- INITIALIZE COUNTERS ---
  const counts = { 
    r1: 0, r2: 0, r3: 0, r4: 0, r5: 0, r6: 0, r7: 0, r8: 0, 
    good: 0, later: 0, bad: 0, notInterested: 0 
  };

  const lastRow = outSheet.getLastRow();
  const lastCol = outSheet.getLastColumn();

  // --- PART 1: COUNT RUNS from 'Outbound Leads' sheet ---
  // This part only runs if there is data in the Outbound Leads sheet.
  if (lastRow >= 2) {
    const header = outSheet.getRange(1, 1, 1, lastCol).getValues()[0];
    const runCol = _findHeaderIndexCi_(header, 'run');
    if (runCol === -1) {
      // Log a warning instead of stopping the script, so lead status can still be counted.
      Logger.log('Warning: "Run" column not found in Outbound Leads sheet. Skipping run counts.');
    } else {
      const runVals = outSheet.getRange(2, runCol + 1, lastRow - 1, 1).getValues();
      runVals.forEach(r => {
        const raw = String(r[0] || '').trim().toLowerCase();
        if (!raw) return;

        // This section now only looks for numeric runs.
        const m = raw.match(/\d+/);
        if (m) {
          const n = parseInt(m[0], 10);
          if      (n === 1) counts.r1++;
          else if (n === 2) counts.r2++;
          else if (n === 3) counts.r3++;
          else if (n === 4) counts.r4++;
          else if (n === 5) counts.r5++;
          else if (n === 6) counts.r6++;
          else if (n === 7) counts.r7++;
          else if (n === 8) counts.r8++;
        }
      });
    }
  }

  // --- PART 2: COUNT LEAD STATUSES from separate spreadsheet ---
  try {
    const leadsSsId = PropertiesService.getScriptProperties().getProperty('RESULTS_SS_ID');
    const leadsSs = SpreadsheetApp.openById(leadsSsId);

    // Helper function to get row count from a sheet, assuming 1 header row.
    const getLeadCount = (sheetName) => {
      const sheet = leadsSs.getSheetByName(sheetName);
      // Math.max(0, ...) ensures we don't get a negative count if the sheet is empty or just has a header.
      return sheet ? Math.max(0, sheet.getLastRow() - 1) : 0;
    };

    counts.good          = getLeadCount('Good Leads');
    counts.later         = getLeadCount('Good Leads for Later');
    counts.bad           = getLeadCount('Bad Leads');
    counts.notInterested = getLeadCount('Not Interested Leads');

  } catch (e) {
    Logger.log(`Error accessing or counting leads from spreadsheet ID specified in RESULTS_SS_ID: ${e.message}`);
    // If the spreadsheet is inaccessible, the counts for these categories will remain 0.
  }

  // --- WRITE ALL COUNTS to Credit sheet ---
  const values = [
    [counts.r1],           // F4
    [counts.r2],           // F5
    [counts.r3],           // F6
    [counts.r4],           // F7
    [counts.r5],           // F8
    [counts.r6],           // F9
    [counts.r7],           // F10
    [counts.r8],           // F11
    [counts.good],         // F12
    [counts.later],        // F13
    [counts.bad],          // F14
    [counts.notInterested] // F15
  ];
  
  // Write all values (including any zeros) to the specified range.
  credit.getRange('F4:F15').setValues(values);
}

/** * Case/space-insensitive header index finder. Returns 0-based index or -1. 
 * This function is unchanged.
 */
function _findHeaderIndexCi_(headers, wanted) {
  const target = String(wanted).toLowerCase().replace(/\s+/g, '');
  for (let i = 0; i < headers.length; i++) {
    const h = String(headers[i] || '').toLowerCase().replace(/\s+/g, '');
    if (h === target) return i;
  }
  // secondary: contains (less strict) to tolerate odd labels
  for (let i = 0; i < headers.length; i++) {
    const h = String(headers[i] || '').toLowerCase();
    if (h.indexOf(String(wanted).toLowerCase()) !== -1) return i;
  }
  return -1;
}

/**
 * Trigger handler to refresh counters when Credit!Z1 changes.
 * @param {GoogleAppsScript.Events.SheetsOnEdit} e
 */
function handleEdit(e) {
  const range = e && e.range;
  if (!range) return;
  const sheet = range.getSheet();
  if (sheet.getName() === 'Credit' && range.getA1Notation() === 'Z1') {
    refreshOutboundCounters();
  }
}

/**
 * Installs the onEdit trigger for handleEdit once.
 * Note: This requires CFG() to be available to identify the correct spreadsheet.
 */
function createInstallableOnEditTrigger() {
  const spreadsheetId = CFG().OUTBOUND_SS_ID;
  if (!spreadsheetId) {
    Logger.log('OUTBOUND_SS_ID not found in config. Cannot create trigger.');
    return;
  }

  const triggers = ScriptApp.getProjectTriggers();
  const triggerExists = triggers.some(function(t) {
    return t.getHandlerFunction() === 'handleEdit' &&
           t.getTriggerSource() === ScriptApp.TriggerSource.SPREADSHEETS &&
           t.getTriggerSourceId() === spreadsheetId;
  });

  if (!triggerExists) {
    ScriptApp.newTrigger('handleEdit')
      .forSpreadsheet(spreadsheetId)
      .onEdit()
      .create();
    Logger.log('Created onEdit trigger for handleEdit on spreadsheet ID ' + spreadsheetId);
  } else {
    Logger.log('onEdit trigger for handleEdit already exists for spreadsheet ID ' + spreadsheetId);
  }
}