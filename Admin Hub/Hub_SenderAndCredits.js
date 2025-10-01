/** Credits helpers (stored in Outbound: Credit!B1 and ledger rows under A3:C3) */
function getBalance_(){
  const ss=ssById_(CFG().OUTBOUND_SS_ID); const sh=sh_(ss, TAB_CREDIT);
  return Number(sh.getRange('B1').getValue())||0;
}
function setBalance_(v){
  const ss=ssById_(CFG().OUTBOUND_SS_ID); const sh=sh_(ss, TAB_CREDIT);
  sh.getRange('B1').setValue(v);
}
function logCreditMove_(date, sent, refunded){
  const ss=ssById_(CFG().OUTBOUND_SS_ID); const sh=sh_(ss, TAB_CREDIT);
  if (sh.getLastRow()<3) { sh.getRange('A3:C3').setValues([['Date','Calls Sent','Credits Refunded']]); }
  sh.appendRow([date, sent||0, refunded||0]);
}

/** Maintain hidden Sent Index */
function appendSentIndex_(phones, run) {
  const ss = ssById_(CFG().OUTBOUND_SS_ID);
  const sh = sh_(ss, TAB_SENT_INDEX);
  ensureHeaders_(sh, ['Phone','Run','BatchTime']);
  sh.hideSheet();
  const t = Utilities.formatDate(new Date(), CFG().CT_TZ, "yyyy-MM-dd HH:mm:ss");
  
  // UPDATED: Convert +1XXXXXXXXXX to XXXXXXXXXX for storage
  const rows = phones.map(p => {
    let normalizedPhone = p;
    // Remove +1 prefix if present
    if (p.startsWith('+1') && p.length === 12) {
      normalizedPhone = p.substring(2);  // Remove '+1'
    } else if (p.startsWith('1') && p.length === 11) {
      normalizedPhone = p.substring(1);   // Remove '1'
    }
    return [normalizedPhone, `Run ${run}`, t];
  });
  
  if (rows.length) {
    sh.getRange(sh.getLastRow()+1,1,rows.length,3).setValues(rows);
  }
}

/** Build up to MAX_PER_RUN tasks from a given sheet (Outbound Leads or Recycle) */
function collectTasksForRun_(sourceTab, runNumber){
  const ss = ssById_(CFG().OUTBOUND_SS_ID);
  const sh = sh_(ss, sourceTab);
  const {headers, rows} = getSheetData_(sh);
  if (!headers.length) return {tasks:[], rowsUsed: []};

  const H = headerMap_(headers);
  const idx = {
    first: H['first name'], last: H['last name'], phone: H['phone'],
    addr: H['address'], city: H['city'], state: H['state'], zip: H['zip'],
    email: H['email'], run: H['run']
  };
  if (idx.phone==null) throw new Error('Phone column missing in '+sourceTab);

  const max = CFG().MAX_PER_RUN;
  const tasks = [];
  const rowsUsed = []; // {rowNumber, phone}

  for (let r=0; r<rows.length && tasks.length<max; r++){
    const row = rows[r];
    const rawRun = (idx.run!=null ? String(row[idx.run]||'').trim() : '');
    const currentRun = rawRun ? rawRun.replace(/run\s*/i,'').trim() : '';
    const isBlank = !rawRun;

    // Selection rule: Run1 takes blank or 1; RunN takes N
    const eligible = (runNumber===1 && (isBlank || currentRun==='1' || /run\s*1/i.test(rawRun)))
                  || (runNumber>1 && (currentRun===String(runNumber) || new RegExp(`run\\s*${runNumber}`,'i').test(rawRun)));
    if (!eligible) continue;

    const phoneRaw = row[idx.phone];
    const phone = normalizePhoneE164_(phoneRaw);  // enforce E.164 for Retell
    if (!phone) continue; // skip rows that can't normalize

    // Build dynamic variables (strings only; protects zip etc.)
    const vars = coerceToStringMap_({
      run: `Run ${runNumber}`,
      first_name: idx.first!=null ? row[idx.first] : '',
      last_name:  idx.last!=null  ? row[idx.last]  : '',
      address:    idx.addr!=null  ? row[idx.addr]  : '',
      city:       idx.city!=null  ? row[idx.city]  : '',
      state:      idx.state!=null ? row[idx.state] : '',
      zip:        idx.zip!=null   ? row[idx.zip]   : '',
      email:      idx.email!=null ? row[idx.email] : ''
    });

    const t = {
      to_number: phone,
      agent_id:  CFG().AGENT[runNumber],
      retell_llm_dynamic_variables: vars
    };
    tasks.push(t);
    rowsUsed.push({rowNumber: r+2, phone});
  }
  return {tasks, rowsUsed};
}

function sendLeadViaCF(lead){
  const {CLOUD_FUNCTION_URL, FROM_NUMBER, AGENT_ID} = CFG();
  if (!CLOUD_FUNCTION_URL) return false;

  // Belt & suspenders: enforce shapes here too
  lead = lead || {};
  const phone = normalizePhoneE164_(lead.phone || lead.to_number || '');
  if (!phone) return false; // don't send invalid
  const vars  = coerceToStringMap_(lead.meta || lead.vars || {}); // if you pass any

  try{
    const res = UrlFetchApp.fetch(CLOUD_FUNCTION_URL, {
      method:'post',
      contentType:'application/json',
      muteHttpExceptions:true,
      payload: JSON.stringify({
        action:'send_one',
        lead: {
          first_name: lead.first_name || '',
          last_name:  lead.last_name  || '',
          address:    lead.address    || '',
          city:       lead.city       || '',
          state:      lead.state      || '',
          zip:        String(lead.zip || ''),   // string cast here too
          email:      lead.email      || '',
          phone:      phone,
          meta:       vars
        },
        config: { from_number: FROM_NUMBER, agent_id: AGENT_ID }
      })
    });
    const code = res.getResponseCode();
    if (code>=200 && code<300){
      const body = JSON.parse(res.getContentText()||'{}');
      return !!(body && body.ok!==false);
    }
    return false;
  } catch(e){ return false; }
}

/** Call Retell batch API (create-batch-call). Returns {ok, accepted, message}. */
function sendBatchToRetell_(runNumber, tasks) {
  if (!tasks.length) return {ok:false, accepted:0, message:'No tasks'};

  // Guard: ensure required per-run config exists
  const agent = CFG().AGENT[runNumber];
  const from  = CFG().FROM[runNumber];
  if (!agent || !from) {
    return {ok:false, accepted:0, message:`Missing AGENT or FROM for Run ${runNumber}`};
  }

  const payload = {
    from_number: from,
    name: `Batch Run ${runNumber} – ${Utilities.formatDate(new Date(), CFG().CT_TZ, 'yyyy-MM-dd HH:mm:ss')}`,
    trigger_timestamp: Date.now(),
    tasks
  };
  try{
    const res = UrlFetchApp.fetch('https://api.retellai.com/create-batch-call', {
      method: 'post',
      contentType: 'application/json',
      muteHttpExceptions: true,
      headers: { 'Authorization': 'Bearer '+CFG().RETELL_API_KEY },
      payload: JSON.stringify(payload)
    });
    const code = res.getResponseCode();
    if (code>=200 && code<300) {
      // Best effort: accept count = tasks length (API may return details; if so, parse here)
      return {ok:true, accepted:tasks.length, message:'ok'};
    }
    return {ok:false, accepted:0, message:`HTTP ${code} - ${res.getContentText()}`};
  } catch(e){ return {ok:false, accepted:0, message:String(e)}; }
}

/** Orchestrate a batch send with credit enforcement + stamp Last Call + robust run-bump */
/** Orchestrate a batch send with credit enforcement (+ optional per-request limit) */
function hubSendRun_(sourceTab, runNumber, limitOverride){
  const bal = getBalance_();
  const {tasks, rowsUsed} = collectTasksForRun_(sourceTab, runNumber);

  const perRequest = (limitOverride && isFinite(limitOverride)) ? limitOverride : CFG().MAX_PER_RUN;
  const need = Math.min(tasks.length, perRequest, 1000); // hard cap 1,000
  if (!need) return {ok:false, error:'No eligible rows found for this run.'};
  if (bal < need) return {ok:false, error:`INSUFFICIENT_CREDITS: need ${need}, have ${bal}`};

  // Trim
  const finalRowsUsed = rowsUsed.slice(0, need);         // [{rowNumber, phone}]
  const finalPhones   = finalRowsUsed.map(x => x.phone); // E.164
  const finalTasks    = tasks.slice(0, need);

  // Debit
  const ts = Utilities.formatDate(new Date(), CFG().CT_TZ, 'yyyy-MM-dd HH:mm:ss');
  setBalance_(bal - need);

  // Send to Retell
  const result = sendBatchToRetell_(runNumber, finalTasks);
  if (!result.ok) {
    // refund & log
    setBalance_(getBalance_() + need);
    logCreditMove_(ts, 0, need);
    return {ok:false, error:`Retell error: ${result.message}`, refunded:need};
  }

  // Log debit
  logCreditMove_(ts, need, 0);

  // Sent index for archive run stamping
  appendSentIndex_(finalPhones, runNumber);

  // Stamp Last Call today + clear Next Call (ingest will set if neutral)
  try {
    setLastCallForPhones_('Outbound Leads', finalPhones);
  } catch(e){
    Logger.log('setLastCallForPhones_ warning: ' + e);
  }

  // Bump Run (by row numbers first; fallback by phone)
  try {
    const nextStr = String(Math.min(Number(runNumber||1) + 1, 9)); // "2","3",…
    const rowNums = finalRowsUsed.map(x => x.rowNumber);
    const wroteByRow = setRunsByRowNumbers_('Outbound Leads', rowNums, nextStr);
    const wroteByPhone = wroteByRow ? 0 : setRunsForPhones_('Outbound Leads', finalPhones, nextStr);
    Logger.log(`Run bump: rows=${wroteByRow ? rowNums.length : 0}, phones=${wroteByPhone}, next=${nextStr}`);
  } catch(e){
    Logger.log('Run bump error: ' + e);
  }

  return {ok:true, sent:need, balance_after:getBalance_()};
}

/** Set Run for specific 1-based row numbers on a tab. Returns true if any write occurred. */
function setRunsByRowNumbers_(tabName, rowNumbers, valueToWrite){
  if (!rowNumbers || !rowNumbers.length) return false;
  const ss = ssById_(CFG().OUTBOUND_SS_ID);
  const sh = sh_(ss, tabName);
  const {headers} = getSheetData_(sh);
  if (!headers.length) return false;
  const H = headerMap_(headers);
  const rCol = H['run'];
  if (rCol == null) { Logger.log('setRunsByRowNumbers_: missing Run column'); return false; }

  // unique, valid, sorted
  const rows = Array.from(new Set(rowNumbers)).filter(n => n && n>=2).sort((a,b)=>a-b);
  if (!rows.length) return false;

  let wrote = false;
  let i = 0;
  while (i < rows.length){
    const start = rows[i];
    let end = start, j = i+1;
    while (j < rows.length && rows[j] === end+1) { end = rows[j]; j++; }
    const len = end - start + 1;
    sh.getRange(start, rCol+1, len, 1).setValues(Array(len).fill([valueToWrite]));
    wrote = true;
    i = j;
  }
  return wrote;
}

/** Fallback: set Run for a list of phones on a given tab. Returns rows written count. */
function setRunsForPhones_(tabName, phoneList, valueToWrite){
  if (!phoneList || !phoneList.length) return 0;
  const ss = ssById_(CFG().OUTBOUND_SS_ID);
  const sh = sh_(ss, tabName);
  const {headers, rows} = getSheetData_(sh);
  if (!headers.length || !rows.length) return 0;

  const H = headerMap_(headers);
  const pCol = H['phone'], rCol = H['run'];
  if (pCol == null || rCol == null) { Logger.log(`setRunsForPhones_: missing Phone/Run on "${tabName}"`); return 0; }

  // phone → row#
  const indexByPhone = {};
  for (let i=0;i<rows.length;i++){
    const norm = normalizePhone_(rows[i][pCol]);
    if (norm) indexByPhone[norm] = i+2;
  }

  const targets = [];
  phoneList.forEach(p=>{
    const ri = indexByPhone[normalizePhone_(p)];
    if (ri) targets.push(ri);
  });
  if (!targets.length) return 0;

  targets.sort((a,b)=>a-b);
  let written = 0, i=0;
  while (i<targets.length){
    const start = targets[i];
    let end = start, j=i+1;
    while (j<targets.length && targets[j]===end+1){ end=targets[j]; j++; }
    const len = end-start+1;
    sh.getRange(start, rCol+1, len, 1).setValues(Array(len).fill([valueToWrite]));
    written += len; i=j;
  }
  return written;
}

/** Stamp “Last Call” (yyyy-MM-dd) and clear “Next Call” so ingest can set it if neutral later. */
function setLastCallForPhones_(tabName, phoneList) {
  if (!phoneList || !phoneList.length) return;
  const ss = ssById_(CFG().OUTBOUND_SS_ID);
  const sh = ss.getSheetByName(tabName);
  if (!sh) return;

  const {headers, rows} = getSheetData_(sh);
  const H = headerMap_(headers);
  const pCol = H['phone'], lcCol = H['last call'], ncCol = H['next call'];
  if (pCol==null) { Logger.log('setLastCallForPhones_: missing Phone column'); return; }

  // Build phone → row#
  const map = {};
  for (let i=0;i<rows.length;i++){
    const norm = normalizePhone_(rows[i][pCol]);
    if (norm) map[norm] = i+2;
  }
  const ymd = Utilities.formatDate(new Date(), CFG().CT_TZ, 'yyyy-MM-dd');

  const targets = [];
  phoneList.forEach(ph=>{
    const ri = map[normalizePhone_(ph)];
    if (ri) targets.push(ri);
  });
  if (!targets.length) return;

  targets.sort((a,b)=>a-b);
  let i=0;
  while (i<targets.length){
    const start = targets[i];
    let end = start, j=i+1;
    while (j<targets.length && targets[j]===end+1){ end=targets[j]; j++; }
    const len = end-start+1;

    if (lcCol!=null) {
      const rngLC = sh.getRange(start, lcCol+1, len, 1);
      rngLC.setNumberFormat('yyyy-mm-dd');
      rngLC.setValues(Array(len).fill([ymd]));
    }
    if (ncCol!=null) {
      const rngNC = sh.getRange(start, ncCol+1, len, 1);
      rngNC.setValue(''); // clear; ingest will set if neutral outcome later
    }
    i=j;
  }
}

const SCRIPT_PROPS = PropertiesService.getScriptProperties();
const TRIGGER_FUNCTION_NAME = 'processSendRunBatch';

/**
 * Orchestrator function to start a long-running send process.
 * This function sets up the initial state and creates a trigger to start the work.
 * @param {string} sourceTab - The name of the sheet to read tasks from.
 * @param {number} runNumber - The current run number.
 * @param {number} limit - The total number of tasks to process.
 * @returns {Object} An object indicating that the process has started.
 */
function startHubSendRun(sourceTab, runNumber, limit) {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) {
    return { ok: false, error: 'A run is already in progress. Please wait.' };
  }

  try {
    // Clean up any old trigger and properties first
    _deleteTrigger(TRIGGER_FUNCTION_NAME);
    SCRIPT_PROPS.deleteProperty('sendRunState');

    const { tasks, rowsUsed } = collectTasksForRun_(sourceTab, runNumber);

    if (!tasks.length) {
      return { ok: false, error: `No eligible leads for Run ${runNumber}.` };
    }

    const balance = getBalance_();
    const perRequest = (limit && isFinite(limit)) ? limit : CFG().MAX_PER_RUN;
    const toSendCount = Math.min(tasks.length, balance, perRequest, 1000); // hard cap 1,000

    if (toSendCount <= 0) {
      const reason = (balance < 1) ? 'Insufficient credits' : 'No tasks to send';
      return { ok: false, error: `${reason}.` };
    }

    const state = {
      sourceTab: sourceTab,
      runNumber: runNumber,
      totalToSend: toSendCount,
      tasks: tasks.slice(0, toSendCount),
      rowsUsed: rowsUsed.slice(0, toSendCount),
      processedCount: 0,
      okCount: 0,
      failCount: 0,
      startTime: new Date().getTime(),
      user: Session.getActiveUser().getEmail()
    };

    SCRIPT_PROPS.setProperty('sendRunState', JSON.stringify(state));

    ScriptApp.newTrigger(TRIGGER_FUNCTION_NAME)
      .timeBased()
      .after(100) // 100 ms delay
      .create();

    const message = `Started processing ${toSendCount} tasks for Run ${runNumber}.`;
    return { ok: true, message: message };

  } catch (e) {
    return { ok: false, error: `Failed to start run: ${e.toString()}` };
  } finally {
    lock.releaseLock();
  }
}

/**
 * Deletes a trigger by its handler function name.
 * @param {string} functionName - The name of the function the trigger calls.
 */
function _deleteTrigger(functionName) {
  try {
    const triggers = ScriptApp.getProjectTriggers();
    for (const trigger of triggers) {
      if (trigger.getHandlerFunction() === functionName) {
        ScriptApp.deleteTrigger(trigger);
      }
    }
  } catch (e) {
    // ignore errors
  }
}

/**
 * The worker function that processes a batch of tasks.
 */
function processSendRunBatch() {
  const startTime = Date.now();
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(15000)) {
    return;
  }

  let state;
  try {
    const stateJson = SCRIPT_PROPS.getProperty('sendRunState');
    if (!stateJson) {
      _deleteTrigger(TRIGGER_FUNCTION_NAME);
      return;
    }
    state = JSON.parse(stateJson);

    const BATCH_SIZE_MAX = 100; // Keep batches smaller
    const timeRemaining = (300 - (Date.now() - startTime) / 1000);
    const dynamicBatchSize = timeRemaining < 30 ? 20 : (timeRemaining < 60 ? 50 : BATCH_SIZE_MAX);

    const remainingTasks = state.tasks.slice(state.processedCount);
    const batchTasks = remainingTasks.slice(0, dynamicBatchSize);
    const batchRowsUsed = state.rowsUsed.slice(state.processedCount, state.processedCount + batchTasks.length);

    if (batchTasks.length === 0) {
      _finalizeRun(state);
      return;
    }

    const ts = Utilities.formatDate(new Date(), CFG().CT_TZ, 'yyyy-MM-dd HH:mm:ss');
    const bal = getBalance_();
    const need = batchTasks.length;

    // Debit credits for the batch
    setBalance_(bal - need);

    const result = sendBatchToRetell_(state.runNumber, batchTasks);

    if (!result.ok) {
      // Refund and log failure for the whole batch
      setBalance_(getBalance_() + need);
      logCreditMove_(ts, 0, need);
      state.failCount += need;
    } else {
      // Log success for the whole batch
      logCreditMove_(ts, need, 0);
      state.okCount += need;

      const phonesSent = batchRowsUsed.map(r => r.phone);
      const rowsSent = batchRowsUsed.map(r => r.rowNumber);

      appendSentIndex_(phonesSent, state.runNumber);
      setLastCallForPhones_(state.sourceTab, phonesSent);

      const nextRun = Math.min(state.runNumber + 1, 9);
      setRunsByRowNumbers_(state.sourceTab, rowsSent, String(nextRun));
    }

    state.processedCount += batchTasks.length;
    SCRIPT_PROPS.setProperty('sendRunState', JSON.stringify(state));

    if (state.processedCount >= state.totalToSend) {
      _finalizeRun(state);
    } else {
      _ensureTrigger(TRIGGER_FUNCTION_NAME, 1);
    }

  } catch (e) {
    if(state) {
      _finalizeRun(state, `Error: ${e.toString()}`);
    } else {
      _deleteTrigger(TRIGGER_FUNCTION_NAME);
      SCRIPT_PROPS.deleteProperty('sendRunState');
    }
  } finally {
    lock.releaseLock();
  }
}

/**
 * Ensures a trigger for the given function exists.
 */
function _ensureTrigger(functionName, minutes) {
  _deleteTrigger(functionName);
  ScriptApp.newTrigger(functionName)
    .timeBased()
    .after(minutes * 60 * 1000)
    .create();
}

/**
 * Finalizes a run, cleaning up resources.
 */
function _finalizeRun(state, errorMessage) {
  _deleteTrigger(TRIGGER_FUNCTION_NAME);
  SCRIPT_PROPS.deleteProperty('sendRunState');

  // Mark cooldown only if at least one call was successful
  if (state.okCount > 0 && typeof markCooldownNow_ === 'function') {
    markCooldownNow_('GLOBAL');
  }

  // Final logging can be added here if needed
}