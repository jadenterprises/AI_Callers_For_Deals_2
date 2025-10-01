/** Daily: pull due recalls (Next Call Date <= today && Processed blank) into Outbound:Recycle with Run+1 */
function hubSweepRecalls(){
  // Open current archive file (you can loop previous months too if needed)
  const {id} = ensureMonthlyArchive();
  const ssA = ssById_(id);
  const shA = sh_(ssA, 'Archive'); ensureHeaders_(shA, ARCHIVE_HEADERS);
  const {headers, rows} = getSheetData_(shA);
  if (!headers.length || !rows.length) return;

  const H = headerMap_(headers); // lower-cased keys
  const idx = name => (H[name] != null ? H[name] : -1);

  const ix = {
    date: idx('date'),
    phone: idx('phone'),
    next: idx('next call date'),
    run: idx('run'),
    processed: idx('processed'),
    first: idx('first name'),
    last: idx('last name'),
    addr: idx('address'),
    city: idx('city'),
    state: idx('input state'),
    stategiven: idx('state given'),            // ← NEW: prefer this when present
    zip: idx('zip'),
    email: idx('input email')
  };

  const today = Utilities.formatDate(new Date(), CFG().CT_TZ, 'yyyy-MM-dd');
  const dueRows = rows.filter(r=>{
    const next = String(r[ix.next]||'');
    const proc = String(r[ix.processed]||'').trim();
    return next && next <= today && !proc;
  });

  if (!dueRows.length) return;

  // Move into Outbound:Recycle with Run+1 (cap at 9)
  const ssOut = ssById_(CFG().OUTBOUND_SS_ID);
  const shR   = sh_(ssOut, TAB_RECYCLE);
  ensureHeaders_(shR, ['First Name','Last Name','Phone','Address','City','State','Zip','Email','Run']);

  const append = [];
  const mark   = [];
  dueRows.forEach((r)=>{
    const phone = normalizePhone_(r[ix.phone]);
    const runN  = (String(r[ix.run]||'').match(/(\d+)/) ? Number(RegExp.$1) : 1);
    const nextRun = Math.min(runN+1, 9);

    // Prefer "State Given" over "Input State"
    const stateVal = (ix.stategiven >= 0 ? r[ix.stategiven] : '') ||
                     (ix.state      >= 0 ? r[ix.state]      : '') || '';

    append.push([
      r[ix.first]||'', r[ix.last]||'', phone, r[ix.addr]||'', r[ix.city]||'',
      stateVal, r[ix.zip]||'', r[ix.email]||'', `Run ${nextRun}`
    ]);

    // mark processed
    mark.push({row:r, val:`Recycled ${today}`});
  });

  if (append.length) {
    shR.getRange(shR.getLastRow()+1,1,append.length,9).setValues(append);
  }

  // Mark Processed flag in Archive
  mark.forEach((m)=>{
    const rowIdx = rows.indexOf(m.row) + 2;
    shA.getRange(rowIdx, ix.processed+1).setValue(m.val);
  });
}

/** Web API: run send by source and run number (called by stub) */
function apiSendRun_(sourceTab, runNumber, limitOverride){
  return startHubSendRun(sourceTab, runNumber, limitOverride);   // startHubSendRun accepts optional limit
}