// Hub_Config.js - Central configuration
// This file defines all configuration constants and retrieval functions

/** Central config pulled from Script Properties */
function CFG() {
  const P = PropertiesService.getScriptProperties();
  const v = k => P.getProperty(k) || '';
  const i = k => parseInt(P.getProperty(k) || '0', 10);
  return {
    FOLDER_ID:        v('FOLDER_ID'),
    OUTBOUND_SS_ID:   v('OUTBOUND_SS_ID'),
    RESULTS_SS_ID:    v('RESULTS_SS_ID'),
    RETELL_API_KEY:   v('RETELL_API_KEY'),
    RETELL_URL:       v('RETELL_URL') || 'https://api.retellai.com/create-batch-call', // Default URL
    
    AGENT: {
      1: v('AGENT_RUN_1'),
      2: v('AGENT_RUN_2'),
      3: v('AGENT_RUN_3'),
      4: v('AGENT_RUN_4'),
      5: v('AGENT_RUN_5'),
      6: v('AGENT_RUN_6'),
      7: v('AGENT_RUN_7'),
      8: v('AGENT_RUN_8')
    },
    FROM: {
      1: v('FROM_RUN_1'),
      2: v('FROM_RUN_2'),
      3: v('FROM_RUN_3'),
      4: v('FROM_RUN_4'),
      5: v('FROM_RUN_5'),
      6: v('FROM_RUN_6'),
      7: v('FROM_RUN_7'),
      8: v('FROM_RUN_8')
    },
    CLIENT_TOKEN:      v('CLIENT_TOKEN'),
    PORTAL_PASSWORD:   v('PORTAL_PASSWORD'),
    GCS_BUCKET:        v('GCS_BUCKET') || 'vista-retell-calling-reference-data',
    GCS_RESULTS_PATH:  v('GCS_RESULTS_PATH') || 'raw_leads/inbound_webhook_vista.csv',
    MAX_PER_RUN:       i('MAX_PER_RUN') || 1000,
    DEFAULT_RUN_LIMIT: i('DEFAULT_RUN_LIMIT') || i('MAX_PER_RUN') || 1000,
    CT_TZ:             v('CT_TZ') || 'America/Chicago',
    INGEST_COOLDOWN_MIN: i('INGEST_COOLDOWN_MIN') || 20
  };
}

/** Constants for recall rules */
const STATUS_GROUP_5D  = [
  'voicemail - no name', 
  'voicemail - correct name', 
  'no answer', 
  'voicemail - company name only',
  'voicemail_reached',
  'dial_no_answer',
  ''
]; 

const STATUS_GROUP_30D = ["prospect reached"];

/** Archive naming */
function archiveNameFor_(d) {
  const month = Utilities.formatDate(d, CFG().CT_TZ, 'MMMM yyyy');
  return `Archive - ${month}`;
}

/** Canonical headers for Archive sheets */
const ARCHIVE_HEADERS = [
  'Date', 'First Name', 'Last Name', 'Phone', 'Address', 'City',
  'Input State', 'State Given', 'Zip', 'Input Email', 'Email Given',
  'Accredited', 'Interested', 'New Investments', 'Liquid To Invest',
  'Past Experience', 'Job', 'Follow Up', 'Summary', 'Quality',
  'Recording', 'Call Time', 'Correct Name', 'DNC', 'Disconnection Reason',
  'Run', 'Next Call Date'
];

/** Canonical headers for Results sheets */
const RESULTS_HEADERS = [
  'Date', 'First Name', 'Last Name', 'Phone', 'Address', 'City',
  'Input State', 'State Given', 'Zip', 'Input Email', 'Email Given',
  'Accredited', 'Interested', 'New Investments', 'Liquid To Invest',
  'Past Experience', 'Job', 'Follow Up', 'Summary', 'Quality',
  'Recording', 'Call Time', 'Correct Name', 'DNC', 'Disconnection Reason',
  'Run', 'Next Call Date'
];

/** Outbound tab names */
const TAB_OUTBOUND = 'Outbound Leads';
const TAB_RECYCLE  = 'Recycle';
const TAB_CREDIT   = 'Credit';

/** Hidden phone→run index to stamp Run in Archive */
const TAB_SENT_INDEX = '_Sent Index'; // hidden; headers: Phone | Run | BatchTime

/** Client UI cooldown (minutes) */
const COOLDOWN_MINUTES_CLIENT = 21;

/** Hidden sheet to store last send timestamps */
const TAB_RATELIMIT = '_RateLimit';  // headers: Key | LastAt

// Call window in Central Time (inclusive start, exclusive end)
const CT_TZ = (typeof CFG !== 'undefined' && CFG().CT_TZ) ? CFG().CT_TZ : 'America/Chicago';
const CALL_START_HOUR_CT = 7;   // 7 AM CT
const CALL_END_HOUR_CT   = 20;  // 8 PM CT

// Optional: allow-list specific configurations
function CFG_EXTRA() {
  const P = PropertiesService.getScriptProperties();
  return {
    ALLOWED_SHEET_ID: P.getProperty('ALLOWED_SHEET_ID') || '',
    ALLOWED_EMAILS: (P.getProperty('ALLOWED_EMAILS') || '').split(',').map(s => s.trim()).filter(Boolean)
  };
}