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
    RETELL_URL:       v('RETELL_URL'),

    AGENT: {1:v('AGENT_RUN_1'),2:v('AGENT_RUN_2'),3:v('AGENT_RUN_3'),4:v('AGENT_RUN_4'),5:v('AGENT_RUN_5'),6:v('AGENT_RUN_6'),7:v('AGENT_RUN_7'),8:v('AGENT_RUN_8')},
    FROM:  {1:v('FROM_RUN_1'), 2:v('FROM_RUN_2'), 3:v('FROM_RUN_3'), 4:v('FROM_RUN_4'), 5:v('FROM_RUN_5'), 6:v('FROM_RUN_6'), 7:v('FROM_RUN_7'), 8:v('FROM_RUN_8')},
    CLIENT_TOKEN:     v('CLIENT_TOKEN'),
    GCS_BUCKET:       v('GCS_BUCKET') || 'vista-retell-calling-reference-data',
    GCS_RESULTS_PATH: v('GCS_RESULTS_PATH') || 'raw_leads/inbound_webhook_vista.csv',
    MAX_PER_RUN:      i('MAX_PER_RUN') || 1000,
    CT_TZ:            v('CT_TZ') || 'America/Chicago'
  };
}