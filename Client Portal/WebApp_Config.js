function WEB_CFG_() {
  const P = PropertiesService.getScriptProperties();
  const num = (k, d) => Number(P.getProperty(k) || d);
  const val = k => (P.getProperty(k) || '').trim();
  return {
    HUB_URL:        val('HUB_URL'),                          // Hub Web App URL (…/exec)
    CLIENT_TOKEN:   val('CLIENT_TOKEN'),                     // client token
    OUTBOUND_SS_ID: val('OUTBOUND_SS_ID'),                   // Outbound Console file id
    RESULTS_SS_ID:  val('RESULTS_SS_ID'),                    // Results workbook id (can be same file)
    OUTBOUND_TAB:   val('OUTBOUND_TAB')   || 'Outbound Leads',
    CREDIT_TAB:     val('CREDIT_TAB')     || 'Credit',
    GOOD_TAB:       val('GOOD_TAB')       || 'Good Leads',
    LATER_TAB:      val('LATER_TAB')      || 'Good Leads For Later',
    BAD_TAB:        val('BAD_TAB')        || 'Bad Leads',
    NI_TAB:         val('NI_TAB')         || 'Not Interested Leads',  // ← NEW
    DEFAULT_RUN_LIMIT:  num('DEFAULT_RUN_LIMIT', num('MAX_PER_RUN', 1000)),
    PORTAL_PASSWORD: val('PORTAL_PASSWORD'),                         // optional local fallback
    CT_TZ:           val('CT_TZ') || 'America/Chicago'
  };
}

function sanitizeHubUrl_(input) {
  const m = String(input||'').match(/https:\/\/script\.google\.com\/macros\/s\/[A-Za-z0-9\-_]+\/exec/g);
  return (m && m.length) ? m[m.length-1] : '';
}