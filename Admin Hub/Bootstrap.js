// Bootstrap.js - Handles initial setup and routing

function __bootstrapApplyProps_(p) {
  try {
    var P = PropertiesService.getScriptProperties();
    P.setProperties(p || {}, true);
    P.setProperty('BOOTSTRAPPED', '1');
    return { ok: true, keys: Object.keys(p || {}).length };
  } catch (e) { 
    return { ok: false, error: String(e) }; 
  }
}

function __bootstrapSetProps(p) { 
  return __bootstrapApplyProps_(p); 
}

function __bootstrapGetProps(keys) {
  try {
    var P = PropertiesService.getScriptProperties();
    var all = P.getProperties();
    if (Object.prototype.toString.call(keys) === '[object Array]' && keys.length) {
      var out = {}; 
      keys.forEach(function(k) { 
        if (all.hasOwnProperty(k)) out[k] = all[k]; 
      });
      return out;
    }
    return all;
  } catch (e) { 
    return { ok: false, error: String(e) }; 
  }
}

function doGet(e) {
  try {
    // Handle bootstrap operations
    if (e && e.parameter && e.parameter.apply) {
      var json = Utilities.newBlob(Utilities.base64DecodeWebSafe(e.parameter.apply)).getDataAsString();
      var props = JSON.parse(json);
      var r = __bootstrapApplyProps_(props);
      return ContentService.createTextOutput(JSON.stringify(r)).setMimeType(ContentService.MimeType.JSON);
    }
    
    if (e && e.parameter && e.parameter.ping) {
      var want = [];
      try {
        if (e.parameter.want) {
          var wjson = Utilities.newBlob(Utilities.base64DecodeWebSafe(e.parameter.want)).getDataAsString();
          want = JSON.parse(wjson);
        }
      } catch (_) {}
      var P = PropertiesService.getScriptProperties();
      var all = P.getProperties();
      var has = {};
      (want || []).forEach(function(k) { 
        has[k] = !!all[k]; 
      });
      return ContentService.createTextOutput(JSON.stringify({
        ok: true, 
        authorized: true, 
        anyProps: Object.keys(all || {}).length > 0, 
        hasProps: has
      })).setMimeType(ContentService.MimeType.JSON);
    }
    
    // Default GET response
    return ContentService.createTextOutput("Hub API is active. Use POST for actions.")
      .setMimeType(ContentService.MimeType.TEXT);
    
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({ 
      ok: false, 
      error: String(err) 
    })).setMimeType(ContentService.MimeType.JSON);
  }
}

function doPost(e) {
  try {
    // Check for bootstrap initialization first
    var P = PropertiesService.getScriptProperties();
    var initialized = !!(P.getProperty('CLIENT_TOKEN') || P.getProperty('BOOTSTRAPPED'));
    
    if (!initialized) {
      var body = e && e.postData && e.postData.contents ? JSON.parse(e.postData.contents) : {};
      if (body && body.__bootstrapProps) {
        var r = __bootstrapApplyProps_(body.__bootstrapProps);
        return ContentService.createTextOutput(JSON.stringify(r))
               .setMimeType(ContentService.MimeType.JSON);
      }
      // Not initialized and no bootstrap props - return error
      return ContentService.createTextOutput(JSON.stringify({ 
        ok: false, 
        error: 'NOT_INITIALIZED' 
      })).setMimeType(ContentService.MimeType.JSON);
    }
    
    // System is initialized - delegate to Hub API handler
    return doPostHub(e);
    
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({ 
      ok: false, 
      error: String(err) 
    })).setMimeType(ContentService.MimeType.JSON);
  }
}