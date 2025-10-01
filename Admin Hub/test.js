function testHubEndpoint() {
  // 1. PASTE the Web App URL from your Hub deployment here
  const HUB_URL = "https://script.google.com/macros/s/AKfycbzq5Y4uaIWHVm_VVTy1H8uySi9zs6ZZfXfoDe4vdBSPy_wCGBgubc4Oba_-ylWjbzB0Jw/exec"; 
  
  // 2. PASTE your Client Token from the Hub's script properties here
  const CLIENT_TOKEN = "tok_9mxqxyei19cszn03yocqbf";

  const payload = {
    token: CLIENT_TOKEN,
    action: 'balance' // Using a simple POST action for this test
  };

  const options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  console.log("--- Starting Independent Hub Test ---");
  console.log(`Calling URL: ${HUB_URL}`);
  
  const response = UrlFetchApp.fetch(HUB_URL, options);
  const responseCode = response.getResponseCode();
  const responseText = response.getContentText();

  console.log("--- Test Results ---");
  console.log(`HTTP Status Code: ${responseCode}`); // This is the critical line
  console.log(`Response Text: ${responseText}`);
  console.log("--------------------");
}

function testStartHubSendRun() {
  console.log("--- Starting test for startHubSendRun ---");

  // Mock dependencies
  const sourceTab = 'Outbound Leads';
  const runNumber = 1;
  const limit = 10;

  // Call the function
  const result = startHubSendRun(sourceTab, runNumber, limit);
  console.log(`Result from startHubSendRun: ${JSON.stringify(result)}`);

  if (!result.ok) {
    if (result.error === 'Insufficient credits or no tasks to send.') {
      console.warn("Test could not run due to insufficient credits or no tasks. This is not a failure of the new logic.");
      return;
    }
    console.error("Test failed: startHubSendRun did not return ok:true.");
    return;
  }

  // Check properties
  const state = PropertiesService.getScriptProperties().getProperty('sendRunState');
  if (!state) {
    console.error("Test failed: sendRunState was not created in PropertiesService.");
  } else {
    console.log("Test passed: sendRunState was created.");
  }

  // Check trigger
  const triggers = ScriptApp.getProjectTriggers();
  const triggerExists = triggers.some(t => t.getHandlerFunction() === 'processSendRunBatch');
  if (!triggerExists) {
    console.error("Test failed: Trigger for processSendRunBatch was not created.");
  } else {
    console.log("Test passed: Trigger was created.");
  }

  // Cleanup
  console.log("--- Cleaning up test artifacts ---");
  _deleteTrigger('processSendRunBatch');
  PropertiesService.getScriptProperties().deleteProperty('sendRunState');
  console.log("Cleanup complete.");
}