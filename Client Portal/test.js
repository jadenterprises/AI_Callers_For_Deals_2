function debugClientPortalConfig() {
  const cfg = WEB_CFG_();
  Logger.log('HUB_URL: ' + cfg.HUB_URL);
  Logger.log('CLIENT_TOKEN: ' + cfg.CLIENT_TOKEN);
}
