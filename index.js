require('dotenv').config();
const express = require('express');
const cron = require('node-cron');
const { runWatcher: checkVA } = require('./watchers/va');
const { checkEsundhedUpdate: checkEsundhed } = require('./watchers/esundhed');

const app = express();
const PORT = process.env.PORT || 3000;

// Cron Jobs
cron.schedule('0 12-18 * * *', () => {
  console.log(`[⏰] ${new Date().toISOString()} — Cron triggered: checkVA()`);
  checkVA();
});

cron.schedule('0 12-18 * * *', () => {
  console.log(`[⏰] ${new Date().toISOString()} — Cron triggered: checkEsundhed()`);
  checkEsundhed();
});

// Endpoints
app.get('/', (_, res) => res.send('<h1>✅ Universal Watcher is live!</h1>'));
app.get('/ping', (_, res) => res.send('pong'));

app.get('/scrape/va', async (_, res) => {
  await checkVA();
  res.send('VA scrape complete!');
});

app.get('/scrape/esundhed', async (_, res) => {
  await checkEsundhed();
  res.send('eSundhed scrape complete!');
});

app.listen(PORT, () => {
  console.log(`\u{1F310} Server running on port ${PORT}`);
  checkVA();
  checkEsundhed();
});