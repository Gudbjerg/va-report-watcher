require('dotenv').config();
const express = require('express');
const cron = require('node-cron');
const { runWatcher: checkVA } = require('./watchers/va');
const { checkEsundhedUpdate: checkEsundhed } = require('./watchers/esundhed');

const app = express();
const PORT = process.env.PORT || 3000;

// Status memory
let lastVA = { time: null, month: null };
let lastEsundhed = { time: null, filename: null };

// Patch watchers to update status
async function updateVA() {
  console.log(`[⏰] ${new Date().toISOString()} — Cron triggered: checkVA()`);
  const result = await checkVA();
  if (result?.month) {
    lastVA = { time: new Date(), month: result.month };
  }
}

async function updateEsundhed() {
  console.log(`[⏰] ${new Date().toISOString()} — Cron triggered: checkEsundhed()`);
  const result = await checkEsundhed();
  if (result?.filename) {
    lastEsundhed = { time: new Date(), filename: result.filename };
  }
}

// Cron Jobs: DK local hours (UTC+2)
cron.schedule('0 4,8,12,16,20 * * *', updateVA); // 06, 10, 14, 18, 22 DK
cron.schedule('0 4,8,12,16,20 * * *', updateEsundhed);

// Endpoints
app.get('/', (_, res) => {
  res.send(`
    <html lang="en">
      <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Status Dashboard</title>
        <script src="https://cdn.tailwindcss.com"></script>
      </head>
      <body class="bg-gray-100 text-gray-800 font-sans p-6">
        <div class="max-w-2xl mx-auto bg-white rounded-xl shadow-md p-6">
          <h1 class="text-2xl font-bold mb-4">✅ Universal Watcher Status</h1>
          <p class="mb-2">Service is <span class="font-semibold text-green-600">LIVE</span> and actively monitoring both VA & eSundhed reports.</p>

          <div class="mt-6">
            <h2 class="text-xl font-semibold mb-2">VA Report</h2>
            <p><strong>Last Check:</strong> ${lastVA.time ? new Date(lastVA.time).toLocaleString('da-DK') : '—'}</p>
            <p><strong>Latest Month:</strong> ${lastVA.month || '—'}</p>
          </div>

          <div class="mt-6">
            <h2 class="text-xl font-semibold mb-2">eSundhed Report</h2>
            <p><strong>Last Check:</strong> ${lastEsundhed.time ? new Date(lastEsundhed.time).toLocaleString('da-DK') : '—'}</p>
            <p><strong>Latest File:</strong> ${lastEsundhed.filename || '—'}</p>
          </div>

          <div class="mt-6 text-sm text-gray-500">Last refreshed at ${new Date().toLocaleString('da-DK')}</div>
        </div>
      </body>
    </html>
  `);
});

app.get('/ping', (_, res) => res.send('pong'));

app.get('/scrape/va', async (_, res) => {
  await updateVA();
  res.send('VA scrape complete!');
});

app.get('/scrape/esundhed', async (_, res) => {
  await updateEsundhed();
  res.send('eSundhed scrape complete!');
});

app.listen(PORT, () => {
  console.log(`\u{1F310} Server running on port ${PORT}`);
  updateVA();
  updateEsundhed();
});
