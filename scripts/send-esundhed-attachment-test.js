require('dotenv').config();
const https = require('https');
const path = require('path');
const { sendViaSendinblue } = require('../lib/sendViaSendinblue');

const XLSX_URL = 'https://sundhedsdatabank.dk/Media/638960206152639194/Statistik%20over%20forbrug%20af%20Ozempic,%20Saxenda,%20Wegovy%20og%20Mounjaro%20i%20perioden%202022%20tom%20august%202025.XLSX';

function fetchBuffer(url) {
    return new Promise((resolve, reject) => {
        https.get(url, res => {
            if (res.statusCode !== 200) return reject(new Error('Fetch failed: ' + res.statusCode));
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => resolve(Buffer.concat(chunks)));
        }).on('error', reject);
    });
}

(async () => {
    try {
        const to = process.env.ESUNDHED_TO_EMAIL || process.env.EMAIL_TO || process.env.EMAIL_USER;
        const from = process.env.ESUNDHED_FROM_EMAIL || process.env.EMAIL_USER;

        console.log('[attachment-test] downloading xlsx...');
        const buf = await fetchBuffer(XLSX_URL);
        console.log('[attachment-test] downloaded', buf.length, 'bytes');

        const filename = path.basename(XLSX_URL);
        const result = await sendViaSendinblue({
            from,
            to,
            subject: 'Attachment test: eSundhed XLSX',
            text: 'This email contains the XLSX as attachment.',
            attachments: [{ filename, content: buf }]
        });

        console.log('[attachment-test] send result:', result);
        process.exit(0);
    } catch (err) {
        console.error('[attachment-test] failed:', err && err.message ? err.message : err);
        process.exit(1);
    }
})();
