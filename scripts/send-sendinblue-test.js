require('dotenv').config();
const { sendViaSendinblue } = require('../lib/sendViaSendinblue');

(async () => {
    try {
        const from = process.env.ESUNDHED_FROM_EMAIL || process.env.EMAIL_USER;
        const to = process.env.ESUNDHED_TO_EMAIL || process.env.EMAIL_TO || from;

        console.log('[test-sendinblue] from:', from, 'to:', to);

        const result = await sendViaSendinblue({
            from,
            to,
            subject: 'va-watcher Sendinblue test',
            text: 'This is a single test email sent via the Sendinblue transactional API (from va-watcher).'
        });

        console.log('[test-sendinblue] result:', result);
        process.exit(0);
    } catch (err) {
        console.error('[test-sendinblue] failed:', err && err.message ? err.message : err);
        process.exit(1);
    }
})();
