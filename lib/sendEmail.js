require('dotenv').config();
const nodemailer = require('nodemailer');

// Centralized email sender used by watchers and the test endpoint.
// Behavior/contract:
// - Inputs: { to, from, subject, text, html, attachments }
//   - `to` may be a string (single email or comma-separated), array, or omitted.
//   - If `to` is omitted, falls back to process.env.TO_EMAIL -> EMAIL_USER.
// - attachments: nodemailer-compatible array (filename/content or path)
// - Honors DISABLE_EMAIL=true to skip sends safely.

function parseRecipients(to) {
    if (!to) return null;
    if (Array.isArray(to)) return to.map(s => String(s).trim()).filter(Boolean);
    if (typeof to === 'string') {
        return to.split(',').map(s => s.trim()).filter(Boolean);
    }
    return null;
}

function createTransport() {
    const user = process.env.EMAIL_USER;
    const pass = process.env.EMAIL_PASS;

    if (!user || !pass) {
        throw new Error('Missing EMAIL_USER or EMAIL_PASS for SMTP transport');
    }

    // Allow overriding SMTP host/port/secure via env. Defaults target Gmail.
    const host = process.env.SMTP_HOST || 'smtp.gmail.com';
    const port = process.env.SMTP_PORT ? Number(process.env.SMTP_PORT) : 465;
    const secure = process.env.SMTP_SECURE ? process.env.SMTP_SECURE === 'true' : (port === 465);

    return nodemailer.createTransport({
        host,
        port,
        secure,
        auth: { user, pass }
    });
}

async function sendMail({ to, from, subject, text, html, attachments }) {
    if (process.env.DISABLE_EMAIL === 'true') {
        console.log('[sendEmail] DISABLE_EMAIL=true — skipping send', { to, subject });
        return { skipped: true };
    }

    const recipients = parseRecipients(to) || parseRecipients(process.env.TO_EMAIL) || [process.env.EMAIL_USER];
    const fromAddr = from || process.env.FROM_EMAIL || process.env.EMAIL_USER;

    const transport = createTransport();

    // normalize attachments (if buffer objects are passed through, nodemailer accepts them)
    // Append a short legal footer to all outgoing emails so recipients are
    // reminded this is internal tooling and can find the full disclaimer.
    const site = (process.env.SITE_URL || '').replace(/\/$/, '');
    const legalLink = site ? `${site}/legal` : '/legal';
    const legalText = `\n\nNote: This tooling is internal and not an official ABG Sundal Collier product. See ${legalLink} for the full disclaimer.`;
    const legalHtml = `<hr/><p style="font-size:12px;color:#666">Note: This tooling is internal and not an official ABG Sundal Collier product. See <a href=\"${legalLink}\">legal disclaimer</a> for details.</p>`;

    const opts = { from: fromAddr, to: recipients, subject };
    opts.text = (text || '') + legalText;
    opts.html = (html || '') + legalHtml;
    if (attachments) opts.attachments = attachments;

    await transport.verify();
    const result = await transport.sendMail(opts);
    console.log('[sendEmail] sent', { messageId: result && result.messageId });
    return result;
}

module.exports = { sendMail, parseRecipients };
