const fetch = require('node-fetch');

// attachments: optional array of { filename, content } where content can be Buffer or base64 string
async function sendViaSendinblue({ from, to, subject, text, html, attachments }) {
    const key = process.env.SENDINBLUE_API_KEY;
    if (!key) throw new Error('No SENDINBLUE_API_KEY');
    // Common mistake: using an SMTP key instead of an API v3 key. Give a helpful error.
    // SMTP keys sometimes start with 'xsmtpsib-' or contain 'smtp'. API v3 keys look different.
    if (/^xsmtpsib-|smtp/i.test(key)) {
        throw new Error('Detected a Sendinblue SMTP key. Create and use an API v3 key (SMTP keys will not work with the HTTP transactional API). Add it to SENDINBLUE_API_KEY.');
    }
    if (!from) throw new Error('Missing "from"');
    if (!to) throw new Error('Missing "to"');

    const body = {
        sender: { email: from },
        to: Array.isArray(to) ? to.map(t => ({ email: t })) : [{ email: to }],
        subject,
    };

    if (text) body.textContent = text;
    if (html) body.htmlContent = html;

    if (attachments && Array.isArray(attachments) && attachments.length) {
        // Sendinblue expects attachments: [{ name, content }] where content is base64
        body.attachment = attachments.map(att => {
            const name = att.filename || att.name || 'attachment';
            let contentBase64;
            if (Buffer.isBuffer(att.content)) {
                contentBase64 = att.content.toString('base64');
            } else if (typeof att.content === 'string') {
                // assume already base64 or raw string; encode to base64 to be safe
                contentBase64 = Buffer.from(att.content, 'utf8').toString('base64');
            } else {
                throw new Error('Unsupported attachment content type');
            }
            return { name, content: contentBase64 };
        });
    }

    const res = await fetch('https://api.sendinblue.com/v3/smtp/email', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'api-key': key
        },
        body: JSON.stringify(body)
    });

    const respText = await res.text().catch(() => '');
    if (!res.ok) {
        throw new Error(`Sendinblue error ${res.status}: ${respText}`);
    }

    // Try to parse JSON body if present
    let parsed;
    try { parsed = respText ? JSON.parse(respText) : null; } catch (e) { parsed = respText; }
    return { status: res.status, body: parsed };
}

module.exports = { sendViaSendinblue };
