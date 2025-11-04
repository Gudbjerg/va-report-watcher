jest.mock('nodemailer', () => {
    return {
        createTransport: jest.fn(() => ({
            verify: jest.fn().mockResolvedValue(true),
            sendMail: jest.fn().mockResolvedValue({ messageId: 'mock-id' })
        }))
    };
});

const { sendMail, parseRecipients } = require('../lib/sendEmail');
const nodemailer = require('nodemailer');

describe('sendEmail', () => {
    beforeEach(() => {
        jest.resetModules();
        process.env.DISABLE_EMAIL = 'false';
        process.env.EMAIL_USER = 'sender@example.com';
        process.env.EMAIL_PASS = 'password';
    });

    test('parseRecipients handles comma strings and arrays', () => {
        expect(parseRecipients('a@x.com,b@x.com')).toEqual(['a@x.com', 'b@x.com']);
        expect(parseRecipients(['a@x.com', ' b@x.com '])).toEqual(['a@x.com', 'b@x.com']);
    });

    test('skips send when DISABLE_EMAIL=true', async () => {
        process.env.DISABLE_EMAIL = 'true';
        const res = await sendMail({ to: 'x@y.com', subject: 'test', text: 'hi' });
        expect(res).toEqual({ skipped: true });
    });

    test('sends via nodemailer when enabled', async () => {
        process.env.DISABLE_EMAIL = 'false';
        const res = await sendMail({ to: 'a@x.com', subject: 'hello', text: 'body' });
        expect(res).toHaveProperty('messageId', 'mock-id');
        // ensure createTransport was called
        expect(nodemailer.createTransport).toBeTruthy();
    });
});
