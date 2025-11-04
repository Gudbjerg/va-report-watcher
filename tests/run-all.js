const { readdirSync } = require('fs');
const { spawnSync } = require('child_process');
const path = require('path');

const testsDir = __dirname;
const files = readdirSync(testsDir)
    .filter(f => f.endsWith('.js') && f !== path.basename(__filename))
    .sort();

if (files.length === 0) {
    console.log('No test files found in', testsDir);
    process.exit(0);
}

let overallExit = 0;
for (const f of files) {
    const full = path.join(testsDir, f);
    console.log('\n=== Running', f, '===');
    const res = spawnSync(process.execPath, [full], { stdio: 'inherit' });
    if (res.error) {
        console.error('Failed to run', f, res.error);
        overallExit = overallExit || 1;
    }
    if (typeof res.status === 'number' && res.status !== 0) overallExit = res.status;
}

process.exit(overallExit);
