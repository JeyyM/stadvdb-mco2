const axios = require('axios');

// ============================================================================
// CONFIGURATION
// ============================================================================
const NODE_CENTRAL = 'http://127.0.0.1:60751/api/titles';

// Using the same target as the functional tests
const TARGET_ID    = 'tt_2025_fed';
const TARGET_TITLE = 'Federation 2025';
const REQUEST_COUNT = 100; // Total concurrent requests

// ============================================================================
// HELPERS
// ============================================================================
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function resetData() {
    try {
        await axios.post(`${NODE_CENTRAL}/distributed-delete`, { tconst: TARGET_ID });
    } catch (e) {}
    await sleep(200);
    try {
        // Ensure startYear is 2025 to match the specific partition logic of this ID
        await axios.post(`${NODE_CENTRAL}/distributed-insert`, {
            tconst: TARGET_ID,
            primaryTitle: TARGET_TITLE,
            runtimeMinutes: 100, averageRating: 5.0, numVotes: 0, startYear: 2025
        });
        console.log(`   [Setup] Reset ${TARGET_ID} successful.`);
    } catch (e) {
        console.log(`   [Setup] Reset failed: ${e.message}`);
    }
}

// ============================================================================
// STRESS WORKER
// ============================================================================
async function performStressRequest(id, isoLevel) {
    const start = Date.now();
    try {
        // We simulate a simple update
        // We overwrite with random data to trigger row locks on the specific target
        await axios.post(`${NODE_CENTRAL}/distributed-update`, {
            tconst: TARGET_ID,
            primaryTitle: `${TARGET_TITLE} - Stress Req ${id}`,
            runtimeMinutes: 100,
            averageRating: 5.0,
            numVotes: id, // changing value
            startYear: 2025, // Keep year consistent to avoid node migration overhead during stress test
            sleepSeconds: 0, // No artificial sleep, we want pure DB performance
            isolationLevel: isoLevel
        });
        const duration = Date.now() - start;
        return { id, success: true, duration };
    } catch (e) {
        const duration = Date.now() - start;
        // 409 usually means Deadlock/Serialization Failure
        const isDeadlock = e.response && e.response.status === 409;
        return { id, success: false, duration, error: isDeadlock ? "DEADLOCK" : e.message };
    }
}

// ============================================================================
// BENCHMARK RUNNER
// ============================================================================
async function runBenchmark(isoLevel) {
    console.log(`\n==================================================`);
    console.log(`🔥 STRESS TEST: ${isoLevel}`);
    console.log(`   Target: ${TARGET_TITLE} (${TARGET_ID})`);
    console.log(`   Firing ${REQUEST_COUNT} concurrent requests...`);
    console.log(`==================================================`);

    await resetData();
    await sleep(500); // Cool down

    const promises = [];
    const globalStart = Date.now();

    // 1. Launch all requests "simultaneously"
    for (let i = 0; i < REQUEST_COUNT; i++) {
        promises.push(performStressRequest(i + 1, isoLevel));
    }

    // 2. Wait for all to finish
    const results = await Promise.all(promises);
    const globalDuration = Date.now() - globalStart;

    // 3. Analyze Results
    const successes = results.filter(r => r.success);
    const failures  = results.filter(r => !r.success);
    const deadlocks = failures.filter(r => r.error === "DEADLOCK");

    // Calculate Stats
    const totalTime = results.reduce((acc, curr) => acc + curr.duration, 0);
    const avgTime   = totalTime / REQUEST_COUNT;
    const maxTime   = Math.max(...results.map(r => r.duration));
    const minTime   = Math.min(...results.map(r => r.duration));

    // 4. Report
    console.log(`\n📊 RESULTS for ${isoLevel}:`);
    console.log(`   ------------------------------------------`);
    console.log(`   ✅ Success Rate:     ${successes.length}/${REQUEST_COUNT} (${(successes.length/REQUEST_COUNT)*100}%)`);
    console.log(`   ❌ Failures:         ${failures.length}`);
    console.log(`   🔒 Deadlocks:        ${deadlocks.length}`);
    console.log(`   ------------------------------------------`);
    console.log(`   ⏱️  Total Wall Time:  ${globalDuration} ms`);
    console.log(`   ⏱️  Avg Req Time:     ${avgTime.toFixed(2)} ms`);
    console.log(`   ⏱️  Fastest Req:      ${minTime} ms`);
    console.log(`   ⏱️  Slowest Req:      ${maxTime} ms`);
    console.log(`   ------------------------------------------`);

    if (failures.length > 0) {
        console.log(`   ⚠️ Note: High failure/deadlock rates indicate strict locking contention.`);
    }
}

// ============================================================================
// MAIN
// ============================================================================
(async () => {
    // Warmup
    try { await axios.get(`${NODE_CENTRAL}/distributed-search?search_term=test`); } catch(e){}

    const LEVELS = [
        'READ UNCOMMITTED',
        'READ COMMITTED',
        'REPEATABLE READ',
        'SERIALIZABLE'
    ];

    for (const level of LEVELS) {
        await runBenchmark(level);
        await sleep(2000); // Wait for DB to settle
    }

    console.log("\n🚀 BENCHMARK COMPLETE.");
})();