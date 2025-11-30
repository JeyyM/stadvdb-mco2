const axios = require('axios');

// ============================================================================
// CONFIGURATION
// ============================================================================
const NODE_CENTRAL = 'http://127.0.0.1:5000/api/titles';
const NODE_A       = 'http://127.0.0.1:5001/api/titles';

// CRITICAL: These must match what is in reset_and_seed.js
const TARGET_ID    = 'tt0000001';
const TARGET_TITLE = 'TARGET_MOVIE_FOR_TEST';

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function getCurrentMovieInfo() {
    try {
        const res = await axios.get(`${NODE_CENTRAL}/distributed-search`, {
            params: { search_term: TARGET_TITLE, limit_count: 1 }
        });
        if (res.data.data && res.data.data.length > 0) {
            return res.data.data[0];
        }
        // Fallback: search by ID
        const res2 = await axios.get(`${NODE_CENTRAL}/distributed-search`, {
            params: { search_term: TARGET_ID, limit_count: 1 }
        });
        if (res2.data.data && res2.data.data.length > 0) {
            return res2.data.data[0];
        }
        return null;
    } catch (e) {
        return null;
    }
}

const createUpdatePayload = (movie, newTitle, sleepSeconds, isolationLevel) => ({
    tconst: movie.tconst,
    primaryTitle: newTitle,
    runtimeMinutes: movie.runtimeMinutes,
    averageRating: movie.averageRating,
    numVotes: movie.numVotes,
    startYear: movie.startYear,
    sleepSeconds,
    isolationLevel
});

// ============================================================================
// TEST CASES
// ============================================================================

async function runCase1(isoLevel) {
    process.stdout.write(`   🔹 Testing Case 1 (Read/Read)... `);
    try {
        const p1 = axios.get(`${NODE_CENTRAL}/distributed-search`, {
            params: { search_term: 'Test', isolationLevel: isoLevel, limit_count: 1 }
        });
        const p2 = axios.get(`${NODE_A}/distributed-search`, {
            params: { search_term: 'Test', isolationLevel: isoLevel, limit_count: 1 }
        });

        const start = Date.now();
        await Promise.all([p1, p2]);
        const duration = Date.now() - start;

        console.log(`✅ Success (${duration}ms)`);
    } catch (e) {
        console.log(`❌ Failed: ${e.message}`);
    }
}

async function runCase2(isoLevel) {
    process.stdout.write(`   🔸 Testing Case 2 (Write/Read)... `);

    const movie = await getCurrentMovieInfo();
    if (!movie) {
        console.log(`❌ Error: Could not find target movie.`);
        return;
    }

    const DIRTY_TITLE = `DIRTY_${isoLevel.replace(' ', '_')}`;

    // 1. START WRITER (Updates and sleeps 15s)
    const writeReq = axios.post(`${NODE_CENTRAL}/distributed-update`,
        createUpdatePayload(movie, DIRTY_TITLE, 15, isoLevel)
    );

    // 2. WAIT 10s (Ensure Writer is sleeping)
    await sleep(10000);

    // 3. START READER
    const start = Date.now();
    let resultMsg = "";

    try {
        // Search for the DIRTY title specifically
        const readRes = await axios.get(`${NODE_CENTRAL}/distributed-search`, {
            params: {
                search_term: DIRTY_TITLE,
                isolationLevel: isoLevel,
                limit_count: 1
            }
        });
        const duration = Date.now() - start;
        const data = readRes.data.data;

        if (data && data.length > 0) {
            resultMsg = `✅ DIRTY READ SUCCESS! (Reader saw: "${data[0].primaryTitle}" in ${duration}ms)`;
        } else {
            resultMsg = `✅ Clean Read (Reader saw: Nothing/Old Data in ${duration}ms)`;
        }
    } catch (e) {
        resultMsg = `🛡️ Read Blocked/Timeout (${e.message})`;
    }

    // Cleanup writer
    try { await writeReq; } catch(e){}

    // Reset title
    try {
        await axios.post(`${NODE_CENTRAL}/distributed-update`,
            createUpdatePayload(movie, TARGET_TITLE, 0, 'READ COMMITTED')
        );
    } catch(e) {}

    console.log(resultMsg);
}

async function runCase3(isoLevel) {
    process.stdout.write(`   🔴 Testing Case 3 (Write/Write)... `);

    const movie = await getCurrentMovieInfo();
    if (!movie) return;

    // User A locks for 15s
    const reqA = axios.post(`${NODE_CENTRAL}/distributed-update`,
        createUpdatePayload(movie, 'User-A-Win', 15, isoLevel)
    );

    await sleep(1000);

    // User B tries to overwrite
    const start = Date.now();
    let resultMsg = "";

    try {
        const reqB = axios.post(`${NODE_CENTRAL}/distributed-update`,
            createUpdatePayload(movie, 'User-B-Win', 0, isoLevel)
        );

        await Promise.all([reqA, reqB]);
        const duration = Date.now() - start;

        if (duration > 12000) {
            resultMsg = `✅ Locked & Waited (${duration}ms)`;
        } else {
            resultMsg = `⚠️ No Wait Detected (${duration}ms)`;
        }
    } catch (e) {
        if (e.response && e.response.status === 409) {
            resultMsg = `🛡️ DEADLOCK DETECTED (Success for Serializable)`;
        } else {
            resultMsg = `❌ Error: ${e.message}`;
        }
    }

    // Reset title
    try {
        await axios.post(`${NODE_CENTRAL}/distributed-update`,
            createUpdatePayload(movie, TARGET_TITLE, 0, 'READ COMMITTED')
        );
    } catch(e) {}

    console.log(resultMsg);
}

// ============================================================================
// MAIN RUNNER
// ============================================================================
(async () => {
    const ISOLATION_LEVELS = [
        'READ UNCOMMITTED',
        'READ COMMITTED',
        'REPEATABLE READ',
        'SERIALIZABLE'
    ];

    console.log(`🚀 STARTING SIMULATION (Target: ${TARGET_TITLE})\n`);
    try { await axios.get(`${NODE_CENTRAL}/distributed-search?search_term=test`); } catch(e){}

    for (const level of ISOLATION_LEVELS) {
        console.log(`\n==================================================`);
        console.log(`🧪 TEST SUITE: ${level}`);
        console.log(`==================================================`);

        await runCase1(level);
        await sleep(500);

        await runCase2(level);
        await sleep(500);

        await runCase3(level);
        await sleep(2000);
    }

    console.log("\n✅ ALL TESTS COMPLETED.");
})();