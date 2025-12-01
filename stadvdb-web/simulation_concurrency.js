const axios = require('axios');

// ============================================================================
// CONFIGURATION
// ============================================================================
const NODE_CENTRAL = 'http://127.0.0.1:60751/api/titles';
const NODE_A       = 'http://127.0.0.1:60752/api/titles';

// TARGETS
const TARGET_ID       = 'tt_2025_fed';
const TARGET_TITLE    = 'Federation 2025';

const PHANTOM_ID_UPD  = 'tt_phantom_u';
const SAFE_TITLE      = 'Safe Title'; // Initial state
const PHANTOM_TITLE   = 'Phantom Title'; // Target state

const PHANTOM_ID_DEL  = 'tt_phantom_d';
const DELETE_TITLE    = 'Delete Me';

// ============================================================================
// HELPERS
// ============================================================================
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function getCurrentMovieInfo(id, title) {
    try {
        const res = await axios.get(`${NODE_CENTRAL}/distributed-search`, {
            params: { search_term: title, limit_count: 1 }
        });
        // Filter strictly because search is fuzzy
        return res.data.data?.find(m => m.tconst === id) || null;
    } catch (e) {
        return null;
    }
}

const createUpdatePayload = (id, newTitle, sleepSeconds, isolationLevel) => ({
    tconst: id,
    primaryTitle: newTitle,
    runtimeMinutes: 120, averageRating: 5.0, numVotes: 100, startYear: 2025,
    sleepSeconds, isolationLevel
});

// ROBUST RESET
async function resetData() {
    // 1. Reset Main Target
    try { await axios.post(`${NODE_CENTRAL}/distributed-delete`, { tconst: TARGET_ID }); } catch(e){}
    await sleep(100);
    try {
        await axios.post(`${NODE_CENTRAL}/distributed-insert`, {
            tconst: TARGET_ID, primaryTitle: TARGET_TITLE,
            runtimeMinutes: 120, averageRating: 8.5, numVotes: 500, startYear: 2025
        });
    } catch(e) {
        await axios.post(`${NODE_CENTRAL}/distributed-update`,
            createUpdatePayload(TARGET_ID, TARGET_TITLE, 0, 'READ COMMITTED'));
    }

    // 2. Reset Phantom Update Target (Set to "Safe Title")
    try { await axios.post(`${NODE_CENTRAL}/distributed-delete`, { tconst: PHANTOM_ID_UPD }); } catch(e){}
    await sleep(100);
    try {
        await axios.post(`${NODE_CENTRAL}/distributed-insert`, {
            tconst: PHANTOM_ID_UPD, primaryTitle: SAFE_TITLE,
            runtimeMinutes: 100, averageRating: 5.0, numVotes: 10, startYear: 2025
        });
    } catch(e) {}

    // 3. Reset Phantom Delete Target (Set to "Delete Me")
    try { await axios.post(`${NODE_CENTRAL}/distributed-delete`, { tconst: PHANTOM_ID_DEL }); } catch(e){}
    await sleep(100);
    try {
        await axios.post(`${NODE_CENTRAL}/distributed-insert`, {
            tconst: PHANTOM_ID_DEL, primaryTitle: DELETE_TITLE,
            runtimeMinutes: 100, averageRating: 5.0, numVotes: 10, startYear: 2025
        });
    } catch(e) {}
}

// ============================================================================
// TEST CASES
// ============================================================================

async function runCase1(isoLevel) {
    process.stdout.write(`   🔹 Case 1 (Read/Read): `);
    try {
        const p1 = axios.get(`${NODE_CENTRAL}/distributed-search`, {
            params: { search_term: 'Federation', isolationLevel: isoLevel, limit_count: 1 }
        });
        const p2 = axios.get(`${NODE_A}/distributed-search`, {
            params: { search_term: 'Federation', isolationLevel: isoLevel, limit_count: 1 }
        });
        const start = Date.now();
        await Promise.all([p1, p2]);
        console.log(`✅ Success (${Date.now() - start}ms)`);
    } catch (e) {
        console.log(`❌ Failed: ${e.message}`);
    }
}

async function runCase2(isoLevel) {
    process.stdout.write(`   🔸 Case 2 (Write/Read - Dirty Read): `);
    const DIRTY_TITLE = `DIRTY_${isoLevel.replace(' ', '_')}`;

    // Writer Updates & Sleeps
    const writeReq = axios.post(`${NODE_CENTRAL}/distributed-update`,
        createUpdatePayload(TARGET_ID, DIRTY_TITLE, 3, isoLevel)
    ).catch(e => {});

    await sleep(500);

    // Reader searches for Dirty Data
    const start = Date.now();
    let resultMsg = "";
    try {
        const readRes = await axios.get(`${NODE_CENTRAL}/distributed-search`, {
            params: { search_term: DIRTY_TITLE, isolationLevel: isoLevel, limit_count: 1 }
        });
        const data = readRes.data.data?.[0];

        if (data && data.primaryTitle === DIRTY_TITLE) {
            resultMsg = `⚠️ DIRTY READ DETECTED! (Saw "${DIRTY_TITLE}")`;
        } else {
            resultMsg = `✅ Clean Read (Ignored uncommitted data)`;
        }
    } catch (e) {
        resultMsg = `🛡️ Read Blocked/Timeout (${e.message})`;
    }
    await sleep(3000);
    console.log(resultMsg);
}

async function runCase3(isoLevel) {
    process.stdout.write(`   🔴 Case 3 (Write/Write - Lost Update): `);
    const TITLE_A = "User A Win";
    const TITLE_B = "User B Win";

    const reqA = axios.post(`${NODE_CENTRAL}/distributed-update`,
        createUpdatePayload(TARGET_ID, TITLE_A, 3, isoLevel)
    ).catch(e => {});
    await sleep(500);

    const start = Date.now();
    let resultMsg = "";
    try {
        const reqB = axios.post(`${NODE_CENTRAL}/distributed-update`,
            createUpdatePayload(TARGET_ID, TITLE_B, 0, isoLevel)
        );
        await reqB;
        const duration = Date.now() - start;

        await sleep(3000); // Verify final state
        const finalState = await getCurrentMovieInfo(TARGET_ID, TARGET_TITLE); // Search general to find ID
        // Note: getCurrentMovieInfo might fail if title changed, let's trust duration mostly

        if (duration > 2500) {
            resultMsg = `✅ LOCKING SUCCESS. Waited ${duration}ms.`;
        } else {
            resultMsg = `⚠️ No Wait (${duration}ms). Risk of Lost Update.`;
        }

        if (finalState && finalState.primaryTitle === "User B Win" || "User A Win")
            resultMsg = `${resultMsg} ❌ Lost Update.`
    } catch (e) {
        if (e.response && e.response.status === 409) {
            resultMsg = `🛡️ DEADLOCK (Expected for Serializable)`;
        } else {
            resultMsg = `❌ Error: ${e.message}`;
        }
    }
    try { await reqA; } catch(e) {}
    console.log(resultMsg);
}

// === NEW CASE: PHANTOM VIA UPDATE ===
// Row changes FROM "Safe Title" TO "Phantom Title"
async function runCase4_UpdatePhantom(isoLevel) {
    process.stdout.write(`   👻 Case 4 (Phantom Update): `);

    // 1. Writer updates 'Safe Title' -> 'Phantom Title' and sleeps
    const writeReq = axios.post(`${NODE_CENTRAL}/distributed-update`,
        createUpdatePayload(PHANTOM_ID_UPD, PHANTOM_TITLE, 3, isoLevel)
    ).catch(e => {});

    await sleep(500);

    // 2. Reader searches for "Phantom Title"
    // Ideally, it shouldn't exist yet (RC/RR). If found, it's a phantom/dirty read.
    const start = Date.now();
    let resultMsg = "";
    try {
        const readRes = await axios.get(`${NODE_CENTRAL}/distributed-search`, {
            params: { search_term: 'Phantom', isolationLevel: isoLevel, limit_count: 5 }
        });
        const found = readRes.data.data?.some(m => m.tconst === PHANTOM_ID_UPD);

        if (found) {
            resultMsg = `⚠️ PHANTOM FOUND! (Saw uncommitted row '${PHANTOM_TITLE}')`;
        } else {
            resultMsg = `✅ Safe. (Phantom row hidden)`;
        }
    } catch (e) {
        resultMsg = `🛡️ Read Blocked (${e.message})`;
    }

    await sleep(3000);
    console.log(resultMsg);
}

// === NEW CASE: PHANTOM VIA DELETE ===
// Row "Delete Me" exists. Writer deletes it. Reader tries to find it.
async function runCase5_DeletePhantom(isoLevel) {
    process.stdout.write(`   💀 Case 5 (Phantom Delete): `);

    // 1. Writer deletes 'Delete Me' and sleeps
    const writeReq = axios.post(`${NODE_CENTRAL}/distributed-delete`, {
        tconst: PHANTOM_ID_DEL,
        sleepSeconds: 3,
        isolationLevel: isoLevel
    }).catch(e => {});

    await sleep(500);

    // 2. Reader searches for "Delete Me"
    // RU: Should be GONE (Empty).
    // RC/RR: Should be FOUND (Old version exists).
    let resultMsg = "";
    try {
        const readRes = await axios.get(`${NODE_CENTRAL}/distributed-search`, {
            params: { search_term: DELETE_TITLE, isolationLevel: isoLevel, limit_count: 5 }
        });
        const found = readRes.data.data?.some(m => m.tconst === PHANTOM_ID_DEL);

        if (!found) {
            resultMsg = `⚠️ ROW VANISHED! (Reader saw the uncommitted delete)`;
        } else {
            resultMsg = `✅ Row Visible. (Reader sees old data)`;
        }
    } catch (e) {
        resultMsg = `🛡️ Read Blocked (${e.message})`;
    }

    await sleep(3000);
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

    console.log(`🚀 STARTING EXTENDED SIMULATION`);
    try { await axios.get(`${NODE_CENTRAL}/distributed-search?search_term=test`); } catch(e){}

    for (const level of ISOLATION_LEVELS) {
        console.log(`\n=== ${level} ===`);
        await resetData();

        await runCase1(level);
        await sleep(200);

        await runCase2(level);
        await sleep(200);

        await runCase3(level);
        await sleep(200);

        // New Cases
        await runCase4_UpdatePhantom(level);
        await sleep(200);

        await runCase5_DeletePhantom(level);
        await sleep(500);
    }

    console.log("\n✅ ALL TESTS COMPLETED.");
})();