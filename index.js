'use strict';


console.log('CRDB-Locking-Queries');



const POLL_INTERVAL_MS = 2000; // 2 seconds;
const TRACK_LOCKS_BEYOND_SECONDS = 20;


const MyPGModule = require('pg');

const MyPGPool = new MyPGModule.Pool(
    {
        connectionString: process.env.CRDBConnString,
        //connectionString: process.env.Test2ConnString,
        max: 10,
        idleTimeoutMillis: 15000,
        ssl: {
            rejectUnauthorized: false
        }
    });


const URLQuery = new URL(process.env.CRDBPrometheusData);

const PromScrapeOptions = {
    //            host: 'localhost',
    //            port: 3002,
    host: URLQuery.host,
    path: URLQuery.pathname,
    method: 'GET',
    headers: {
        'User-Agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        'cookie': 'remote_node_id=1'
    }
};


const HTTPS = require('https');


function ScrapePrometheus() {
    return new Promise((resolve, reject) => {
        const myReq = HTTPS.request(PromScrapeOptions, responseObj => {
            const myData = [];
            //responseController.setEncoding('utf8');
            responseObj.on('data', chunk => {
                myData.push(chunk);
            });
            responseObj.on('end', () => {
                const dataStr = myData.join('');
                resolve(dataStr);
            });
        });

        myReq.on('error', onError => {
            reject(onError);
        });

        myReq.end();
    });
};

setInterval(async () => {
    let resultString;
    try {
        // resultString = await ScrapePrometheus();
        // const lock_wait_strings = resultString.split('\nkv_concurrency_max_lock_wait_duration_nanos');
        // const endOfLineIndex = lock_wait_strings[1].indexOf('\n');
        // const max_lock_complete_string = lock_wait_strings[1].substring(0, endOfLineIndex);
        // const max_lock_value_string = max_lock_complete_string.split(' ');
        // const durationUS = parseFloat(max_lock_value_string[1]) / 1000;
        // const durationSeconds = durationUS / 1000000.0;
        // if (durationSeconds > TRACK_LOCKS_BEYOND_SECONDS) {
        //           console.log(`Found lock with duration (s): ${durationSeconds}`);

        //           const NowTimeResultSet = await MyPGPool.query('select now()');
        //           const NowDateTime = NowTimeResultSet.rows[0].now;

        const CQResultSet = await MyPGPool.query(`select * from crdb_internal.cluster_queries where (cast(now() as int) - ${TRACK_LOCKS_BEYOND_SECONDS}) > cast(start as int)`);
        const ClusterQueries = CQResultSet.rows;

        let ClusterTransactions;
        if (ClusterQueries.length > 0) {
            const CTResultSet = await MyPGPool.query(`select * from crdb_internal.cluster_transactions where (cast(now() as int) - ${TRACK_LOCKS_BEYOND_SECONDS}) > cast(start as int)`);
            ClusterTransactions = CTResultSet.rows;
        };

        //            const CSResultSet = await MyPGPool.query(`select * from crdb_internal.cluster_sessions where (cast(now() as int) - ${TRACK_LOCKS_BEYOND_SECONDS}) > cast(active_query_start as int)`);
        //            const ClusterSessions = CSResultSet.rows;

        // extract table name from earlier query
        for (const waitingQueries of ClusterQueries) {
            try {
                const FromTableStrings = waitingQueries.query.toLowerCase().split(' from ');
                if (FromTableStrings[0].startsWith('create ') || FromTableStrings[0].startsWith('with ')) {
                    continue;
                };
                const FromTableItem = FromTableStrings[1].split(' ');
                const FromTableName = FromTableItem[0];
                const WaitingTableIDQueryResultSet = await MyPGPool.query(`select '${FromTableName}'::regclass::oid`);
                const TableID = WaitingTableIDQueryResultSet.rows[0].oid;
                const lockingTransaction = ClusterTransactions.find(rowItem => rowItem.txn_string.includes('lock=true') && rowItem.txn_string.includes(`/Table/${TableID}/`));
                if (lockingTransaction) {
                    const keyIndexStart = lockingTransaction.txn_string.indexOf(' key=');
                    const keyIndexEnd = lockingTransaction.txn_string.lastIndexOf(' pri=');
                    const keyItem = lockingTransaction.txn_string.substring(keyIndexStart + 5, keyIndexEnd);

                    console.log(`The query "${waitingQueries.query}" (Transaction Execution ID ${waitingQueries.txn_id}) is waiting on Transaction Execution ID ${lockingTransaction.id} due to key: ${keyItem}`);
                    break;
                };
            } catch (err) {

            };
        };
        // };
        // console.log(max_lock_string);
    } catch (err) {
        //        console.log(err.message);
    };
}, POLL_INTERVAL_MS);
