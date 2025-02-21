// index.js
// This script connects to a Snowflake server and executes queries to measure performance,
// including tracking per-query-type average response times.

// Import dotenv to load values from the .env file
require('dotenv').config({ override: true });

const snowflake = require('snowflake-sdk');
const fs = require('fs');

// Load SQL queries from sql_queries.json
const sqlQueries = JSON.parse(fs.readFileSync('sql_queries.json', 'utf-8'));
const queryKeys = Object.keys(sqlQueries);

// Create an object to hold metrics (count, total time, and average time) per query type
const queryMetrics = {};
queryKeys.forEach(key => {
  queryMetrics[key] = { count: 0, totalTime: 0, avg: 0 };
});

// Configuration
const MAX_CONCURRENT_QUERIES = 5;
const TOTAL_REQUESTS = 100;

// Metrics for overall queries
let successCount = 0;
let failureCount = 0;
let startTime;
let queryCount = 0;

let snowflakeConfig = {
  account: process.env.account,
  host: process.env.host,
  username: process.env.username,
  database: process.env.database,
  schema: process.env.schema,
  warehouse: process.env.warehouse,
  role: process.env.role // Optional: specify role if needed
}

const password = process.env.password;
const privateKeyPath = process.env.privateKeyPath;
const privateKeyPass = process.env.privateKeyPass;

if (password) {
  snowflakeConfig.password = password;
} else if (privateKeyPath) {
  snowflakeConfig.authenticator = 'SNOWFLAKE_JWT';
  snowflakeConfig.privateKeyPath = privateKeyPath;
  if (privateKeyPass) {
    snowflakeConfig.privateKeyPass = privateKeyPass;
  }
}

// Create a connection object with your Snowflake credentials from the .env file
const connection = snowflake.createConnection(snowflakeConfig);


// Function to execute a query
async function executeQuery() {
  return new Promise((resolve, reject) => {
    // Randomly pick a query key from the loaded queries
    const queryKey = queryKeys[Math.floor(Math.random() * queryKeys.length)];
    const sqlText = sqlQueries[queryKey];
    const queryStartTime = process.hrtime();

    connection.execute({
      sqlText: sqlText,
      complete: (err, stmt, rows) => {
        // Measure query duration
        const queryEndTime = process.hrtime(queryStartTime);
        const queryTimeMs = queryEndTime[0] * 1000 + queryEndTime[1] / 1000000;

        // Update the metrics for the specific query type
        const metrics = queryMetrics[queryKey];
        metrics.count++;
        metrics.totalTime += queryTimeMs;
        metrics.avg = metrics.totalTime / metrics.count;

        if (err) {
          console.error('Failed to execute query: ' + err.message);
          failureCount++;
          reject(err);
        } else {
          successCount++;
          resolve();
        }
      }
    });
  });
}

// Function to run queries with concurrency control
async function runQueries() {
  startTime = Date.now();
  
  const executeNext = () => {

    if (queryCount >= TOTAL_REQUESTS) {
      return;
    }
    queryCount++;
    
    executeQuery()
      .then(() => {
        console.log(`Query ${queryCount} sent`);
        if (queryCount < TOTAL_REQUESTS) {
          executeNext();
        } else {
          // All queries are submitted, wait for completion
          return;
        }
      })
      .catch(() => {
        if (queryCount < TOTAL_REQUESTS) {
          executeNext();
        } else {
          // All queries are submitted, wait for completion
          return;
        }
      })
      .finally(() => {
        if (successCount + failureCount === TOTAL_REQUESTS) {
          const endTime = Date.now();
          const duration = (endTime - startTime) / 1000;
          const queriesPerSecond = TOTAL_REQUESTS / duration;

          console.log('-----------------------------------');
          console.log('Total Requests: ' + TOTAL_REQUESTS);
          console.log('Success Count: ' + successCount);
          console.log('Failure Count: ' + failureCount);
          console.log('Duration: ' + duration + ' seconds');
          console.log('Queries per second: ' + queriesPerSecond);

          // Log the metric details per query type
          console.log('--- Query Metrics ---');
          for (const key in queryMetrics) {
            const metric = queryMetrics[key];
            if (metric.count > 0) {
              console.log(`Query "${key}": Executed ${metric.count} times, Avg Response Time: ${metric.avg.toFixed(2)} ms`);
            } else {
              console.log(`Query "${key}": Not executed`);
            }
          }

          // Cleanly disconnect from Snowflake
          connection.destroy((err, conn) => {
            if (err) {
              console.error('Error disconnecting: ' + err.message);
            } else {
              console.log('Disconnected connection with ID: ' + conn.getId());
            }
          });
        }
      });
  };

  // Initial pool of concurrent queries
  for (let i = 0; i < Math.min(MAX_CONCURRENT_QUERIES, TOTAL_REQUESTS); i++) {
    console.log(`Initializing thread: ${i}`);
    executeNext();
  }
}

// Connect to Snowflake
connection.connect((err, conn) => {
  if (err) {
    console.error('Unable to connect: ' + err.message);
    return;
  }

  console.log('-----------------------------------');
  console.log('Successfully connected as ID: ' + conn.getId());
  console.log('-----------------------------------');

  runQueries();
});
