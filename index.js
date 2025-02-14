// index.js
// This script connects to a Snowflake server and executes a query 
// to get the current timestamp.

// Import dotenv to load values from the .env file
require('dotenv').config();

const snowflake = require('snowflake-sdk');
const fs = require('fs');

// Load SQL queries from sql_queries.json
const sqlQueries = JSON.parse(fs.readFileSync('sql_queries.json', 'utf-8'));
const queryKeys = Object.keys(sqlQueries);

// Configuration
const MAX_CONCURRENT_QUERIES = 2;
const TOTAL_REQUESTS = 6;

// Metrics
let successCount = 0;
let failureCount = 0;
let startTime;
let queryCount = 0;

// Create a connection object with your Snowflake credentials from the .env file
const connection = snowflake.createConnection({
  account: process.env.account,
  host: process.env.host,
  username: process.env.username,
  password: process.env.password,
  database: process.env.database,
  schema: process.env.schema,
  warehouse: process.env.warehouse,
  role: process.env.role // Optional: specify role if needed
});

// Function to execute a query
async function executeQuery() {
  return new Promise((resolve, reject) => {
    const queryKey = queryKeys[Math.floor(Math.random() * queryKeys.length)];
    const sqlText = sqlQueries[queryKey];
    const queryStartTime = process.hrtime();

    console.log(`Executing query: ${queryKey}`);

    connection.execute({
      sqlText: sqlText,
      complete: (err, stmt, rows) => {
        const queryEndTime = process.hrtime(queryStartTime);
        const queryTimeMs = queryEndTime[0] * 1000 + queryEndTime[1] / 1000000;

        if (err) {
          console.error('Failed to execute query: ' + err.message);
          failureCount++;
          reject(err);
        } else {
          successCount++;
          console.log(`Query "${queryKey}" completed in ${queryTimeMs} ms, Result rows: ${rows.length}`);
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
    executeNext();
  }
}

// Connect to Snowflake
connection.connect((err, conn) => {
  if (err) {
    console.error('Unable to connect: ' + err.message);
    return;
  }
  console.log('Successfully connected as ID: ' + conn.getId());

  runQueries();
});
