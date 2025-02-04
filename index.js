// index.js
// This script connects to a Snowflake server and executes a query 
// to get the current timestamp.

// Import dotenv to load values from the .env file
require('dotenv').config();

const snowflake = require('snowflake-sdk');

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

// Connect to Snowflake
connection.connect((err, conn) => {
  if (err) {
    console.error('Unable to connect: ' + err.message);
    return;
  }
  console.log('Successfully connected as ID: ' + conn.getId());

  const SELECT_TIMESTAMP = 'SELECT CURRENT_TIMESTAMP() AS CURRENT_TIME';
  const SELECT_ALL_RAW_CUSTOMERS = 'SELECT * FROM DEVELOPER.ARSHAM_E_SCHEMA.RAW_CUSTOMERS';

  // Start timing the query execution
  console.time('Query Execution Time');
  
  // Execute a query to get all raw customers
  connection.execute({
    sqlText: SELECT_ALL_RAW_CUSTOMERS,
    complete: (err, stmt, rows) => {
      // End timing once the query completes
      console.timeEnd('Query Execution Time');

      if (err) {
        console.error('Failed to execute query: ' + err.message);
      } else {
        console.log('Query results:', rows.length);
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
});
