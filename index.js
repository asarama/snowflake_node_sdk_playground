// index.js
// This script connects to a Snowflake server and executes a query 
// to get the current timestamp.

// Import dotenv to load values from the .env file
require('dotenv').config();

const snowflake = require('snowflake-sdk');

// Create a connection object with your Snowflake credentials from the .env file
const connection = snowflake.createConnection({
  account: process.env.account,
  // host: process.env.SNOWFLAKE_HOST,
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

  // Execute a simple SELECT query to get the current timestamp
  connection.execute({
    sqlText: 'SELECT CURRENT_TIMESTAMP() AS CURRENT_TIME',
    complete: (err, stmt, rows) => {
      if (err) {
        console.error('Failed to execute query: ' + err.message);
      } else {
        console.log('Query results:', rows);
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
