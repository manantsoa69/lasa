const Redis = require('ioredis');
const express = require('express');
const mysql = require('mysql2');
const cron = require('node-cron');
require('dotenv').config();

const redis = new Redis(process.env.REDIS_URL);
const connection = mysql.createConnection(process.env.DATABASE_URL);
const app = express();
app.use(express.json());

// Function to handle expiration events
async function handleExpirationEvent(key) {
  // Check if the value is already 'E' in Redis
  const redisValue = await redis.get(key);
  if (redisValue === 'E') {
    console.log(`FBID ${key} has already expired. Skipping unnecessary update.`);
    return;
  }

  // Update the value to 'E' when it expires
  await redis.set(key, 'E');
  console.log(`FBID ${key} has expired. Value updated to 'E' in Redis.`);

  // Update the expiration date for expired subscriptions in the MySQL database
  const updateQuery = 'UPDATE users SET expireDate = ? WHERE fbid = ?';
  const fbid = key;

  try {
    // Perform the update in MySQL
    const [updateResult] = await connection.promise().query(updateQuery, ['E', fbid]);
    if (updateResult.affectedRows === 1) {
      console.log(`FBID ${key} has expired. Value updated to 'E' in MySQL.`);
    }
  } catch (error) {
    console.error(`Error updating subscription in MySQL for FBID ${key}:`, error);
  }

  // Update the cache with the fetched subscription information
  const expireDate = 'E';
  await redis.set(fbid, expireDate);
  console.log('Subscription saved in cache:', expireDate);
}

// Function to perform batch expiration checks
async function performBatchExpirationCheck() {
  try {
    const currentDate = new Date();
    const keys = await redis.keys('*');

    if (keys.length === 0) {
      console.log('No keys found in Redis.');
      return;
    }

    const expireDates = await Promise.all(keys.map((key) => redis.get(key)));

    expireDates.forEach((expireDate, index) => {
      const key = keys[index];
      if (expireDate) {
        if (expireDate === 'E') {
          handleExpirationEvent(key);
        } else {
          const expireDateObj = new Date(expireDate);

          if (expireDateObj <= currentDate) {
            handleExpirationEvent(key);
          } else {
            const timeDifference = expireDateObj.getTime() - currentDate.getTime();
            setTimeout(() => {
              handleExpirationEvent(key);
            }, timeDifference);
            console.log(`FBID ${key} will expire on ${expireDateObj}`);
          }
        }
      } else {
        console.log(`FBID ${key} not found in Redis.`);
      }
    });
  } catch (error) {
    console.error('Error performing batch expiration check:', error);
  }
}

// Call the function to perform batch expiration check
performBatchExpirationCheck();

// Cron job to run the expiration check every minute
cron.schedule('* * * * *', async () => {
  console.log('Running batch expiration check...');
  await performBatchExpirationCheck();
});

// Route to check all data in Redis
app.get('/api/check', async (req, res) => {
  try {
    await performBatchExpirationCheck();
    res.status(200).json({ message: 'Data in Redis checked.' });
  } catch (error) {
    console.error('Error checking data in Redis:', error);
    res.status(500).json({ error: 'Failed to check data in Redis.' });
  }
});

// Default route
app.get('/', (req, res) => {
  res.send('Hello, World!');
});

// Start the server
const port = process.env.PORT || 3000;
const server = app.listen(port, () => {
  console.log(`Worker ${process.pid} listening on port ${port}.`);
});


