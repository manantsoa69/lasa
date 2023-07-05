const cluster = require('cluster');
const os = require('os');

const MAX_WORKERS = os.cpus().length; // Maximum number of workers (CPU cores)

if (cluster.isMaster) {
  const workers = new Map();

  function updateWorkers() {
    const numWorkers = workers.size;

    if (numWorkers < MAX_WORKERS) {
      const worker = cluster.fork();
      workers.set(worker.id, worker);

      worker.on('exit', (code, signal) => {
        workers.delete(worker.id);
        updateWorkers();
      });
    }
  }

  // Initial worker creation
  const worker = cluster.fork();
  workers.set(worker.id, worker);

  cluster.on('exit', (worker, code, signal) => {
    workers.delete(worker.id);
    updateWorkers();
  });
} else {
  const Redis = require('ioredis');
  const express = require('express');
  const mysql = require('mysql2');
  require('dotenv').config();

  // Create a Redis client
  const redis = new Redis(process.env.REDIS_URL);

  // Create a MySQL connection
  
  const connection = mysql.createConnection(process.env.DATABASE_URL);

  // Create an Express application
  const app = express();
  app.use(express.json());

  // Function to handle expiration events
  async function handleExpirationEvent(key) {
    // Update the value to 'E' when it expires
    await redis.set(key, 'E');
    console.log(`FBID ${key} has expired. Value updated to 'E'.`);

    // Update the expiration date for expired subscriptions in the MySQL database
    const updateQuery = 'UPDATE users SET expireDate = ? WHERE fbid = ?';
    const fbid = key;

    try {
      const [subscriptionStatus] = await connection.promise().query(updateQuery, ['E', fbid]);
      if (subscriptionStatus === 'E') {
        console.log('Subscription updated in database:', subscriptionStatus);
      }
    } catch (error) {
      console.error(`Error updating subscription in MySQL for FBID ${key}:`, error);
    }

    // Update the cache with the fetched subscription information
    const expireDate = 'E';
    await redis.set(fbid, expireDate);
    console.log('Subscription saved in cache:', expireDate);
  }

  // Function to retrieve and compare expire dates for all keys in Redis
  async function checkAllFBIDExpireDates() {
    try {
      const keys = await redis.keys('*');

      if (keys.length === 0) {
        console.log('No keys found in Redis.');
        return;
      }

      for (const key of keys) {
        const expireDate = await redis.get(key);
        if (expireDate) {
          const currentDate = new Date();
          const [expireDateString, specificValue] = expireDate.split(' (specific)');
          const expireDateObj = new Date(expireDateString);

          if (expireDateObj <= currentDate) {
            handleExpirationEvent(key);
          } else {
            const timeDifference = expireDateObj.getTime() - currentDate.getTime();
            setTimeout(() => {
              handleExpirationEvent(key);
            }, timeDifference);
            console.log(`FBID ${key} will expire on ${expireDateObj} ${specificValue}`);
          }
        } else {
          console.log(`FBID ${key} not found in Redis.`);
        }
      }
    } catch (error) {
      console.error('Error retrieving and comparing expire dates:', error);
    }
  }

  // Call the function to check all FBID expire dates
  checkAllFBIDExpireDates();

  // Route to check all data in Redis
  app.get('/api/check', async (req, res) => {
    try {
      await checkAllFBIDExpireDates();
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
  app.listen(port, () => {
    console.log(`Worker ${process.pid} listening on port ${port}.`);
  });
}
