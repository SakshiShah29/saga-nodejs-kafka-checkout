import dotenv from 'dotenv';
dotenv.config();

import express from 'express';
import cors from 'cors';
import { connectDatabase, isDatabaseConnected } from './config/database';
import { initializeKafka } from './events/eventHandler';
import inventoryRoutes from './routes/inventoryRoutes';
import { logStartup } from './utils/logger';

const app = express();
const PORT = parseInt(process.env.PORT || '3002', 10);


app.use(cors());
app.use(express.json());


app.use((req, _res, next) => {
  console.log(`[Inventory API] ${req.method} ${req.path}`);
  next();
});

app.get('/health', (_req, res) => {
  res.json({
    service: 'inventory-service',
    status: 'running',
    timestamp: new Date().toISOString(),
    mongodb:  isDatabaseConnected() ? 'connected' : 'disconnected',
    uptime: process.uptime(),
  });
});


app.use('/', inventoryRoutes);

async function startServer(): Promise<void> {
  try {
    // 1. Connect to MongoDB
    await connectDatabase();

    // 2. Initialize Kafka
    await initializeKafka();

    // 3. Start Express server
    app.listen(PORT, () => {
      logStartup('📦 Inventory Service', PORT);

      console.log('  REST Endpoints:');
      console.log(`  GET    http://localhost:${PORT}/health`);
      console.log(`  GET    http://localhost:${PORT}/products`);
      console.log(`  GET    http://localhost:${PORT}/products/:productId`);
      console.log(`  POST   http://localhost:${PORT}/products`);
      console.log(`  PATCH  http://localhost:${PORT}/products/:productId/stock`);
      console.log(`  POST   http://localhost:${PORT}/reservations/reserve`);
      console.log(`  POST   http://localhost:${PORT}/reservations/release`);
      console.log(`  POST   http://localhost:${PORT}/reservations/commit`);
      console.log(`  GET    http://localhost:${PORT}/reservations`);
      console.log(`  GET    http://localhost:${PORT}/reservations/saga/:sagaId`);
      console.log('');
      console.log('  Kafka Commands Listening:');
      console.log('  📨 RESERVE_INVENTORY  → reserves stock');
      console.log('  📨 RELEASE_INVENTORY  → releases stock (compensation)');
      console.log('');
    });
  } catch (error) {
    console.error('[Inventory Service] ❌ Failed to start:', error);
    process.exit(1);
  }
}

process.on('SIGINT', async () => {
  console.log('\n[Inventory Service] Shutting down gracefully...');
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n[Inventory Service] Received SIGTERM...');
  process.exit(0);
});

startServer();