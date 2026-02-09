import express, { Express, Request, Response, NextFunction } from 'express';
import cors from 'cors';
import dotenv from 'dotenv';

dotenv.config();

import { connectDatabase, disconnectDatabase } from "./config/database";
import orderRoutes from './routes/orderRoutes';
import { orderEventHandler } from './events/eventHandler';
import logger from './utils/logger';

const PORT = process.env.PORT || 3001;
const NODE_ENV = process.env.NODE_ENV || 'development';

const app: Express = express();

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use((req: Request, res: Response, next: NextFunction) => {
  logger.debug(`${req.method} ${req.path}`, {
    query: req.query,
    body: req.method !== 'GET' ? req.body : undefined,
  });
  next();
});

app.use('/', orderRoutes);


app.use((req: Request, res: Response) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint not found',
    path: req.path,
  });
});

app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
  logger.error('Unhandled error', err);
  res.status(500).json({
    success: false,
    error: NODE_ENV === 'development' ? err.message : 'Internal server error',
  });
});

async function startServer():Promise<void> {
    try { 
 logger.info('Starting Order Service...');
        logger.info(`Environment: ${NODE_ENV}`);
        
        await connectDatabase();

        await orderEventHandler.start();

         app.listen(PORT, () => {
      logger.success(`Order Service is running on port ${PORT}`);
      logger.info('Available endpoints:');
      logger.info(`  GET    /health`);
      logger.info(`  POST   /orders`);
      logger.info(`  GET    /orders`);
      logger.info(`  GET    /orders/:orderId`);
      logger.info(`  GET    /orders/saga/:sagaId`);
      logger.info(`  GET    /orders/customer/:customerId`);
      logger.info(`  PATCH  /orders/:orderId/status`);
      logger.info(`  POST   /orders/:orderId/complete`);
      logger.info(`  POST   /orders/:orderId/cancel`);
         });
        


    } catch (error) {
    logger.error('Failed to start Order Service', error);
    process.exit(1);
  }
}

async function shutdown(signal: string): Promise<void> {
  logger.info(`Received ${signal}. Starting graceful shutdown...`);
  
  try {
    await orderEventHandler.stop();
    await disconnectDatabase();
    
    logger.info('Graceful shutdown completed');
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown', error);
    process.exit(1);
  }
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

process.on('uncaughtException', (error) => {
  logger.error('Uncaught exception', error);
  shutdown('uncaughtException');
});


process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled rejection', { reason, promise });
});

startServer();