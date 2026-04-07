import mongoose from 'mongoose';

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/saga_payment';


export async function connectDatabase(): Promise<void>{
     try {
    console.log('[Payment Service] Connecting to MongoDB...');
    console.log(`[Payment Service] URI: ${MONGODB_URI.replace(/\/\/.*@/, '//<credentials>@')}`);

    await mongoose.connect(MONGODB_URI);

    console.log('[Payment Service]  MongoDB connected successfully');

    // Log connection events
    mongoose.connection.on('error', (error) => {
      console.error('[Payment Service] MongoDB connection error:', error);
    });

    mongoose.connection.on('disconnected', () => {
      console.warn('[Payment Service] MongoDB disconnected');
    });

    mongoose.connection.on('reconnected', () => {
      console.log('[Payment Service] MongoDB reconnected');
    });

  } catch (error) {
    console.error('[Payment Service] ❌ Failed to connect to MongoDB:', error);
    throw error;
  }
}

export async function disconnectDatabase(): Promise<void> {
  try {
    await mongoose.disconnect();
    console.log('[Payment Service] MongoDB disconnected');
  } catch (error) {
    console.error('[Payment Service] Error disconnecting from MongoDB:', error);
    throw error;
  }
}

export function isDatabaseConnected(): boolean {
  return mongoose.connection.readyState === 1;
}