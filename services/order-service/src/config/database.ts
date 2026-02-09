import mongoose from 'mongoose';

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/saga-orders';

const options: mongoose.ConnectOptions = {
   
};

export async function connectDatabase():Promise<void> {
    try {
        console.log("Order Service: Connecting to MongoDB...");
        console.log(`[Order Service] URI: ${MONGODB_URI.replace(/\/\/.*@/, '//<credentials>@')}`);
        await mongoose.connect(MONGODB_URI);

        console.log('[Order Service] Connected to MongoDB');

        //logging connection requests
        mongoose.connection.on('error', (error) => {
      console.error('[Order Service] MongoDB connection error:', error);
    });
    
    mongoose.connection.on('disconnected', () => {
      console.warn('[Order Service] MongoDB disconnected');
    });
    
    mongoose.connection.on('reconnected', () => {
      console.log('[Order Service] MongoDB reconnected');
    });
    

    } catch (error) {
        console.error('Error connecting to MongoDB:', error);
        throw error;
    }
}

export async function disconnectDatabase (): Promise<void> {
    try {
        await mongoose.disconnect();
        console.log('[Order Service] Disconnected from MongoDB');
    } catch (error) {
        console.error('Error disconnecting from MongoDB:', error);
        throw error;
    }
}

export async function isDatabaseConnected (): Promise<boolean>  {
    return mongoose.connection.readyState === 1;
}