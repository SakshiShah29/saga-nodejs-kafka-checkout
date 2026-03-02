import mongoose from "mongoose";

const MONGODB_URI =
  process.env.MONGODB_URI || "mongodb://localhost:27017/saga_inventory";

export async function connectDatabase(): Promise<void> {
  try {
    console.log("[Inventory Service] Connecting to MongoDB...");
    console.log(
      `[Inventory Service] URI: ${MONGODB_URI.replace(
        /\/\/.*@/,
        "//<credentials>@"
      )}`
    );

    await mongoose.connect(MONGODB_URI);

    console.log("[Inventory Service] ✅ MongoDB connected successfully");

    mongoose.connection.on("error", (error) => {
      console.error("[Inventory Service] MongoDB connection error:", error);
    });

    mongoose.connection.on("disconnected", () => {
      console.warn("[Inventory Service] MongoDB disconnected");
    });

    mongoose.connection.on("reconnected", () => {
      console.log("[Inventory Service] MongoDB reconnected");
    });
  } catch (error) {
    console.error(
      "[Inventory Service] ❌ Failed to connect to MongoDB:",
      error
    );
    throw error;
  }
}

export async function disconnectDatabase(): Promise<void> {
  try {
    await mongoose.disconnect();
    console.log("[Inventory Service] Disconnected from MongoDB");
  } catch (error) {
    console.error(
      "[Inventory Service] Error disconnecting from MongoDB:",
      error
    );
    throw error;
  }
}

export function isDatabaseConnected(): boolean {
  return mongoose.connection.readyState === 1;
}
