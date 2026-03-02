// ===========================================
// Kafka Event Handler - Inventory Service
// ===========================================
// Listens to INVENTORY_SERVICE topic for commands:
//   - RESERVE_INVENTORY  → reserve stock → reply INVENTORY_RESERVED
//   - RELEASE_INVENTORY  → release stock → reply INVENTORY_RELEASED
// 
// Sends replies to SERVICE_REPLY topic
// ===========================================

import { Consumer, EachMessagePayload, Kafka, Producer } from "kafkajs";
import * as inventoryController from '../controllers/inventoryController';
import {
  ReserveInventoryRequest,
  ReleaseInventoryRequest,
  ReservationSuccessData,
  ReleaseSuccessData,
} from '../controllers/inventoryController';


const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || "inventory-service";
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID || "inventory-service-group";

const TOPICS = {
    INVENTORY_SERVICE: "INVENTORY_SERVICE",
    SERVICE_REPLY: "SERVICE_REPLY",
}

enum InventoryCommand{
    RESERVE_INVENTORY = "RESERVE_INVENTORY",
    RELEASE_INVENTORY = "RELEASE_INVENTORY",
}

enum InventoryReply{
    INVENTORY_RESERVED = "INVENTORY_RESERVED",
    INVENTORY_RELEASED = "INVENTORY_RELEASED",
    INVENTORY_RESERVATION_FAILED = "INVENTORY_RESERVATION_FAILED",
    INVENTORY_RELEASE_FAILED = "INVENTORY_RELEASE_FAILED",
}

const kafka = new Kafka({
    clientId: KAFKA_CLIENT_ID,
    brokers: KAFKA_BROKERS,
    retry: {
        initialRetryTime: 1000,
        retries: 5,
    }
});

let consumer: Consumer;
let producer: Producer;

async function handleMessage({ topic, partition, message }: EachMessagePayload): Promise<void>{
    const rawValue = message.value?.toString();

    if(!rawValue) {
        console.warn(`[Inventory Kafka] Received empty message on topic ${topic}`);
        return;
    }

    try {
        const parsed = JSON.parse(rawValue);
        const { type, payload } = parsed;
        
        console.log(`[Inventory Kafka] Received message: ${type} with payload:`, payload);
        console.log(`[Inventory Kafka] Message details - topic: ${topic}, partition: ${partition}, offset: ${message.offset}`);
        console.log(`[Inventory Kafka] Payload:`, JSON.stringify(payload, null, 2));

        switch (type) {
      case InventoryCommand.RESERVE_INVENTORY:
        await handleReserveInventory(payload);
        break;

      case InventoryCommand.RELEASE_INVENTORY:
        await handleReleaseInventory(payload);
        break;

      default:
        console.warn(`[Inventory Kafka] Unknown command type: ${type}`);
    }
    } catch (error) {
         console.error('[Inventory Kafka] ❌ Error processing message:', error);
     }
}


//HANDLERS

async function handleReserveInventory(payload: ReserveInventoryRequest): Promise<void>{
    const { sagaId, orderId, items } = payload;

    console.log(`[Inventory Kafka] Handling RESERVE_INVENTORY for sagaId: ${sagaId}, orderId: ${orderId}`);

    const result = await inventoryController.reserveInventory({
        sagaId,
        orderId,
        items,
    });

    if (result.success && result.data) {
        const data = result.data as ReservationSuccessData;
        await sendReply({
            type: InventoryReply.INVENTORY_RESERVED,
            sagaId,
            payload: {
                sagaId,
                orderId,
                reservationId: data.reservationId,
                items: data.items,
                status: 'RESERVED',
            },
        });
        console.log(`[Inventory Kafka] Inventory reserved successfully for sagaId: ${sagaId}, orderId: ${orderId}`);
    } else {
         await sendReply({
            type: InventoryReply.INVENTORY_RESERVATION_FAILED,
            sagaId,
            payload: {
                sagaId,
                orderId,
                error: result.error,
                status: 'FAILED',
            },
         });
        console.log(`[Inventory Kafka] ❌ INVENTORY_RESERVATION_FAILED reply sent for saga: ${sagaId}`);
    }
}

async function handleReleaseInventory(payload: ReleaseInventoryRequest): Promise<void> {
  const { sagaId, orderId } = payload;

  console.log(`[Inventory Kafka] 🔄 Processing RELEASE_INVENTORY (compensation) for saga: ${sagaId}`);

  const result = await inventoryController.releaseInventory({ sagaId, orderId });

  if (result.success && result.data) {
    const data = result.data as ReleaseSuccessData;
    await sendReply({
      type: InventoryReply.INVENTORY_RELEASED,
      sagaId,
      payload: {
        ...data,
        status: 'RELEASED',
      },
    });
    console.log(`[Inventory Kafka] ✅ INVENTORY_RELEASED reply sent for saga: ${sagaId}`);
  } else {
    await sendReply({
      type: InventoryReply.INVENTORY_RELEASE_FAILED,
      sagaId,
      payload: {
        sagaId,
        orderId,
        error: result.error,
        status: 'RELEASE_FAILED',
      },
    });
    console.error(`[Inventory Kafka] ❌ INVENTORY_RELEASE_FAILED for saga: ${sagaId}`);
  }
}



interface ReplyMessage {
  type: string;
  sagaId: string;
  payload: Record<string, unknown>;
}

async function sendReply(message: ReplyMessage): Promise<void> {
  try {
    await producer.send({
      topic: TOPICS.SERVICE_REPLY,
      messages: [
        {
          key: message.sagaId,
          value: JSON.stringify(message),
        },
      ],
    });
  } catch (error) {
    console.error('[Inventory Kafka] ❌ Error sending reply:', error);
    throw error;
  }
}

export async function initializeKafka(): Promise<void> {
  try {
    console.log('[Inventory Kafka] Initializing Kafka...');
    console.log(`[Inventory Kafka] Brokers: ${KAFKA_BROKERS.join(', ')}`);
    console.log(`[Inventory Kafka] Client ID: ${KAFKA_CLIENT_ID}`);
    console.log(`[Inventory Kafka] Group ID: ${KAFKA_GROUP_ID}`);

    // Create and connect producer
    producer = kafka.producer();
    await producer.connect();
    console.log('[Inventory Kafka] ✅ Producer connected');

    // Create and connect consumer
    consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });
    await consumer.connect();
    console.log('[Inventory Kafka] ✅ Consumer connected');

    // Subscribe to INVENTORY_SERVICE topic
    await consumer.subscribe({
      topic: TOPICS.INVENTORY_SERVICE,
      fromBeginning: false,
    });
    console.log(`[Inventory Kafka] ✅ Subscribed to topic: ${TOPICS.INVENTORY_SERVICE}`);

    // Start consuming messages
    await consumer.run({
      eachMessage: handleMessage,
    });

    console.log('[Inventory Kafka] ✅ Kafka initialized — listening for commands');
  } catch (error) {
    console.error('[Inventory Kafka] ❌ Failed to initialize Kafka:', error);
    throw error;
  }
}

export async function disconnectKafka(): Promise<void> {
  try {
    if (consumer) await consumer.disconnect();
    if (producer) await producer.disconnect();
    console.log('[Inventory Kafka] Disconnected');
  } catch (error) {
    console.error('[Inventory Kafka] Error disconnecting:', error);
  }
}