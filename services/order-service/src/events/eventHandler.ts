import { Kafka, Consumer, Producer, EachMessagePayload } from "kafkajs";
import orderController from "../controllers/orderController";
import { IOrderItem, OrderStatus } from "../models/Order";
import logger from "../utils/logger";

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(
  ","
);
const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || "order-service";
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID || "order-service-group";

const TOPICS = {
  ORDER_SERVICE: "ORDER_SERVICE", // Commands TO this service
  SERVICE_REPLY: "SERVICE_REPLY", // Replies FROM this service
};

enum OrderEventType {
  // Commands (received from Orchestrator)
  CREATE_ORDER = "CREATE_ORDER",
  COMPLETE_ORDER = "COMPLETE_ORDER",
  CANCEL_ORDER = "CANCEL_ORDER",

  // Replies (sent to Orchestrator)
  ORDER_CREATED = "ORDER_CREATED",
  ORDER_COMPLETED = "ORDER_COMPLETED",
  ORDER_CANCELLED = "ORDER_CANCELLED",
  ORDER_FAILED = "ORDER_FAILED",
}

// Base structure for all messages (commands and replies)
interface BaseMessage {
  sagaId: string;
  type: string;
  timestamp: string;
}

interface CreateOrderCommand extends BaseMessage {
  type: OrderEventType.CREATE_ORDER;
  payload: {
    customerId: string;
    items: Array<{
      productId: string;
      productName: string;
      quantity: number;
      price: number;
    }>;
  };
}

interface CompleteOrderCommand extends BaseMessage {
  type: OrderEventType.COMPLETE_ORDER;
  payload: {
    orderId: string;
  };
}

interface CancelOrderCommand extends BaseMessage {
  type: OrderEventType.CANCEL_ORDER;
  payload: {
    orderId: string;
    customerId: string;
    reason?: string;
    items: IOrderItem[];
  };
}

type OrderCommand =
  | CreateOrderCommand
  | CompleteOrderCommand
  | CancelOrderCommand;

interface ReplyMessage {
  sagaId: string;
  type: OrderEventType;
  success: boolean;
  timestamp: string;
  payload?: Record<string, unknown>;
  error?: string;
}

export class OrderEventHandler {
  private kafka: Kafka;
  private consumer: Consumer;
  private producer: Producer;
  private isConnected: boolean = false;

  constructor() {
    this.kafka = new Kafka({
      clientId: KAFKA_CLIENT_ID,
      brokers: KAFKA_BROKERS,
      retry: {
        initialRetryTime: 100,
        retries: 8,
      },
    });

    this.consumer = this.kafka.consumer({
      groupId: KAFKA_GROUP_ID,
    });

    this.producer = this.kafka.producer();
  }

  /**
   * Connect to Kafka and start consuming messages
   */
  async start(): Promise<void> {
    try {
      logger.info("Connecting to Kafka...");

      // Connect producer
      await this.producer.connect();
      logger.success("Kafka producer connected");

      // Connect consumer
      await this.consumer.connect();
      logger.success("Kafka consumer connected");

      // Subscribe to ORDER_SERVICE topic
      await this.consumer.subscribe({
        topic: TOPICS.ORDER_SERVICE,
        fromBeginning: false,
      });
      logger.info(`Subscribed to topic: ${TOPICS.ORDER_SERVICE}`);

      // Start consuming messages
      await this.consumer.run({
        eachMessage: async (payload: EachMessagePayload) => {
          await this.handleMessage(payload);
        },
      });

      this.isConnected = true;
      logger.success("Kafka event handler started");
    } catch (error) {
      logger.error("Failed to start Kafka event handler", error);
      throw error;
    }
  }

  //Handling incoming messages
  private async handleMessage(payload: EachMessagePayload): Promise<void> {
    const { topic, partition, message } = payload;

    if (!message.value) {
      logger.warn("Received message with empty value, skipping", {
        topic,
        partition,
      });
      return;
    }

    try {
      const messageStr = message.value.toString();
      const command = JSON.parse(messageStr) as OrderCommand;

      logger.info("Received command", {
        topic,
        partition,
        type: command.type,
        sagaId: command.sagaId,
      });
      //route

      switch (command.type) {
        case OrderEventType.CREATE_ORDER:
          await this.handleCreateOrder(command as CreateOrderCommand);
          break;
        case OrderEventType.COMPLETE_ORDER:
          await this.handleCompleteOrder(command as CompleteOrderCommand);
          break;
        case OrderEventType.CANCEL_ORDER:
          await this.handleCancelOrder(command as CancelOrderCommand);
          break;
        default:
          logger.warn("Unknown command type", {
            type: (command as BaseMessage).type,
          });
      }
    } catch (error) {
      logger.error("Failed to process message", error);
    }
  }

  private async handleCreateOrder(command: CreateOrderCommand): Promise<void> {
    const { sagaId, payload } = command;
    logger.info("Handling CREATE_ORDER", { sagaId });

    try {
      const result = await orderController.createOrder({
        sagaId,
        customerId: payload.customerId,
        items: payload.items,
      });

      if (result.success && result.data) {
        await this.sendReply({
          sagaId,
          type: OrderEventType.ORDER_CREATED,
          success: true,
          timestamp: new Date().toISOString(),
          payload: {
            orderId: result.data.orderId,
            customerId: result.data.customerId,
            totalAmount: result.data.totalAmount,
            status: result.data.status,
          },
        });
      } else {
        await this.sendReply({
          sagaId,
          type: OrderEventType.ORDER_FAILED,
          success: false,
          timestamp: new Date().toISOString(),
          error: result.error || "Failed to create order",
        });
      }
    } catch (error) {
      logger.error("Error handling CREATE_ORDER", error);

      await this.sendReply({
        sagaId,
        type: OrderEventType.ORDER_FAILED,
        success: false,
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : "Unknown error",
      });
    }
  }

  private async handleCompleteOrder(
    command: CompleteOrderCommand
  ): Promise<void> {
    const { sagaId, payload } = command;

    logger.info("Handling COMPLETE_ORDER", {
      sagaId,
      orderId: payload.orderId,
    });

    try {
      const result = await orderController.completeOrder(payload.orderId);

      if (result.success && result.data) {
        await this.sendReply({
          sagaId,
          type: OrderEventType.ORDER_COMPLETED,
          success: true,
          timestamp: new Date().toISOString(),
          payload: {
            orderId: result.data.orderId,
            status: result.data.status,
          },
        });
      } else {
        await this.sendReply({
          sagaId,
          type: OrderEventType.ORDER_FAILED,
          success: false,
          timestamp: new Date().toISOString(),
          error: result.error || "Failed to complete order",
        });
      }
    } catch (error) {
      logger.error("Error handling COMPLETE_ORDER", error);

      await this.sendReply({
        sagaId,
        type: OrderEventType.ORDER_FAILED,
        success: false,
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : "Unknown error",
      });
    }
  }

  private async handleCancelOrder(command: CancelOrderCommand): Promise<void> {
    const { sagaId, payload } = command;

    logger.info("Handling CANCEL_ORDER (compensation)", {
      sagaId,
      orderId: payload.orderId,
    });

    try {
      const result = await orderController.cancelOrder(
        payload.orderId,
        payload.reason
      );

      if (result.success && result.data) {
        await this.sendReply({
          sagaId,
          type: OrderEventType.ORDER_CANCELLED,
          success: true,
          timestamp: new Date().toISOString(),
          payload: {
            orderId: result.data.orderId,
            status: result.data.status,
          },
        });
      } else {
        await this.sendReply({
          sagaId,
          type: OrderEventType.ORDER_FAILED,
          success: false,
          timestamp: new Date().toISOString(),
          error: result.error || "Failed to cancel order",
        });
      }
    } catch (error) {
      logger.error("Error handling CANCEL_ORDER", error);

      await this.sendReply({
        sagaId,
        type: OrderEventType.ORDER_FAILED,
        success: false,
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : "Unknown error",
      });
    }
  }

  private async sendReply(reply: ReplyMessage): Promise<void> {
    try {
      await this.producer.send({
        topic: TOPICS.SERVICE_REPLY,
        messages: [
          {
            key: reply.sagaId,
            value: JSON.stringify(reply),
          },
        ],
      });

      logger.info("Sent reply", {
        topic: TOPICS.SERVICE_REPLY,
        type: reply.type,
        sagaId: reply.sagaId,
        success: reply.success,
      });
    } catch (error) {
      logger.error("Failed to send reply", error);
      throw error;
    }
  }

  async stop(): Promise<void> {
    try {
      await this.consumer.disconnect();
      await this.producer.disconnect();
      this.isConnected = false;
      logger.info("Kafka event handler stopped");
    } catch (error) {
      logger.error("Error stopping Kafka event handler", error);
      throw error;
    }
  }

  isReady(): boolean {
    return this.isConnected;
  }
}

export const orderEventHandler = new OrderEventHandler();
export default orderEventHandler;
