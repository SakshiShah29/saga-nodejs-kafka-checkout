// ===========================
// Kafka Consumer Service
// ===========================
import { Kafka, Consumer, EachMessagePayload, ConsumerConfig } from "kafkajs";
import { KafkaTopic, CommandMessage, ReplyMessage, EventType } from "../types";

export interface KafkaConsumerConfig {
  brokers: string[];
  clientId: string;
  groupId: string;
}

const defaultConfig = {
  brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
  clientId: process.env.KAFKA_CLIENT_ID || "saga-checkout",
};

export type MessageHandler = (
  message: CommandMessage | ReplyMessage,
  topic: string
) => Promise<void>;

export class KafkaConsumer {
  private kafka: Kafka;
  private consumer: Consumer;
  private serviceName: string;
  private isConnected: boolean = false;
  private handlers: Map<EventType, MessageHandler[]> = new Map();
  private topicHandlers: Map<string, MessageHandler[]> = new Map();

  constructor(
    serviceName: string,
    groupId: string,
    config: Partial<KafkaConsumerConfig> = {}
  ) {
    const finalConfig = { ...defaultConfig, ...config };

    this.serviceName = serviceName;
    this.kafka = new Kafka({
      clientId: `${finalConfig.clientId}-${serviceName}`,
      brokers: finalConfig.brokers,
      retry: {
        initialRetryTime: 100,
        retries: 8,
      },
    });

    this.consumer = this.kafka.consumer({
      groupId: groupId,
    });
  }

  /**
   * Connect and subscribe to topics
   */
  async connect(topics: KafkaTopic[]): Promise<void> {
    if (this.isConnected) return;

    try {
      await this.consumer.connect();

      for (const topic of topics) {
        await this.consumer.subscribe({ topic, fromBeginning: false });
        console.log(`[${this.serviceName}] Subscribed to: ${topic}`);
      }

      this.isConnected = true;
      console.log(`[${this.serviceName}] Kafka consumer connected`);
    } catch (error) {
      console.error(`[${this.serviceName}] Failed to connect consumer:`, error);
      throw error;
    }
  }

  /**
   * Register a handler for a specific event type
   */
  on(eventType: EventType, handler: MessageHandler): void {
    const handlers = this.handlers.get(eventType) || [];
    handlers.push(handler);
    this.handlers.set(eventType, handlers);
    console.log(`[${this.serviceName}] Registered handler for: ${eventType}`);
  }

  /**
   * Register a handler for all messages on a topic
   */
  onTopic(topic: KafkaTopic, handler: MessageHandler): void {
    const handlers = this.topicHandlers.get(topic) || [];
    handlers.push(handler);
    this.topicHandlers.set(topic, handlers);
  }

  /**
   * Start consuming messages
   */
  async run(): Promise<void> {
    await this.consumer.run({
      eachMessage: async (payload: EachMessagePayload) => {
        const { topic, message } = payload;

        if (!message.value) {
          console.warn(
            `[${this.serviceName}] Received empty message on ${topic}`
          );
          return;
        }

        try {
          const parsedMessage = JSON.parse(message.value.toString()) as
            | CommandMessage
            | ReplyMessage;

          console.log(`[${this.serviceName}] Received from ${topic}:`, {
            type: parsedMessage.type,
            sagaId: parsedMessage.sagaId,
          });

          // Call event type handlers
          const typeHandlers = this.handlers.get(parsedMessage.type) || [];
          for (const handler of typeHandlers) {
            try {
              await handler(parsedMessage, topic);
            } catch (error) {
              console.error(
                `[${this.serviceName}] Handler error for ${parsedMessage.type}:`,
                error
              );
            }
          }

          // Call topic handlers
          const topicHandlersList =
            this.topicHandlers.get(topic as KafkaTopic) || [];
          for (const handler of topicHandlersList) {
            try {
              await handler(parsedMessage, topic);
            } catch (error) {
              console.error(
                `[${this.serviceName}] Topic handler error for ${topic}:`,
                error
              );
            }
          }
        } catch (error) {
          console.error(
            `[${this.serviceName}] Failed to parse message:`,
            error
          );
        }
      },
    });

    console.log(`[${this.serviceName}] Consumer is running...`);
  }

  /**
   * Disconnect from Kafka
   */
  async disconnect(): Promise<void> {
    if (!this.isConnected) return;

    try {
      await this.consumer.disconnect();
      this.isConnected = false;
      console.log(`[${this.serviceName}] Kafka consumer disconnected`);
    } catch (error) {
      console.error(
        `[${this.serviceName}] Failed to disconnect consumer:`,
        error
      );
      throw error;
    }
  }
}

export function createConsumer(
  serviceName: string,
  groupId: string,
  config?: Partial<KafkaConsumerConfig>
): KafkaConsumer {
  return new KafkaConsumer(serviceName, groupId, config);
}
