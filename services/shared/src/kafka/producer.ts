// =================================
// Kafka Producer Service
// =================================

import { Kafka, Producer, ProducerRecord, RecordMetadata } from "kafkajs";
import { v4 as uuidv4 } from "uuid";
import { KafkaTopic, CommandMessage, EventType } from "../types";

export interface KafkaProducerConfig {
  clientId: string;
  brokers: string[];
}

const defaultConfig: KafkaProducerConfig = {
  brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
  clientId: process.env.KAFKA_CLIENT_ID || "saga-checkout",
};

export class KafkaProducer {
  private kafka: Kafka;
  private producer: Producer;
  private serviceName: string;
  private isConnected: boolean = false;

  constructor(serviceName: string, config: Partial<KafkaProducerConfig> = {}) {
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

    this.producer = this.kafka.producer();
  }

  async connect(): Promise<void> {
    if (this.isConnected) return;
    try {
      await this.producer.connect();
      this.isConnected = true;
      console.log(
        `[KafkaProducer - ${this.serviceName}] Connected to Kafka brokers.`
      );
    } catch (error) {
      console.error(
        `[KafkaProducer - ${this.serviceName}] Failed to connect to Kafka brokers:`,
        error
      );
      throw error;
    }
  }

  // Send a message to a Kafka
  async send(
    topic: KafkaTopic,
    type: EventType,
    payload: Record<string, unknown>,
    sagaId: string
  ): Promise<RecordMetadata[]> {
    if (!this.isConnected) {
      await this.connect();
    }
    const message: CommandMessage = {
      sagaId: sagaId,
      timestamp: new Date().toISOString(),
      type,
      payload,
    };

    const record: ProducerRecord = {
      topic,
      messages: [
        {
          key: sagaId,
          value: JSON.stringify(message),
        },
      ],
    };
    try {
      const result = await this.producer.send(record);
      console.log(`[${this.serviceName}] Sent to ${topic}:`, {
        type,
        sagaId,
        partition: result[0]?.partition,
        offset: result[0]?.offset,
      });
      return result;
    } catch (error) {
      console.error(`[${this.serviceName}] Failed to send message:`, error);
      throw error;
    }
  }

  // Used by services to send replies back to the orchestrator
  async sendReply(
    type: EventType,
    sagaId: string,
    success: boolean,
    payload?: Record<string, unknown>,
    error?: string
  ): Promise<RecordMetadata[]> {
    if (!this.isConnected) {
      await this.connect();
    }

    const message = {
      sagaId,
      type,
      success,
      payload,
      error,
      timestamp: new Date().toISOString(),
    };

    const record: ProducerRecord = {
      topic: "SERVICE_REPLY",
      messages: [
        {
          key: sagaId,
          value: JSON.stringify(message),
        },
      ],
    };

    try {
      const result = await this.producer.send(record);
      console.log(`[${this.serviceName}] Sent reply:`, {
        type,
        sagaId,
        success,
      });
      return result;
    } catch (error) {
      console.error(`[${this.serviceName}] Failed to send reply:`, error);
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    if (!this.isConnected) return;

    try {
      await this.producer.disconnect();
      this.isConnected = false;
      console.log(`[${this.serviceName}] Kafka producer disconnected`);
    } catch (error) {
      console.error(
        `[${this.serviceName}] Failed to disconnect producer:`,
        error
      );
      throw error;
    }
  }
}

export function createProducer(
  serviceName: string,
  config?: Partial<KafkaProducerConfig>
): KafkaProducer {
  return new KafkaProducer(serviceName, config);
}
