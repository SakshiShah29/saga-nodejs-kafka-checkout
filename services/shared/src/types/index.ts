export const KAFKA_TOPICS = {
  // SERVICE COMMAND TOPICS
  ORDER_SERVICE: "ORDER_SERVICE",
  PAYMENT_SERVICE: "PAYMENT_SERVICE",
  INVENTORY_SERVICE: "INVENTORY_SERVICE",

  //REPLY TOPICS
  SERVICE_REPLY: "SERVICE_REPLY",
} as const;

export type KafkaTopic = (typeof KAFKA_TOPICS)[keyof typeof KAFKA_TOPICS];

export enum EventType {
  CREATE_ORDER = "CREATE_ORDER",
  ORDER_CREATED = "ORDER_CREATED",
  CANCEL_ORDER = "CANCEL_ORDER",
  ORDER_CANCELLED = "ORDER_CANCELLED",
  COMPLETE_ORDER = "COMPLETE_ORDER",
  ORDER_COMPLETED = "ORDER_COMPLETED",
  ORDER_FAILED = "ORDER_FAILED",

  //INVENTORY EVENTS AND COMMANDS
  RESERVE_INVENTORY = "RESERVE_INVENTORY",
  INVENTORY_RESERVED = "INVENTORY_RESERVED",
  RELEASE_INVENTORY = "RELEASE_INVENTORY",
  INVENTORY_RELEASED = "INVENTORY_RELEASED",
  INVENTORY_RESERVATION_FAILED = "INVENTORY_RESERVATION_FAILED",

  //PAYMENT EVENTS AND COMMANDS
  PROCESS_PAYMENT = "PROCESS_PAYMENT",
  PAYMENT_COMPLETED = "PAYMENT_COMPLETED",
  REFUND_PAYMENT = "REFUND_PAYMENT",
  PAYMENT_REFUNDED = "PAYMENT_REFUNDED",
  PAYMENT_FAILED = "PAYMENT_FAILED",
}

export enum OrderStatus {
  PENDING = "PENDING",
  COMPLETED = "COMPLETED",
  CANCELLED = "CANCELLED",
  FAILED = "FAILED",
  INVENTORY_RESERVED = "INVENTORY_RESERVED",
  PAYMENT_PROCESSING = "PAYMENT_PROCESSING",
  PAYMENT_COMPLETED = "PAYMENT_COMPLETED",
}

export interface OrderItem {
  productId: string;
  quantity: number;
  productName: string;
  price: number;
}

export interface Order {
  orderId: string;
  sagaId: string;
  customerId: string;
  items: OrderItem[];
  totalAmount: number;
  status: OrderStatus;
  createdAt: string;
  updatedAt: string;
  failerReason?: string;
}

export interface CreateOrderRequest {
  customerId: string;
  items: {
    productId: string;
    quantity: number;
  }[];
}

export interface InventoryItem {
  productId: string;
  productName: string;
  totalQuantity: number;
  reservedQuantity: number;
  price: number;
}

export interface ReserveInventoryRequest {
  orderId: string;
  sagaId: string;
  items: Array<{
    productId: string;
    quantity: number;
  }>;
}

export interface InventoryReservation {
  reservationId: string;
  orderId: string;
  sagaId: string;
  items: Array<{
    productId: string;
    quantity: number;
  }>;
  status: "ACTIVE" | "RELEASED";
  createdAt: Date;
}

export enum PaymentStatus {
  PENDING = "PENDING",
  PROCESSING = "PROCESSING",
  COMPLETED = "COMPLETED",
  FAILED = "FAILED",
  REFUNDED = "REFUNDED",
}

export interface Payment {
  paymentId: string;
  orderId: string;
  sagaId: string;
  amount: number;
  status: PaymentStatus;
  transactionId?: string;
  failureReason?: string;
  createdAt: Date;
  updatedAt: Date;
}

export interface ProcessPaymentRequest {
  orderId: string;
  sagaId: string;
  amount: number;
  customerId: string;
}

export enum SagaStatus {
  STARTED = "STARTED",
  RUNNING = "RUNNING",
  COMPLETED = "COMPLETED",
  COMPENSATING = "COMPENSATING",
  COMPENSATED = "COMPENSATED",
  FAILED = "FAILED",
}

export enum SagaStepName {
  CREATE_ORDER = "CREATE_ORDER",
  RESERVE_INVENTORY = "RESERVE_INVENTORY",
  PROCESS_PAYMENT = "PROCESS_PAYMENT",
  COMPLETE_ORDER = "COMPLETE_ORDER",
}

export interface SagaStep {
  name: SagaStepName;
  status: "PENDING" | "EXECUTING" | "COMPLETED" | "FAILED" | "COMPENSATED";
  executedAt?: Date;
  compensatedAt?: Date;
  error?: string;
}

export interface SagaState {
  sagaId: string;
  orderId?: string;
  status: SagaStatus;
  currentStep: SagaStepName;
  steps: SagaStep[];
  orderData: CreateOrderRequest;
  createdAt: Date;
  updatedAt: Date;
  completedAt?: Date;
  failureReason?: string;
}

// -------------------------------------------
// Kafka Message Types
// -------------------------------------------

export interface BaseMessage {
  sagaId: string;
  timestamp: string;
}

export interface CommandMessage extends BaseMessage {
  type: EventType;
  payload: Record<string, unknown>;
}

export interface ReplyMessage extends BaseMessage {
  type: EventType;
  success: boolean;
  payload?: Record<string, unknown>;
  error?: string;
}

export interface ApiResponse<T = unknown> {
  success: boolean;
  data?: T;
  error?: {
    code: string;
    message: string;
    details?: unknown;
  };
  timestamp: Date;
}

export interface CheckoutResponse {
  sagaId: string;
  orderId?: string;
  status: SagaStatus;
  message: string;
}
