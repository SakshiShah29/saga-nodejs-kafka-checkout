import mongoose, { Document, Schema } from "mongoose";

export enum PaymentStatus {
  PENDING = "PENDING",
  PROCESSING = "PROCESSING",
  COMPLETED = "COMPLETED",
  FAILED = "FAILED",
  REFUNDED = "REFUNDED",
}

export enum PaymentMethod {
  CREDIT_CARD = "CREDIT_CARD",
  DEBIT_CARD = "DEBIT_CARD",
  WALLET = "WALLET",
  BANK_TRANSFER = "BANK_TRANSFER",
  MOCK = "MOCK",
}

export interface IPayment extends Document {
  orderId: string;
  sagaId: string;
  customerId: string;
  amount: number;
  currency: string;
  method: PaymentMethod;
  status: PaymentStatus;
  transactionId: string;
  failureReason?: string;
  metadata?: Record<string, any>;
  processedAt?: Date;
  refundedAt?: Date;
  createdAt: Date;
  updatedAt: Date;
}

const PaymentSchema = new Schema<IPayment>(
  {
    orderId: {
      type: String,
      required: true,
      index: true,
    },
    sagaId: {
      type: String,
      required: true,
      index: true,
    },
    customerId: {
      type: String,
      required: true,
      index: true,
    },
    amount: {
      type: Number,
      required: true,
      min: 0,
    },
    currency: {
      type: String,
      default: "USD",
    },
    method: {
      type: String,
      enum: Object.values(PaymentMethod),
      default: PaymentMethod.MOCK,
    },
    status: {
      type: String,
      enum: Object.values(PaymentStatus),
      default: PaymentStatus.PENDING,
    },
    transactionId: {
      type: String,
      required: true,
      unique: true,
    },
    failureReason: {
      type: String,
      default: null,
    },
    metadata: {
      type: Schema.Types.Mixed,
      default: {},
    },
    processedAt: {
      type: Date,
      default: null,
    },
    refundedAt: {
      type: Date,
      default: null,
    },
  },
  {
    timestamps: true,
  }
);

PaymentSchema.index({ orderId: 1, sagaId: 1 });

PaymentSchema.index({ status: 1 });

export const Payment = mongoose.model<IPayment>("Payment", PaymentSchema);
