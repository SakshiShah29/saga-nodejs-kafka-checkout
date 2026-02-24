// ===========================================
// Reservation Model - MongoDB Schema
// ===========================================
// Tracks individual inventory reservations tied
// to saga transactions. This is the "paper trail"
// that lets us know what to release during compensation.
// ===========================================

import mongoose, { Document, Schema, Model } from "mongoose";

export enum ReservationStatus {
  // ACTIVE: Items are held for this order, waiting for payment
  // → availableStock decreased, reservedStock increased
  ACTIVE = "ACTIVE",

  // RELEASED: Saga failed (e.g., payment declined) - compensation happened
  // → Items returned to availableStock, reservedStock decreased
  RELEASED = "RELEASED",

  // COMMITTED: Saga succeeded - order complete, items shipped/sold
  // → reservedStock decreased, totalStock decreased (items left warehouse)
  COMMITTED = "COMMITTED",

  // EXPIRED: Reservation held too long without saga completion
  // → Same effect as RELEASED, items return to availableStock
  EXPIRED = "EXPIRED",
}

export interface IReservationItem {
  productId: string;
  productName: string;
  quantity: number;
  price: number;
}

export interface IReservation {
  reservationId: string;
  sagaId: string; // Correlation ID for the saga transaction
  orderId: string; // Associated order ID for reference
  items: IReservationItem[]; // List of products reserved
  status: ReservationStatus; // ACTIVE, RELEASED, COMMITTED, EXPIRED
  reservedAt: Date; // Timestamp when reservation was made
  updatedAt: Date; // Timestamp for last update (status change)
  releasedAt?: Date; // Timestamp when reservation was released (if applicable)
  committedAt?: Date; // Timestamp when reservation was committed to saga
  failureReason?: string; // Optional field to log reason for release (e.g., payment failure)
  createdAt: Date;
}

export interface IReservationDocument extends IReservation, Document {}

const ReservationItemSchema = new Schema(
  {
    productId: { type: String, required: true },
    productName: { type: String, required: true },
    quantity: { type: Number, required: true, min: 1 },
    price: { type: Number, required: true, min: 0 },
  },
  { _id: false }
);

const ReservationSchema = new Schema<IReservationDocument>(
  {
    reservationId: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
    sagaId: {
      type: String,
      required: true,
      index: true,
    },
    orderId: {
      type: String,
      required: true,
      index: true,
    },
    items: {
      type: [ReservationItemSchema],
      required: true,
      validate: {
        validator: function (v: IReservationItem[]) {
          return v.length > 0;
        },
        message: "At least one reservation item is required.",
      },
    },
    status: {
      type: String,
      enum: Object.values(ReservationStatus),
      default: ReservationStatus.ACTIVE,
      index: true,
    },
    reservedAt: {
      type: Date,
      default: Date.now,
    },
    updatedAt: {
      type: Date,
      default: Date.now,
    },
    releasedAt: { type: Date },
    committedAt: { type: Date },
    failureReason: {
      type: String,
    },
  },
  {
    timestamps: true,
    toJSON: {
      transform: (doc, ret: Record<string, unknown>) => {
        delete ret.__v;
        return ret;
      },
    },
  }
);

ReservationSchema.index({ sagaId: 1, status: 1 });
ReservationSchema.index({ orderId: 1, status: 1 });
ReservationSchema.index({ status: 1, reservedAt: 1 });

const Reservation: Model<IReservationDocument> =
  mongoose.model<IReservationDocument>("Reservation", ReservationSchema);

export default Reservation;
