// ===========================================
// Product Model - MongoDB Schema
// ===========================================
// Tracks product stock levels. The "available" quantity
// decreases when inventory is reserved and increases
// when reservations are released (compensation).
// ===========================================

import mongoose, { Document, Schema, Model } from "mongoose";

export interface IProduct {
  productId: string;
  productName: string;
  description: string;
  price: number;
  totalStock: number;
  reservedStock: number;
  availableStock: number;
  category?: string; //product category
  isActive: boolean;
  createdAt: Date;
  updatedAt: Date;
}

export interface IProductDocument extends IProduct, Document {}

const ProductSchema = new Schema<IProductDocument>(
  {
    productId: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
    productName: {
      type: String,
      required: true,
      trim: true,
    },
    description: {
      type: String,
      trim: true,
    },
    price: {
      type: Number,
      required: true,
      min: 0,
    },
    totalStock: {
      type: Number,
      required: true,
      min: 0,
      default: 0,
    },
    reservedStock: {
      type: Number,
      required: true,
      min: 0,
      default: 0,
    },
    availableStock: {
      type: Number,
      required: true,
      min: 0,
      default: 0,
    },
    category: {
      type: String,
      trim: true,
    },
    isActive: {
      type: Boolean,
      default: true,
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

//Indexes
ProductSchema.index({ productName: 1 });
ProductSchema.index({ category: 1 });
ProductSchema.index({ isActive: 1, availableStock: 1 });

ProductSchema.methods.hasAvailableStock = function (quantity: number): boolean {
  return this.availableStock >= quantity;
};

const Product: Model<IProductDocument> = mongoose.model<IProductDocument>(
  "Product",
  ProductSchema
);

export default Product;
