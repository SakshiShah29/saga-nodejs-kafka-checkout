import mongoose, { Document, Schema, Model } from 'mongoose';

//Enums
export enum OrderStatus {
    PENDING = 'PENDING',
    INVENTORY_RESERVED = 'INVENTORY_RESERVED',
    PAYMENT_PROCESSING = 'PAYMENT_PROCESSING',
    PAYMENT_COMPLETED = 'PAYMENT_COMPLETED',
    COMPLETED = 'COMPLETED',
    FAILED = 'FAILED',
    CANCELLED = 'CANCELLED'
}


//interfaces
export interface IOrderItem{
    productId: string;
    productName: string;
    quantity: number;
    price: number;
}

export interface IOrder {
    orderId: string;
    sagaId: string;
    customerId: string;
    items: IOrderItem[];
    totalAmount: number;
    status: OrderStatus;
    failureReason?: string;
    createdAt: Date;
    updatedAt: Date;
}

export interface IOrderDocument extends IOrder, Document{ 
    canTransitionTo(newStatus: OrderStatus): boolean;
}

export interface IOrderModel extends Model<IOrderDocument> { 
    findBySagaId(sagaId: string): Promise<IOrderDocument | null>;
    findByCustomerId(customerId: string): Promise<IOrderDocument[]>;
    findByOrderId(orderId: string): Promise<IOrderDocument | null>;
}


//Definition
const OrderItemSchema = new Schema<IOrderItem>(
    {
        productId: {
            type: String,
            required: [true, 'Product ID is required'],
        },
        productName: {
            type: String,
            required: [true, 'Product Name is required'],
        },
        quantity: {
            type: Number,
            required: [true, 'Quantity is required'],
            min: [1, 'Quantity must be at least 1'],
        },
        price: {
            type: Number,
            required: [true, 'Price is required'],
            min: [0, 'Price must be a positive number'],
        },
    },
    { _id: false } //Prevents creation of _id for subdocuments
);

const OrderSchema = new Schema<IOrderDocument>(
    {
        orderId: {
            type: String,
            required: [true, 'Order ID is required'],
            unique: true,
            index: true,
        },
        sagaId: {
            type: String,
            required: [true, 'Saga ID is required'],
            index: true,
        },
        customerId: {
            type: String,
            required: [true, 'Customer ID is required'],
            index: true,
        },
        items: {
            type: [OrderItemSchema],
            required: [true, 'Order items are required'],
            validate: {
                validator: (items: IOrderItem[]) => items.length > 0,
                message: 'Order must contain at least one item',
            },
        },
        totalAmount: {
            type: Number,
            required: [true, 'Total amount is required'],
            min: [0, 'Total amount must be a positive number'],
        },
        status: {
            type: String,
            enum: Object.values(OrderStatus),
            default: OrderStatus.PENDING,
        },
        failureReason: {
            type: String,
        },
    },
    {
        timestamps: true,
        toJSON: { //Removes __v and _id from the response
            transform: function (doc, ret:Record<string,unknown>) {
                delete ret._id; 
                delete ret.__v;
                return ret;
            }
        }
     },
    
);


//Indexes - These indexes will help optimize queries based on customerId and status, which are common query parameters for orders. The createdAt index will help with sorting and filtering orders by date.
OrderSchema.index({ customerId: 1, createdAt: -1 });
OrderSchema.index({ status: 1, createdAt: -1 });


// static methods
// These static methods provide convenient ways to query orders based on different criteria,
//  such as sagaId, customerId, and orderId. They return promises that resolve to the corresponding
//  order documents or arrays of documents.
OrderSchema.statics.findBySagaId = function (
    sagaId: string
): Promise<IOrderDocument | null> {
    return this.findOne({ sagaId })
}


OrderSchema.statics.findByCustomerId = function (
    customerId: string
): Promise<IOrderDocument[]> {
    return this.find({ customerId }).sort({createdAt: -1});
}

OrderSchema.statics.findByOrderId = function (
    orderId: string
): Promise<IOrderDocument | null> {
    return this.findOne({ orderId });
}

OrderSchema.methods.canTransitionTo = function (
    newStatus: OrderStatus
):boolean{
    const allowedTransitions: Record<OrderStatus, OrderStatus[]> = {
        [OrderStatus.PENDING]: [
            OrderStatus.INVENTORY_RESERVED,
            OrderStatus.FAILED,
            OrderStatus.CANCELLED
        ],
        [OrderStatus.INVENTORY_RESERVED]: [
            OrderStatus.PAYMENT_PROCESSING,
            OrderStatus.FAILED,
            OrderStatus.CANCELLED
        ],
        [OrderStatus.PAYMENT_PROCESSING]: [
            OrderStatus.PAYMENT_COMPLETED,
            OrderStatus.FAILED,
            OrderStatus.CANCELLED
        ],
        [OrderStatus.PAYMENT_COMPLETED]: [
            OrderStatus.COMPLETED,
            OrderStatus.FAILED,
            OrderStatus.CANCELLED
        ],
        [OrderStatus.COMPLETED]: [],
        [OrderStatus.FAILED]: [],
        [OrderStatus.CANCELLED]: [],
    };

    return allowedTransitions[this.status as OrderStatus]?.includes(newStatus) ?? false;
}


// This pre-save hook calculates the total amount of the order by summing up the price multiplied by the quantity for each item in the order. It ensures that the total amount is always accurate before saving the order document to the database.
OrderSchema.pre('save', function (next) {
  if (this.isModified('items') || this.isNew) {
    this.totalAmount = this.items.reduce(
      (sum, item) => sum + item.price * item.quantity,
      0
    );
  }
  next();
});

export const Order: IOrderModel = mongoose.model<IOrderDocument, IOrderModel>('Order', OrderSchema);

export default Order;