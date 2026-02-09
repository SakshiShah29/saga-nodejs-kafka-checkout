import { v4 as uuidv4 } from 'uuid';
import Order, { IOrderDocument, OrderStatus, IOrderItem } from '../models/Order';
import logger from '../utils/logger';

export interface CreateOrderInput{
    sagaId: string;
    customerId: string;
    items: IOrderItem[];
}

export interface UpdateOrderStatusInput{
    orderId: string;
    status: OrderStatus;
    failureReason?: string;
}

export interface OrderResponse{
    success: boolean;
    data?: IOrderDocument | null;
    error?: string;
}

export class OrderController{

    //Create a new Order
    //Called when orchestrator send CREATE_ORDER command
    async createOrder(input: CreateOrderInput): Promise<OrderResponse> {
        const { sagaId, customerId, items } = input;
        logger.info('Creating new order', { sagaId, customerId, itemCount: items.length, items });
        
        try {
            if (!sagaId || !customerId || !items || items.length === 0) {
                return {
                    success: false,
                    error: 'Invalid input. sagaId, customerId and items are required.',
                }
            }

            const orderId = `ORD-${uuidv4().substring(0, 8).toUpperCase()}`;
            const totalAmount = items.reduce((sum, item) => sum + item.price * item.quantity, 0);
            
            const order = new Order({
                orderId,
                sagaId,
                customerId,
                items,
                totalAmount,
                status: OrderStatus.PENDING,
            });
            await order.save();
            logger.success('Order created successfully', { orderId, sagaId, totalAmount, status: order.status });
            
            return {
                success: true,
                data: order,
            }
        } catch (error) {
            logger.error('Error creating order', error);
            return {
                success: false,
                error: error instanceof Error ? error.message : 'Unknown error creating order',
            }
        }
    }

    async getOrderById(orderId: string): Promise<OrderResponse> {
    logger.debug('Getting order by ID', { orderId });
    
    try {
      const order = await Order.findByOrderId(orderId);
      
      if (!order) {
        return {
          success: false,
          error: `Order not found: ${orderId}`,
        };
      }
      
      return {
        success: true,
        data: order,
      };
    } catch (error) {
      logger.error('Failed to get order', error);
      
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error getting order',
      };
    }
  }

      async getOrderBySagaId(sagaId: string): Promise<OrderResponse> {
    logger.debug('Getting order by Saga ID', { sagaId });
    
    try {
      const order = await Order.findBySagaId(sagaId);
      
      if (!order) {
        return {
          success: false,
          error: `Order not found for saga: ${sagaId}`,
        };
      }
      
      return {
        success: true,
        data: order,
      };
    } catch (error) {
      logger.error('Failed to get order by saga ID', error);
      
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error getting order',
      };
    }
      }
    
      async getOrdersByCustomerId(customerId: string): Promise<{
    success: boolean;
    data?: IOrderDocument[];
    error?: string;
  }> {
    logger.debug('Getting orders by customer ID', { customerId });
    
    try {
      const orders = await Order.findByCustomerId(customerId);
      
      return {
        success: true,
        data: orders,
      };
    } catch (error) {
      logger.error('Failed to get orders by customer ID', error);
      
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error getting orders',
      };
    }
      }
    
    //This will be used dureing saga progression to update order status based on the outcome of each step
    async updateOrderStatus(input: UpdateOrderStatusInput): Promise<OrderResponse> {
    const { orderId, status, failureReason } = input;
    
    logger.info('Updating order status', { orderId, status, failureReason });
    
    try {
      const order = await Order.findByOrderId(orderId);
      
      if (!order) {
        return {
          success: false,
          error: `Order not found: ${orderId}`,
        };
      }
      
      // Checking if transition is allowed
      if (!order.canTransitionTo(status)) {
        return {
          success: false,
          error: `Invalid status transition: ${order.status} -> ${status}`,
        };
      }
      
      order.status = status;
      
      if (failureReason) {
        order.failureReason = failureReason;
      }
      
      await order.save();
      
      logger.success('Order status updated', {
        orderId,
        oldStatus: order.status,
        newStatus: status,
      });
      
      return {
        success: true,
        data: order,
      };
    } catch (error) {
      logger.error('Failed to update order status', error);
      
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error updating order',
      };
    }
    }
    
    async completeOrder(orderId: string): Promise<OrderResponse> {
    logger.info('Completing order', { orderId });
    
    return this.updateOrderStatus({
      orderId,
      status: OrderStatus.COMPLETED,
    });
    }
    

    async cancelOrder(orderId: string, reason?: string): Promise<OrderResponse> {
    logger.info('Cancelling order (compensation)', { orderId, reason });
    
    try {
      const order = await Order.findByOrderId(orderId);
      
      if (!order) {
        return {
          success: false,
          error: `Order not found: ${orderId}`,
        };
      }
      
      // Allow cancellation from any non-final state
      if (order.status === OrderStatus.COMPLETED) {
        return {
          success: false,
          error: 'Cannot cancel a completed order',
        };
      }
      
      if (order.status === OrderStatus.CANCELLED) {
        // Already cancelled, return success (idempotent)
        logger.warn('Order already cancelled', { orderId });
        return {
          success: true,
          data: order,
        };
      }
      
      order.status = OrderStatus.CANCELLED;
      order.failureReason = reason || 'Order cancelled due to saga compensation';
      
      await order.save();
      
      logger.success('Order cancelled (compensation complete)', { orderId });
      
      return {
        success: true,
        data: order,
      };
    } catch (error) {
      logger.error('Failed to cancel order', error);
      
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error cancelling order',
      };
    }
  }

    async getAllOrders(
    page: number = 1,
    limit: number = 10
  ): Promise<{
    success: boolean;
    data?: {
      orders: IOrderDocument[];
      total: number;
      page: number;
      pages: number;
    };
    error?: string;
  }> {
    try {
      const skip = (page - 1) * limit;
      
      const [orders, total] = await Promise.all([
        Order.find().sort({ createdAt: -1 }).skip(skip).limit(limit),
        Order.countDocuments(),
      ]);
      
      return {
        success: true,
        data: {
          orders,
          total,
          page,
          pages: Math.ceil(total / limit),
        },
      };
    } catch (error) {
      logger.error('Failed to get all orders', error);
      
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error getting orders',
      };
    }
  }
  
}

export const orderController = new OrderController();
export default orderController;

