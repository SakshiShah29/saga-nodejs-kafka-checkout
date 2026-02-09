import { Router, Request, Response } from 'express';
import orderController from '../controllers/orderController';
import { OrderStatus } from '../models/Order';
import logger from '../utils/logger';

const router=Router();

router.get('/health', (req: Request, res: Response) => {
  res.json({
    success: true,
    service: 'order-service',
    status: 'healthy',
    timestamp: new Date().toISOString(),
  });
});

/**
 * POST /orders
 * Create a new order (typically called by Orchestrator via Kafka, but exposed for testing)
 * 
 * Body:
 * {
 *   "sagaId": "string",
 *   "customerId": "string",
 *   "items": [
 *     { "productId": "string", "productName": "string", "quantity": number, "price": number }
 *   ]
 * }
 */
router.post('/orders', async (req: Request, res: Response) => {
    try {
        const { sagaId, customerId, items } = req.body;
        if (!sagaId || !customerId || !items) {
            return res.status(400).json({
                success: false,
                error: 'Missing required fields: sagaId, customerId, items',
            });
        }
        const result = await orderController.createOrder({
            sagaId,
            customerId,
            items,
        })
        if (result.success) {
            res.status(201).json(result);
        } else {
            res.status(400).json(result);
        }

    } catch (error) {
        logger.error('Error in POST /orders', error);
        return res.status(500).json({
            success: false,
            error: 'Internal server error',
        });
    }
});


//Get all orders with pagination
router.get('/orders', async (req: Request, res: Response) => {
    try {
          const page = parseInt(req.query.page as string) || 1;
        const limit = parseInt(req.query.limit as string) || 10;
        
        const result = await orderController.getAllOrders(page, limit);
        if (result.success) {
            res.json(result);
        } else {
            res.status(500).json(result);
        }
    } catch (error) {
        logger.error('Error in GET /orders', error);
        return res.status(500).json({
            success: false,
            error: 'Internal server error',
        });
    }
});



router.get('/orders/saga/:sagaId', async (req: Request, res: Response) => {
  try {
    const { sagaId } = req.params;
    
    const result = await orderController.getOrderBySagaId(sagaId);
    
    if (result.success) {
      return res.json(result);
    } else {
      return res.status(404).json(result);
    }
  } catch (error) {
    logger.error('Error in GET /orders/saga/:sagaId', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
    });
  }
});

router.get('/orders/customer/:customerId', async (req: Request, res: Response) => {
  try {
    const { customerId } = req.params;
    
    const result = await orderController.getOrdersByCustomerId(customerId);
    
    if (result.success) {
      return res.json(result);
    } else {
      return res.status(500).json(result);
    }
  } catch (error) {
    logger.error('Error in GET /orders/customer/:customerId', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
    });
  }
});

router.get('/orders/:orderId', async (req: Request, res: Response) => {
    try {
        const { orderId } = req.params;
        const result = await orderController.getOrderById(orderId);
        if (result.success) {
            res.json(result);
        } else {
            res.status(404).json(result);
        }
    } catch (error) {
        logger.error('Error in GET /orders/:orderId', error);
        return res.status(500).json({
            success: false,
            error: 'Internal server error',
        });
    }
});

/**
 * PATCH /orders/:orderId/status
 * Update order status
 * 
 * Body:
 * {
 *   "status": "INVENTORY_RESERVED" | "PAYMENT_PROCESSING" | "PAYMENT_COMPLETED" | "COMPLETED" | "FAILED" | "CANCELLED",
 *   "failureReason": "string" (optional)
 * }
 */
router.patch('/orders/:orderId/status', async (req: Request, res: Response) => {
  try {
    const { orderId } = req.params;
    const { status, failureReason } = req.body;
    
    if (!status || !Object.values(OrderStatus).includes(status)) {
      return res.status(400).json({
        success: false,
        error: `Invalid status. Must be one of: ${Object.values(OrderStatus).join(', ')}`,
      });
    }
    
    const result = await orderController.updateOrderStatus({
      orderId,
      status,
      failureReason,
    });
    
    if (result.success) {
      return res.json(result);
    } else {
      return res.status(400).json(result);
    }
  } catch (error) {
    logger.error('Error in PATCH /orders/:orderId/status', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
    });
  }
});

router.post('/orders/:orderId/complete', async (req: Request, res: Response) => {
  try {
    const { orderId } = req.params;
    
    const result = await orderController.completeOrder(orderId);
    
    if (result.success) {
      return res.json(result);
    } else {
      return res.status(400).json(result);
    }
  } catch (error) {
    logger.error('Error in POST /orders/:orderId/complete', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
    });
  }
});


router.post('/orders/:orderId/cancel', async (req: Request, res: Response) => {
  try {
    const { orderId } = req.params;
    const { reason } = req.body;
    
    const result = await orderController.cancelOrder(orderId, reason);
    
    if (result.success) {
      return res.json(result);
    } else {
      return res.status(400).json(result);
    }
  } catch (error) {
    logger.error('Error in POST /orders/:orderId/cancel', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
    });
  }
});

export default router;