import { v4 as uuidv4 } from "uuid";
import {
  Payment,
  PaymentStatus,
  PaymentMethod,
  IPayment,
} from "../models/Payment";

const FAILURE_RATE = parseFloat(process.env.PAYMENT_FAILURE_RATE || "0.2");
const PROCESSING_DELAY_MS = parseInt(
  process.env.PAYMENT_PROCESSING_DELAY_MS || "1000",
  10
);

/**
 *
 * Simulates a payment gateway processing with a configurable failure rate and processing delay.
 * @param amount
 * @returns A promise that resolves to an object containing the success status, transaction ID, and optionally a failure reason.
 */
async function simulatePaaymentGateway(amount: number): Promise<{
  success: boolean;
  transactionId: string;
  failureReason?: string;
}> {
  await new Promise((resolve) => setTimeout(resolve, PROCESSING_DELAY_MS));
  const transactionId = `txn_${uuidv4().replace(/-/g, "").substring(0, 16)}`;
  const random = Math.random();
  if (random < FAILURE_RATE) {
    const failureReasons = [
      "INSUFFICIENT_FUNDS",
      "CARD_DECLINED",
      "GATEWAY_TIMEOUT",
      "FRAUD_SUSPECTED",
      "CARD_EXPIRED",
    ];
    const reason =
      failureReasons[Math.floor(Math.random() * failureReasons.length)];

    return {
      success: false,
      transactionId,
      failureReason: reason,
    };
  }
  return {
    success: true,
    transactionId,
  };
}

function generateTransactionId(): string {
  return `txn_${uuidv4().replace(/-/g, "").substring(0, 16)}`;
}

// =============================================
// SAGA OPERATIONS (called by Kafka handlers)
// =============================================

export async function processPayment(data: {
  orderId: string;
  sagaId: string;
  customerId: string;
  amount: number;
  currency?: string;
  method?: PaymentMethod;
}): Promise<{
  success: boolean;
  payment: IPayment;
  message: string;
}> {
  const {
    orderId,
    sagaId,
    customerId,
    amount,
    currency = "USD",
    method = PaymentMethod.MOCK,
  } = data;

  console.log(
    `[Payment Service] Processing payment for order: ${orderId}, saga: ${sagaId}`
  );
  console.log(
    `[Payment Service] Amount: ${amount} ${currency}, Customer: ${customerId}`
  );
  const existingPayment = await Payment.findOne({ orderId, sagaId });
  if (existingPayment) {
    console.log(
      `[Payment Service] Duplicate request detected for saga: ${sagaId}`
    );
    console.log(
      `[Payment Service] Returning existing payment: ${existingPayment.transactionId}`
    );

    return {
      success: existingPayment.status === PaymentStatus.COMPLETED,
      payment: existingPayment,
      message: `Duplicate request — returning existing payment (status: ${existingPayment.status})`,
    };
  }

  if (amount <= 0) {
    // Create a failed payment record for audit trail
    const failedPayment = await Payment.create({
      orderId,
      sagaId,
      customerId,
      amount,
      currency,
      method,
      status: PaymentStatus.FAILED,
      transactionId: generateTransactionId(),
      failureReason: "INVALID_AMOUNT",
    });

    return {
      success: false,
      payment: failedPayment,
      message: "Payment amount must be greater than 0",
    };
  }
  const transactionId = generateTransactionId();
  const payment = await Payment.create({
    orderId,
    sagaId,
    customerId,
    amount,
    currency,
    method,
    status: PaymentStatus.PROCESSING,
    transactionId,
  });

  console.log(
    `[Payment Service] Payment created: ${transactionId} (PROCESSING)`
  );

  try {
    const gatewayResult = await simulatePaaymentGateway(amount);
    if (gatewayResult.success) {
      payment.status = PaymentStatus.COMPLETED;
      payment.processedAt = new Date();
      await payment.save();

      console.log(`[Payment Service] Payment completed: ${transactionId}`);

      return {
        success: true,
        payment,
        message: "Payment processed successfully",
      };
    } else {
      payment.status = PaymentStatus.FAILED;
      payment.failureReason = gatewayResult.failureReason || "UNKNOWN_ERROR";
      await payment.save();

      console.log(
        `[Payment Service] Payment failed: ${transactionId}, Reason: ${gatewayResult.failureReason}`
      );

      return {
        success: false,
        payment,
        message: `Payment failed: ${gatewayResult.failureReason}`,
      };
    }
  } catch (error) {
    payment.status = PaymentStatus.FAILED;
    payment.failureReason = "GATEWAY_ERROR";
    await payment.save();

    console.error(
      `[Payment Service] Error processing payment: ${transactionId}`,
      error
    );

    return {
      success: false,
      payment,
      message: `Error processing payment: ${payment.failureReason}`,
    };
  }
}

export async function refundPayment(data: {
  orderId: string;
  sagaId: string;
}): Promise<{
  sucess: boolean;
  payment: IPayment | null;
  message: string;
}> {
  const { orderId, sagaId } = data;
  console.log(
    `[Payment Service] Refunding payment for order: ${orderId}, saga: ${sagaId}`
  );

  const payment = await Payment.findOne({ orderId, sagaId });

  if (!payment) {
    // No payment found — this could mean:
    // 1. Payment was never processed (saga failed before payment step)
    // 2. Invalid orderId/sagaId
    // In either case, compensation is considered successful
    console.log(
      `[Payment Service] No payment found for order: ${orderId}, saga: ${sagaId}. Assuming no action needed for refund.`
    );
    return {
      sucess: true,
      payment: null,
      message: "No payment found — compensation considered successful",
    };
  }

  //Idempotency check: if already refunded, return success
  if (payment.status === PaymentStatus.REFUNDED) {
    console.log(
      `[Payment Service] Payment already refunded: ${payment.transactionId}`
    );
    return {
      sucess: true,
      payment,
      message: "Payment already refunded",
    };
  }

  //can only refund COMPLETED payments
  if (payment.status === PaymentStatus.FAILED) {
    console.log(
      `[Payment Service] Payment failed, no refund needed: ${payment.transactionId}`
    );
    return {
      sucess: true,
      payment,
      message: "Payment failed, no refund needed",
    };
  }

  if (payment.status !== PaymentStatus.COMPLETED) {
    console.log(
      `[Payment Service] Payment in non-final state (${payment.status}), cannot refund: ${payment.transactionId}`
    );
    return {
      sucess: false,
      payment,
      message: `Cannot refund payment in status: ${payment.status}`,
    };
  }

  //in a real system this would call a payment gateways refund api
  payment.status = PaymentStatus.REFUNDED;
  payment.refundedAt = new Date();
  payment.metadata = {
    ...payment.metadata,
    refundReason: "SAGA_COMPENSATION",
    refundedBySaga: sagaId,
  };
  await payment.save();
  console.log(`[Payment Service] Payment refunded: ${payment.transactionId}`);

  return {
    sucess: true,
    payment,
    message: "Payment refunded successfully",
  };
}

// =============================================
// REST API OPERATIONS (called by Express routes)
// =============================================

export async function getAllPayments(
  filters: {
    status?: PaymentStatus;
    customerId?: string;
    limit?: number;
  } = {}
): Promise<IPayment[]> {
  const query: any = {};

  if (filters.status) query.status = filters.status;
  if (filters.customerId) query.customerId = filters.customerId;

  return Payment.find(query)
    .sort({ createdAt: -1 })
    .limit(filters.limit || 50);
}

/**
 * Get payment by ID
 */
export async function getPaymentById(
  paymentId: string
): Promise<IPayment | null> {
  return Payment.findById(paymentId);
}

/**
 * Get payment by transaction ID
 */
export async function getPaymentByTransactionId(
  transactionId: string
): Promise<IPayment | null> {
  return Payment.findOne({ transactionId });
}

/**
 * Get payment by order ID
 */
export async function getPaymentByOrderId(
  orderId: string
): Promise<IPayment | null> {
  return Payment.findOne({ orderId });
}

/**
 * Get payment by saga ID
 */
export async function getPaymentBySagaId(
  sagaId: string
): Promise<IPayment | null> {
  return Payment.findOne({ sagaId });
}

/**
 * Get payment statistics
 */
export async function getPaymentStats(): Promise<{
  total: number;
  pending: number;
  processing: number;
  completed: number;
  failed: number;
  refunded: number;
  totalAmount: number;
  refundedAmount: number;
}> {
  const [stats] = await Payment.aggregate([
    {
      $group: {
        _id: null,
        total: { $sum: 1 },
        pending: { $sum: { $cond: [{ $eq: ["$status", "PENDING"] }, 1, 0] } },
        processing: {
          $sum: { $cond: [{ $eq: ["$status", "PROCESSING"] }, 1, 0] },
        },
        completed: {
          $sum: { $cond: [{ $eq: ["$status", "COMPLETED"] }, 1, 0] },
        },
        failed: { $sum: { $cond: [{ $eq: ["$status", "FAILED"] }, 1, 0] } },
        refunded: { $sum: { $cond: [{ $eq: ["$status", "REFUNDED"] }, 1, 0] } },
        totalAmount: {
          $sum: { $cond: [{ $eq: ["$status", "COMPLETED"] }, "$amount", 0] },
        },
        refundedAmount: {
          $sum: { $cond: [{ $eq: ["$status", "REFUNDED"] }, "$amount", 0] },
        },
      },
    },
  ]);

  return (
    stats || {
      total: 0,
      pending: 0,
      processing: 0,
      completed: 0,
      failed: 0,
      refunded: 0,
      totalAmount: 0,
      refundedAmount: 0,
    }
  );
}
