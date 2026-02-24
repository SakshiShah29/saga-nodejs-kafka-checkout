import { v4 as uuidv4 } from "uuid";
import Product, { IProduct } from "../models/Product";
import Reservation, {
  ReservationStatus,
  IReservationDocument,
} from "../models/Reservation";

interface ReserveItem {
  productId: string;
  quantity: number;
  price: number;
}

interface ReserveInventoryRequest {
  sagaId: string;
  orderId: string;
  items: ReserveItem[];
}

interface ReleaseInventoryRequest {
  sagaId: string;
  orderId: string;
}

interface InventoryResult<T = unknown> {
  success: boolean;
  data?: T;
  error?: string;
}

export async function getAllProducts(): Promise<InventoryResult> {
  try {
    const products = await Product.find({ isActive: true })
      .sort({ productName: 1 })
      .lean<IProduct[]>()
      .exec();
    return { success: true, data: products };
  } catch (error: any) {
    console.error("[Inventory] Error fetching products:", error);
    return { success: false, error: error.message };
  }
}

export async function getProductById(
  productId: string
): Promise<InventoryResult> {
  try {
    const product = await Product.findOne({ productId })
      .lean<IProduct>()
      .exec();
    if (!product) {
      return { success: false, error: "Product not found" };
    }
    return { success: true, data: product };
  } catch (error: any) {
    console.error(`[Inventory] Error fetching product ${productId}:`, error);
    return { success: false, error: error.message };
  }
}

export async function createProduct(productData: {
  productId: string;
  productName: string;
  description?: string;
  price: number;
  totalStock: number;
  category?: string;
}): Promise<InventoryResult> {
  try {
    const product = new Product({
      ...productData,
      reservedStock: 0,
      availableStock: productData.totalStock,
      isActive: true,
    });

    await product.save();
    console.log(
      `[Inventory] Product created: ${product.productName} (stock: ${product.totalStock})`
    );

    return { success: true, data: product.toJSON() };
  } catch (error: any) {
    console.error("[Inventory] Error creating product:", error);
    return { success: false, error: error.message };
  }
}

export async function addStock(
  productId: string,
  quantity: number
): Promise<InventoryResult> {
  try {
    const product = await Product.findByIdAndUpdate(
      productId,
      {
        $inc: {
          totalStock: quantity,
          availableStock: quantity,
        },
      },
      { new: true }
    );
    if (!product) {
      return { success: false, error: "Product not found" };
    }
    console.log(
      `[Inventory] Added ${quantity} stock to product ${product.productName}. New total stock: ${product.totalStock}`
    );
    return { success: true, data: product.toJSON() };
  } catch (error: any) {
    console.error(
      `[Inventory] Error adding stock to product ${productId}:`,
      error
    );
    return { success: false, error: error.message };
  }
}

/**
 * * This is the main saga operation. When the orchestrator
 * sends RESERVE_INVENTORY, we:
 * 1. Check if all items have enough stock
 * 2. Atomically decrease availableStock and increase reservedStock
 * 3. Create a Reservation record to track what we reserved
 * 4. Reply with INVENTORY_RESERVED or INVENTORY_RESERVATION_FAILED
 */
export async function reserveInventory(
  request: ReserveInventoryRequest
): Promise<InventoryResult> {
  const { sagaId, orderId, items } = request;

  console.log(
    `[Inventory] 📦 Reserving inventory for saga: ${sagaId}, order: ${orderId}`
  );
  console.log(
    `[Inventory] Items to reserve:`,
    items.map((i) => `${i.productId} x${i.quantity}`).join(", ")
  );
  try {
    //step 1: check stock availability
    for (const item of items) {
      const product = await Product.findOne({
        productId: item.productId,
      }).exec();
      if (!product) {
        return {
          success: false,
          error: `Product not found: ${item.productId}`,
        };
      }

      if (!product.isActive) {
        return {
          success: false,
          error: `Product is inactive: ${item.productId}`,
        };
      }

      if (product.availableStock < item.quantity) {
        return {
          success: false,
          error: `Insufficient stock for product ${item.productId}. Available: ${product.availableStock}, requested: ${item.quantity}`,
        };
      }
    }

    //step 2: reserve stock atomically
    const reservedItems = [];
    for (const item of items) {
      const updatedProduct = await Product.findOneAndUpdate(
        { productId: item.productId, availableStock: { $gte: item.quantity } },
        {
          $inc: {
            availableStock: -item.quantity,
            reservedStock: item.quantity,
          },
        },
        { new: true }
      ).exec();

      if (!updatedProduct) {
        // Race condition: stock was taken between check and update
        // Roll back any items we already reserved in this batch
        console.error(
          `[Inventory] ❌ Race condition on ${item.productId}. Rolling back...`
        );

        for (const reserved of reservedItems) {
          await Product.findOneAndUpdate(
            { productId: reserved.productId },
            {
              $inc: {
                reservedStock: -reserved.quantity,
                availableStock: reserved.quantity,
              },
            }
          );
        }

        return {
          success: false,
          error: `Failed to reserve ${item.productId} — stock unavailable (race condition)`,
        };
      }
      reservedItems.push({
        productId: item.productId,
        productName: updatedProduct.productName,
        quantity: item.quantity,
        price: item.price,
      });
      console.log(
        `[Inventory] Reserved ${item.quantity} of ${updatedProduct.productName} (ID: ${item.productId}). Remaining stock: ${updatedProduct.availableStock}`
      );
    }

    //step 3: create reservation record
    const reservation = new Reservation({
      reservationId: uuidv4(),
      sagaId,
      orderId,
      items: reservedItems,
      status: ReservationStatus.ACTIVE,
      reservedAt: new Date(),
    });
    await reservation.save();

    console.log(
      `[Inventory] Inventory reserved successfully for order ${orderId}. Reservation ID: ${reservation.reservationId}`
    );
    console.log(
      `[Inventory] Inventory reserved successfully for saga: ${sagaId}, order: ${orderId}. Reservation ID: ${reservation.reservationId}`
    );

    return {
      success: true,
      data: {
        reservationId: reservation.reservationId,
        items: reservedItems,
        sagaId,
        orderId,
        status: reservation.status,
      },
    };
  } catch (error: any) {
    console.error(
      `[Inventory] Error reserving inventory for order ${request.orderId}:`,
      error
    );
    return { success: false, error: error.message };
  }
}


