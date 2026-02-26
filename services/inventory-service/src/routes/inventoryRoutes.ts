import { Router, Request, Response } from "express";
import * as inventoryController from "../controllers/inventoryController";

const router = Router();

router.get("/products", async (_req: Request, res: Response) => {
  const result = await inventoryController.getAllProducts();

  if (result.success) {
    res.json({ success: true, data: result.data });
  } else {
    res.status(500).json({ success: false, error: result.error });
  }
});

router.get("/products/:productId", async (req: Request, res: Response) => {
  const result = await inventoryController.getProductById(req.params.productId);

  if (result.success) {
    res.json({ success: true, data: result.data });
  } else {
    res.status(404).json({ success: false, error: result.error });
  }
});

router.post("/products", async (req: Request, res: Response) => {
  const { productId, productName, description, price, totalStock, category } =
    req.body;

  if (
    !productId ||
    !productName ||
    price === undefined ||
    totalStock === undefined
  ) {
    res.status(400).json({
      success: false,
      error:
        "Missing required fields: productId, productName, price, totalStock",
    });
    return;
  }

  const result = await inventoryController.createProduct({
    productId,
    productName,
    description,
    price,
    totalStock,
    category,
  });

  if (result.success) {
    res.status(201).json({ success: true, data: result.data });
  } else {
    res.status(400).json({ success: false, error: result.error });
  }
});

router.patch(
  "/products/:productId/stock",
  async (req: Request, res: Response) => {
    const { quantity } = req.body;

    if (!quantity || quantity <= 0) {
      res
        .status(400)
        .json({ success: false, error: "Quantity must be a positive number" });
      return;
    }

    const result = await inventoryController.addStock(
      req.params.productId,
      quantity
    );

    if (result.success) {
      res.json({ success: true, data: result.data });
    } else {
      res.status(404).json({ success: false, error: result.error });
    }
  }
);

//Remove after testing
router.post("/reservations/reserve", async (req: Request, res: Response) => {
  const { sagaId, orderId, items } = req.body;

  if (
    !sagaId ||
    !orderId ||
    !items ||
    !Array.isArray(items) ||
    items.length === 0
  ) {
    res.status(400).json({
      success: false,
      error: "Missing required fields: sagaId, orderId, items[]",
    });
    return;
  }

  const result = await inventoryController.reserveInventory({
    sagaId,
    orderId,
    items,
  });

  if (result.success) {
    res.status(201).json({ success: true, data: result.data });
  } else {
    res.status(400).json({ success: false, error: result.error });
  }
});

router.post("/reservations/release", async (req: Request, res: Response) => {
  const { sagaId, orderId } = req.body;

  if (!sagaId || !orderId) {
    res.status(400).json({
      success: false,
      error: "Missing required fields: sagaId, orderId",
    });
    return;
  }

  const result = await inventoryController.releaseInventory({
    sagaId,
    orderId,
  });

  if (result.success) {
    res.json({ success: true, data: result.data });
  } else {
    res.status(400).json({ success: false, error: result.error });
  }
});

router.post("/reservations/commit", async (req: Request, res: Response) => {
  const { sagaId, orderId } = req.body;

  if (!sagaId || !orderId) {
    res.status(400).json({
      success: false,
      error: "Missing required fields: sagaId, orderId",
    });
    return;
  }

  const result = await inventoryController.commitReservation(sagaId, orderId);

  if (result.success) {
    res.json({ success: true, data: result.data });
  } else {
    res.status(400).json({ success: false, error: result.error });
  }
});

//-------------------------------------------------

router.get("/reservations", async (_req: Request, res: Response) => {
  const result = await inventoryController.getaAllReservation();

  if (result.success) {
    res.json({ success: true, data: result.data });
  } else {
    res.status(500).json({ success: false, error: result.error });
  }
});

router.get(
  "/reservations/saga/:sagaId",
  async (req: Request, res: Response) => {
    const result = await inventoryController.getReservationBySagaId(
      req.params.sagaId
    );

    if (result.success) {
      res.json({ success: true, data: result.data });
    } else {
      res.status(404).json({ success: false, error: result.error });
    }
  }
);

export default router;
