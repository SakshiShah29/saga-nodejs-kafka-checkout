import dotenv from 'dotenv';
dotenv.config();

import mongoose from 'mongoose';
import Product from '../models/Product';

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/saga_inventory';

const seedProducts = [
  {
    productId: 'p1',
    productName: 'Wireless Mouse',
    description: 'Ergonomic wireless mouse with USB receiver',
    price: 29.99,
    totalStock: 100,
    availableStock: 100,
    reservedStock: 0,
    category: 'Electronics',
    isActive: true,
  },
  {
    productId: 'p2',
    productName: 'Mechanical Keyboard',
    description: 'RGB mechanical keyboard with Cherry MX switches',
    price: 89.99,
    totalStock: 50,
    availableStock: 50,
    reservedStock: 0,
    category: 'Electronics',
    isActive: true,
  },
  {
    productId: 'p3',
    productName: 'USB-C Hub',
    description: '7-in-1 USB-C hub with HDMI, USB 3.0, SD card reader',
    price: 49.99,
    totalStock: 75,
    availableStock: 75,
    reservedStock: 0,
    category: 'Accessories',
    isActive: true,
  },
  {
    productId: 'p4',
    productName: 'Laptop Stand',
    description: 'Adjustable aluminum laptop stand',
    price: 39.99,
    totalStock: 60,
    availableStock: 60,
    reservedStock: 0,
    category: 'Accessories',
    isActive: true,
  },
  {
    productId: 'p5',
    productName: 'Webcam HD',
    description: '1080p HD webcam with built-in microphone',
    price: 59.99,
    totalStock: 40,
    availableStock: 40,
    reservedStock: 0,
    category: 'Electronics',
    isActive: true,
  },
];

async function seed(): Promise<void> {
  try {
    console.log('[Seed] Connecting to MongoDB...');
    await mongoose.connect(MONGODB_URI);
    console.log('[Seed] ✅ Connected');

    // Clear existing products
    await Product.deleteMany({});
    console.log('[Seed] Cleared existing products');

    // Insert seed products
    const created = await Product.insertMany(seedProducts);
    console.log(`[Seed] ✅ Inserted ${created.length} products:`);

    created.forEach((p) => {
      console.log(`  - ${p.productId}: ${p.productName} (stock: ${p.totalStock}, price: $${p.price})`);
    });

    console.log('\n[Seed] ✅ Seeding complete!');
  } catch (error) {
    console.error('[Seed] ❌ Error:', error);
  } finally {
    await mongoose.disconnect();
    process.exit(0);
  }
}

seed();