const SERVICE_NAME = 'Order Service';

export const logger = {
  info: (message: string, data?: Record<string, unknown>) => {
    console.log(`[${SERVICE_NAME}] ${message}`, data ? JSON.stringify(data) : '');
  },
  
  error: (message: string, error?: unknown) => {
    console.error(`[${SERVICE_NAME}] ❌ ${message}`, error);
  },
  
  warn: (message: string, data?: Record<string, unknown>) => {
    console.warn(`[${SERVICE_NAME}] ⚠️ ${message}`, data ? JSON.stringify(data) : '');
  },
  
  debug: (message: string, data?: Record<string, unknown>) => {
    if (process.env.NODE_ENV === 'development') {
      console.debug(`[${SERVICE_NAME}] 🔍 ${message}`, data ? JSON.stringify(data) : '');
    }
  },
  
  success: (message: string, data?: Record<string, unknown>) => {
    console.log(`[${SERVICE_NAME}] ✅ ${message}`, data ? JSON.stringify(data) : '');
  },
};

export default logger;