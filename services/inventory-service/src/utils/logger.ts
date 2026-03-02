export function logStartup(serviceName: string, port: number): void {
  console.log('');
  console.log('═══════════════════════════════════════════');
  console.log(`  ${serviceName}`);
  console.log('═══════════════════════════════════════════');
  console.log(`  Port:        ${port}`);
  console.log(`  Environment: ${process.env.NODE_ENV || 'development'}`);
  console.log(`  MongoDB:     ${process.env.MONGODB_URI || 'localhost:27017'}`);
  console.log(`  Kafka:       ${process.env.KAFKA_BROKERS || 'localhost:9092'}`);
  console.log('═══════════════════════════════════════════');
  console.log('');
}