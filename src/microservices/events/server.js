const express = require('express');
const { Kafka } = require('kafkajs');

const PORT = process.env.PORT || 8082;
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const TOPICS = ['movie-events', 'user-events', 'payment-events'];

const kafka = new Kafka({
  clientId: 'events-service',
  brokers: KAFKA_BROKERS,
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'events-service-group' });

async function connectWithRetry(connectFn, label, attempts = 30, delayMs = 2000) {
  for (let i = 1; i <= attempts; i++) {
    try {
      await connectFn();
      console.log(`${label} connected`);
      return;
    } catch (err) {
      console.log(`${label} attempt ${i}/${attempts} failed: ${err.message}`);
      if (i === attempts) {
        throw err;
      }
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

async function initKafka() {
  await connectWithRetry(() => producer.connect(), 'Kafka producer');
  await connectWithRetry(() => consumer.connect(), 'Kafka consumer');

  await consumer.subscribe({ topics: TOPICS, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const payload = message.value ? message.value.toString() : '';
      console.log(`[Consumer] topic=${topic} partition=${partition} offset=${message.offset} payload=${payload}`);
    },
  });
}

async function publishEvent(topic, payload) {
  await producer.send({
    topic,
    messages: [{ value: JSON.stringify(payload) }],
  });
}

const app = express();
app.use(express.json());

app.get('/api/events/health', (_req, res) => {
  res.status(200).json({ status: true });
});

app.post('/api/events/movie', async (req, res) => {
  try {
    await publishEvent('movie-events', req.body);
    res.status(201).json({ status: 'success' });
  } catch (err) {
    console.error('Failed to publish movie event:', err.message);
    res.status(500).json({ error: 'Failed to publish event' });
  }
});

app.post('/api/events/user', async (req, res) => {
  try {
    await publishEvent('user-events', req.body);
    res.status(201).json({ status: 'success' });
  } catch (err) {
    console.error('Failed to publish user event:', err.message);
    res.status(500).json({ error: 'Failed to publish event' });
  }
});

app.post('/api/events/payment', async (req, res) => {
  try {
    await publishEvent('payment-events', req.body);
    res.status(201).json({ status: 'success' });
  } catch (err) {
    console.error('Failed to publish payment event:', err.message);
    res.status(500).json({ error: 'Failed to publish event' });
  }
});

async function start() {
  await initKafka();

  app.listen(PORT, () => {
    console.log(`Events service listening on port ${PORT}`);
    console.log(`Kafka brokers: ${KAFKA_BROKERS.join(', ')}`);
  });
}

start().catch((err) => {
  console.error('Failed to start events service:', err);
  process.exit(1);
});
