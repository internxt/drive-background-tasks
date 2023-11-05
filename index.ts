import { config } from 'dotenv';
import amqp from 'amqplib';
import { v4 } from 'uuid';

import { createLogger } from './src/utils';
import { Consumer } from './src/consumer';
import { Producer } from './src/producer';
import { DriveDatabase } from './src/drive';
import { DeletedFoldersIterator } from './src/tasks/process-folder-deletion/deleted-folders.iterator';

const [,, ...args] = process.argv;
const [type] = args;

if (!type) {
  console.error('Missing argument: type');
  process.exit(1);
}

if (type !== 'producer' && type !== 'consumer') {
  console.error('Invalid argument: type. Accepted values are "producer" or "consumer"');
  process.exit(1);
}
const processId = v4();
const logger = createLogger(processId);

config();

const amqpServer = process.env.AMQP_SERVER;
const queueName = process.env.MARK_DELETED_ITEMS_QUEUE_NAME;
const maxEnqueuedItems = process.env.TASK_MARK_DELETED_ITEMS_PRODUCER_MAX_ENQUEUED_ITEMS;
const maxConcurrentItems = process.env.TASK_MARK_DELETED_ITEMS_CONSUMER_MAX_CONCURRENT_ITEMS;

if (!maxEnqueuedItems) {
  logger.log('Missing env var: TASK_MARK_DELETED_ITEMS_PRODUCER_MAX_ENQUEUED_ITEMS');
  process.exit(1);
}

if (!maxConcurrentItems) {
  logger.log('Missing env var: TASK_MARK_DELETED_ITEMS_CONSUMER_MAX_CONCURRENT_ITEMS');
  process.exit(1);
}

logger.log(`params: process_type -> ${type}, env -> ${
  JSON.stringify({ 
    maxConcurrentItems, 
    maxEnqueuedItems, 
    queueName 
  })
}`);

let db: DriveDatabase;
let connection: amqp.Connection;

function handleStop() {
  const dbProm = db && db.disconnect().then(() => {
    logger.log('DB disconnected')
  }).catch((err) => {
    logger.error('Error disconnecting DB', err);
  });

  const queueProm = connection && connection.close().then(() => {
    logger.log('RabbitMQ connection closed')
  }).catch((err) => {
    logger.error('Error closing RabbitMQ connection', err);
  });

  if (dbProm && queueProm) {
    Promise.all([dbProm, queueProm]).then(() => {
      logger.log('Exit succesfully!');
      process.exit(0);
    }).catch((err) => {
      logger.error('Error exiting', err);
      process.exit(1);
    });
  }
}

async function start(): Promise<{ connection: amqp.Connection, db: DriveDatabase }> {
  db = new DriveDatabase();

  logger.log('(drive-db) connecting ...');
  await db.connect();
  logger.log('(drive-db) connected !');

  logger.log('(rabbit) connecting ...');
  connection = await amqp.connect(amqpServer as string);
  logger.log('(rabbit) connected !');

  return { connection, db };
}

start().then(({ connection, db }) => {
  if (type === 'producer') {
    const deletedFoldersIterator = new DeletedFoldersIterator(db);

    return connection.createChannel().then((channel) => {
      const producer = new Producer(
        channel, 
        queueName as string, 
        deletedFoldersIterator,
        maxEnqueuedItems ? parseInt(maxEnqueuedItems as string) : undefined,
      );

      producer.on('enqueue', (item) => {
        logger.log(`enqueued item: + ${JSON.stringify(item)}`, 'producer');
      });

      producer.on('queue-full', () => {
        logger.log(`queue full, waiting 1s...`, 'producer');
      });

      return producer.run();
    });
  } else {
    return connection.createChannel().then((channel) => {
      const consumer = new Consumer<{ 
        folder_id: string,
        processed: boolean,
        created_at: Date,
        updated_at: Date,
        processed_at: Date,
      }>(
        channel, 
        queueName as string,
        async (taskPayload) => {
          logger.log(`received item: + ${JSON.stringify(taskPayload)}`, 'consumer');
  
          await db.markChildrenFilesAsDeleted(taskPayload.folder_id);
          await db.markChildrenFoldersAsDeleted(taskPayload.folder_id);
          await db.markDeletedFolderAsProcessed([taskPayload.folder_id]);
        },
        maxConcurrentItems ? parseInt(maxConcurrentItems as string) : undefined,
      );

      consumer.on('error', ({ err, msg }) => {
        logger.error(`error processing item: ${JSON.stringify(msg.content)}`, err, 'consumer');
      });
      
      return consumer.run();
    });
  }
}).then(() => {
  handleStop();
}).catch((err) => {
  logger.error('Error starting', err);
  process.exit(1);
})

process.on('uncaughtException', (err) => {
  logger.error('Uncaught exception', err);
  handleStop();
});
process.on('SIGINT', () => handleStop());
process.on('SIGTERM',() => handleStop());
