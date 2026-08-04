import { config } from 'dotenv';
import amqp from 'amqplib';
import { v4 } from 'uuid';

import { createLogger } from './src/utils';
import { DriveDatabase } from './src/drive';
import { taskTypes, tasks } from './src/tasks';
import { ProcessType } from './src/tasks/process';

config();

const taskType = process.env.TASK_TYPE as undefined | string;

if (!taskType || !taskTypes.includes(taskType)) {
  console.error(`Invalid or missing task type. Expected ${
    taskTypes.map(t => `'${t}'`).join(', ')
  } but got '${taskType}'`);
  process.exit(1);
}

const processType = process.env.PROCESS_TYPE as undefined | ProcessType;

if (!processType || !Object.values(ProcessType).includes(processType)) {
  console.error(`Invalid or missing process type. Expected ${
    Object.values(ProcessType).map(t => `'${t}'`).join(', ')
  } but got '${processType}'`);
  process.exit(1);
}

const processId = v4();
const logger = createLogger(processId);

const amqpServer = process.env.QUEUE_SERVER;

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

async function start(): Promise<void> {
  db = new DriveDatabase();

  logger.log('(drive-db) connecting ...');
  await db.connect();
  logger.log('(drive-db) connected !');

  logger.log('(rabbit) connecting ...');
  connection = await amqp.connect(amqpServer as string);
  logger.log('(rabbit) connected !');

  await tasks[taskType as string](processType as 'consumer' | 'producer', { db }, connection);
}

start().catch((err) => {
  logger.error('Error starting', err);
  process.exit(1);
});

process.on('uncaughtException', (err) => {
  logger.error('Uncaught exception', err);
  handleStop();
});
process.on('SIGINT', () => handleStop());
process.on('SIGTERM',() => handleStop());
