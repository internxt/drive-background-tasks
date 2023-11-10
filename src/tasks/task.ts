import amqp from 'amqplib';

import { DriveDatabase } from '../drive';

export type TaskFunction = (
  processType: 'producer' | 'consumer',
  drive: {
    db: DriveDatabase
  },
  connection: amqp.Connection,
) => Promise<void>;
