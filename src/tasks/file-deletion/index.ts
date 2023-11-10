import { v4 } from 'uuid';
import axios, { AxiosRequestConfig } from 'axios';
import { sign } from 'jsonwebtoken'

import { createLogger } from '../../utils';
import { Consumer } from '../../consumer';
import { Producer } from '../../producer';
import { DeletedFilesIterator } from './deleted-files.iterator';
import { TaskFunction } from '../task';

const task: TaskFunction = async (
  processType,
  drive,
  connection,
) => {
  const processId = v4();
  const logger = createLogger(processId);

  type DeleteFilesResponse = {
    message: {
      confirmed: string[],
      notConfirmed: string[]
    }
  }

  function signToken(duration: string, secret: string) {
    return sign(
      {},
      Buffer.from(secret, 'base64').toString('utf8'),
      {
        algorithm: 'RS256',
        expiresIn: duration
      }
    );
  }

  function deleteFiles(endpoint: string, fileIds: string[]): Promise<DeleteFilesResponse> {
    const params: AxiosRequestConfig = {
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${signToken(
          '5m', 
          process.env.NETWORK_GATEWAY_DELETE_FILES_SECRET as string
        )}`
      },
      data: {
        files: fileIds
      }
    };

    return axios.delete<DeleteFilesResponse>(endpoint, params)
      .then((res) => res.data);
  }

  const queueName = `${process.env.TASK_TYPE}-${process.env.NODE_ENV}`;
  const maxEnqueuedItems = process.env.TASK_DELETE_FILES_PRODUCER_MAX_ENQUEUED_ITEMS;
  const maxConcurrentItems = process.env.TASK_DELETE_FILES_CONSUMER_MAX_CONCURRENT_ITEMS;

  if (!maxEnqueuedItems) {
    logger.log('Missing env var: TASK_MARK_DELETED_ITEMS_PRODUCER_MAX_ENQUEUED_ITEMS');
    process.exit(1);
  }

  if (!maxConcurrentItems) {
    logger.log('Missing env var: TASK_MARK_DELETED_ITEMS_CONSUMER_MAX_CONCURRENT_ITEMS');
    process.exit(1);
  }

  logger.log(`params: process_type -> ${processType}, env -> ${
    JSON.stringify({ 
      maxConcurrentItems, 
      maxEnqueuedItems, 
      queueName 
    })
  }`);


  if (processType === 'producer') {
    const deletedFilesIterator = new DeletedFilesIterator(drive.db);

    return connection.createChannel().then((channel) => {
      const producer = new Producer(
        channel, 
        queueName as string, 
        deletedFilesIterator,
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
    connection.createChannel().then((channel) => {
      const consumer = new Consumer<{ 
        payload: {
          fileId: string,
          processed: boolean,
          createdAt: Date,
          updatedAt: Date,
          processedAt: Date,
          networkFileId: string,
        }[]
      }>(
        channel, 
        queueName as string,
        async (task) => {
          logger.log(`received item: + ${JSON.stringify(task)}`, 'consumer');

          const networkFileIdsToDelete = task.payload.map((file) => file.networkFileId);
          const res = await deleteFiles(process.env.NETWORK_GATEWAY_DELETE_FILES_ENDPOINT as string, networkFileIdsToDelete);
          const fileIdsDeletedSuccesfully = res.message.confirmed;
          const filesToMarkAsDeleted = task.payload.filter((file) => fileIdsDeletedSuccesfully.includes(file.networkFileId));

          await drive.db.markDeletedFilesAsProcessed(filesToMarkAsDeleted.map(f => f.fileId));
        },
        maxConcurrentItems ? parseInt(maxConcurrentItems as string) : undefined,
      );

      consumer.on('error', ({ err, msg }) => {
        logger.error(`error processing item: ${JSON.stringify(msg.content)}`, err, 'consumer');
      });
      
      consumer.run();
    });
  }
}

export default task;
