import { v4 } from 'uuid';

import { DeletedFoldersIterator } from './deleted-folders.iterator';
import { Producer } from '../../producer';
import { Consumer } from '../../consumer';
import { createLogger } from '../../utils';
import { TaskFunction } from '../task';

const task: TaskFunction = async(
  processType, 
  drive,
  connection,
) => {
  const processId = v4();
  const logger = createLogger(processId);

  const queueName = process.env.MARK_DELETED_ITEMS_QUEUE_NAME;
  const maxEnqueuedItems = process.env.TASK_MARK_DELETED_ITEMS_PRODUCER_MAX_ENQUEUED_ITEMS;
  const maxConcurrentItems = process.env.TASK_MARK_DELETED_ITEMS_CONSUMER_MAX_CONCURRENT_ITEMS;

  if (processType === 'producer') {
    const deletedFoldersIterator = new DeletedFoldersIterator(drive.db);

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
    connection.createChannel().then((channel) => {
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
  
          await drive.db.markChildrenFilesAsDeleted(taskPayload.folder_id);
          await drive.db.markChildrenFoldersAsDeleted(taskPayload.folder_id);
          await drive.db.markDeletedFolderAsProcessed([taskPayload.folder_id]);
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
