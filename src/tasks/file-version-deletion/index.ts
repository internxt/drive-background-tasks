import { v4 } from 'uuid';

import { createLogger } from '../../utils';
import { Consumer } from '../../consumer';
import { Producer } from '../../producer';
import { DeletedFileVersionsIterator } from './deleted-file-versions.iterator';
import { TaskFunction } from '../task';
import { deleteFiles } from '../../network';

const task: TaskFunction = async (
  processType,
  drive,
  connection,
) => {
  const processId = v4();
  const logger = createLogger(processId);

  const queueName = `${process.env.TASK_TYPE}-${process.env.NODE_ENV}`;
  const maxEnqueuedItems = process.env.TASK_DELETE_FILE_VERSIONS_PRODUCER_MAX_ENQUEUED_ITEMS;
  const maxConcurrentItems = process.env.TASK_DELETE_FILE_VERSIONS_CONSUMER_MAX_CONCURRENT_ITEMS;

  if (!maxEnqueuedItems) {
    logger.log('Missing env var: TASK_DELETE_FILE_VERSIONS_PRODUCER_MAX_ENQUEUED_ITEMS');
    process.exit(1);
  }

  if (!maxConcurrentItems) {
    logger.log('Missing env var: TASK_DELETE_FILE_VERSIONS_CONSUMER_MAX_CONCURRENT_ITEMS');
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
    const deletedFileVersionsIterator = new DeletedFileVersionsIterator(drive.db);

    return connection.createChannel().then((channel) => {
      const producer = new Producer(
        channel,
        queueName as string,
        deletedFileVersionsIterator,
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
          fileVersionId: string,
          fileId: string,
          networkFileId: string,
          size: bigint,
          processed: boolean,
          enqueued: boolean,
          createdAt: Date,
          updatedAt: Date,
          processedAt: Date,
        }[]
      }>(
        channel,
        queueName as string,
        async (task) => {
          logger.log(`received item: + ${JSON.stringify(task)}`, 'consumer');

          const networkFileIdsToDelete = task.payload.map((version) => version.networkFileId);

          const res = await deleteFiles(process.env.NETWORK_GATEWAY_DELETE_FILES_ENDPOINT as string, networkFileIdsToDelete);
          const versionIdsDeletedSuccessfully = res.message.confirmed;
          const versionsToMarkAsProcessed = task.payload.filter((version) => versionIdsDeletedSuccessfully.includes(version.networkFileId));

          await drive.db.markDeletedFileVersionsAsProcessed(versionsToMarkAsProcessed.map(v => v.fileVersionId));
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
