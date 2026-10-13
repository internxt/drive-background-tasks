import { Channel } from 'amqplib';
import EventEmitter from 'events';
import { wait } from './utils';

export class Producer<T> extends EventEmitter {
  constructor(
    private readonly channel: Channel,
    private readonly queueName: string,
    private readonly iterator: AsyncIterable<T>,
    private readonly maxQueueSize: number = 1000, 
  ) {
    super();
  }

  async run() {
    await this.channel.assertQueue(this.queueName);

    for await (const item of this.iterator) {
      this.emit('enqueue', item);

      this.channel.sendToQueue(
        this.queueName, 
        Buffer.from(
          (item as any[]).length ? JSON.stringify({ payload: item }) : JSON.stringify(item),
        ),
      );

      let drained = false;

      while (!drained) {
        const queueInfo = await this.channel.checkQueue(this.queueName);
        drained = queueInfo.messageCount < this.maxQueueSize;

        if (!drained) {
          this.emit('queue-full');
          await wait(1000);
        }
      }    
    }
  }
}
