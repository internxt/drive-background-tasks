import { Channel } from 'amqplib';
import { EventEmitter } from 'stream';
import { wait } from './utils';

export class Consumer<TaskPayload> extends EventEmitter {
  constructor(
    private readonly channel: Channel, 
    private readonly queueName: string,
    private readonly handleTask: (taskPayload: TaskPayload) => Promise<void>,
    private readonly maxItemsBeingHandled: number = 10,
  ) {
    super();
  }

  async run() {
    let itemsBeingHandled = 0;

    await this.channel.assertQueue(this.queueName);

    while (true) {
      console.log(`Handling ${itemsBeingHandled} items...`);
      if (itemsBeingHandled >= this.maxItemsBeingHandled) {
        await wait(1000);
        continue;
      }

      const msg = await this.channel.get(this.queueName);

      if (!msg) {
        console.log('No messages to handle...');
        await wait(1000);
        continue;
      }
      itemsBeingHandled++;
      this.handleTask(JSON.parse(msg.content.toString()) as TaskPayload).then(() => {
        this.channel.ack(msg);
      }).catch((err) => {
        this.emit('error', { err, msg });
        this.channel.nack(msg, undefined, false);
      }).finally(() => {
        itemsBeingHandled--;
      });
    }
  }

  async stop() {
    await this.channel.close();
  }
}
