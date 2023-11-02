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
    await this.channel.prefetch(this.maxItemsBeingHandled);

    this.channel.consume(this.queueName, async (msg) => {
      console.log(`Handling ${itemsBeingHandled} items...`);
      try {
        itemsBeingHandled++;
        if (msg) {
          await this.handleTask(JSON.parse(msg.content.toString()) as TaskPayload);
          this.channel.ack(msg);
        } else {
          console.log('No messages to handle...');
          await wait(1000);
        }
      } catch (err) {
        this.emit('error', { err, msg });
        if (msg) {
          this.channel.nack(msg, undefined, false);
        }
      } finally {
        itemsBeingHandled--;
      }
    });
  }

  async stop() {
    await this.channel.close();
  }
}
