import { DriveDatabase } from "../../drive";

export class DeletedFileVersionsIterator {
  constructor(private readonly db: DriveDatabase) {}

  async * [Symbol.asyncIterator]() {
    let rows : {
      fileVersionId: string,
      fileId: string,
      networkFileId: string;
      size: bigint,
      processed: boolean,
      enqueued: boolean,
      createdAt: Date,
      updatedAt: Date,
      processedAt: Date,
    }[] = [];
    let n = 50;

    do {
      const rows = await this.db.getDeletedFileVersions();
      if (rows.length === 0) {
        console.log('No file versions to process, waiting 1s...');
        await new Promise(resolve => setTimeout(resolve, 1000));
      } else {
        await this.db.setFileVersionsAsEnqueued(rows.map(row => row.fileVersionId));
        while (rows.length >= n) {
          const chunk = rows.splice(0, n);
          yield chunk;
        }

        if (rows.length > 0) {
          yield rows;
        }
      }
    } while (true);
  }
}
