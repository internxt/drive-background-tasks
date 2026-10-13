import { DriveDatabase } from "../../drive";

export class DeletedFilesIterator {
  constructor(private readonly db: DriveDatabase) {}

  async * [Symbol.asyncIterator]() {
    let rows : { 
      file_id: string,
      network_file_id: string;
      processed: boolean,
      created_at: Date,
      updated_at: Date,
      processed_at: Date,
    }[] = [];
    let n = 10;

    do {
      const rows = await this.db.getDeletedFiles();
      if (rows.length === 0) {
        // Wait for a short period before checking for new data.
        console.log('No data to process, waiting 1s...');
        await new Promise(resolve => setTimeout(resolve, 1000));
      } else {
        await this.db.setFilesAsEnqueued(rows.map(row => row.fileId));
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
