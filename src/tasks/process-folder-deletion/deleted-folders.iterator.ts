import { DriveDatabase } from "../../drive";

export class DeletedFoldersIterator {
  constructor(private readonly db: DriveDatabase) {}

  async * [Symbol.asyncIterator]() {
    let rows : { 
      folder_id: string,
      processed: boolean,
      created_at: Date,
      updated_at: Date,
      processed_at: Date,
    }[] = [];

    do {
      const rows = await this.db.getChildrenFoldersOfDeletedFolders();
      if (rows.length === 0) {
        // Wait for a short period before checking for new data.
        console.log('No data to process, waiting 1s...');
        await new Promise(resolve => setTimeout(resolve, 1000));
      } else {
        await this.db.setFoldersAsEnqueued(rows.map(row => row.folder_id));
        for (const row of rows) {
          yield row;
        }
      }
    } while (true);
  }
}
