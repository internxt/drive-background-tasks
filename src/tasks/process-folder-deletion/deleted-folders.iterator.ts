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
      rows = await this.db.getChildrenFoldersOfDeletedFolders();
      if (rows.length === 0) break;
      await this.db.setFoldersAsEnqueued(rows.map((row) => row.folder_id));
      for (const row of rows) yield row;

    } while (rows.length > 0);
  }
}
