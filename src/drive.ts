import { Client } from 'pg';

export class NotFoundError extends Error {
  entityMetadata:  { name: string, id: number | string };

  constructor(entityMetadata: { name: string, id: number | string }) {
    super('Resource not found');

    this.entityMetadata = entityMetadata;

    Object.setPrototypeOf(this, NotFoundError.prototype);
  }

  getExtendedInfo(): string {
    return JSON.stringify({ message: this.message, ...this.entityMetadata }, null, 2);
  }
}

export class DriveDatabase {
  private client: Client;

  URI: string;

  constructor() {
    const user = process.env.DRIVE_DB_USER as string;
    const host = process.env.DRIVE_DB_HOST as string;
    const name = process.env.DRIVE_DB_NAME as string;
    const pass = process.env.DRIVE_DB_PASS as string;
    const port = parseInt(process.env.DRIVE_DB_PORT as string);

    this.URI = '';
    this.client = new Client({
      user,
      host,
      database: name,
      password: pass,
      port,
      ssl: {
        rejectUnauthorized: false,
      }
    });
  }

  async connect(): Promise<void> {
    await this.client.connect();
  }

  async disconnect(): Promise<void> {
    await this.client.end();
  }

  async getChildrenFoldersOfDeletedFolders(): Promise<{ 
    folder_id: string,
    processed: boolean,
    created_at: Date,
    updated_at: Date,
    processed_at: Date,
  }[]> {
    const query = 'SELECT * FROM deleted_folders WHERE processed = false AND enqueued = false LIMIT 100';

    const result = await this.client.query(query);

    return result.rows;
  }

  async setFoldersAsEnqueued(folderIds: string[]): Promise<void> {
    const query = `
      UPDATE deleted_folders 
      SET enqueued = true, enqueued_at = NOW(), updated_at = NOW()
      WHERE folder_id IN (${folderIds.map((folderId) => `'${folderId}'`).join(', ')})
    `;

    await this.client.query(query);
  }

  async markDeletedFolderAsProcessed(uuids: string[]): Promise<void> {
    const query = `
      UPDATE deleted_folders
      SET processed = true, processed_at = NOW(), updated_at = NOW()
      WHERE folder_id IN (${uuids.map((uuid) => `'${uuid}'`).join(', ')})
    `;

    await this.client.query(query);
  }

  /**
   * Marks children files as deleted
   * @param folderId 
   */
  async markChildrenFilesAsDeleted(folderId: string): Promise<void> {
    let count = 0;
    do {
      const query = `
        UPDATE files
        SET updated_at = NOW(), status = 'DELETED'
        WHERE id IN (
          SELECT id 
          FROM files 
          WHERE folder_id = (
            SELECT id 
            FROM folders 
            WHERE uuid = '${folderId}'
          )
          AND status != 'DELETED'
          LIMIT 1000
        ) 
        RETURNING *;
      `;

      const result = await this.client.query(query);

      count = result.rowCount;
    } while (count === 1000);
  }

  /**
   * Marks children folders as deleted
   * @param folderId 
   */
  async markChildrenFoldersAsDeleted(folderId: string): Promise<void> {
    let count = 0;
    do {
      const query = `
        UPDATE folders
        SET updated_at = NOW(), removed = true, removed_at = NOW()
        WHERE id IN (
          SELECT id 
          FROM folders 
          WHERE parent_id = (
            SELECT id 
            FROM folders 
            WHERE uuid = '${folderId}'
          )
          AND removed = false
          LIMIT 1000
        )
        RETURNING *;
      `;

      const result = await this.client.query(query);

      count = result.rowCount;
    } while (count === 1000);
  }
}