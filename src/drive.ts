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

  async getDeletedFiles(): Promise<{
    fileId: string;
    processed: boolean,
    createdAt: Date,
    updatedAt: Date,
    processedAt: Date,
  }[]> {
    const query = 'SELECT * FROM deleted_files WHERE processed = false AND enqueued = false LIMIT 100';

    const result = await this.client.query(query);

    return result.rows.map(r => ({
      fileId: r.file_id,
      processed: r.processed,
      createdAt: r.created_at,
      updatedAt: r.updated_at,
      processedAt: r.processed_at,
      networkFileId: r.network_file_id,
    }));
  }

  async setFilesAsEnqueued(fileIds: string[]): Promise<void> {
    const query = `
      UPDATE deleted_files
      SET enqueued = true, enqueued_at = NOW(), updated_at = NOW()
      WHERE file_id IN (${fileIds.map((fileIds) => `'${fileIds}'`).join(', ')})
    `;

    await this.client.query(query);
  }

  async markDeletedFilesAsProcessed(uuids: string[]): Promise<void> {
    const query = `
      UPDATE deleted_files
      SET processed = true, processed_at = NOW(), updated_at = NOW()
      WHERE file_id IN (${uuids.map((uuid) => `'${uuid}'`).join(', ')})
    `;

    await this.client.query(query);
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

      /**
     * Gets network file IDs for existing file versions in batches
     * @param fileIds
     */
    async getFileVersionsByFileId(fileIds: string[]): Promise<
        {
            id: string;
            fileId: string;
            networkFileId: string;
        }[]
    > {
        const placeholders = fileIds.map((_, i) => `$${i + 1}`).join(", ");
        const query = `
            SELECT network_file_id, file_id, id
            FROM file_versions
            WHERE file_id IN (${placeholders})
            AND status = 'EXISTS'
        `;

        const result = await this.client.query(query, fileIds);

        return result.rows.map((r) => ({
            id: r.id,
            networkFileId: r.network_file_id,
            fileId: r.file_id,
        }));
    }


    /**
     * Mark file versions as deleted
     * @param versionIds
     */
    async markFileVersionsAsDeleted(versionIds: string[]): Promise<number> {
        const placeholders = versionIds.map((_, i) => `$${i + 1}`).join(", ");
        if (placeholders.length === 0) {
            return 0;
        }

        const query = `
            UPDATE file_versions
            SET status = 'DELETED', updated_at = NOW()
            WHERE id IN (${placeholders})
            AND status = 'EXISTS'
        `;
        const result = await this.client.query(query, versionIds);

        return result.rowCount;
    }

    /**
     * Gets deleted file versions pending processing
     * @returns Array of deleted file versions
     */
    async getDeletedFileVersions(): Promise<{
        fileVersionId: string;
        fileId: string;
        networkFileId: string;
        size: bigint;
        processed: boolean;
        enqueued: boolean;
        createdAt: Date;
        updatedAt: Date;
        processedAt: Date;
    }[]> {
        const query = `
            SELECT
                file_version_id,
                file_id,
                network_file_id,
                size,
                processed,
                enqueued,
                created_at,
                updated_at,
                processed_at
            FROM deleted_file_versions
            WHERE processed = false AND enqueued = false
            LIMIT 100
        `;

        const result = await this.client.query(query);

        return result.rows.map(r => ({
            fileVersionId: r.file_version_id,
            fileId: r.file_id,
            networkFileId: r.network_file_id,
            size: r.size,
            processed: r.processed,
            enqueued: r.enqueued,
            createdAt: r.created_at,
            updatedAt: r.updated_at,
            processedAt: r.processed_at,
        }));
    }

    /**
     * Mark file versions as enqueued for deletion
     * @param versionIds
     */
    async setFileVersionsAsEnqueued(versionIds: string[]): Promise<void> {
        const placeholders = versionIds.map((_, i) => `$${i + 1}`).join(", ");
        const query = `
            UPDATE deleted_file_versions
            SET enqueued = true, enqueued_at = NOW(), updated_at = NOW()
            WHERE file_version_id IN (${placeholders})
        `;
        await this.client.query(query, versionIds);
    }

    /**
     * Mark deleted file versions as processed after successful deletion from network
     * @param versionIds
     */
    async markDeletedFileVersionsAsProcessed(versionIds: string[]): Promise<void> {
        const placeholders = versionIds.map((_, i) => `$${i + 1}`).join(", ");
        const query = `
            UPDATE deleted_file_versions
            SET processed = true, processed_at = NOW(), updated_at = NOW()
            WHERE file_version_id IN (${placeholders})
        `;
        await this.client.query(query, versionIds);
    }
}