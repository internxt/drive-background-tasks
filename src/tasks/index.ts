import { TaskFunction } from './task';

import fileDeletion from './file-deletion';
import folderDeletion from './folder-deletion';
import fileVersionDeletion from './file-version-deletion';

export const tasks: Record<string, TaskFunction> = {
  'delete-files': fileDeletion,
  'delete-folders': folderDeletion,
  'delete-file-versions': fileVersionDeletion,
};

export const taskTypes = Object.keys(tasks);
