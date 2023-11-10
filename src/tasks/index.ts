import { TaskFunction } from './task';

import fileDeletion from './file-deletion';
import folderDeletion from './folder-deletion';

export const tasks: Record<string, TaskFunction> = {
  'delete-files': fileDeletion,
  'delete-folders': folderDeletion,
};

export const taskTypes = Object.keys(tasks);
