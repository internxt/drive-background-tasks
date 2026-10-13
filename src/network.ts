import axios, { AxiosRequestConfig } from 'axios';
import { sign } from 'jsonwebtoken';

export type DeleteFilesResponse = {
  message: {
    confirmed: string[],
    notConfirmed: string[]
  }
}

export function signToken(duration: string, secret: string): string {
  return sign(
    {},
    Buffer.from(secret, 'base64').toString('utf8'),
    {
      algorithm: 'RS256',
      expiresIn: duration
    }
  );
}

export function deleteFiles(endpoint: string, fileIds: string[]): Promise<DeleteFilesResponse> {
  const params: AxiosRequestConfig = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${signToken(
        '5m',
        process.env.NETWORK_GATEWAY_DELETE_FILES_SECRET as string
      )}`
    },
    data: {
      files: fileIds
    }
  };

  return axios.delete<DeleteFilesResponse>(endpoint, params)
    .then((res) => res.data);
}
