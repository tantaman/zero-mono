import {fileURLToPath} from 'node:url';
import {FsObjectStore} from './fs-object-store.ts';
import type {ObjectStore} from './object-store.ts';

const LEADING_SLASH = /^\//;

export type ObjectStoreOptions = {
  /** The S3-compatible endpoint, for non-AWS services. */
  endpoint?: string | undefined;
  /** The AWS region of the bucket. */
  region?: string | undefined;
};

/**
 * Creates the {@link ObjectStore} addressed by an archive URL:
 * `s3://bucket/prefix` in production, `file:///path` for local development
 * and integration tests.
 */
export async function createObjectStore(
  url: string,
  options: ObjectStoreOptions = {},
): Promise<ObjectStore> {
  const parsed = new URL(url);
  switch (parsed.protocol) {
    case 'file:':
      return new FsObjectStore(fileURLToPath(parsed));
    case 's3:': {
      // Loaded lazily so that deployments in litestream mode (the default)
      // never pay for the AWS SDK — which is also not a declared dependency
      // of the published zero package; a deployment that opts into an s3://
      // archive installs it.
      let s3;
      try {
        s3 = await Promise.all([
          import('@aws-sdk/client-s3'),
          import('./s3-object-store.ts'),
        ]);
      } catch (e) {
        throw new Error(
          `an s3:// archive URL requires the @aws-sdk/client-s3 package, ` +
            `which is not installed`,
          {cause: e},
        );
      }
      const [{S3Client}, {S3ObjectStore}] = s3;
      const {endpoint, region} = options;
      const client = new S3Client({
        ...(endpoint ? {endpoint, forcePathStyle: true} : {}),
        ...(region ? {region} : {}),
      });
      const bucket = parsed.hostname;
      const prefix = parsed.pathname.replace(LEADING_SLASH, '');
      return new S3ObjectStore(client, bucket, prefix);
    }
    default:
      throw new Error(
        `unsupported archive URL "${url}": only s3:// and file:// are supported`,
      );
  }
}
