import type { Context } from "../context.ts";
import { Resource } from "../resource.ts";
import { CloudflareApiError, handleApiError } from "./api-error.ts";
import {
  createCloudflareApi,
  type CloudflareApi,
  type CloudflareApiOptions,
} from "./api.ts";
import type { R2Bucket } from "./bucket.ts";
import type { BucketEventFilter, BucketEventType } from "./event-source.ts";
import type { Queue } from "./queue.ts";
import { isBucket } from "./bucket.ts";
import { isQueue } from "./queue.ts";

export const DEFAULT_BUCKET_EVENT_TYPES: BucketEventType[] = [
  "object-create",
  "object-delete",
];

export interface BucketEventNotificationProps extends CloudflareApiOptions {
  bucket: R2Bucket | string;
  queue: Queue | string;
  eventTypes?: BucketEventType[];
  filter?: BucketEventFilter;
  description?: string;
  enabled?: boolean;
  delete?: boolean;
  adopt?: boolean;
  dev?: boolean;
}

export interface BucketEventNotification {
  id: string;
  bucket: string;
  queue: string;
  eventTypes: BucketEventType[];
  filter?: BucketEventFilter;
  description?: string;
}

export const BucketEventNotification = Resource(
  "cloudflare::BucketEventNotification",
  async function (
    this: Context<BucketEventNotification>,
    id: string,
    props: BucketEventNotificationProps,
  ): Promise<BucketEventNotification> {
    const bucketName = isBucket(props.bucket)
      ? props.bucket.name
      : props.bucket;
    const queueId = isQueue(props.queue) ? props.queue.id : props.queue;
    const queueName = isQueue(props.queue) ? props.queue.name : props.queue;

    if (this.scope.local && props.dev) {
      return {
        id,
        bucket: bucketName,
        queue: queueName,
        filter: props.filter,
        eventTypes: props.eventTypes ?? DEFAULT_BUCKET_EVENT_TYPES,
        description: props.description,
      };
    }

    const api = await createCloudflareApi(props);

    if (this.phase === "delete") {
      if (props.delete !== false) {
        await deleteBucketEventNotification(api, bucketName, queueId);
      }
      return this.destroy();
    }

    const eventTypes = props.eventTypes ?? DEFAULT_BUCKET_EVENT_TYPES;
    await upsertBucketEventNotification(api, {
      bucketName,
      queueId,
      filter: props.filter,
      eventTypes,
      enabled: props.enabled ?? true,
      description: props.description,
    });

    return {
      id,
      bucket: bucketName,
      queue: queueName,
      filter: props.filter,
      eventTypes,
      description: props.description,
    };
  },
);

async function upsertBucketEventNotification(
  api: CloudflareApi,
  props: {
    bucketName: string;
    queueId: string;
    filter?: BucketEventFilter;
    eventTypes: BucketEventType[];
    enabled: boolean;
    description?: string;
  },
) {
  const response = await api.put(
    `/accounts/${api.accountId}/event_notifications/r2/${props.bucketName}/configuration/queues/${props.queueId}`,
    {
      rules: [
        {
          description: props.description,
          enabled: props.enabled,
          filters: props.filter,
          event_types: props.eventTypes,
        },
      ],
    },
  );

  if (!response.ok) {
    await handleApiError(
      response,
      "upserting",
      "BucketEventNotification",
      props.bucketName,
    );
  }
}

async function deleteBucketEventNotification(
  api: CloudflareApi,
  bucketName: string,
  queueId: string,
) {
  const response = await api.delete(
    `/accounts/${api.accountId}/event_notifications/r2/${bucketName}/configuration/queues/${queueId}`,
  );

  if (!response.ok && response.status !== 404) {
    throw new CloudflareApiError(
      `Failed deleting bucket event notifications for bucket '${bucketName}' and queue '${queueId}'`,
      response,
    );
  }
}
