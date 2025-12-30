import type { R2Bucket } from "./bucket.ts";
import type { QueueConsumerSettings } from "./queue-consumer.ts";
import type { Queue } from "./queue.ts";
import { isBucket } from "./bucket.ts";

/**
 * Base interface for event sources that can be bound to a Worker
 */
export type EventSource = QueueEventSource | Queue;

export type BucketEventType = "object-create" | "object-delete";

export interface BucketEventFilter {
  prefix?: string;
  suffix?: string;
}

export interface BucketEventSource {
  readonly bucket: R2Bucket;
  readonly queue?: Queue | string;
  readonly eventTypes?: BucketEventType[];
  readonly filter?: BucketEventFilter;
  readonly settings?: QueueConsumerSettings;
  readonly description?: string;
}

export type WorkerEventSource = EventSource | BucketEventSource | R2Bucket;

/**
 * Configuration for a Queue as an event source for a Worker
 */
export interface QueueEventSource {
  /**
   * The queue to consume messages from
   */
  readonly queue: Queue;

  /**
   * Optional settings for configuring how the Worker consumes the queue
   */
  readonly settings?: QueueConsumerSettings;
}

/**
 * Checks if an event source is a QueueEventSource
 * @param eventSource - The event source to check
 * @returns true if the event source is a QueueEventSource, false otherwise
 */
export function isQueueEventSource(
  eventSource: any,
): eventSource is QueueEventSource {
  return "queue" in eventSource;
}

export function isBucketEventSource(
  eventSource: any,
): eventSource is BucketEventSource {
  if (!eventSource) return false;
  if (isBucket(eventSource)) return true;
  return "bucket" in eventSource && isBucket(eventSource.bucket);
}
