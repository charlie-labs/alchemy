import type { Resource } from "../resource.ts";
import { ResourceFQN, ResourceID, ResourceKind } from "../resource.ts";
import { logger } from "../util/logger.ts";

export type DestroyFn = (
  resource: Resource,
  options?: unknown,
) => Promise<void>;

const CF_QUEUE_DELETE_REFERENCED_WORKER_RE =
  /referenced by binding in Worker\s+['"]?([^'"]+?)['"]?(?:\b|$)/i;

export async function tryHandleCloudflareOrphanPruneError(args: {
  resource: Resource;
  error: unknown;
  remaining: Resource[];
  destroy: DestroyFn;
  options: unknown;
}): Promise<{ handled: boolean; skipKeys: string[] }> {
  if (args.resource?.[ResourceKind] !== "cloudflare::Queue") {
    return { handled: false, skipKeys: [] };
  }

  const referencedWorkerName = getReferencedWorkerNameFromCloudflareError(
    args.error,
  );
  if (!referencedWorkerName) {
    return { handled: false, skipKeys: [] };
  }

  const skipKeys: string[] = [];

  const worker = args.remaining.find(
    (r) =>
      r?.[ResourceKind] === "cloudflare::Worker" &&
      getResourceName(r) === referencedWorkerName,
  );

  if (worker) {
    try {
      await args.destroy(worker, args.options);
      const key = getResourceKey(worker);
      if (key) {
        skipKeys.push(key);
      }
    } catch (workerError) {
      logger.warn(
        `Failed to delete orphaned Cloudflare Worker "${getResourceName(worker) ?? worker?.[ResourceFQN] ?? worker?.[ResourceID]}" while attempting to prune orphaned Queue "${args.resource?.[ResourceFQN] ?? args.resource?.[ResourceID]}". Skipping orphan cleanup for this Worker.`,
      );
      logger.warn(workerError);
    }
  }

  try {
    await args.destroy(args.resource, args.options);
  } catch (retryError) {
    logCloudflareQueueDeleteReferencedWorkerWarning(
      args.resource,
      referencedWorkerName,
      retryError,
    );
  }

  return { handled: true, skipKeys };
}

function getReferencedWorkerNameFromCloudflareError(
  error: unknown,
): string | undefined {
  if (!(error instanceof Error)) {
    return undefined;
  }

  const status = (error as { status?: unknown }).status;
  if (typeof status === "number" && (status < 400 || status >= 500)) {
    return undefined;
  }

  const match = error.message.match(CF_QUEUE_DELETE_REFERENCED_WORKER_RE);
  return match?.[1];
}

function getResourceName(resource: unknown): string | undefined {
  if (
    resource &&
    typeof resource === "object" &&
    Object.prototype.hasOwnProperty.call(resource, "name")
  ) {
    const { name } = resource as { name?: unknown };
    if (typeof name === "string") {
      return name;
    }
  }
  return undefined;
}

function getResourceKey(resource: Resource): string | undefined {
  const fqn = resource?.[ResourceFQN];
  if (typeof fqn === "string") {
    return fqn;
  }
  const id = resource?.[ResourceID];
  if (typeof id === "string") {
    return id;
  }
  return undefined;
}

function logCloudflareQueueDeleteReferencedWorkerWarning(
  resource: Resource,
  referencedWorkerName: string,
  error: unknown,
) {
  logger.warn(
    `Failed to delete orphaned Cloudflare Queue "${resource?.[ResourceFQN] ?? resource?.[ResourceID]}" because it is referenced by Worker "${referencedWorkerName}". Skipping orphan cleanup for this Queue.`,
  );
  logger.warn(error);
}
