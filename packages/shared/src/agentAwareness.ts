import type {
  EnvironmentId,
  OrchestrationV2ThreadShell,
  Project,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { backgroundWorkHoldsCompletion } from "./orchestrationV2PendingBackgroundWork.ts";

export type AgentAwarenessPhase =
  | "starting"
  | "running"
  | "waiting_for_approval"
  | "waiting_for_input"
  | "completed"
  | "failed"
  | "stale";

export interface AgentAwarenessState {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly projectTitle: string;
  readonly threadTitle: string;
  readonly phase: AgentAwarenessPhase;
  readonly headline: string;
  readonly detail?: string;
  readonly modelTitle: string;
  readonly updatedAt: string;
  readonly deepLink: string;
}

function buildAgentAwarenessDeepLink(input: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}): string {
  return `/threads/${encodeURIComponent(input.environmentId)}/${encodeURIComponent(input.threadId)}`;
}

export interface ProjectThreadAwarenessV2Input {
  readonly environmentId: EnvironmentId;
  readonly project: Pick<Project, "title">;
  readonly thread: Pick<
    OrchestrationV2ThreadShell,
    | "activityRunStatus"
    | "id"
    | "latestRunRequestedAt"
    | "latestUserAuthoredMessageAt"
    | "lineage"
    | "modelSelection"
    | "pendingBackgroundTasks"
    | "pendingRuntimeRequest"
    | "settledOverride"
    | "status"
    | "title"
    | "updatedAt"
  >;
}

export function projectThreadAwarenessV2(
  input: ProjectThreadAwarenessV2Input,
): AgentAwarenessState | null {
  const { environmentId, project, thread } = input;
  if (thread.lineage.relationshipToParent === "subagent") return null;
  const phase = resolveThreadAwarenessPhaseV2(thread);
  if (phase === null) {
    return null;
  }
  // Resolve the phase first so live or waiting work stays visible after settlement.
  if (
    thread.settledOverride === "settled" &&
    (phase === "completed" || phase === "failed") &&
    !backgroundWorkHoldsCompletion(thread.pendingBackgroundTasks ?? [])
  ) {
    return null;
  }
  const detail =
    phase === "completed"
      ? "Review the completed task."
      : phase === "failed"
        ? "The agent run failed."
        : undefined;
  return {
    environmentId,
    threadId: thread.id,
    projectTitle: project.title,
    threadTitle: thread.title,
    phase,
    headline: headlineForPhase(phase),
    ...(detail === undefined ? {} : { detail }),
    modelTitle: thread.modelSelection.model,
    updatedAt: DateTime.formatIso(thread.updatedAt),
    deepLink: buildAgentAwarenessDeepLink({ environmentId, threadId: thread.id }),
  };
}

function resolveThreadAwarenessPhaseV2(
  thread: ProjectThreadAwarenessV2Input["thread"],
): AgentAwarenessPhase | null {
  const runStatus = thread.activityRunStatus ?? thread.status;
  if (
    thread.pendingRuntimeRequest?.kind === "user_input" &&
    (thread.pendingRuntimeRequest.blocking !== false ||
      !(runStatus === "preparing" || runStatus === "starting" || runStatus === "running"))
  ) {
    return "waiting_for_input";
  }
  if (
    thread.pendingRuntimeRequest !== null &&
    thread.pendingRuntimeRequest.kind !== "user_input" &&
    thread.pendingRuntimeRequest.kind !== "auth_refresh"
  ) {
    return "waiting_for_approval";
  }
  switch (runStatus) {
    case "preparing":
    case "starting":
      return "starting";
    case "running":
    case "waiting":
      return "running";
    case "completed":
      if (backgroundWorkHoldsCompletion(thread.pendingBackgroundTasks ?? [])) return "running";
      // A turn the user didn't start (another agent's message, a cron, a background job finishing) ends
      // quietly: the thread leaves the activity list instead of sending a Done notification.
      return startedByUser(thread) ? "completed" : null;
    case "failed":
      return "failed";
    case "idle":
    case "queued":
    case "interrupted":
    case "cancelled":
    case "rolled_back":
      return null;
  }
}

// A run and the message that requests it share one timestamp, so a user message at or after the latest
// run's request either started that run or steered it.
function startedByUser(thread: ProjectThreadAwarenessV2Input["thread"]): boolean {
  if (thread.latestRunRequestedAt == null || thread.latestUserAuthoredMessageAt === undefined) {
    return true;
  }
  return (
    thread.latestUserAuthoredMessageAt !== null &&
    DateTime.toEpochMillis(thread.latestUserAuthoredMessageAt) >=
      DateTime.toEpochMillis(thread.latestRunRequestedAt)
  );
}

function headlineForPhase(phase: AgentAwarenessPhase): string {
  switch (phase) {
    case "starting":
      return "Starting agent";
    case "running":
      return "Agent is working";
    case "waiting_for_approval":
      return "Approval needed";
    case "waiting_for_input":
      return "Waiting for input";
    case "completed":
      return "Agent finished";
    case "failed":
      return "Agent failed";
    case "stale":
      return "Update delayed";
  }
}
