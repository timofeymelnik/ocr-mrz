import { type Step } from "@/features/workspace/constants";

type UseWorkspaceStepGuardParams = {
  hasDocument: boolean;
  hasPayload: boolean;
  hasClientMatch: boolean;
  hasMergeSources: boolean;
};

type WorkspaceStepGuard = {
  canAccessStep: (nextStep: Step) => boolean;
  fallbackStep: (nextStep: Step) => Step;
};

export function useWorkspaceStepGuard({
  hasDocument,
  hasPayload,
  hasClientMatch,
  hasMergeSources,
}: UseWorkspaceStepGuardParams): WorkspaceStepGuard {
  const canAccessStep = (nextStep: Step): boolean => {
    if (nextStep === "upload") return true;
    if (!hasDocument) return false;
    if (nextStep === "match") return hasClientMatch;
    if (nextStep === "merge") return hasMergeSources;
    return hasPayload;
  };

  const fallbackStep = (nextStep: Step): Step => {
    if (nextStep === "merge" && !hasMergeSources) {
      return canAccessStep("review") ? "review" : "upload";
    }
    if (nextStep === "match" && !hasClientMatch) {
      return canAccessStep("review") ? "review" : "upload";
    }
    if (!canAccessStep(nextStep)) {
      return "upload";
    }
    return nextStep;
  };

  return { canAccessStep, fallbackStep };
}
