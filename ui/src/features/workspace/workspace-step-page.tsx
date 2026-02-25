"use client";

import { Suspense } from "react";

import { type Step } from "@/features/workspace/constants";
import { WorkspaceFlowPage } from "@/features/workspace/workspace-flow-page";

type WorkspaceStepPageProps = {
  step: Step;
};

export function WorkspaceStepPage({ step }: WorkspaceStepPageProps) {
  return (
    <Suspense
      fallback={
        <main className="min-h-screen bg-gradient-to-b from-background to-muted/30" />
      }
    >
      <WorkspaceFlowPage routeStep={step} />
    </Suspense>
  );
}
