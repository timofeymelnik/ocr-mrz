"use client";

import { useRouter } from "next/navigation";
import { AlertCircle } from "lucide-react";

import { Card, CardContent } from "@/components/ui/card";
import { UploadCrmPanel } from "@/features/workspace/upload-crm-panel";
import { WorkspaceHeader } from "@/features/workspace/workspace-header";
import { useCrmDocuments } from "@/features/workspace/use-crm-documents";

export default function CrmPage() {
  const router = useRouter();
  const {
    deletingDocumentId,
    error,
    loadingSavedDocs,
    savedDocs,
    savedDocsFilter,
    setSavedDocsFilter,
    loadSavedDocuments,
    deleteSavedDocument,
  } = useCrmDocuments();

  return (
    <main className="min-h-screen bg-gradient-to-b from-background to-muted/30">
      <WorkspaceHeader activeTab="crm" workspaceBadge="Готов" />

      <div className="mx-auto max-w-[1600px] p-4 lg:p-6">
        <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold">CRM</h1>
            <p className="text-sm text-muted-foreground">
              Список клиентов и быстрый переход к их документам.
            </p>
          </div>
        </div>

        {error ? (
          <Card className="mb-4 border-red-300">
            <CardContent className="flex items-center gap-2 p-4 text-sm text-red-700">
              <AlertCircle className="h-4 w-4" />
              <span>{error}</span>
            </CardContent>
          </Card>
        ) : null}

        <div className="max-w-[460px]">
          <UploadCrmPanel
            savedDocs={savedDocs}
            savedDocsFilter={savedDocsFilter}
            loadingSavedDocs={loadingSavedDocs}
            deletingDocumentId={deletingDocumentId}
            saving={false}
            onFilterChange={setSavedDocsFilter}
            onRefresh={() => {
              void loadSavedDocuments(savedDocsFilter);
            }}
            onOpenDocument={(documentId) => {
              router.push(`/crm/${encodeURIComponent(documentId)}`);
            }}
            onDeleteDocument={(documentId) => {
              void deleteSavedDocument(documentId);
            }}
          />
        </div>
      </div>
    </main>
  );
}
