"use client";

import { useRouter } from "next/navigation";
import { AlertCircle } from "lucide-react";

import { Card, CardContent } from "@/components/ui/card";
import { UploadCrmPanel } from "@/features/workspace/upload-crm-panel";
import { useCrmClients } from "@/features/workspace/use-crm-clients";
import { WorkspaceHeader } from "@/features/workspace/workspace-header";

export default function CrmPage() {
  const router = useRouter();
  const {
    error,
    loadingClients,
    clients,
    clientsFilter,
    setClientsFilter,
    loadClients,
  } = useCrmClients();

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
            savedDocs={clients}
            savedDocsFilter={clientsFilter}
            loadingSavedDocs={loadingClients}
            saving={false}
            onFilterChange={setClientsFilter}
            onRefresh={() => {
              void loadClients(clientsFilter);
            }}
            onOpenDocument={(documentId, clientId) => {
              const clientKey = (clientId || "").trim();
              if (clientKey) {
                router.push(`/crm/client/${encodeURIComponent(clientKey)}`);
                return;
              }
              router.push(`/crm/${encodeURIComponent(documentId)}`);
            }}
          />
        </div>
      </div>
    </main>
  );
}
