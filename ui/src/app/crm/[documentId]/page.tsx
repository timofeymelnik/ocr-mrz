"use client";

import { useRouter } from "next/navigation";
import { AlertCircle, Loader2 } from "lucide-react";
import { useEffect } from "react";

import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { useCrmDocument } from "@/features/workspace/use-crm-document";
import { WorkspaceHeader } from "@/features/workspace/workspace-header";

type CrmLegacyDocumentPageProps = {
  params: {
    documentId: string;
  };
};

export default function CrmLegacyDocumentPage({ params }: CrmLegacyDocumentPageProps) {
  const router = useRouter();
  const documentId = decodeURIComponent(params.documentId || "");
  const { loading, error, record } = useCrmDocument(documentId);
  const clientId = (record?.client_id || "").trim();

  useEffect(() => {
    if (!clientId) {
      return;
    }
    router.replace(`/crm/client/${encodeURIComponent(clientId)}`);
  }, [clientId, router]);

  return (
    <main className="min-h-screen bg-gradient-to-b from-background to-muted/30">
      <WorkspaceHeader activeTab="crm" workspaceBadge="В работе" />
      <div className="mx-auto max-w-[900px] p-4 lg:p-6">
        <Card>
          <CardContent className="flex min-h-[220px] flex-col items-center justify-center gap-3 p-6 text-center">
            {loading ? (
              <>
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
                <p className="text-sm text-muted-foreground">
                  Переходим к карточке клиента...
                </p>
              </>
            ) : clientId ? (
              <p className="text-sm text-muted-foreground">
                Редирект на клиентскую карточку...
              </p>
            ) : (
              <>
                <AlertCircle className="h-5 w-5 text-red-600" />
                <p className="text-sm text-red-700">
                  {error || "Для документа не найден client_id."}
                </p>
                <Button variant="outline" onClick={() => router.push("/crm")}>
                  Назад в CRM
                </Button>
              </>
            )}
          </CardContent>
        </Card>
      </div>
    </main>
  );
}

