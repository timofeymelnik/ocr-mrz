"use client";

import { useRouter } from "next/navigation";
import { AlertCircle, Loader2 } from "lucide-react";
import { useEffect } from "react";

import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { API_BASE } from "@/features/workspace/constants";
import { useCrmClientCard } from "@/features/workspace/use-crm-client-card";
import { WorkspaceHeader } from "@/features/workspace/workspace-header";

type CrmClientEditRedirectPageProps = {
  params: {
    clientId: string;
  };
};

export default function CrmClientEditRedirectPage({
  params,
}: CrmClientEditRedirectPageProps) {
  const router = useRouter();
  const clientId = decodeURIComponent(params.clientId || "");
  const { card, loading, error } = useCrmClientCard(clientId);
  const documentId = (card?.primary_document_id || "").trim();

  useEffect(() => {
    if (!documentId) {
      return;
    }
    void (async () => {
      let step: "review" | "merge" = "review";
      try {
        const mergeResp = await fetch(
          `${API_BASE}/api/crm/clients/${encodeURIComponent(clientId)}/profile/merge-candidates`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ force: false }),
          },
        );
        if (mergeResp.ok) {
          const mergeData = (await mergeResp.json()) as {
            merge_candidates?: Array<{ document_id?: string }>;
          };
          if ((mergeData.merge_candidates || []).length > 0) {
            step = "merge";
          }
        }
      } catch {
        // Keep review as safe fallback.
      }
      router.replace(
        `/workspace/${step}?documentId=${encodeURIComponent(
          documentId,
        )}&step=${step}&crmClientId=${encodeURIComponent(clientId)}`,
      );
    })();
  }, [clientId, documentId, router]);

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
                  Открываем форму редактирования из загрузки...
                </p>
              </>
            ) : documentId ? (
              <p className="text-sm text-muted-foreground">Переход к форме...</p>
            ) : (
              <>
                <AlertCircle className="h-5 w-5 text-red-600" />
                <p className="text-sm text-red-700">
                  {error || "У клиента нет primary_document_id для открытия формы."}
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
