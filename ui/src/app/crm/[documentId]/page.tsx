"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import {
  AlertCircle,
  ArrowLeft,
  ExternalLink,
  FileSearch,
  Loader2,
  RefreshCw,
  Trash2,
} from "lucide-react";
import { useMemo } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { UploadCrmPanel } from "@/features/workspace/upload-crm-panel";
import { WorkspaceHeader } from "@/features/workspace/workspace-header";
import { useCrmClientDocuments } from "@/features/workspace/use-crm-client-documents";
import { useCrmDocument } from "@/features/workspace/use-crm-document";
import { useCrmDocuments } from "@/features/workspace/use-crm-documents";
import { API_BASE } from "@/features/workspace/constants";
import { toUrl } from "@/features/workspace/utils";

type CrmUserPageProps = {
  params: {
    documentId: string;
  };
};

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-start justify-between gap-3 border-b py-2 last:border-b-0">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="max-w-[70%] text-right text-sm font-medium">{value || "—"}</div>
    </div>
  );
}

export default function CrmUserPage({ params }: CrmUserPageProps) {
  const router = useRouter();
  const documentId = decodeURIComponent(params.documentId || "");

  const {
    deletingDocumentId,
    error: listError,
    loadingSavedDocs,
    savedDocs,
    savedDocsFilter,
    setSavedDocsFilter,
    loadSavedDocuments,
    deleteSavedDocument,
  } = useCrmDocuments({ includeDuplicates: true });

  const { error: detailError, loading, record, reload } =
    useCrmDocument(documentId);

  const ident = record?.payload?.identificacion;
  const domicilio = record?.payload?.domicilio;
  const extra = record?.payload?.extra;
  const previewUrl = toUrl(record?.preview_url || "", API_BASE);
  const isPdf = previewUrl.toLowerCase().includes(".pdf");

  const headerName =
    ident?.nombre_apellidos ||
    savedDocs.find((doc) => doc.document_id === documentId)?.name ||
    "Клиент";

  const workspaceBadge = loading || loadingSavedDocs ? "В работе" : "Готов";
  const activeClientId = (record?.client_id || "").trim();
  const {
    clientDocs,
    clientDocsError,
    loadingClientDocs,
  } = useCrmClientDocuments(activeClientId);
  const normalizedIdentity = useMemo(() => {
    const values = [
      ident?.nif_nie || "",
      ident?.pasaporte || "",
      record?.payload?.identificacion?.nombre_apellidos || "",
    ];
    return values
      .map((value) =>
        String(value)
          .toUpperCase()
          .replace(/[^A-ZА-ЯЁ0-9]+/g, " ")
          .trim(),
      )
      .filter(Boolean);
  }, [ident?.nif_nie, ident?.pasaporte, record?.payload?.identificacion?.nombre_apellidos]);

  const relatedDocs = useMemo(() => {
    if (activeClientId) {
      return clientDocs;
    }
    if (normalizedIdentity.length === 0) return [];
    return savedDocs.filter((doc) => {
      const hay = `${doc.name || ""} ${doc.document_number || ""}`
        .toUpperCase()
        .replace(/[^A-ZА-ЯЁ0-9]+/g, " ")
        .trim();
      return normalizedIdentity.some((needle) => hay.includes(needle));
    });
  }, [activeClientId, clientDocs, normalizedIdentity, savedDocs]);

  return (
    <main className="min-h-screen bg-gradient-to-b from-background to-muted/30">
      <WorkspaceHeader activeTab="crm" workspaceBadge={workspaceBadge} />

      <div className="mx-auto max-w-[1600px] p-4 lg:p-6">
        <div className="mb-4 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <Button asChild variant="outline" size="sm">
              <Link href="/crm">
                <ArrowLeft className="mr-1.5 h-4 w-4" />
                Назад в CRM
              </Link>
            </Button>
            <h1 className="text-xl font-semibold">{headerName}</h1>
            <Badge variant="outline">{documentId}</Badge>
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => void reload()}
              disabled={loading}
            >
              {loading ? (
                <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />
              ) : (
                <RefreshCw className="mr-1.5 h-4 w-4" />
              )}
              Обновить
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="border-red-200 text-red-700 hover:bg-red-50 hover:text-red-800"
              onClick={async () => {
                const ok = await deleteSavedDocument(documentId);
                if (ok) router.push("/crm");
              }}
              disabled={deletingDocumentId === documentId}
            >
              {deletingDocumentId === documentId ? (
                <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />
              ) : (
                <Trash2 className="mr-1.5 h-4 w-4" />
              )}
              Удалить
            </Button>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[420px_1fr]">
          <UploadCrmPanel
            savedDocs={savedDocs}
            savedDocsFilter={savedDocsFilter}
            loadingSavedDocs={loadingSavedDocs}
            deletingDocumentId={deletingDocumentId}
            saving={loading}
            onFilterChange={setSavedDocsFilter}
            onRefresh={() => {
              void loadSavedDocuments(savedDocsFilter);
            }}
            onOpenDocument={(id) => {
              router.push(`/crm/${encodeURIComponent(id)}`);
            }}
            onDeleteDocument={(id) => {
              void deleteSavedDocument(id);
            }}
            activeDocumentId={documentId}
          />

          <div className="space-y-4">
            {listError ? (
              <Card className="border-red-300">
                <CardContent className="flex items-center gap-2 p-4 text-sm text-red-700">
                  <AlertCircle className="h-4 w-4" />
                  <span>{listError}</span>
                </CardContent>
              </Card>
            ) : null}

            {clientDocsError ? (
              <Card className="border-red-300">
                <CardContent className="flex items-center gap-2 p-4 text-sm text-red-700">
                  <AlertCircle className="h-4 w-4" />
                  <span>{clientDocsError}</span>
                </CardContent>
              </Card>
            ) : null}

            {detailError ? (
              <Card className="border-red-300">
                <CardContent className="flex items-center gap-2 p-4 text-sm text-red-700">
                  <AlertCircle className="h-4 w-4" />
                  <span>{detailError}</span>
                </CardContent>
              </Card>
            ) : null}

            {loading && !record ? (
              <Card>
                <CardContent className="flex items-center gap-2 p-6 text-sm text-muted-foreground">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  <span>Загрузка карточки клиента...</span>
                </CardContent>
              </Card>
            ) : null}

            {record ? (
              <>
                <Card>
                  <CardHeader>
                    <CardTitle className="text-base">
                      Документы клиента
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    {loadingClientDocs && activeClientId ? (
                      <div className="mb-2 flex items-center gap-2 text-xs text-muted-foreground">
                        <Loader2 className="h-3.5 w-3.5 animate-spin" />
                        <span>Загружаем документы клиента...</span>
                      </div>
                    ) : null}
                    {relatedDocs.length > 0 ? (
                      <div className="flex flex-wrap gap-2">
                        {relatedDocs.map((doc, index) => {
                          const isActive = doc.document_id === documentId;
                          return (
                            <button
                              key={doc.document_id}
                              type="button"
                              onClick={() => {
                                router.push(
                                  `/crm/${encodeURIComponent(doc.document_id)}`,
                                );
                              }}
                              className={[
                                "rounded-lg border px-3 py-2 text-left transition",
                                isActive
                                  ? "border-primary bg-primary/10 text-primary"
                                  : "border-border bg-background hover:border-primary/40",
                              ].join(" ")}
                            >
                              <div className="text-[11px] uppercase tracking-[0.08em] text-muted-foreground">
                                Документ {index + 1}
                              </div>
                              <div className="text-sm font-semibold">
                                {doc.document_number || "без номера"}
                              </div>
                              <div className="text-xs text-muted-foreground">
                                {doc.updated_at || ""}
                              </div>
                            </button>
                          );
                        })}
                      </div>
                    ) : (
                      <div className="text-sm text-muted-foreground">
                        Для этого клиента найден только текущий документ.
                      </div>
                    )}
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2 text-base">
                      Профиль клиента
                      <Badge variant="secondary">
                        {record.workflow_stage || "review"}
                      </Badge>
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="grid gap-4 md:grid-cols-2">
                    <div>
                      <Row label="ФИО" value={ident?.nombre_apellidos || ""} />
                      <Row label="NIE" value={ident?.nif_nie || ""} />
                      <Row label="Паспорт" value={ident?.pasaporte || ""} />
                      <Row label="Email" value={extra?.email || ""} />
                      <Row label="Телефон" value={domicilio?.telefono || ""} />
                    </div>
                    <div>
                      <Row
                        label="Адрес"
                        value={`${domicilio?.tipo_via || ""} ${domicilio?.nombre_via || ""} ${domicilio?.numero || ""}`.trim()}
                      />
                      <Row label="Город" value={domicilio?.municipio || ""} />
                      <Row
                        label="Провинция"
                        value={domicilio?.provincia || ""}
                      />
                      <Row label="CP" value={domicilio?.cp || ""} />
                      <Row
                        label="Режим"
                        value={record.manual_steps_required?.join(", ") || "—"}
                      />
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle className="text-base">Pipeline и качество</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="flex flex-wrap gap-2">
                      {(record.missing_fields || []).length > 0 ? (
                        (record.missing_fields || []).map((field) => (
                          <Badge
                            key={field}
                            variant="outline"
                            className="border-amber-300 bg-amber-50 text-amber-700"
                          >
                            {field}
                          </Badge>
                        ))
                      ) : (
                        <Badge
                          variant="outline"
                          className="border-emerald-300 bg-emerald-50 text-emerald-700"
                        >
                          Нет пропущенных полей
                        </Badge>
                      )}
                    </div>
                    <div className="text-sm text-muted-foreground">
                      Workflow: {record.workflow_stage || "review"} →{" "}
                      {record.workflow_next_step || "prepare"}
                    </div>
                    <div className="flex gap-2">
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => {
                          router.push(
                            `/?documentId=${encodeURIComponent(documentId)}&step=review`,
                          );
                        }}
                      >
                        Редактировать данные
                      </Button>
                      <Button asChild size="sm" variant="outline">
                        <a
                          href={record.target_url || record.form_url || "#"}
                          target="_blank"
                          rel="noreferrer"
                        >
                          Target URL
                          <ExternalLink className="ml-1.5 h-3.5 w-3.5" />
                        </a>
                      </Button>
                    </div>
                  </CardContent>
                </Card>

                <Card className="h-[520px]">
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2 text-base">
                      <FileSearch className="h-4 w-4" />
                      Документ
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="h-[calc(100%-72px)]">
                    {previewUrl ? (
                      isPdf ? (
                        <iframe
                          src={previewUrl}
                          title="CRM document preview"
                          className="h-full w-full rounded-md border"
                        />
                      ) : (
                        <img
                          src={previewUrl}
                          alt="CRM preview"
                          className="h-full w-full rounded-md border object-contain"
                        />
                      )
                    ) : (
                      <div className="rounded-md border p-4 text-sm text-muted-foreground">
                        Превью документа недоступно.
                      </div>
                    )}
                  </CardContent>
                </Card>
              </>
            ) : null}
          </div>
        </div>
      </div>
    </main>
  );
}
