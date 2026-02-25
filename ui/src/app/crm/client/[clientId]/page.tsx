"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import {
  AlertCircle,
  ArrowLeft,
  FileSearch,
  Loader2,
  Trash2,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { UploadCrmPanel } from "@/features/workspace/upload-crm-panel";
import { API_BASE } from "@/features/workspace/constants";
import { useCrmClientCard } from "@/features/workspace/use-crm-client-card";
import { useCrmClients } from "@/features/workspace/use-crm-clients";
import { useCrmDocument } from "@/features/workspace/use-crm-document";
import { WorkspaceHeader } from "@/features/workspace/workspace-header";
import { readErrorResponse, toUrl } from "@/features/workspace/utils";
import type { MergeCandidate, UploadResponse } from "@/lib/types";

type CrmClientPageProps = {
  params: {
    clientId: string;
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

export default function CrmClientPage({ params }: CrmClientPageProps) {
  const router = useRouter();
  const clientId = decodeURIComponent(params.clientId || "");
  const [deletingDocumentId, setDeletingDocumentId] = useState("");
  const [deletingClient, setDeletingClient] = useState(false);
  const [deleteError, setDeleteError] = useState("");
  const [activeDocumentId, setActiveDocumentId] = useState("");
  const [mergeCandidates, setMergeCandidates] = useState<MergeCandidate[]>([]);
  const [selectedMergeSourceId, setSelectedMergeSourceId] = useState("");
  const [mergePreview, setMergePreview] = useState<
    Array<{
      field: string;
      current_value: string;
      suggested_value: string;
      source?: string;
    }>
  >([]);
  const [mergeLoading, setMergeLoading] = useState(false);
  const [mergeError, setMergeError] = useState("");
  const [generatedDocs, setGeneratedDocs] = useState<
    Array<{
      documentId: string;
      documentNumber: string;
      updatedAt: string;
      filledPdfUrl: string;
    }>
  >([]);
  const [loadingGeneratedDocs, setLoadingGeneratedDocs] = useState(false);
  const [activeTab, setActiveTab] = useState<
    "data" | "uploaded" | "generated"
  >(
    "data",
  );
  const [activeGeneratedDocumentId, setActiveGeneratedDocumentId] =
    useState("");

  const {
    error: listError,
    loadingClients,
    clients,
    clientsFilter,
    setClientsFilter,
    loadClients,
  } = useCrmClients();
  const { error: cardError, loading, card, reload } = useCrmClientCard(clientId);

  const documents = card?.documents || [];
  const resolvedActiveDocumentId = useMemo(() => {
    if (activeDocumentId && documents.some((row) => row.document_id === activeDocumentId)) {
      return activeDocumentId;
    }
    return card?.primary_document_id || documents[0]?.document_id || "";
  }, [activeDocumentId, documents, card?.primary_document_id]);
  const {
    error: detailError,
    loading: loadingDocument,
    record,
    reload: reloadDocument,
  } = useCrmDocument(resolvedActiveDocumentId);

  const profile = card?.profile_payload;
  const profileIdent = profile?.identificacion;
  const profileDomicilio = profile?.domicilio;
  const profileExtra = profile?.extra;
  const previewUrl = toUrl(record?.preview_url || "", API_BASE);
  const isPdf = previewUrl.toLowerCase().includes(".pdf");
  const resolvedActiveGenerated = useMemo(() => {
    if (
      activeGeneratedDocumentId &&
      generatedDocs.some((row) => row.documentId === activeGeneratedDocumentId)
    ) {
      return generatedDocs.find(
        (row) => row.documentId === activeGeneratedDocumentId,
      )!;
    }
    return generatedDocs[0] || null;
  }, [activeGeneratedDocumentId, generatedDocs]);
  const generatedPreviewUrl = resolvedActiveGenerated?.filledPdfUrl || "";
  const generatedIsPdf = generatedPreviewUrl.toLowerCase().includes(".pdf");

  useEffect(() => {
    const load = async (): Promise<void> => {
      if (!clientId) return;
      setMergeLoading(true);
      setMergeError("");
      try {
        const resp = await fetch(
          `${API_BASE}/api/crm/clients/${encodeURIComponent(clientId)}/profile/merge-candidates`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ force: false }),
          },
        );
        if (!resp.ok) {
          throw new Error(await readErrorResponse(resp));
        }
        const data = (await resp.json()) as {
          merge_candidates?: MergeCandidate[];
        };
        const next = data.merge_candidates || [];
        setMergeCandidates(next);
        if (next.length > 0) {
          setSelectedMergeSourceId(next[0].document_id || "");
        } else {
          setSelectedMergeSourceId("");
        }
      } catch (e) {
        setMergeError(
          e instanceof Error ? e.message : "Failed loading merge candidates",
        );
      } finally {
        setMergeLoading(false);
      }
    };
    void load();
  }, [clientId]);

  useEffect(() => {
    const ids = documents.map((row) => row.document_id).filter(Boolean);
    if (ids.length === 0) {
      setGeneratedDocs([]);
      setLoadingGeneratedDocs(false);
      return;
    }
    let canceled = false;
    void (async () => {
      setLoadingGeneratedDocs(true);
      try {
        const rows = await Promise.all(
          ids.map(async (id) => {
            try {
              const resp = await fetch(
                `${API_BASE}/api/crm/documents/${encodeURIComponent(id)}`,
              );
              if (!resp.ok) return null;
              const data = (await resp.json()) as UploadResponse;
              const filledPdfUrl = toUrl(
                data.autofill_preview?.filled_pdf_url || "",
                API_BASE,
              );
              if (!filledPdfUrl) return null;
              const meta = documents.find((doc) => doc.document_id === id);
              return {
                documentId: id,
                documentNumber: meta?.document_number || "без номера",
                updatedAt: meta?.updated_at || "",
                filledPdfUrl,
              };
            } catch {
              return null;
            }
          }),
        );
        if (canceled) return;
        setGeneratedDocs(
          rows.filter(
            (
              item,
            ): item is {
              documentId: string;
              documentNumber: string;
              updatedAt: string;
              filledPdfUrl: string;
            } => Boolean(item),
          ),
        );
      } finally {
        if (!canceled) {
          setLoadingGeneratedDocs(false);
        }
      }
    })();
    return () => {
      canceled = true;
    };
  }, [documents]);

  async function previewMerge(): Promise<void> {
    if (!selectedMergeSourceId) {
      setMergeError("Выберите источник для merge.");
      return;
    }
    setMergeLoading(true);
    setMergeError("");
    try {
      const resp = await fetch(
        `${API_BASE}/api/crm/clients/${encodeURIComponent(clientId)}/profile/enrich-by-identity`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            apply: false,
            source_document_id: selectedMergeSourceId,
            selected_fields: [],
          }),
        },
      );
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as {
        enrichment_preview?: Array<{
          field: string;
          current_value: string;
          suggested_value: string;
          source?: string;
        }>;
      };
      setMergePreview(data.enrichment_preview || []);
    } catch (e) {
      setMergeError(e instanceof Error ? e.message : "Failed previewing merge");
    } finally {
      setMergeLoading(false);
    }
  }

  async function applyMerge(): Promise<void> {
    if (!selectedMergeSourceId) {
      setMergeError("Выберите источник для merge.");
      return;
    }
    setMergeLoading(true);
    setMergeError("");
    try {
      const selectedFields = mergePreview.map((row) => row.field);
      const resp = await fetch(
        `${API_BASE}/api/crm/clients/${encodeURIComponent(clientId)}/profile/enrich-by-identity`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            apply: true,
            source_document_id: selectedMergeSourceId,
            selected_fields: selectedFields,
          }),
        },
      );
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      setMergePreview([]);
      await reload();
    } catch (e) {
      setMergeError(e instanceof Error ? e.message : "Failed applying merge");
    } finally {
      setMergeLoading(false);
    }
  }

  async function deleteClient(): Promise<void> {
    if (!clientId) return;
    const approved = window.confirm(
      "Удалить клиента и все связанные документы? Это действие нельзя отменить.",
    );
    if (!approved) return;
    setDeletingClient(true);
    setDeleteError("");
    try {
      const resp = await fetch(`${API_BASE}/api/crm/clients/${encodeURIComponent(clientId)}`, {
        method: "DELETE",
      });
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      router.push("/crm");
    } catch (e) {
      setDeleteError(e instanceof Error ? e.message : "Failed deleting CRM client");
    } finally {
      setDeletingClient(false);
    }
  }

  async function deleteDocument(documentId: string): Promise<void> {
    const approved = window.confirm("Удалить этот документ из клиента?");
    if (!approved) return;
    setDeletingDocumentId(documentId);
    setDeleteError("");
    try {
      const resp = await fetch(`${API_BASE}/api/crm/documents/${documentId}`, {
        method: "DELETE",
      });
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      if (resolvedActiveDocumentId === documentId) {
        setActiveDocumentId("");
      }
      await reload();
    } catch (e) {
      setDeleteError(
        e instanceof Error ? e.message : "Failed deleting CRM document",
      );
    } finally {
      setDeletingDocumentId("");
    }
  }

  return (
    <main className="min-h-screen bg-gradient-to-b from-background to-muted/30">
      <WorkspaceHeader
        activeTab="crm"
        workspaceBadge={loading || loadingClients ? "В работе" : "Готов"}
      />
      <div className="mx-auto max-w-[1600px] p-4 lg:p-6">
        <div className="mb-4 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <Button asChild variant="outline" size="sm">
              <Link href="/crm">
                <ArrowLeft className="mr-1.5 h-4 w-4" />
                Назад в CRM
              </Link>
            </Button>
            <h1 className="text-xl font-semibold">
              {card?.display_name || profileIdent?.nombre_apellidos || "Клиент"}
            </h1>
            <Badge variant="outline">{clientId}</Badge>
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              className="border-red-200 text-red-700 hover:bg-red-50 hover:text-red-800"
              onClick={() => void deleteClient()}
              disabled={deletingClient}
            >
              {deletingClient ? (
                <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />
              ) : (
                <Trash2 className="mr-1.5 h-4 w-4" />
              )}
              Удалить клиента
            </Button>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[420px_1fr]">
          <UploadCrmPanel
            savedDocs={clients}
            savedDocsFilter={clientsFilter}
            loadingSavedDocs={loadingClients}
            deletingDocumentId={deletingDocumentId}
            saving={loading || deletingClient}
            onFilterChange={setClientsFilter}
            onOpenDocument={(id, clientRowId) => {
              const target = (clientRowId || "").trim();
              if (target) {
                router.push(`/crm/client/${encodeURIComponent(target)}`);
              } else {
                router.push(`/crm/${encodeURIComponent(id)}`);
              }
            }}
            activeDocumentId={card?.primary_document_id || ""}
          />

          <div className="space-y-4">
            {listError || cardError || detailError || deleteError || mergeError ? (
              <Card className="border-red-300">
                <CardContent className="flex items-center gap-2 p-4 text-sm text-red-700">
                  <AlertCircle className="h-4 w-4" />
                  <span>
                    {listError || cardError || detailError || deleteError || mergeError}
                  </span>
                </CardContent>
              </Card>
            ) : null}

            {card ? (
              <>
                <Card>
                  <CardContent className="p-2">
                    <div className="grid grid-cols-3 gap-2">
                      <Button
                        variant={activeTab === "data" ? "default" : "outline"}
                        onClick={() => setActiveTab("data")}
                      >
                        Данные
                      </Button>
                      <Button
                        variant={activeTab === "uploaded" ? "default" : "outline"}
                        onClick={() => setActiveTab("uploaded")}
                      >
                        Загруженные
                      </Button>
                      <Button
                        variant={activeTab === "generated" ? "default" : "outline"}
                        onClick={() => setActiveTab("generated")}
                      >
                        Сгенерированные
                      </Button>
                    </div>
                  </CardContent>
                </Card>

                {activeTab === "data" ? (
                  <>
                    <Card>
                      <CardHeader className="space-y-3">
                        <CardTitle className="flex items-center justify-between gap-2 text-base">
                          <span className="flex items-center gap-2">
                            Профиль клиента
                            <Badge variant="secondary">client</Badge>
                          </span>
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() =>
                              router.push(
                                `/workspace/review?documentId=${encodeURIComponent(
                                  card.primary_document_id || resolvedActiveDocumentId,
                                )}&step=review&crmClientId=${encodeURIComponent(clientId)}`,
                              )
                            }
                            disabled={
                              !card.primary_document_id && !resolvedActiveDocumentId
                            }
                          >
                            Редактировать данные
                          </Button>
                        </CardTitle>
                        <div className="flex flex-wrap gap-2">
                          <Badge variant="outline">
                            Документов: {String(card.documents_count || documents.length || 0)}
                          </Badge>
                          {(card.missing_fields || []).length > 0 ? (
                            <Badge
                              variant="outline"
                              className="border-amber-300 bg-amber-50 text-amber-700"
                            >
                              Пропущенных полей: {(card.missing_fields || []).length}
                            </Badge>
                          ) : (
                            <Badge
                              variant="outline"
                              className="border-emerald-300 bg-emerald-50 text-emerald-700"
                            >
                              Все обязательные поля заполнены
                            </Badge>
                          )}
                        </div>
                      </CardHeader>
                      <CardContent className="space-y-4">
                        {(card.missing_fields || []).length > 0 ? (
                          <div className="flex flex-wrap gap-2">
                            {(card.missing_fields || []).map((field) => (
                              <Badge
                                key={field}
                                variant="outline"
                                className="border-amber-300 bg-amber-50 text-amber-700"
                              >
                                {field}
                              </Badge>
                            ))}
                          </div>
                        ) : null}
                        <div className="grid gap-4 md:grid-cols-2">
                          <div>
                            <Row label="ФИО" value={profileIdent?.nombre_apellidos || ""} />
                            <Row label="NIE" value={profileIdent?.nif_nie || ""} />
                            <Row label="Паспорт" value={profileIdent?.pasaporte || ""} />
                            <Row label="Email" value={profileExtra?.email || ""} />
                            <Row label="Телефон" value={profileDomicilio?.telefono || ""} />
                          </div>
                          <div>
                            <Row
                              label="Адрес"
                              value={`${profileDomicilio?.tipo_via || ""} ${profileDomicilio?.nombre_via || ""} ${profileDomicilio?.numero || ""}`.trim()}
                            />
                            <Row label="Город" value={profileDomicilio?.municipio || ""} />
                            <Row label="Провинция" value={profileDomicilio?.provincia || ""} />
                            <Row label="CP" value={profileDomicilio?.cp || ""} />
                          </div>
                        </div>
                      </CardContent>
                    </Card>

                    {mergeCandidates.length > 0 ? (
                      <Card>
                        <CardHeader>
                          <CardTitle className="text-base">Merge профиля клиента</CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-3">
                          <div className="grid gap-2 md:grid-cols-[1fr_auto_auto]">
                            <select
                              className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                              value={selectedMergeSourceId}
                              onChange={(event) =>
                                setSelectedMergeSourceId(event.target.value)
                              }
                            >
                              <option value="">-- выбрать источник --</option>
                              {mergeCandidates.map((candidate) => (
                                <option
                                  key={candidate.document_id}
                                  value={candidate.document_id}
                                >
                                  {candidate.document_number || candidate.name || "без номера"} · score{" "}
                                  {candidate.score}
                                </option>
                              ))}
                            </select>
                            <Button
                              variant="outline"
                              onClick={() => void previewMerge()}
                              disabled={mergeLoading || !selectedMergeSourceId}
                            >
                              {mergeLoading ? (
                                <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />
                              ) : null}
                              Превью merge
                            </Button>
                            <Button
                              onClick={() => void applyMerge()}
                              disabled={
                                mergeLoading ||
                                !selectedMergeSourceId ||
                                mergePreview.length === 0
                              }
                            >
                              Применить merge
                            </Button>
                          </div>
                          {mergePreview.length > 0 ? (
                            <div className="rounded-lg border">
                              {mergePreview.slice(0, 12).map((row) => (
                                <div
                                  key={row.field}
                                  className="grid grid-cols-[220px_1fr_1fr] gap-2 border-b px-3 py-2 text-xs last:border-b-0"
                                >
                                  <div className="font-mono text-muted-foreground">
                                    {row.field}
                                  </div>
                                  <div>{row.current_value || "—"}</div>
                                  <div className="font-medium text-primary">
                                    {row.suggested_value}
                                  </div>
                                </div>
                              ))}
                            </div>
                          ) : null}
                        </CardContent>
                      </Card>
                    ) : null}
                  </>
                ) : null}

                {activeTab === "uploaded" ? (
                  <Card className="h-[680px]">
                    <CardHeader>
                      <CardTitle className="flex items-center gap-2 text-base">
                        <FileSearch className="h-4 w-4" />
                        Загруженные документы
                        {loadingDocument ? (
                          <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                        ) : null}
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="flex h-[calc(100%-72px)] flex-col gap-3">
                      {documents.length > 0 ? (
                        <div className="flex flex-wrap gap-2">
                          {documents.map((doc, index) => {
                            const isActive = doc.document_id === resolvedActiveDocumentId;
                            return (
                              <div
                                key={doc.document_id}
                                className={[
                                  "relative rounded-lg border bg-background transition",
                                  isActive
                                    ? "border-primary bg-primary/10"
                                    : "border-border hover:border-primary/40",
                                ].join(" ")}
                              >
                                <button
                                  type="button"
                                  onClick={() => {
                                    setActiveDocumentId(doc.document_id);
                                  }}
                                  className="w-full px-3 py-2 pr-10 text-left"
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
                                <button
                                  type="button"
                                  className="absolute right-2 top-2 rounded p-1 text-muted-foreground hover:bg-red-50 hover:text-red-700"
                                  onClick={() => {
                                    void deleteDocument(doc.document_id);
                                  }}
                                  disabled={Boolean(deletingDocumentId) || deletingClient}
                                  aria-label="Удалить документ"
                                >
                                  {deletingDocumentId === doc.document_id ? (
                                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                  ) : (
                                    <Trash2 className="h-3.5 w-3.5" />
                                  )}
                                </button>
                              </div>
                            );
                          })}
                        </div>
                      ) : null}
                      <div className="min-h-0 flex-1">
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
                          Выберите загруженный документ, чтобы увидеть превью.
                        </div>
                      )}
                      </div>
                    </CardContent>
                  </Card>
                ) : null}

                {activeTab === "generated" ? (
                  <>
                    <Card>
                      <CardHeader>
                        <CardTitle className="text-base">Сгенерированные документы</CardTitle>
                      </CardHeader>
                      <CardContent>
                        {loadingGeneratedDocs ? (
                          <div className="flex items-center gap-2 text-sm text-muted-foreground">
                            <Loader2 className="h-4 w-4 animate-spin" />
                            Загружаем заполненные PDF...
                          </div>
                        ) : generatedDocs.length > 0 ? (
                          <div className="flex flex-wrap gap-2">
                            {generatedDocs.map((item, index) => {
                              const isActive =
                                item.documentId ===
                                (resolvedActiveGenerated?.documentId || "");
                              return (
                                <div
                                  key={`${item.documentId}-${item.filledPdfUrl}`}
                                  className={[
                                    "relative rounded-lg border bg-background transition",
                                    isActive
                                      ? "border-primary bg-primary/10"
                                      : "border-border hover:border-primary/40",
                                  ].join(" ")}
                                >
                                  <button
                                    type="button"
                                    onClick={() => {
                                      setActiveGeneratedDocumentId(item.documentId);
                                    }}
                                    className="w-full px-3 py-2 pr-10 text-left"
                                  >
                                    <div className="text-[11px] uppercase tracking-[0.08em] text-muted-foreground">
                                      PDF {index + 1}
                                    </div>
                                    <div className="text-sm font-semibold">
                                      {item.documentNumber || "без номера"}
                                    </div>
                                    <div className="text-xs text-muted-foreground">
                                      {item.updatedAt || ""}
                                    </div>
                                  </button>
                                  <button
                                    type="button"
                                    className="absolute right-2 top-2 rounded p-1 text-muted-foreground hover:bg-red-50 hover:text-red-700"
                                    onClick={() => {
                                      void deleteDocument(item.documentId);
                                    }}
                                    disabled={Boolean(deletingDocumentId) || deletingClient}
                                    aria-label="Удалить документ"
                                  >
                                    {deletingDocumentId === item.documentId ? (
                                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                    ) : (
                                      <Trash2 className="h-3.5 w-3.5" />
                                    )}
                                  </button>
                                </div>
                              );
                            })}
                          </div>
                        ) : (
                          <div className="rounded-md border p-4 text-sm text-muted-foreground">
                            Для этого клиента пока нет сохранённых заполненных PDF.
                          </div>
                        )}
                      </CardContent>
                    </Card>

                    <Card className="h-[560px]">
                      <CardHeader>
                        <CardTitle className="text-base">Превью сгенерированного PDF</CardTitle>
                      </CardHeader>
                      <CardContent className="h-[calc(100%-72px)]">
                        {generatedPreviewUrl ? (
                          generatedIsPdf ? (
                            <iframe
                              src={generatedPreviewUrl}
                              title="Generated PDF preview"
                              className="h-full w-full rounded-md border"
                            />
                          ) : (
                            <img
                              src={generatedPreviewUrl}
                              alt="Generated preview"
                              className="h-full w-full rounded-md border object-contain"
                            />
                          )
                        ) : (
                          <div className="text-sm text-muted-foreground">
                            Выберите сгенерированный документ, чтобы увидеть превью.
                          </div>
                        )}
                      </CardContent>
                    </Card>
                  </>
                ) : null}
              </>
            ) : null}
          </div>
        </div>
      </div>
    </main>
  );
}
