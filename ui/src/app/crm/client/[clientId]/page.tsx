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
import type { MergeCandidate } from "@/lib/types";

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
              onClick={() => {
                void reload();
                if (resolvedActiveDocumentId) {
                  void reloadDocument();
                }
              }}
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
            onRefresh={() => {
              void loadClients(clientsFilter);
            }}
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
            {listError || cardError || detailError || deleteError ? (
              <Card className="border-red-300">
                <CardContent className="flex items-center gap-2 p-4 text-sm text-red-700">
                  <AlertCircle className="h-4 w-4" />
                  <span>{listError || cardError || detailError || deleteError}</span>
                </CardContent>
              </Card>
            ) : null}

            {card ? (
              <>
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2 text-base">
                      Профиль клиента
                      <Badge variant="secondary">client</Badge>
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="grid gap-4 md:grid-cols-2">
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
                      <Row
                        label="Документов"
                        value={String(card.documents_count || documents.length || 0)}
                      />
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle className="text-base">Документы клиента</CardTitle>
                  </CardHeader>
                  <CardContent>
                    {documents.length > 0 ? (
                      <div className="flex flex-wrap gap-2">
                        {documents.map((doc, index) => {
                          const isActive = doc.document_id === resolvedActiveDocumentId;
                          return (
                            <button
                              key={doc.document_id}
                              type="button"
                              onClick={() => setActiveDocumentId(doc.document_id)}
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
                        Документы клиента не найдены.
                      </div>
                    )}
                    {resolvedActiveDocumentId ? (
                      <div className="mt-3">
                        <Button
                          size="sm"
                          variant="outline"
                          className="border-red-200 text-red-700 hover:bg-red-50 hover:text-red-800"
                          onClick={() => void deleteDocument(resolvedActiveDocumentId)}
                          disabled={Boolean(deletingDocumentId) || deletingClient}
                        >
                          {deletingDocumentId === resolvedActiveDocumentId ? (
                            <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />
                          ) : (
                            <Trash2 className="mr-1.5 h-4 w-4" />
                          )}
                          Удалить документ
                        </Button>
                      </div>
                    ) : null}
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle className="text-base">Pipeline и качество</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="flex flex-wrap gap-2">
                      {(card.missing_fields || []).length > 0 ? (
                        (card.missing_fields || []).map((field) => (
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
                    <div className="flex gap-2">
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
                        disabled={!card.primary_document_id && !resolvedActiveDocumentId}
                      >
                        Редактировать данные
                      </Button>
                      <Button asChild size="sm" variant="outline">
                        <a
                          href={record?.target_url || record?.form_url || "#"}
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
                    {mergeError ? (
                      <div className="text-xs text-red-700">{mergeError}</div>
                    ) : null}
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

                <Card className="h-[520px]">
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2 text-base">
                      <FileSearch className="h-4 w-4" />
                      Документ
                      {loadingDocument ? (
                        <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                      ) : null}
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
