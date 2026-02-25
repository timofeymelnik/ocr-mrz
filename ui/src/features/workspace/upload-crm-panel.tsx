import { useMemo, useState } from "react";
import {
  ArrowRight,
  Clock3,
  Loader2,
  RefreshCw,
  Search,
  Trash2,
} from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import type { SavedCrmDocument } from "@/lib/types";

type UploadCrmPanelProps = {
  savedDocs: SavedCrmDocument[];
  savedDocsFilter: string;
  loadingSavedDocs: boolean;
  deletingDocumentId?: string;
  saving: boolean;
  onFilterChange: (value: string) => void;
  onRefresh?: () => void;
  onOpenDocument: (documentId: string, clientId?: string) => void;
  onDeleteDocument?: (documentId: string) => void;
  activeDocumentId?: string;
};

type VisualStatus = "all" | "review" | "ready" | "uploaded" | "other";

const statusTone: Record<string, string> = {
  review: "border-amber-200 bg-amber-50 text-amber-700",
  ready: "border-emerald-200 bg-emerald-50 text-emerald-700",
  uploaded: "border-blue-200 bg-blue-50 text-blue-700",
  other: "border-zinc-200 bg-zinc-100 text-zinc-700",
};

const statusLabel: Record<string, string> = {
  review: "Проверка",
  ready: "Готов",
  uploaded: "Загружен",
  other: "Новый",
};

function resolveVisualStatus(item: SavedCrmDocument): Exclude<VisualStatus, "all"> {
  const raw = (item.status || "").trim().toLowerCase();
  if (item.has_edited) return "review";
  if (raw.includes("confirm") || raw.includes("autofill_done")) return "ready";
  if (raw.includes("upload") || raw.includes("review") || raw.includes("match")) {
    return "uploaded";
  }
  return "other";
}

function formatUpdatedAt(value: string): string {
  if (!value.trim()) return "без даты";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  return new Intl.DateTimeFormat("ru-RU", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(dt);
}

function makeInitials(name: string): string {
  const tokens = name
    .split(/\s+/)
    .map((token) => token.trim())
    .filter(Boolean);
  if (tokens.length === 0) return "--";
  return tokens
    .slice(0, 2)
    .map((token) => token[0]?.toUpperCase() || "")
    .join("");
}

export function UploadCrmPanel({
  savedDocs,
  savedDocsFilter,
  loadingSavedDocs,
  deletingDocumentId = "",
  saving,
  onFilterChange,
  onRefresh,
  onOpenDocument,
  onDeleteDocument,
  activeDocumentId = "",
}: UploadCrmPanelProps) {
  const [activeFilter, setActiveFilter] = useState<VisualStatus>("all");

  const filteredDocs = useMemo(() => {
    if (activeFilter === "all") return savedDocs;
    return savedDocs.filter((item) => resolveVisualStatus(item) === activeFilter);
  }, [activeFilter, savedDocs]);

  const counters = useMemo(() => {
    const all = savedDocs.length;
    const review = savedDocs.filter((item) => resolveVisualStatus(item) === "review").length;
    const ready = savedDocs.filter((item) => resolveVisualStatus(item) === "ready").length;
    const uploaded = savedDocs.filter((item) => resolveVisualStatus(item) === "uploaded").length;
    return { all, review, ready, uploaded };
  }, [savedDocs]);

  const chips: Array<{ key: VisualStatus; label: string; count: number }> = [
    { key: "all", label: "Все", count: counters.all },
    { key: "review", label: "Проверка", count: counters.review },
    { key: "uploaded", label: "Новые", count: counters.uploaded },
    { key: "ready", label: "Готовые", count: counters.ready },
  ];

  return (
    <div className="flex h-[calc(100vh-220px)] flex-col overflow-hidden rounded-2xl border border-zinc-200 bg-gradient-to-b from-zinc-50 to-zinc-100">
      <div className="border-b border-zinc-200 bg-white/80 px-4 pb-4 pt-4 backdrop-blur">
        <div className="mb-3 flex items-center justify-between gap-2">
          <div>
            <div className="text-xs font-semibold uppercase tracking-[0.16em] text-zinc-500">
              CRM
            </div>
            <div className="mt-0.5 text-base font-semibold text-zinc-900">
              Список клиентов
            </div>
          </div>
          <Badge variant="secondary" className="rounded-full px-2.5 py-1 text-xs">
            {filteredDocs.length}
          </Badge>
        </div>

        <div className="relative mb-3">
          <Search className="pointer-events-none absolute left-2.5 top-2.5 h-4 w-4 text-zinc-400" />
          <Input
            className="h-9 rounded-lg border-zinc-200 bg-white pl-8"
            placeholder="Имя или номер документа"
            value={savedDocsFilter}
            onChange={(event) => onFilterChange(event.target.value)}
          />
        </div>

        <div className="mb-3 flex flex-wrap gap-1.5">
          {chips.map((chip) => (
            <button
              key={chip.key}
              type="button"
              onClick={() => setActiveFilter(chip.key)}
              className={cn(
                "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11px] font-medium transition",
                activeFilter === chip.key
                  ? "border-zinc-900 bg-zinc-900 text-white"
                  : "border-zinc-200 bg-white text-zinc-600 hover:border-zinc-300 hover:text-zinc-900",
              )}
            >
              <span>{chip.label}</span>
              <span className="opacity-70">{chip.count}</span>
            </button>
          ))}
        </div>

        {onRefresh ? (
          <Button
            variant="outline"
            className="h-8 w-full rounded-lg border-zinc-200 bg-white text-xs"
            onClick={onRefresh}
            disabled={loadingSavedDocs}
          >
            {loadingSavedDocs ? (
              <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />
            ) : (
              <RefreshCw className="mr-1.5 h-3.5 w-3.5" />
            )}
            Обновить список
          </Button>
        ) : null}
      </div>

      <div className="flex-1 overflow-y-auto px-3 pb-3 pt-3">
        <div className="space-y-2">
          {filteredDocs.map((item) => {
            const visualStatus = resolveVisualStatus(item);
            const isActive = activeDocumentId === item.document_id;
            return (
              <div
                key={item.document_id}
                className={cn(
                  "rounded-xl border bg-white px-3 py-3 shadow-sm transition",
                  isActive
                    ? "border-primary/60 ring-2 ring-primary/20"
                    : "border-zinc-200 hover:border-zinc-300",
                )}
              >
                <div className="mb-2 flex items-start gap-2">
                  <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-zinc-100 text-xs font-semibold text-zinc-700">
                    {makeInitials(item.name || "")}
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-sm font-semibold text-zinc-900">
                      {item.name || "Без имени"}
                    </div>
                    <div className="truncate font-mono text-[11px] text-zinc-500">
                      {item.document_number || "без номера"}
                    </div>
                  </div>
                  <Badge
                    variant="outline"
                    className={cn("rounded-full border px-2 py-0.5 text-[10px]", statusTone[visualStatus])}
                  >
                    {statusLabel[visualStatus]}
                  </Badge>
                </div>
                {Number(item.documents_count || 0) > 1 ? (
                  <div className="mb-2 text-[11px] text-zinc-500">
                    Документов клиента: {item.documents_count}
                  </div>
                ) : null}

                <div className="mb-2.5 flex items-center gap-1.5 text-[11px] text-zinc-500">
                  <Clock3 className="h-3 w-3" />
                  <span>{formatUpdatedAt(item.updated_at)}</span>
                </div>

                <div className="flex gap-2">
                  <Button
                    size="sm"
                    className="h-8 flex-1 rounded-lg"
                    onClick={() =>
                      onOpenDocument(
                        item.primary_document_id || item.document_id,
                        item.client_id || "",
                      )
                    }
                    disabled={saving}
                  >
                    Открыть
                    <ArrowRight className="ml-1 h-3.5 w-3.5" />
                  </Button>
                  {onDeleteDocument ? (
                    <Button
                      size="sm"
                      variant="outline"
                      className="h-8 rounded-lg border-zinc-200 px-3"
                      onClick={() => onDeleteDocument(item.document_id)}
                      disabled={Boolean(deletingDocumentId) || saving}
                    >
                      {deletingDocumentId === item.document_id ? (
                        <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      ) : (
                        <Trash2 className="h-3.5 w-3.5" />
                      )}
                    </Button>
                  ) : null}
                </div>
              </div>
            );
          })}

          {!loadingSavedDocs && filteredDocs.length === 0 ? (
            <div className="rounded-xl border border-dashed border-zinc-300 bg-white/70 px-3 py-8 text-center text-sm text-zinc-500">
              Нет сохраненных документов по выбранному фильтру.
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
