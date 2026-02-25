"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import {
  AlertCircle,
  ArrowRight,
  Check,
  CheckCircle2,
  Combine,
  FileSearch,
  FileUp,
  Globe,
  Loader2,
  Search,
  Sparkles,
  Wand2,
} from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import {
  API_BASE,
  CLIENT_AGENT_BASE,
  TARGET_URL_PRESETS,
  type Step,
  type UploadSourceKind,
} from "@/features/workspace/constants";
import {
  composeDdmmyyyy,
  composeFullName,
  composeNie,
  ddmmyyyyToIso,
  isPdfTargetUrl,
  isoToDdmmyyyy,
  parseNieParts,
  readErrorResponse,
  splitDdmmyyyy,
  splitFullName,
  toUrl,
} from "@/features/workspace/utils";
import { WorkspaceHeader } from "@/features/workspace/workspace-header";
import { useWorkspaceStepGuard } from "@/features/workspace/use-workspace-step-guard";
import {
  PHONE_COUNTRIES,
  composePhone,
  parsePhoneParts,
  type PhoneCountryIso,
  validatePhone,
} from "@/lib/phone";
import { cn } from "@/lib/utils";
import type {
  AddressAutofillResponse,
  AutofillPreviewResponse,
  ClientCardResponse,
  ClientMatchResponse,
  EnrichByIdentityResponse,
  MergeCandidate,
  Payload,
  SavedCrmDocument,
  UploadResponse,
} from "@/lib/types";

type RelatedPreviewItem = {
  documentId: string;
  label: string;
  previewUrl: string;
  isCurrent: boolean;
};

type BatchDocument = {
  documentId: string;
  previewUrl: string;
  payload: Payload;
  missingCount: number;
  filledCount: number;
  matchNie: string;
  matchPassport: string;
  matchBirthDate: string;
  matchNameTokens: string[];
  label: string;
};

type WorkspaceFlowPageProps = {
  routeStep?: Step;
};

const STEP_ROUTE_SEGMENT: Record<Step, string> = {
  upload: "upload",
  match: "match",
  merge: "merge",
  review: "review",
  prepare: "prepare",
  autofill: "autofill",
};

export function WorkspaceFlowPage({ routeStep }: WorkspaceFlowPageProps) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [step, setStep] = useState<Step>("upload");
  const [files, setFiles] = useState<File[]>([]);
  const [uploading, setUploading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [documentId, setDocumentId] = useState("");
  const [payload, setPayload] = useState<Payload | null>(null);
  const [previewUrl, setPreviewUrl] = useState("");
  const [formUrl, setFormUrl] = useState("");
  const [targetUrl, setTargetUrl] = useState("");
  const [targetPresetKey, setTargetPresetKey] = useState("");
  const [browserSessionId, setBrowserSessionId] = useState("");
  const [browserSessionAlive, setBrowserSessionAlive] = useState(false);
  const [browserCurrentUrl, setBrowserCurrentUrl] = useState("");
  const [missingFields, setMissingFields] = useState<string[]>([]);
  const [autofill, setAutofill] = useState<AutofillPreviewResponse | null>(
    null,
  );
  const [filledPdfNonce, setFilledPdfNonce] = useState(0);
  const [niePrefix, setNiePrefix] = useState("");
  const [nieNumber, setNieNumber] = useState("");
  const [nieSuffix, setNieSuffix] = useState("");
  const [primerApellido, setPrimerApellido] = useState("");
  const [segundoApellido, setSegundoApellido] = useState("");
  const [nombreSolo, setNombreSolo] = useState("");
  const [fechaDia, setFechaDia] = useState("");
  const [fechaMes, setFechaMes] = useState("");
  const [fechaAnio, setFechaAnio] = useState("");
  const [fechaNacimientoDia, setFechaNacimientoDia] = useState("");
  const [fechaNacimientoMes, setFechaNacimientoMes] = useState("");
  const [fechaNacimientoAnio, setFechaNacimientoAnio] = useState("");
  const [telefonoCountryIso, setTelefonoCountryIso] =
    useState<PhoneCountryIso>("ES");
  const [telefonoLocalNumber, setTelefonoLocalNumber] = useState("");
  const [savedDocs, setSavedDocs] = useState<SavedCrmDocument[]>([]);
  const [savedDocsFilter, setSavedDocsFilter] = useState("");
  const [loadingSavedDocs, setLoadingSavedDocs] = useState(false);
  const [deletingDocumentId, setDeletingDocumentId] = useState("");
  const [addressLineInput, setAddressLineInput] = useState("");
  const [addressAutofillLoading, setAddressAutofillLoading] = useState(false);
  const [addressLineSeededForDocument, setAddressLineSeededForDocument] =
    useState("");
  const [uploadSourceKind, setUploadSourceKind] =
    useState<UploadSourceKind>("");
  const [mergeCandidates, setMergeCandidates] = useState<MergeCandidate[]>([]);
  const [selectedMergeSourceId, setSelectedMergeSourceId] = useState("");
  const [mergePreview, setMergePreview] = useState<
    EnrichByIdentityResponse["enrichment_preview"]
  >([]);
  const [mergeSkippedPreview, setMergeSkippedPreview] = useState<
    NonNullable<EnrichByIdentityResponse["enrichment_skipped"]>
  >([]);
  const [mergeFieldSelection, setMergeFieldSelection] = useState<
    Record<string, boolean>
  >({});
  const [clientMatch, setClientMatch] = useState<ClientMatchResponse | null>(
    null,
  );
  const [sourceKindDetected, setSourceKindDetected] = useState("");
  const [sourceKindInput, setSourceKindInput] = useState("");
  const [sourceKindConfidence, setSourceKindConfidence] = useState(0);
  const [sourceKindAuto, setSourceKindAuto] = useState(false);
  const [sourceKindRequiresReview, setSourceKindRequiresReview] =
    useState(false);
  const [reviewSourceKind, setReviewSourceKind] = useState<UploadSourceKind>("");
  const [reprocessOcrLoading, setReprocessOcrLoading] = useState(false);
  const [relatedPreviewItems, setRelatedPreviewItems] = useState<
    RelatedPreviewItem[]
  >([]);
  const [activePreviewDocumentId, setActivePreviewDocumentId] = useState("");
  const [previewZoom, setPreviewZoom] = useState(1);
  const [batchDocuments, setBatchDocuments] = useState<BatchDocument[]>([]);
  const [selectedBatchSourceId, setSelectedBatchSourceId] = useState("");
  const [mergeAppliedFields, setMergeAppliedFields] = useState<string[]>([]);
  const [mergeSkippedFields, setMergeSkippedFields] = useState<string[]>([]);
  const [mergeLoading, setMergeLoading] = useState(false);
  const [activeClientId, setActiveClientId] = useState("");
  const [error, setError] = useState("");
  const [dragOver, setDragOver] = useState(false);
  const documentIdFromQuery = (searchParams.get("documentId") || "").trim();
  const crmClientIdFromQuery = (searchParams.get("crmClientId") || "").trim();
  const stepFromQuery = (searchParams.get("step") || "").trim().toLowerCase();
  const forcedStepFromQuery: Step | null =
    stepFromQuery === "upload" ||
    stepFromQuery === "match" ||
    stepFromQuery === "merge" ||
    stepFromQuery === "review" ||
    stepFromQuery === "prepare" ||
    stepFromQuery === "autofill"
      ? (stepFromQuery as Step)
      : null;
  const requestedStep: Step | null = routeStep || forcedStepFromQuery;

  const setStepInUrl = (
    nextStep: Step,
    overrides?: { documentId?: string; clientId?: string },
  ): void => {
    const params = new URLSearchParams(searchParams.toString());
    params.set("step", nextStep);
    const nextDocumentId = (overrides?.documentId ?? documentId).trim();
    const nextClientId = (overrides?.clientId ?? activeClientId).trim();
    if (nextDocumentId) {
      params.set("documentId", nextDocumentId);
    } else {
      params.delete("documentId");
    }
    if (nextClientId) {
      params.set("crmClientId", nextClientId);
    } else {
      params.delete("crmClientId");
    }
    const query = params.toString();
    router.replace(
      `/workspace/${STEP_ROUTE_SEGMENT[nextStep]}${query ? `?${query}` : ""}`,
    );
  };

  const setStepAndSync = (
    nextStep: Step,
    overrides?: { documentId?: string; clientId?: string },
  ): void => {
    setStep(nextStep);
    setStepInUrl(nextStep, overrides);
  };

  const hasMergeSources = useMemo(() => {
    const batchMergeSources = batchDocuments.filter(
      (row) => row.documentId !== documentId,
    );
    const batchDocumentIdSet = new Set(
      batchDocuments.map((row) => row.documentId),
    );
    const externalMergeCandidates = mergeCandidates.filter(
      (candidate) => !batchDocumentIdSet.has(candidate.document_id),
    );
    return (
      batchMergeSources.length > 0 || externalMergeCandidates.length > 0
    );
  }, [batchDocuments, documentId, mergeCandidates]);
  const { fallbackStep } = useWorkspaceStepGuard({
    hasDocument: Boolean(documentId),
    hasPayload: Boolean(payload),
    hasClientMatch: Boolean(clientMatch?.identity_match_found),
    hasMergeSources,
  });

  useEffect(() => {
    if (!payload || !documentId) return;
    if (addressLineSeededForDocument === documentId) return;
    const domicilio = payload.domicilio;
    const composed = [
      domicilio.tipo_via,
      domicilio.nombre_via,
      domicilio.numero,
      domicilio.escalera ? `Esc ${domicilio.escalera}` : "",
      domicilio.piso ? `Piso ${domicilio.piso}` : "",
      domicilio.puerta ? `Puerta ${domicilio.puerta}` : "",
      domicilio.municipio,
      domicilio.cp,
    ]
      .filter((value) => String(value || "").trim().length > 0)
      .join(", ");
    setAddressLineInput(composed);
    setAddressLineSeededForDocument(documentId);
  }, [addressLineSeededForDocument, documentId, payload]);

  useEffect(() => {
    const timer = setTimeout(() => {
      void loadSavedDocuments(savedDocsFilter);
    }, 250);
    return () => clearTimeout(timer);
  }, [savedDocsFilter]);

  useEffect(() => {
    const batchIds = new Set(batchDocuments.map((row) => row.documentId));
    const externalCandidates = mergeCandidates.filter(
      (row) => !batchIds.has(row.document_id),
    );
    if (
      selectedMergeSourceId &&
      externalCandidates.some(
        (row) => row.document_id === selectedMergeSourceId,
      )
    ) {
      return;
    }
    setSelectedMergeSourceId(externalCandidates[0]?.document_id || "");
  }, [mergeCandidates, batchDocuments, selectedMergeSourceId]);

  useEffect(() => {
    const batchSourceIds = batchDocuments
      .map((row) => row.documentId)
      .filter((id) => id !== documentId);
    if (
      selectedBatchSourceId &&
      batchSourceIds.some((id) => id === selectedBatchSourceId)
    ) {
      return;
    }
    setSelectedBatchSourceId(batchSourceIds[0] || "");
  }, [batchDocuments, documentId, selectedBatchSourceId]);

  useEffect(() => {
    if (step !== "merge") return;
    if (mergeLoading || saving) return;
    const batchModeActive = batchDocuments.length > 1;
    const hasPreviewRows =
      (mergePreview && mergePreview.length > 0) ||
      (mergeSkippedPreview && mergeSkippedPreview.length > 0);
    if (hasPreviewRows) return;
    if (selectedMergeSourceId) {
      void runMerge(false, selectedMergeSourceId);
      return;
    }
    if (batchModeActive && selectedBatchSourceId) {
      void runMerge(false, selectedBatchSourceId);
    }
  }, [
    batchDocuments,
    mergeLoading,
    mergePreview,
    mergeSkippedPreview,
    saving,
    selectedBatchSourceId,
    selectedMergeSourceId,
    step,
  ]);

  useEffect(() => {
    if (!documentIdFromQuery) return;
    void (async () => {
      await openSavedDocument(documentIdFromQuery, crmClientIdFromQuery);
      if (requestedStep) {
        setStepAndSync(requestedStep, {
          documentId: documentIdFromQuery,
          clientId: crmClientIdFromQuery,
        });
      }
    })();
  }, [documentIdFromQuery, crmClientIdFromQuery, requestedStep]);

  useEffect(() => {
    if (!requestedStep) return;
    const nextStep = fallbackStep(requestedStep);
    if (nextStep !== step) {
      if (nextStep !== requestedStep) {
        setError("Этот шаг сейчас недоступен. Переводим на доступный экран.");
      }
      setStepAndSync(nextStep);
    }
  }, [requestedStep, step, documentId, payload, hasMergeSources]);

  function applyMergeStateFromCandidates(
    candidates: MergeCandidate[] | undefined,
    preferredSourceId?: string,
  ) {
    const next = candidates || [];
    setMergeCandidates(next);
    const preferred = (preferredSourceId || "").trim();
    if (preferred && next.some((row) => row.document_id === preferred)) {
      setSelectedMergeSourceId(preferred);
      return;
    }
    if (
      selectedMergeSourceId &&
      next.some((row) => row.document_id === selectedMergeSourceId)
    ) {
      return;
    }
    setSelectedMergeSourceId(next[0]?.document_id || "");
  }

  function clearMergePreviewState() {
    setMergePreview([]);
    setMergeSkippedPreview([]);
    setMergeAppliedFields([]);
    setMergeSkippedFields([]);
    setMergeFieldSelection({});
  }

  function normalizeIdentity(value: string): string {
    return value
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toUpperCase()
      .replace(/[^A-ZА-ЯЁ0-9]/g, "");
  }

  function normalizeDateForMatch(value: string): string {
    const raw = value.trim();
    if (!raw) return "";
    const currentTwoDigitsYear = new Date().getFullYear() % 100;
    const toFourDigitsYear = (year: string): string => {
      if (year.length === 4) return year;
      const yy = Number.parseInt(year, 10);
      if (Number.isNaN(yy)) return "";
      return String(yy > currentTwoDigitsYear + 1 ? 1900 + yy : 2000 + yy);
    };
    const ymd = raw.match(/^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$/);
    if (ymd) {
      const [, year, month, day] = ymd;
      return `${year}${month.padStart(2, "0")}${day.padStart(2, "0")}`;
    }
    const dmy = raw.match(/^(\d{1,2})[-/.](\d{1,2})[-/.](\d{2}|\d{4})$/);
    if (dmy) {
      const [, day, month, yearRaw] = dmy;
      const year = toFourDigitsYear(yearRaw);
      if (!year) return "";
      return `${year}${month.padStart(2, "0")}${day.padStart(2, "0")}`;
    }
    return "";
  }

  function normalizeNameTokensForMatch(nextPayload: Payload): string[] {
    const ident = nextPayload.identificacion || {};
    const sourceName =
      ident.nombre_apellidos ||
      [ident.primer_apellido, ident.segundo_apellido, ident.nombre]
        .filter(Boolean)
        .join(" ");
    if (!sourceName.trim()) return [];
    const normalized = sourceName
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toUpperCase()
      .replace(/[^A-ZА-ЯЁ0-9]+/g, " ")
      .trim();
    if (!normalized) return [];
    return normalized.split(/\s+/).filter((token) => token.length >= 2);
  }

  function nameOverlapScore(left: string[], right: string[]): number {
    if (left.length === 0 || right.length === 0) return 0;
    const rightSet = new Set(right);
    let overlap = 0;
    for (const token of left) {
      if (rightSet.has(token)) overlap += 1;
    }
    return overlap / Math.max(left.length, right.length);
  }

  function areLikelySameBatchUser(
    left: BatchDocument,
    right: BatchDocument,
  ): boolean {
    if (left.matchNie && right.matchNie) {
      return left.matchNie === right.matchNie;
    }
    if (left.matchPassport && right.matchPassport) {
      return left.matchPassport === right.matchPassport;
    }
    const sameBirthDate =
      Boolean(left.matchBirthDate) &&
      Boolean(right.matchBirthDate) &&
      left.matchBirthDate === right.matchBirthDate;
    const overlapScore = nameOverlapScore(
      left.matchNameTokens,
      right.matchNameTokens,
    );
    if (sameBirthDate && overlapScore >= 0.5) {
      return true;
    }
    if (!left.matchBirthDate || !right.matchBirthDate) {
      return overlapScore >= 0.8;
    }
    return false;
  }

  function countFilledPayloadValues(nextPayload: Payload): number {
    let count = 0;
    const sections = [
      nextPayload.identificacion,
      nextPayload.domicilio,
      nextPayload.declarante,
      nextPayload.extra,
    ];
    for (const section of sections) {
      if (!section || typeof section !== "object") continue;
      for (const value of Object.values(section)) {
        if (typeof value === "string" && value.trim()) {
          count += 1;
        }
      }
    }
    return count;
  }

  function buildBatchLabel(nextPayload: Payload, fallback: string): string {
    const ident = nextPayload.identificacion || {};
    const number = ident.nif_nie || ident.pasaporte || "";
    const name = ident.nombre_apellidos || "";
    const joined = [number, name].filter(Boolean).join(" · ");
    return joined || fallback;
  }

  function hasMergeSourcesForStep(
    candidates: MergeCandidate[] | undefined,
    hasBatchSources: boolean,
  ): boolean {
    return hasBatchSources || (candidates || []).length > 0;
  }

  function isObviousClientMatch(
    data: ClientMatchResponse | UploadResponse | null | undefined,
  ): boolean {
    if (!data?.identity_match_found || !data.client_match?.document_id) {
      return false;
    }
    const candidateCount = (data.merge_candidates || []).length;
    const score = Number(data.client_match.score) || 0;
    return candidateCount === 1 && score >= 90;
  }

  function resetWorkflow() {
    setStepAndSync("upload");
    setFiles([]);
    setUploading(false);
    setSaving(false);
    setDocumentId("");
    setPayload(null);
    setPreviewUrl("");
    setFormUrl("");
    setTargetUrl("");
    setTargetPresetKey("");
    setBrowserSessionId("");
    setBrowserSessionAlive(false);
    setBrowserCurrentUrl("");
    setMissingFields([]);
    setAutofill(null);
    setNiePrefix("");
    setNieNumber("");
    setNieSuffix("");
    setPrimerApellido("");
    setSegundoApellido("");
    setNombreSolo("");
    setFechaDia("");
    setFechaMes("");
    setFechaAnio("");
    setFechaNacimientoDia("");
    setFechaNacimientoMes("");
    setFechaNacimientoAnio("");
    setTelefonoCountryIso("ES");
    setTelefonoLocalNumber("");
    setUploadSourceKind("");
    setMergeCandidates([]);
    setSelectedMergeSourceId("");
    setClientMatch(null);
    setSourceKindDetected("");
    setSourceKindInput("");
    setSourceKindConfidence(0);
    setSourceKindAuto(false);
    setSourceKindRequiresReview(false);
    setReviewSourceKind("");
    setReprocessOcrLoading(false);
    setRelatedPreviewItems([]);
    setActivePreviewDocumentId("");
    setBatchDocuments([]);
    setSelectedBatchSourceId("");
    setMergePreview([]);
    setMergeSkippedPreview([]);
    setMergeAppliedFields([]);
    setMergeSkippedFields([]);
    setMergeFieldSelection({});
    setMergeLoading(false);
    setActiveClientId("");
    setAddressLineInput("");
    setAddressAutofillLoading(false);
    setAddressLineSeededForDocument("");
    setError("");
    setDragOver(false);
  }

  function goToPrepareForAnotherDocument() {
    setAutofill(null);
    setClientMatch(null);
    setFormUrl("");
    setTargetUrl("");
    setTargetPresetKey("");
    setBrowserSessionId("");
    setBrowserSessionAlive(false);
    setBrowserCurrentUrl("");
    setSelectedBatchSourceId("");
    clearMergePreviewState();
    setError("");
    setStepAndSync("prepare");
  }

  async function loadSavedDocuments(query: string) {
    setLoadingSavedDocs(true);
    try {
      const params = new URLSearchParams();
      if (query.trim()) params.set("query", query.trim());
      params.set("limit", "100");
      const resp = await fetch(
        `${API_BASE}/api/crm/documents?${params.toString()}`,
      );
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as { items?: SavedCrmDocument[] };
      setSavedDocs(data.items || []);
    } catch (e) {
      setError(
        e instanceof Error ? e.message : "Failed loading saved documents",
      );
    } finally {
      setLoadingSavedDocs(false);
    }
  }

  function syncNamePartsFromPayload(nextPayload: Payload | null) {
    if (!nextPayload) {
      setNiePrefix("");
      setNieNumber("");
      setNieSuffix("");
      setPrimerApellido("");
      setSegundoApellido("");
      setNombreSolo("");
      setFechaDia("");
      setFechaMes("");
      setFechaAnio("");
      setFechaNacimientoDia("");
      setFechaNacimientoMes("");
      setFechaNacimientoAnio("");
      setTelefonoCountryIso("ES");
      setTelefonoLocalNumber("");
      return;
    }
    const ident =
      nextPayload.identificacion ||
      ({ nif_nie: "", nombre_apellidos: "" } as Payload["identificacion"]);
    const nie = parseNieParts(ident.nif_nie || "");
    setNiePrefix(nie.prefix);
    setNieNumber(nie.number);
    setNieSuffix(nie.suffix);
    const split = splitFullName(ident.nombre_apellidos || "");
    setPrimerApellido(
      (ident.primer_apellido || split.primer_apellido || "").trim(),
    );
    setSegundoApellido(
      (ident.segundo_apellido || split.segundo_apellido || "").trim(),
    );
    setNombreSolo((ident.nombre || split.nombre || "").trim());
    const decl = splitDdmmyyyy(nextPayload.declarante?.fecha || "");
    setFechaDia((nextPayload.declarante?.fecha_dia || decl.day || "").trim());
    setFechaMes((nextPayload.declarante?.fecha_mes || decl.month || "").trim());
    setFechaAnio(
      (nextPayload.declarante?.fecha_anio || decl.year || "").trim(),
    );
    const birth = splitDdmmyyyy(nextPayload.extra?.fecha_nacimiento || "");
    setFechaNacimientoDia(
      (nextPayload.extra?.fecha_nacimiento_dia || birth.day || "").trim(),
    );
    setFechaNacimientoMes(
      (nextPayload.extra?.fecha_nacimiento_mes || birth.month || "").trim(),
    );
    setFechaNacimientoAnio(
      (nextPayload.extra?.fecha_nacimiento_anio || birth.year || "").trim(),
    );
    const phone = parsePhoneParts(nextPayload.domicilio?.telefono || "");
    const explicitIso =
      ((
        nextPayload.extra?.telefono_country_iso || ""
      ).toUpperCase() as PhoneCountryIso) || phone.countryIso;
    setTelefonoCountryIso(
      PHONE_COUNTRIES.some((item) => item.iso === explicitIso)
        ? explicitIso
        : phone.countryIso || "ES",
    );
    setTelefonoLocalNumber(phone.localNumber || "");
  }

  function resolveStepAfterLoad(
    data: UploadResponse,
    hasBatchSources: boolean = false,
  ): Step {
    const workflowStage = (data.workflow_stage || "").toLowerCase();
    if (workflowStage === "client_match") {
      return "match";
    }
    if (data.identity_match_found && data.client_match) {
      return "match";
    }
    if (workflowStage === "prepare") {
      return "prepare";
    }
    if (workflowStage === "autofill") {
      return "autofill";
    }
    return hasMergeSourcesForStep(data.merge_candidates, hasBatchSources)
      ? "merge"
      : "review";
  }

  function applySourceKindMeta(data: UploadResponse) {
    const sourceMeta = (data.source || {}) as Record<string, unknown>;
    const detected =
      String(
        data.source_kind_detected ||
          sourceMeta.source_kind_detected ||
          sourceMeta.source_kind ||
          "",
      ) || "";
    const input =
      String(data.source_kind_input || sourceMeta.source_kind_input || "") ||
      "";
    const confidenceRaw =
      data.source_kind_confidence ?? sourceMeta.source_kind_confidence ?? 0;
    const confidenceNumber = Number(confidenceRaw);
    const autoRaw =
      data.source_kind_auto ?? sourceMeta.source_kind_auto ?? false;
    const reviewRaw =
      data.source_kind_requires_review ??
      sourceMeta.source_kind_requires_review ??
      false;
    setSourceKindDetected(detected);
    setSourceKindInput(input);
    setSourceKindConfidence(
      Number.isFinite(confidenceNumber) ? confidenceNumber : 0,
    );
    setSourceKindAuto(Boolean(autoRaw));
    setSourceKindRequiresReview(Boolean(reviewRaw));
    const manual = (input || detected) as UploadSourceKind;
    if (
      manual === "anketa" ||
      manual === "fmiliar" ||
      manual === "passport" ||
      manual === "nie_tie" ||
      manual === "visa"
    ) {
      setReviewSourceKind(manual);
    } else {
      setReviewSourceKind("");
    }
  }

  async function reprocessOcrWithManualSourceKind() {
    if (!documentId) return;
    if (!reviewSourceKind) {
      setError("Выберите тип документа для перезапуска OCR.");
      return;
    }
    setReprocessOcrLoading(true);
    setError("");
    try {
      const resp = await fetch(
        `${API_BASE}/api/documents/${documentId}/reprocess-ocr`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            source_kind: reviewSourceKind,
          }),
        },
      );
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as UploadResponse;
      setActiveClientId((data.client_id || "").trim());
      setPayload(data.payload);
      applySourceKindMeta(data);
      syncNamePartsFromPayload(data.payload);
      setPreviewUrl(toUrl(data.preview_url || "", API_BASE));
      setFormUrl(data.form_url);
      setTargetUrl(data.target_url || data.form_url);
      setMissingFields(data.missing_fields || []);
      applyMergeStateFromCandidates(
        data.merge_candidates,
        data.identity_source_document_id || "",
      );
      setMergePreview(data.enrichment_preview || []);
      setMergeSkippedPreview(data.enrichment_skipped || []);
      setMergeAppliedFields((data.enrichment_preview || []).map((row) => row.field));
      setMergeSkippedFields([]);
      setMergeFieldSelection(
        Object.fromEntries(
          (data.enrichment_preview || []).map((row) => [row.field, true]),
        ),
      );
      await loadRelatedPreviews(
        documentId,
        toUrl(data.preview_url || "", API_BASE),
        data.merge_candidates || [],
        batchDocuments.map((row) => row.documentId),
      );
    } catch (e) {
      setError(
        e instanceof Error ? e.message : "Failed reprocessing OCR for document",
      );
    } finally {
      setReprocessOcrLoading(false);
    }
  }

  async function loadRelatedPreviews(
    currentDocumentId: string,
    currentPreviewUrl: string,
    candidates: MergeCandidate[] | undefined,
    extraDocumentIds: string[] = [],
  ) {
    const candidateIds = (candidates || [])
      .map((row) => row.document_id)
      .filter(Boolean);
    const uniqueIds = Array.from(
      new Set(
        [currentDocumentId, ...candidateIds, ...extraDocumentIds].filter(
          Boolean,
        ),
      ),
    );
    if (uniqueIds.length === 0) {
      setRelatedPreviewItems([]);
      setActivePreviewDocumentId("");
      return;
    }

    const currentName = composeFullName(
      primerApellido,
      segundoApellido,
      nombreSolo,
    );
    const currentDocNumber =
      payload?.identificacion?.nif_nie ||
      payload?.identificacion?.pasaporte ||
      "";

    const rows = await Promise.all(
      uniqueIds.map(async (id) => {
        if (id === currentDocumentId) {
          return {
            documentId: id,
            label: currentDocNumber
              ? `${currentDocNumber} · ${currentName || "Текущий"}`
              : currentName || "Текущий документ",
            previewUrl: currentPreviewUrl,
            isCurrent: true,
          } satisfies RelatedPreviewItem;
        }
        try {
          const resp = await fetch(`${API_BASE}/api/crm/documents/${id}`);
          if (!resp.ok) {
            return null;
          }
          const data = (await resp.json()) as UploadResponse;
          const ident =
            data.payload?.identificacion || ({} as Payload["identificacion"]);
          const label = [
            ident.nif_nie || ident.pasaporte || "Документ",
            ident.nombre_apellidos || "Связанный",
          ]
            .filter(Boolean)
            .join(" · ");
          return {
            documentId: id,
            label,
            previewUrl: toUrl(data.preview_url || "", API_BASE),
            isCurrent: false,
          } satisfies RelatedPreviewItem;
        } catch {
          return null;
        }
      }),
    );

    const filtered = rows.filter((row): row is RelatedPreviewItem =>
      Boolean(row),
    );
    setRelatedPreviewItems(filtered);
    if (!filtered.some((row) => row.documentId === activePreviewDocumentId)) {
      setActivePreviewDocumentId(currentDocumentId);
    }
  }

  async function loadClientMatch(documentIdValue: string) {
    try {
      const resp = await fetch(
        `${API_BASE}/api/documents/${documentIdValue}/client-match`,
      );
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as ClientMatchResponse;
      setClientMatch(data);
      if (data.merge_candidates?.length) {
        applyMergeStateFromCandidates(
          data.merge_candidates,
          data.identity_source_document_id || "",
        );
      }
      if (isObviousClientMatch(data)) {
        await resolveClientMatch(
          "confirm",
          data.identity_source_document_id ||
            data.client_match?.document_id ||
            "",
          documentIdValue,
        );
      }
    } catch (e) {
      setError(
        e instanceof Error ? e.message : "Failed loading client match data",
      );
    }
  }

  async function resolveClientMatch(
    action: "confirm" | "reject",
    sourceDocumentIdOverride?: string,
    documentIdOverride?: string,
  ) {
    const targetDocumentId = documentIdOverride || documentId;
    if (!targetDocumentId) return;
    setSaving(true);
    setError("");
    try {
      const sourceId =
        (sourceDocumentIdOverride || "").trim() ||
        (clientMatch?.identity_source_document_id || "").trim() ||
        (clientMatch?.client_match?.document_id || "").trim();
      const resp = await fetch(
        `${API_BASE}/api/documents/${targetDocumentId}/client-match`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            action,
            source_document_id: sourceId || undefined,
          }),
        },
      );
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as UploadResponse;
      setActiveClientId((data.client_id || "").trim());
      setPayload(data.payload);
      applySourceKindMeta(data);
      syncNamePartsFromPayload(data.payload);
      setMissingFields(data.missing_fields || []);
      applyMergeStateFromCandidates(
        data.merge_candidates,
        data.identity_source_document_id || "",
      );
      setMergePreview(data.enrichment_preview || []);
      setMergeSkippedPreview(data.enrichment_skipped || []);
      setMergeAppliedFields(
        (data.enrichment_preview || []).map((row) => row.field),
      );
      setMergeSkippedFields([]);
      setMergeFieldSelection(
        Object.fromEntries(
          (data.enrichment_preview || []).map((row) => [row.field, true]),
        ),
      );
      await loadRelatedPreviews(
        targetDocumentId,
        previewUrl,
        data.merge_candidates || [],
        batchDocuments.map((row) => row.documentId),
      );
      const hasBatchSources = batchDocuments.some(
        (row) => row.documentId !== targetDocumentId,
      );
      setStepAndSync(
        hasMergeSourcesForStep(data.merge_candidates, hasBatchSources)
          ? "merge"
          : "review",
      );
    } catch (e) {
      setError(
        e instanceof Error ? e.message : "Failed resolving client match",
      );
    } finally {
      setSaving(false);
    }
  }

  async function openSavedDocument(
    documentIdToOpen: string,
    crmClientIdOverride = "",
  ) {
    setSaving(true);
    setError("");
    try {
      const resp = await fetch(
        `${API_BASE}/api/crm/documents/${documentIdToOpen}`,
      );
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as UploadResponse;
      let effectivePayload = data.payload;
      let effectiveMissingFields = data.missing_fields || [];
      let clientDocumentIds: string[] = [];
      const clientId = (crmClientIdOverride || data.client_id || "").trim();
      if (clientId) {
        try {
          const clientResp = await fetch(
            `${API_BASE}/api/crm/clients/${encodeURIComponent(clientId)}`,
          );
          if (clientResp.ok) {
            const clientCard = (await clientResp.json()) as ClientCardResponse;
            if (clientCard.profile_payload) {
              effectivePayload = clientCard.profile_payload;
            }
            if (Array.isArray(clientCard.missing_fields)) {
              effectiveMissingFields = clientCard.missing_fields;
            }
            clientDocumentIds = (clientCard.documents || [])
              .map((row) => row.document_id)
              .filter(Boolean);
          }
        } catch {
          // keep document payload fallback for resilience
        }
      }
      setActiveClientId(clientId);
      setDocumentId(data.document_id);
      setPayload(effectivePayload);
      applySourceKindMeta(data);
      syncNamePartsFromPayload(effectivePayload);
      setPreviewUrl(toUrl(data.preview_url || "", API_BASE));
      setFormUrl(data.form_url);
      setTargetUrl(data.target_url || data.form_url);
      setBrowserSessionId("");
      setBrowserSessionAlive(false);
      setBrowserCurrentUrl("");
      setMissingFields(effectiveMissingFields);
      applyMergeStateFromCandidates(
        data.merge_candidates,
        data.identity_source_document_id || "",
      );
      setMergePreview(data.enrichment_preview || []);
      setMergeSkippedPreview(data.enrichment_skipped || []);
      setMergeAppliedFields(
        (data.enrichment_preview || []).map((row) => row.field),
      );
      setMergeSkippedFields([]);
      setMergeFieldSelection(
        Object.fromEntries(
          (data.enrichment_preview || []).map((row) => [row.field, true]),
        ),
      );
      setAutofill(null);
      setClientMatch(null);
      setBatchDocuments([]);
      setSelectedBatchSourceId("");
      await loadRelatedPreviews(
        data.document_id,
        toUrl(data.preview_url || "", API_BASE),
        data.merge_candidates || [],
        clientDocumentIds,
      );
      const nextStep = resolveStepAfterLoad(data);
      setStepAndSync(nextStep);
      if (nextStep === "match") {
        await loadClientMatch(data.document_id);
      }
    } catch (e) {
      setError(
        e instanceof Error ? e.message : "Failed opening saved document",
      );
    } finally {
      setSaving(false);
    }
  }

  async function deleteSavedDocument(documentIdToDelete: string) {
    const approved = window.confirm(
      "Удалить документ из CRM? Это действие нельзя отменить.",
    );
    if (!approved) return;
    setDeletingDocumentId(documentIdToDelete);
    setError("");
    try {
      const resp = await fetch(
        `${API_BASE}/api/crm/documents/${documentIdToDelete}`,
        {
          method: "DELETE",
        },
      );
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      if (documentId === documentIdToDelete) {
        resetWorkflow();
      }
      await loadSavedDocuments(savedDocsFilter);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed deleting CRM document");
    } finally {
      setDeletingDocumentId("");
    }
  }

  function patchPayload(section: keyof Payload, key: string, value: string) {
    if (!payload) return;
    const nextPayload = {
      ...payload,
      [section]: {
        ...(payload[section] as Record<string, unknown>),
        [key]: value,
      },
    };
    if (
      section === "domicilio" &&
      key === "municipio" &&
      value.trim() &&
      !String(nextPayload.declarante?.localidad || "").trim()
    ) {
      nextPayload.declarante = {
        ...nextPayload.declarante,
        localidad: value.trim(),
      };
    }
    setPayload(nextPayload);
  }

  function patchNiePart(kind: "prefix" | "number" | "suffix", value: string) {
    const nextPrefix = kind === "prefix" ? value : niePrefix;
    const nextNumber = kind === "number" ? value : nieNumber;
    const nextSuffix = kind === "suffix" ? value : nieSuffix;
    if (kind === "prefix")
      setNiePrefix(
        value
          .toUpperCase()
          .replace(/[^XYZ]/g, "")
          .slice(0, 1),
      );
    if (kind === "number") setNieNumber(value.replace(/\D/g, "").slice(0, 7));
    if (kind === "suffix")
      setNieSuffix(
        value
          .toUpperCase()
          .replace(/[^A-Z]/g, "")
          .slice(0, 1),
      );
    const composed = composeNie(nextPrefix, nextNumber, nextSuffix);
    if (composed) patchPayload("identificacion", "nif_nie", composed);
  }

  function patchPhonePart(kind: "countryIso" | "localNumber", value: string) {
    const nextCountry =
      kind === "countryIso" ? (value as PhoneCountryIso) : telefonoCountryIso;
    const nextLocal =
      kind === "localNumber"
        ? value.replace(/\D/g, "").slice(0, 15)
        : telefonoLocalNumber;
    if (kind === "countryIso") setTelefonoCountryIso(nextCountry);
    if (kind === "localNumber") setTelefonoLocalNumber(nextLocal);
    patchPayload("domicilio", "telefono", composePhone(nextCountry, nextLocal));
    patchExtra("telefono_country_iso", nextCountry);
  }

  async function autofillAddressFromStreet() {
    if (!payload) return;
    const sourceLine = addressLineInput.trim();
    if (!sourceLine) {
      setError("Введите адресную строку для дозаполнения.");
      return;
    }
    if (!documentId.trim()) {
      setError("Сначала загрузите или откройте документ.");
      return;
    }

    setAddressAutofillLoading(true);
    setError("");
    try {
      const resp = await fetch(
        `${API_BASE}/api/documents/${encodeURIComponent(documentId)}/address-autofill`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ address_line: sourceLine }),
        },
      );
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as AddressAutofillResponse;
      const domicilio = payload.domicilio;
      const filled = data.domicilio;
      const resolvedMunicipio = domicilio.municipio || filled.municipio || "";
      setPayload({
        ...payload,
        domicilio: {
          ...domicilio,
          tipo_via: domicilio.tipo_via || filled.tipo_via || "",
          nombre_via: domicilio.nombre_via || filled.nombre_via || "",
          numero: domicilio.numero || filled.numero || "",
          escalera: domicilio.escalera || filled.escalera || "",
          piso: domicilio.piso || filled.piso || "",
          puerta: domicilio.puerta || filled.puerta || "",
          municipio: resolvedMunicipio,
          provincia: domicilio.provincia || filled.provincia || "",
          cp: domicilio.cp || filled.cp || "",
        },
        declarante: {
          ...payload.declarante,
          localidad:
            payload.declarante.localidad || resolvedMunicipio || "",
        },
      });
      if (data.normalized_address) {
        setAddressLineInput(data.normalized_address);
      }
    } catch (e) {
      setError(
        e instanceof Error
          ? e.message
          : "Не удалось дозаполнить адрес из строки.",
      );
    } finally {
      setAddressAutofillLoading(false);
    }
  }

  function patchSplitNameAndCompose(
    kind: "primer_apellido" | "segundo_apellido" | "nombre",
    value: string,
  ) {
    const nextPrimer = kind === "primer_apellido" ? value : primerApellido;
    const nextSegundo = kind === "segundo_apellido" ? value : segundoApellido;
    const nextNombre = kind === "nombre" ? value : nombreSolo;
    if (kind === "primer_apellido") setPrimerApellido(value);
    if (kind === "segundo_apellido") setSegundoApellido(value);
    if (kind === "nombre") setNombreSolo(value);
    patchPayload("identificacion", "primer_apellido", nextPrimer);
    patchPayload("identificacion", "segundo_apellido", nextSegundo);
    patchPayload("identificacion", "nombre", nextNombre);
    patchPayload(
      "identificacion",
      "nombre_apellidos",
      composeFullName(nextPrimer, nextSegundo, nextNombre),
    );
  }

  function patchExtra(key: string, value: string) {
    if (!payload) return;
    setPayload({
      ...payload,
      extra: {
        ...(payload.extra || {}),
        [key]: value,
      },
    });
  }

  function patchDeclaranteDatePart(
    kind: "day" | "month" | "year",
    value: string,
  ) {
    const clean = value.replace(/\D/g, "").slice(0, kind === "year" ? 4 : 2);
    const nextDay = kind === "day" ? clean : fechaDia;
    const nextMonth = kind === "month" ? clean : fechaMes;
    const nextYear = kind === "year" ? clean : fechaAnio;
    if (kind === "day") setFechaDia(clean);
    if (kind === "month") setFechaMes(clean);
    if (kind === "year") setFechaAnio(clean);
    patchPayload("declarante", "fecha_dia", nextDay);
    patchPayload("declarante", "fecha_mes", nextMonth);
    patchPayload("declarante", "fecha_anio", nextYear);
    const composed = composeDdmmyyyy(nextDay, nextMonth, nextYear);
    if (composed) patchPayload("declarante", "fecha", composed);
  }

  function patchNacimientoDatePart(
    kind: "day" | "month" | "year",
    value: string,
  ) {
    const clean = value.replace(/\D/g, "").slice(0, kind === "year" ? 4 : 2);
    const nextDay = kind === "day" ? clean : fechaNacimientoDia;
    const nextMonth = kind === "month" ? clean : fechaNacimientoMes;
    const nextYear = kind === "year" ? clean : fechaNacimientoAnio;
    if (kind === "day") setFechaNacimientoDia(clean);
    if (kind === "month") setFechaNacimientoMes(clean);
    if (kind === "year") setFechaNacimientoAnio(clean);
    patchExtra("fecha_nacimiento_dia", nextDay);
    patchExtra("fecha_nacimiento_mes", nextMonth);
    patchExtra("fecha_nacimiento_anio", nextYear);
    const composed = composeDdmmyyyy(nextDay, nextMonth, nextYear);
    if (composed) patchExtra("fecha_nacimiento", composed);
  }

  function patchDeclaranteDate(day: string, month: string, year: string) {
    setFechaDia(day);
    setFechaMes(month);
    setFechaAnio(year);
    patchPayload("declarante", "fecha_dia", day);
    patchPayload("declarante", "fecha_mes", month);
    patchPayload("declarante", "fecha_anio", year);
    const composed = composeDdmmyyyy(day, month, year);
    if (composed) patchPayload("declarante", "fecha", composed);
  }

  function patchNacimientoDate(day: string, month: string, year: string) {
    setFechaNacimientoDia(day);
    setFechaNacimientoMes(month);
    setFechaNacimientoAnio(year);
    patchExtra("fecha_nacimiento_dia", day);
    patchExtra("fecha_nacimiento_mes", month);
    patchExtra("fecha_nacimiento_anio", year);
    const composed = composeDdmmyyyy(day, month, year);
    if (composed) patchExtra("fecha_nacimiento", composed);
  }

  function onFilesSelected(next: File[]) {
    setFiles(next);
    setError("");
  }

  async function uploadDocument() {
    if (files.length === 0) {
      setError("Выберите минимум один файл .jpg/.jpeg/.png/.pdf");
      return;
    }
    setUploading(true);
    setError("");
    try {
      const successes: UploadResponse[] = [];
      const failedNames: string[] = [];

      for (const currentFile of files) {
        try {
          const formData = new FormData();
          formData.append("file", currentFile);
          formData.append("source_kind", uploadSourceKind);
          const resp = await fetch(`${API_BASE}/api/documents/upload`, {
            method: "POST",
            body: formData,
          });
          if (!resp.ok) {
            const text = await resp.text();
            throw new Error(text || `Upload failed (${resp.status})`);
          }
          const data: UploadResponse = await resp.json();
          successes.push(data);
        } catch {
          failedNames.push(currentFile.name);
        }
      }

      if (successes.length === 0) {
        setError(
          failedNames.length > 0
            ? `Не удалось загрузить файлы: ${failedNames.join(", ")}`
            : "Upload failed",
        );
        return;
      }
      const uploadedBatch = successes.map((row) => {
        const batchPayload = row.payload;
        const ident = batchPayload.identificacion || {};
        const missingCount = (row.missing_fields || []).length;
        const filledCount = countFilledPayloadValues(batchPayload);
        return {
          documentId: row.document_id,
          previewUrl: toUrl(row.preview_url || "", API_BASE),
          payload: batchPayload,
          missingCount,
          filledCount,
          matchNie: normalizeIdentity(ident.nif_nie || ""),
          matchPassport: normalizeIdentity(ident.pasaporte || ""),
          matchBirthDate: normalizeDateForMatch(
            batchPayload.extra?.fecha_nacimiento || "",
          ),
          matchNameTokens: normalizeNameTokensForMatch(batchPayload),
          label: buildBatchLabel(batchPayload, row.document_id),
        } satisfies BatchDocument;
      });

      const softGroups: BatchDocument[][] = [];
      const sortedByQuality = [...uploadedBatch].sort(
        (left, right) =>
          right.filledCount - left.filledCount ||
          left.missingCount - right.missingCount,
      );
      for (const item of sortedByQuality) {
        const groupIndex = softGroups.findIndex((group) =>
          group.some((member) => areLikelySameBatchUser(item, member)),
        );
        if (groupIndex >= 0) {
          softGroups[groupIndex].push(item);
        } else {
          softGroups.push([item]);
        }
      }
      const dominantGroup =
        softGroups.sort(
          (left, right) =>
            right.length - left.length ||
            right[0].filledCount - left[0].filledCount,
        )[0] || [];
      const batchGroup = dominantGroup.length > 1 ? dominantGroup : [];
      const primaryBatchDoc =
        [...(batchGroup.length > 0 ? batchGroup : uploadedBatch)].sort(
          (left, right) =>
            right.filledCount - left.filledCount ||
            left.missingCount - right.missingCount,
        )[0] ||
        batchGroup[0] ||
        uploadedBatch[0];

      if (!primaryBatchDoc) {
        setError("Upload completed, но не удалось выбрать основной документ.");
        return;
      }

      setBatchDocuments(batchGroup.length > 1 ? batchGroup : [primaryBatchDoc]);
      clearMergePreviewState();
      const primarySource = successes.find(
        (row) => row.document_id === primaryBatchDoc.documentId,
      );
      const data = primarySource || successes[successes.length - 1];
      setActiveClientId((data.client_id || "").trim());
      setDocumentId(data.document_id);
      setPayload(data.payload);
      applySourceKindMeta(data);
      syncNamePartsFromPayload(data.payload);
      setPreviewUrl(toUrl(data.preview_url, API_BASE));
      setFormUrl(data.form_url);
      setTargetUrl(data.target_url || data.form_url);
      setBrowserSessionId("");
      setBrowserSessionAlive(false);
      setBrowserCurrentUrl("");
      setMissingFields(data.missing_fields || []);
      applyMergeStateFromCandidates(
        data.merge_candidates,
        data.identity_source_document_id || "",
      );
      setClientMatch(null);
      await loadRelatedPreviews(
        data.document_id,
        toUrl(data.preview_url || "", API_BASE),
        data.merge_candidates || [],
        batchGroup.length > 1
          ? batchGroup.map((row) => row.documentId)
          : [primaryBatchDoc.documentId],
      );
      const nextStep = resolveStepAfterLoad(data, batchGroup.length > 1);
      setStepAndSync(nextStep);
      if (nextStep === "match") {
        await loadClientMatch(data.document_id);
      }
      if (failedNames.length > 0) {
        setError(`Часть файлов не загрузилась: ${failedNames.join(", ")}`);
      } else if (successes.length > 1 && batchGroup.length <= 1) {
        setError(
          "Загружено несколько файлов, но мягкое сопоставление не нашло единую группу клиента. Merge из пакета отключен — проверьте документы отдельно.",
        );
      }
      await loadSavedDocuments(savedDocsFilter);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Upload failed");
    } finally {
      setUploading(false);
    }
  }

  async function confirmData() {
    if (!payload || !documentId) return;
    const phoneCheck = validatePhone(telefonoCountryIso, telefonoLocalNumber);
    if (!phoneCheck.valid) {
      setError(phoneCheck.message);
      return;
    }
    setSaving(true);
    setError("");
    try {
      const composedNie = composeNie(niePrefix, nieNumber, nieSuffix);
      const normalizedPayload: Payload = {
        ...payload,
        identificacion: {
          ...payload.identificacion,
          nif_nie: composedNie || payload.identificacion.nif_nie,
          primer_apellido: primerApellido,
          segundo_apellido: segundoApellido,
          nombre: nombreSolo,
          nombre_apellidos: composeFullName(
            primerApellido,
            segundoApellido,
            nombreSolo,
          ),
        },
        domicilio: {
          ...payload.domicilio,
          telefono:
            composePhone(telefonoCountryIso, telefonoLocalNumber) ||
            payload.domicilio.telefono,
        },
        declarante: {
          ...payload.declarante,
          localidad:
            payload.declarante.localidad || payload.domicilio.municipio || "",
          fecha:
            composeDdmmyyyy(fechaDia, fechaMes, fechaAnio) ||
            payload.declarante.fecha,
          fecha_dia: fechaDia,
          fecha_mes: fechaMes,
          fecha_anio: fechaAnio,
        },
        extra: {
          ...(payload.extra || {}),
          telefono_country_iso: telefonoCountryIso,
          fecha_nacimiento:
            composeDdmmyyyy(
              fechaNacimientoDia,
              fechaNacimientoMes,
              fechaNacimientoAnio,
            ) ||
            payload.extra?.fecha_nacimiento ||
            "",
          fecha_nacimiento_dia: fechaNacimientoDia,
          fecha_nacimiento_mes: fechaNacimientoMes,
          fecha_nacimiento_anio: fechaNacimientoAnio,
        },
      };
      const confirmResp = await fetch(
        `${API_BASE}/api/documents/${documentId}/confirm`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ payload: normalizedPayload }),
        },
      );
      if (!confirmResp.ok) {
        throw new Error(await readErrorResponse(confirmResp));
      }
      const confirmed = await confirmResp.json();
      setActiveClientId(String(confirmed.client_id || "").trim());
      const confirmedPayload =
        (confirmed.payload as Payload) || normalizedPayload;
      setPayload(confirmedPayload);
      syncNamePartsFromPayload(confirmedPayload);
      setMissingFields(confirmed.missing_fields || []);
      applyMergeStateFromCandidates(
        confirmed.merge_candidates,
        confirmed.identity_source_document_id || "",
      );
      setMergePreview(confirmed.enrichment_preview || []);
      setMergeSkippedPreview(confirmed.enrichment_skipped || []);
      setMergeAppliedFields(
        (confirmed.enrichment_preview || []).map(
          (row: { field: string }) => row.field,
        ),
      );
      setMergeSkippedFields([]);
      setMergeFieldSelection(
        Object.fromEntries(
          (
            (confirmed.enrichment_preview || []) as Array<{ field: string }>
          ).map((row) => [row.field, true]),
        ),
      );
      await loadRelatedPreviews(
        documentId,
        previewUrl,
        confirmed.merge_candidates || [],
        batchDocuments.map((row) => row.documentId),
      );
      setStepAndSync("prepare");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Confirm failed");
    } finally {
      setSaving(false);
    }
  }

  async function refreshMergeCandidates() {
    if (!documentId) return;
    setMergeLoading(true);
    setError("");
    try {
      const resp = await fetch(
        `${API_BASE}/api/documents/${documentId}/merge-candidates`,
      );
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as {
        merge_candidates?: MergeCandidate[];
      };
      applyMergeStateFromCandidates(data.merge_candidates);
      clearMergePreviewState();
      await loadRelatedPreviews(
        documentId,
        previewUrl,
        data.merge_candidates || [],
        batchDocuments.map((row) => row.documentId),
      );
    } catch (e) {
      setError(
        e instanceof Error ? e.message : "Failed loading merge suggestions",
      );
    } finally {
      setMergeLoading(false);
    }
  }

  async function runMerge(
    apply: boolean,
    sourceDocumentId: string = selectedMergeSourceId,
  ) {
    if (!documentId) return;
    if (!sourceDocumentId) {
      setError("Выберите источник данных для merge.");
      return;
    }
    if (apply) {
      const selectedFields = mergePreview
        .map((row) => row.field)
        .filter((field) => Boolean(mergeFieldSelection[field]));
      if (selectedFields.length === 0) {
        setError("Отметьте минимум одно поле для применения.");
        return;
      }
      const ok = window.confirm(
        "Применить merge предложенных данных в текущий документ?",
      );
      if (!ok) return;
    }
    setMergeLoading(true);
    setError("");
    try {
      const resp = await fetch(
        `${API_BASE}/api/documents/${documentId}/enrich-by-identity`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            apply,
            source_document_id: sourceDocumentId,
            selected_fields: apply
              ? mergePreview
                  .map((row) => row.field)
                  .filter((field) => Boolean(mergeFieldSelection[field]))
              : undefined,
          }),
        },
      );
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as EnrichByIdentityResponse;
      setMergePreview(data.enrichment_preview || []);
      setMergeSkippedPreview(data.enrichment_skipped || []);
      setMergeAppliedFields(data.applied_fields || []);
      setMergeSkippedFields(data.skipped_fields || []);
      setMergeFieldSelection(
        Object.fromEntries(
          (data.enrichment_preview || []).map((row) => [row.field, true]),
        ),
      );
      setMissingFields(data.missing_fields || []);
      applyMergeStateFromCandidates(
        data.merge_candidates,
        data.identity_source_document_id || sourceDocumentId,
      );
      await loadRelatedPreviews(
        documentId,
        previewUrl,
        data.merge_candidates,
        batchDocuments.map((row) => row.documentId),
      );
      if (apply && data.payload) {
        setPayload(data.payload);
        syncNamePartsFromPayload(data.payload);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Merge failed");
    } finally {
      setMergeLoading(false);
    }
  }

  async function openManagedSession(
    targetUrlOverride?: string,
    options?: { headless?: boolean; slowmo?: number },
  ): Promise<{
    session_id: string;
    current_url: string;
    target_url: string;
  } | null> {
    if (!documentId) return null;
    const resolvedTargetUrl = (targetUrlOverride ?? targetUrl).trim();
    if (!resolvedTargetUrl) {
      setError("Укажите адрес страницы или PDF.");
      return null;
    }
    setSaving(true);
    setError("");
    try {
      const resp = await fetch(
        `${CLIENT_AGENT_BASE}/api/browser-session/open`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            target_url: resolvedTargetUrl,
            headless: options?.headless ?? false,
            slowmo: options?.slowmo ?? 40,
            timeout_ms: 30000,
          }),
        },
      );
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = await resp.json();
      setBrowserSessionId(data.session_id || "");
      setBrowserSessionAlive(Boolean(data.alive));
      setBrowserCurrentUrl(data.current_url || "");
      setTargetUrl(data.target_url || resolvedTargetUrl);
      return {
        session_id: data.session_id || "",
        current_url: data.current_url || "",
        target_url: data.target_url || resolvedTargetUrl,
      };
    } catch (e) {
      setError(
        e instanceof Error ? e.message : "Failed to open browser session",
      );
      return null;
    } finally {
      setSaving(false);
    }
  }

  async function runAutofillFromManagedSession(
    sessionIdOverride?: string,
    currentUrlOverride?: string,
    openFilledPdfAfter = false,
  ): Promise<string> {
    if (!payload || !documentId) return "";
    setAutofill(null);
    setFilledPdfNonce(0);
    const sessionId = sessionIdOverride || browserSessionId;
    if (!sessionId) {
      setError(
        "Сначала нажмите 'Перейти по адресу' и откройте управляемую сессию.",
      );
      return "";
    }
    setSaving(true);
    setError("");
    try {
      let payloadForAutofill = payload;
      if (activeClientId.trim()) {
        try {
          const clientResp = await fetch(
            `${API_BASE}/api/crm/clients/${encodeURIComponent(activeClientId)}`,
          );
          if (clientResp.ok) {
            const clientCard = (await clientResp.json()) as ClientCardResponse;
            if (clientCard.profile_payload) {
              payloadForAutofill = clientCard.profile_payload;
            }
          }
        } catch {
          // Keep local payload fallback for resilience.
        }
      }
      let currentUrl = (currentUrlOverride || "").trim();
      if (currentUrl === "about:blank") currentUrl = "";
      if (!currentUrl) {
        const stateResp = await fetch(
          `${CLIENT_AGENT_BASE}/api/browser-session/${sessionId}/state`,
        );
        if (!stateResp.ok) {
          throw new Error(await readErrorResponse(stateResp));
        }
        const stateData = await stateResp.json();
        currentUrl = stateData.current_url || targetUrl || formUrl;
      }
      if (currentUrl === "about:blank") {
        currentUrl = (targetUrl || formUrl).trim();
      }
      if (!currentUrl) {
        throw new Error("Current URL is empty in browser session.");
      }

      const templateResp = await fetch(
        `${API_BASE}/api/documents/${documentId}/browser-session/template`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            current_url: currentUrl,
            payload: payloadForAutofill,
            fill_strategy: "strict_template",
          }),
        },
      );
      if (!templateResp.ok) {
        throw new Error(await readErrorResponse(templateResp));
      }
      const templateData = await templateResp.json();

      const autoResp = await fetch(
        `${CLIENT_AGENT_BASE}/api/browser-session/${sessionId}/fill`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            payload: payloadForAutofill,
            timeout_ms: 25000,
            explicit_mappings: templateData.effective_mappings || [],
            fill_strategy: templateData.fill_strategy || "strict_template",
            document_id: documentId,
          }),
        },
      );
      if (!autoResp.ok) {
        throw new Error(await readErrorResponse(autoResp));
      }
      const autoData: AutofillPreviewResponse = await autoResp.json();
      const filledPdfUrl = autoData.filled_pdf_url
        ? toUrl(autoData.filled_pdf_url, CLIENT_AGENT_BASE)
        : "";
      setAutofill({
        ...autoData,
        filled_pdf_url: filledPdfUrl,
      });
      setFilledPdfNonce(Date.now());
      setFormUrl(currentUrl);
      setBrowserCurrentUrl(currentUrl);
      setStepAndSync("autofill");
      if (openFilledPdfAfter && filledPdfUrl) {
        const sep = filledPdfUrl.includes("?") ? "&" : "?";
        window.open(
          `${filledPdfUrl}${sep}v=${Date.now()}`,
          "_blank",
          "noopener,noreferrer",
        );
      }
      return filledPdfUrl;
    } catch (e) {
      setError(
        e instanceof Error
          ? e.message
          : "Autofill in opened browser session failed",
      );
      return "";
    } finally {
      setSaving(false);
    }
  }

  async function downloadFilledPdfForPdfTarget() {
    if (!targetUrl.trim()) {
      setError("Укажите адрес страницы или PDF.");
      return;
    }
    const opened = await openManagedSession(targetUrl.trim(), {
      headless: true,
      slowmo: 0,
    });
    if (!opened?.session_id) return;
    const currentUrl =
      opened.target_url ||
      (opened.current_url === "about:blank" ? "" : opened.current_url) ||
      targetUrl.trim();
    const filledPdfUrl = await runAutofillFromManagedSession(
      opened.session_id,
      currentUrl,
      true,
    );
    if (!filledPdfUrl) {
      setError(
        "Заполненный PDF не был сформирован. Проверьте маппинг полей для этого документа.",
      );
    }
    try {
      await fetch(
        `${CLIENT_AGENT_BASE}/api/browser-session/${opened.session_id}/close`,
        {
          method: "POST",
        },
      );
    } catch {
      // best-effort close
    }
    setBrowserSessionId("");
    setBrowserSessionAlive(false);
    setBrowserCurrentUrl("");
  }

  const phoneCountry =
    PHONE_COUNTRIES.find((item) => item.iso === telefonoCountryIso) ||
    PHONE_COUNTRIES[0];
  const phoneValidation = validatePhone(
    telefonoCountryIso,
    telefonoLocalNumber,
  );
  const isPdfTarget = isPdfTargetUrl(targetUrl);
  const batchDocumentIdSet = new Set(
    batchDocuments.map((row) => row.documentId),
  );
  const externalMergeCandidates = mergeCandidates.filter(
    (candidate) => !batchDocumentIdSet.has(candidate.document_id),
  );
  const batchMergeSources = batchDocuments.filter(
    (row) => row.documentId !== documentId,
  );
  const showClientMatchStep =
    step === "match" || Boolean(clientMatch?.identity_match_found);
  const workflowSteps: Array<{ key: Step; label: string }> = [
    { key: "upload", label: "Загрузка" },
    ...(showClientMatchStep ? [{ key: "match" as const, label: "Client Match" }] : []),
    ...(hasMergeSources ? [{ key: "merge" as const, label: "Merge" }] : []),
    { key: "review", label: "Проверка" },
    { key: "prepare", label: "Подготовка" },
    { key: "autofill", label: "Передача" },
  ];
  const activeStepIndex = workflowSteps.findIndex((item) => item.key === step);
  const workspaceBadge = saving || uploading ? "В работе" : "Готов";
  const totalProblems = missingFields.length + mergeSkippedFields.length;
  const sourceKindConfidencePct = Math.round(sourceKindConfidence * 100);
  const fieldLabelMap: Record<string, string> = {
    "domicilio.telefono": "Teléfono",
    "extra.email": "Email",
    "domicilio.nombre_via": "Domicilio",
    "identificacion.nif_nie": "NIE",
    "extra.fecha_nacimiento": "Fecha nac.",
  };
  const isBatchMode = batchDocuments.length > 1;
  const sourceScoreById = new Map(
    mergeCandidates.map((candidate) => [
      candidate.document_id,
      Number(candidate.score) || 0,
    ]),
  );
  const mergeDiffRows = [
    ...(mergePreview || []).map((row) => ({
      field: row.field,
      current: row.current_value || "",
      suggested: row.suggested_value || "",
      source: row.source || "",
      kind: "apply" as const,
    })),
    ...(mergeSkippedPreview || []).map((row) => ({
      field: row.field,
      current: row.current_value || "",
      suggested: row.suggested_value || "",
      source: row.source || "",
      kind: row.reason === "equal" ? ("equal" as const) : ("conflict" as const),
    })),
  ].filter((row) => {
    if (!row.suggested.trim()) return false;
    if (row.kind === "equal") return false;
    return (
      row.current.trim().toUpperCase() !== row.suggested.trim().toUpperCase()
    );
  });
  const resolveRowConfidence = (
    row: (typeof mergeDiffRows)[number],
  ): number => {
    const sourceId =
      row.source || selectedBatchSourceId || selectedMergeSourceId || "";
    const score = sourceScoreById.get(sourceId);
    let confidence: number;
    if (typeof score === "number" && score > 0) {
      confidence = Math.max(55, Math.min(99, Math.round(60 + score * 0.35)));
    } else if (sourceId && batchDocumentIdSet.has(sourceId)) {
      confidence = row.kind === "apply" ? 92 : 48;
    } else {
      confidence = row.kind === "apply" ? 80 : 45;
    }
    if (row.kind === "conflict") {
      confidence = Math.min(confidence, 55);
    }
    return confidence;
  };
  const fallbackPreviewItems: RelatedPreviewItem[] =
    documentId && previewUrl
      ? [
          {
            documentId,
            label: "Текущий документ",
            previewUrl,
            isCurrent: true,
          },
        ]
      : [];
  const effectivePreviewItems =
    relatedPreviewItems.length > 0 ? relatedPreviewItems : fallbackPreviewItems;
  const activePreviewItem =
    effectivePreviewItems.find(
      (item) => item.documentId === activePreviewDocumentId,
    ) || effectivePreviewItems[0];
  const activePreviewUrl = activePreviewItem?.previewUrl || "";
  const activePreviewIsPdf = activePreviewUrl.toLowerCase().includes(".pdf");
  const activePreviewIsImage = Boolean(activePreviewUrl) && !activePreviewIsPdf;
  const preferredPresetKey = "tasa_790_012";
  const tasaPresets = TARGET_URL_PRESETS.filter((preset) =>
    preset.key.startsWith("tasa_"),
  );
  const formPresets = TARGET_URL_PRESETS.filter(
    (preset) => !preset.key.startsWith("tasa_"),
  );

  useEffect(() => {
    setPreviewZoom(1);
  }, [activePreviewDocumentId, activePreviewUrl]);

  useEffect(() => {
    if (step === "merge" && !hasMergeSources) {
      setStepAndSync("review");
    }
  }, [hasMergeSources, step]);

  return (
    <main className="min-h-screen bg-gradient-to-b from-background to-muted/30">
      <WorkspaceHeader activeTab="upload" workspaceBadge={workspaceBadge} />

      <div className="mx-auto max-w-[1600px] p-4 lg:p-6">
        <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold">Tasa OCR Workspace</h1>
            <p className="text-sm text-muted-foreground">
              Поток: загрузка, client match, merge, проверка данных и handoff
              в autofill.
            </p>
          </div>
          <div className="flex items-center gap-2">
            {step !== "upload" ? (
              <Button
                variant="secondary"
                onClick={resetWorkflow}
                disabled={saving || uploading}
              >
                Начать сначала
              </Button>
            ) : null}
            <Badge variant="outline">{step.toUpperCase()}</Badge>
          </div>
        </div>

        <div className="mb-5 flex flex-wrap gap-2 rounded-xl border bg-card p-3">
          {workflowSteps.map((item, index) => {
            const state =
              index < activeStepIndex
                ? "done"
                : index === activeStepIndex
                  ? "active"
                  : "idle";
            return (
              <div
                key={item.key}
                className={cn(
                  "flex min-w-[160px] flex-1 items-center gap-2 rounded-lg border px-3 py-2 transition",
                  state === "active" && "border-primary bg-primary/10",
                  state === "done" &&
                    "border-emerald-300 bg-emerald-50 text-emerald-700",
                  state === "idle" &&
                    "border-border bg-background text-muted-foreground",
                )}
              >
                <div
                  className={cn(
                    "flex h-5 w-5 items-center justify-center rounded-full text-[11px] font-semibold",
                    state === "active" && "bg-primary text-primary-foreground",
                    state === "done" && "bg-emerald-600 text-white",
                    state === "idle" && "bg-muted text-muted-foreground",
                  )}
                >
                  {state === "done" ? (
                    <Check className="h-3.5 w-3.5" />
                  ) : (
                    index + 1
                  )}
                </div>
                <span className="text-xs font-medium">{item.label}</span>
              </div>
            );
          })}
        </div>

        {error ? (
          <Card className="mb-4 border-red-300">
            <CardContent className="flex items-center gap-2 p-4 text-sm text-red-700">
              <AlertCircle className="h-4 w-4" />
              <span>{error}</span>
            </CardContent>
          </Card>
        ) : null}
      </div>

      {step === "upload" ? (
        <div className="mx-auto grid max-w-[1200px] grid-cols-1 gap-4 px-4 pb-6 lg:px-6">
          <Card className="overflow-hidden">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <FileUp className="h-5 w-5 text-primary" />
                Загрузка исходника
              </CardTitle>
              <CardDescription>
                Поддерживаются изображения и PDF до 20MB.
              </CardDescription>
            </CardHeader>
            <CardContent className="relative space-y-4">
              <div
                className={`rounded-xl border-2 border-dashed p-10 text-center transition ${
                  dragOver ? "border-primary bg-primary/5" : "border-border"
                }`}
                onDragOver={(e) => {
                  e.preventDefault();
                  setDragOver(true);
                }}
                onDragLeave={(e) => {
                  e.preventDefault();
                  setDragOver(false);
                }}
                onDrop={(e) => {
                  e.preventDefault();
                  setDragOver(false);
                  const next = Array.from(e.dataTransfer.files || []);
                  onFilesSelected(next);
                }}
              >
                <div className="mx-auto mb-4 flex h-16 w-16 items-center justify-center rounded-2xl bg-muted">
                  <FileUp className="h-8 w-8 text-muted-foreground" />
                </div>
                <p className="text-base font-semibold">
                  Перетащите документ клиента
                </p>
                <p className="mt-1 text-sm text-muted-foreground">
                  Паспорт, NIE, виза, анкета, PDF/PNG/JPG
                </p>
                <Input
                  type="file"
                  accept=".jpg,.jpeg,.png,.pdf"
                  multiple
                  className="mx-auto mt-4 max-w-md"
                  onChange={(e) =>
                    onFilesSelected(Array.from(e.target.files || []))
                  }
                />
                {files.length > 0 ? (
                  <p className="mt-3 text-sm font-medium">
                    Выбрано файлов: {files.length}
                  </p>
                ) : null}
              </div>
              <div className="grid gap-4 lg:grid-cols-[1fr_auto]">
                <div className="space-y-2">
                  <Label>Тип документа (опционально)</Label>
                  <select
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    value={uploadSourceKind}
                    onChange={(e) =>
                      setUploadSourceKind(e.target.value as UploadSourceKind)
                    }
                  >
                    <option value="">-- автоопределение --</option>
                    <option value="anketa">Анкета</option>
                    <option value="fmiliar">Анкета familiar</option>
                    <option value="passport">Паспорт</option>
                    <option value="nie_tie">NIE/TIE/DNI</option>
                    <option value="visa">Виза</option>
                  </select>
                </div>
                <Button
                  className="self-end"
                  onClick={uploadDocument}
                  disabled={files.length === 0 || uploading}
                >
                  {uploading ? (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  ) : (
                    <Sparkles className="mr-2 h-4 w-4" />
                  )}
                  Запустить OCR
                </Button>
              </div>
              {uploading ? (
                <div className="absolute inset-0 z-20 flex items-center justify-center rounded-lg bg-background/95 p-6 backdrop-blur-sm">
                  <div className="w-full max-w-md rounded-xl border bg-card p-4 shadow-sm">
                    <div className="mb-3 flex items-center justify-between gap-2">
                      <div className="text-sm font-semibold">
                        OCR в процессе...
                      </div>
                      <Badge variant="secondary">анализ документов</Badge>
                    </div>
                    <div className="flex items-center gap-4">
                      <div className="ocr-scan-card">
                        <div className="ocr-scan-line" />
                      </div>
                      <div className="space-y-1 text-xs text-muted-foreground">
                        <div className="flex items-center gap-2">
                          <Loader2 className="h-3.5 w-3.5 animate-spin text-primary" />
                          Извлечение полей и проверка идентичности
                        </div>
                        <div>
                          Определение типа документа и подготовка payload
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              ) : null}
            </CardContent>
          </Card>
        </div>
      ) : null}

      {step === "match" && payload ? (
        <div className="mx-auto grid max-w-[1600px] grid-cols-1 gap-4 px-4 pb-6 lg:px-6">
          <Card className="overflow-auto xl:h-[calc(100vh-220px)]">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Search className="h-5 w-5 text-primary" />
                Client Match
              </CardTitle>
              <CardDescription>
                На этом шаге решается, связывать ли текущий документ с
                существующей карточкой CRM. От этого зависит, какие данные
                система предложит для merge.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="rounded-xl border bg-muted/30 p-3 text-sm">
                <div className="mb-1 font-semibold">Как работает шаг</div>
                <div className="text-muted-foreground">
                  Если найден один сильный кандидат (score 90+), система
                  автоматически подтверждает его и сразу ведёт дальше.
                </div>
              </div>
              {!clientMatch?.identity_match_found ? (
                <div className="rounded-xl border border-amber-300 bg-amber-50 p-4 text-sm text-amber-900">
                  Совпадений по клиенту не найдено. Документ останется
                  самостоятельным, можно перейти к проверке вручную.
                </div>
              ) : (
                <div className="rounded-xl border border-emerald-300 bg-emerald-50 p-4">
                  <div className="mb-1 text-xs uppercase tracking-wide text-emerald-700">
                    Найдено совпадение
                  </div>
                  <div className="text-base font-semibold">
                    {clientMatch.client_match?.name || "Клиент из CRM"}
                  </div>
                  <div className="text-sm text-emerald-800">
                    Документ:{" "}
                    {clientMatch.client_match?.document_number || "без номера"}{" "}
                    · score {clientMatch.client_match?.score ?? 0}
                  </div>
                </div>
              )}

              <div className="grid gap-3 sm:grid-cols-3">
                <div className="rounded-lg border p-3">
                  <div className="text-xs text-muted-foreground">
                    Текущий NIE
                  </div>
                  <div className="text-sm font-semibold">
                    {payload.identificacion?.nif_nie || "—"}
                  </div>
                </div>
                <div className="rounded-lg border p-3">
                  <div className="text-xs text-muted-foreground">Решение</div>
                  <div className="text-sm font-semibold">
                    {clientMatch?.client_match_decision || "pending"}
                  </div>
                </div>
                <div className="rounded-lg border p-3">
                  <div className="text-xs text-muted-foreground">
                    Кандидатов
                  </div>
                  <div className="text-sm font-semibold">
                    {clientMatch?.merge_candidates?.length || 0}
                  </div>
                </div>
              </div>

              <div className="grid gap-3 md:grid-cols-3">
                <div className="rounded-lg border p-3">
                  <Button
                    className="w-full"
                    onClick={() => void resolveClientMatch("confirm")}
                    disabled={
                      saving ||
                      !clientMatch?.identity_match_found ||
                      !clientMatch?.client_match?.document_id
                    }
                  >
                    {saving ? (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    ) : (
                      <Check className="mr-2 h-4 w-4" />
                    )}
                    Связать с клиентом
                  </Button>
                  <p className="mt-2 text-xs text-muted-foreground">
                    Подтверждает найденного клиента и включает merge из этой
                    карточки.
                  </p>
                </div>
                <div className="rounded-lg border p-3">
                  <Button
                    variant="outline"
                    className="w-full"
                    onClick={() => void resolveClientMatch("reject")}
                    disabled={saving}
                  >
                    Оставить отдельным
                  </Button>
                  <p className="mt-2 text-xs text-muted-foreground">
                    Не связывает документ с CRM-клиентом. Merge по этому
                    совпадению не используется.
                  </p>
                </div>
                <div className="rounded-lg border p-3">
                  <Button
                    variant="secondary"
                    className="w-full"
                    onClick={() => setStepAndSync(hasMergeSources ? "merge" : "review")}
                    disabled={saving}
                  >
                    Перейти без решения
                  </Button>
                  <p className="mt-2 text-xs text-muted-foreground">
                    Просто открывает следующий шаг без подтверждения матча.
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      ) : null}

      {step === "merge" && payload ? (
        <div className="mx-auto grid max-w-[1600px] grid-cols-1 gap-4 px-4 pb-6 lg:px-6">
          <Card className="overflow-auto xl:h-[calc(100vh-220px)]">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Combine className="h-5 w-5 text-primary" />
                Merge данных
              </CardTitle>
              <CardDescription>
                Сначала объедините данные из связанных источников, затем
                переходите к ручной проверке полей.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-5">
              {isBatchMode ? (
                <section className="space-y-2 rounded-xl border border-emerald-300 bg-emerald-50/60 p-3">
                  <div className="flex items-center justify-between gap-2">
                    <h3 className="text-sm font-semibold">
                      Пакет документов клиента
                    </h3>
                    <Badge variant="secondary">
                      в пакете: {batchDocuments.length}
                    </Badge>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Файлы загружены как один набор. Сначала объедините поля из
                    документов пакета, затем при необходимости используйте
                    внешний merge из CRM.
                  </p>
                  <Label>Источник из пакета</Label>
                  <select
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    value={selectedBatchSourceId}
                    onChange={(e) => {
                      setSelectedBatchSourceId(e.target.value);
                      clearMergePreviewState();
                    }}
                  >
                    <option value="">-- выбрать документ из пакета --</option>
                    {batchMergeSources.map((candidate) => (
                      <option
                        key={candidate.documentId}
                        value={candidate.documentId}
                      >
                        {candidate.label}
                      </option>
                    ))}
                  </select>
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => runMerge(false, selectedBatchSourceId)}
                      disabled={
                        mergeLoading || saving || !selectedBatchSourceId
                      }
                    >
                      Показать diff
                    </Button>
                    <Button
                      size="sm"
                      onClick={() => runMerge(true, selectedBatchSourceId)}
                      disabled={
                        mergeLoading || saving || !selectedBatchSourceId
                      }
                    >
                      Применить из пакета
                    </Button>
                  </div>
                </section>
              ) : null}

              <section className="space-y-2 rounded-xl border p-3">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-sm font-semibold">Merge данных</h3>
                  <Badge variant="secondary">
                    внешних источников: {externalMergeCandidates.length}
                  </Badge>
                </div>
                <p className="text-xs text-muted-foreground">
                  Система предлагает дозаполнение из схожих карточек CRM вне
                  текущего пакета.
                </p>
                <Label>Источник для merge</Label>
                <select
                  className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  value={selectedMergeSourceId}
                  onChange={(e) => {
                    const nextSourceId = e.target.value;
                    setSelectedMergeSourceId(nextSourceId);
                    clearMergePreviewState();
                    if (nextSourceId) {
                      void runMerge(false, nextSourceId);
                    }
                  }}
                >
                  <option value="">-- выбрать документ --</option>
                  {externalMergeCandidates.map((candidate) => (
                    <option
                      key={candidate.document_id}
                      value={candidate.document_id}
                    >
                      {candidate.name || "Без имени"} |{" "}
                      {candidate.document_number || "без номера"} | score{" "}
                      {candidate.score}
                    </option>
                  ))}
                </select>
                {externalMergeCandidates.length === 0 ? (
                  <div className="text-xs text-muted-foreground">
                    Внешних кандидатов CRM нет. Используйте merge из пакета
                    документов выше.
                  </div>
                ) : null}
                {mergeDiffRows.length > 0 ? (
                  <div className="overflow-auto rounded-xl border">
                    <table className="min-w-full text-sm">
                      <thead className="bg-muted/60">
                        <tr>
                          <th className="px-3 py-3 text-left font-semibold">
                            Поле
                          </th>
                          <th className="px-3 py-3 text-left font-semibold">
                            Текущее
                          </th>
                          <th className="px-3 py-3 text-left font-semibold">
                            Из донора
                          </th>
                          <th className="px-3 py-3 text-left font-semibold">
                            Применить
                          </th>
                          <th className="px-3 py-3 text-left font-semibold">
                            Уверенность
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {mergeDiffRows.map((row) => (
                          <tr
                            key={`${row.field}-${row.current}-${row.suggested}-${row.kind}`}
                            className="border-t"
                          >
                            <td className="px-3 py-3 font-medium">
                              {fieldLabelMap[row.field] || row.field}
                            </td>
                            <td className="px-3 py-3">
                              {row.current || "(пусто)"}
                            </td>
                            <td className="px-3 py-3">{row.suggested}</td>
                            <td className="px-3 py-3">
                              {row.kind === "apply" ? (
                                <label className="inline-flex cursor-pointer items-center gap-2 text-emerald-700">
                                  <input
                                    type="checkbox"
                                    className="h-4 w-4"
                                    checked={Boolean(
                                      mergeFieldSelection[row.field],
                                    )}
                                    onChange={(e) =>
                                      setMergeFieldSelection((prev) => ({
                                        ...prev,
                                        [row.field]: e.target.checked,
                                      }))
                                    }
                                  />
                                  <span>Применить</span>
                                </label>
                              ) : (
                                <label className="inline-flex items-center gap-2 text-amber-700">
                                  <input
                                    type="checkbox"
                                    className="h-4 w-4"
                                    checked={false}
                                    disabled
                                  />
                                  <span>Конфликт</span>
                                </label>
                              )}
                            </td>
                            <td className="px-3 py-3">
                              <Badge
                                variant="outline"
                                className={
                                  resolveRowConfidence(row) >= 85
                                    ? "border-emerald-300 text-emerald-700"
                                    : resolveRowConfidence(row) >= 65
                                      ? "border-amber-300 text-amber-700"
                                      : "border-red-300 text-red-700"
                                }
                              >
                                {resolveRowConfidence(row)}%
                              </Badge>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="text-xs text-muted-foreground">
                    Загружаем предложения merge. Если таблица не появилась,
                    выберите источник вручную.
                  </div>
                )}
              </section>

              <div className="grid grid-cols-1 gap-2 md:grid-cols-3">
                <div className="space-y-1 rounded-lg border p-3">
                  <Button
                    className="w-full"
                    onClick={() => runMerge(true)}
                    disabled={
                      mergeLoading ||
                      saving ||
                      !selectedMergeSourceId ||
                      mergePreview.length === 0 ||
                      mergePreview
                        .map((row) => row.field)
                        .filter((field) => Boolean(mergeFieldSelection[field]))
                        .length === 0
                    }
                  >
                    {mergeLoading ? (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    ) : (
                      <Combine className="mr-2 h-4 w-4" />
                    )}
                    Применить merge
                  </Button>
                  <p className="text-xs text-muted-foreground">
                    Применит отмеченные поля из выбранного источника в текущий
                    документ.
                  </p>
                </div>
                <div className="space-y-1 rounded-lg border p-3">
                  <Button
                    variant="secondary"
                    className="w-full"
                    onClick={() => setStepAndSync("review")}
                    disabled={saving || mergeLoading}
                  >
                    Перейти к проверке полей
                  </Button>
                  <p className="text-xs text-muted-foreground">
                    Откроет форму ручной проверки с текущими значениями, включая
                    результат merge.
                  </p>
                </div>
                <div className="space-y-1 rounded-lg border p-3">
                  <Button
                    variant="outline"
                    className="w-full"
                    onClick={() => setStepAndSync("review")}
                    disabled={saving || mergeLoading}
                  >
                    Продолжить без merge
                  </Button>
                  <p className="text-xs text-muted-foreground">
                    Пропустит перенос полей и оставит данные документа как есть.
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      ) : null}

      {step === "review" && payload ? (
        <div className="mx-auto grid max-w-[1600px] grid-cols-1 gap-4 px-4 pb-6 lg:px-6 xl:grid-cols-[560px_1fr]">
          <div className="flex min-h-0 flex-col gap-3">
            <Card className="overflow-auto xl:h-[calc(100vh-300px)]">
              <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Wand2 className="h-5 w-5 text-primary" />
                Проверка и правка данных
              </CardTitle>
              <CardDescription>
                Проверьте значения и подтвердите документ перед handoff.
              </CardDescription>
              </CardHeader>
              <CardContent className="space-y-5">
              <section
                className={cn(
                  "rounded-xl border p-3 text-sm",
                  sourceKindRequiresReview
                    ? "border-amber-300 bg-amber-50"
                    : "border-emerald-300 bg-emerald-50",
                )}
              >
                <div className="flex flex-wrap items-center gap-2">
                  <span className="font-semibold">Тип документа:</span>
                  <Badge variant="outline">
                    {sourceKindDetected || "не определен"}
                  </Badge>
                  {sourceKindDetected ? (
                    sourceKindAuto ? (
                      <Badge variant="secondary">авто</Badge>
                    ) : (
                      <Badge variant="secondary">ручной override</Badge>
                    )
                  ) : null}
                  {!sourceKindAuto && sourceKindInput ? (
                    <span className="text-xs text-muted-foreground">
                      input: {sourceKindInput}
                    </span>
                  ) : null}
                  {sourceKindDetected ? (
                    <span className="text-xs text-muted-foreground">
                      confidence: {sourceKindConfidencePct}%
                    </span>
                  ) : null}
                </div>
                {sourceKindRequiresReview ? (
                  <p className="mt-2 text-xs text-amber-900">
                    Низкая уверенность автоопределения. Можно перезапустить OCR
                    ниже с ручным выбором типа документа.
                  </p>
                ) : null}
                <div className="mt-3 grid gap-2 md:grid-cols-[1fr_auto]">
                  <select
                    className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    value={reviewSourceKind}
                    onChange={(e) =>
                      setReviewSourceKind(e.target.value as UploadSourceKind)
                    }
                  >
                    <option value="">-- выбрать тип --</option>
                    <option value="anketa">Анкета</option>
                    <option value="fmiliar">Анкета familiar</option>
                    <option value="passport">Паспорт</option>
                    <option value="nie_tie">NIE/TIE/DNI</option>
                    <option value="visa">Виза</option>
                  </select>
                  <Button
                    variant="outline"
                    onClick={() => void reprocessOcrWithManualSourceKind()}
                    disabled={reprocessOcrLoading || saving}
                  >
                    {reprocessOcrLoading ? (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    ) : (
                      <Wand2 className="mr-2 h-4 w-4" />
                    )}
                    Перезапустить OCR
                  </Button>
                </div>
              </section>

              <Separator />

              <section className="space-y-2">
                <h3 className="text-sm font-semibold">Identificación</h3>
                <Label>NIE (буква + 7 цифр + буква)</Label>
                <div className="grid grid-cols-3 gap-2">
                  <Input
                    placeholder="Y"
                    value={niePrefix}
                    onChange={(e) => patchNiePart("prefix", e.target.value)}
                  />
                  <Input
                    placeholder="1234567"
                    value={nieNumber}
                    onChange={(e) => patchNiePart("number", e.target.value)}
                  />
                  <Input
                    placeholder="X"
                    value={nieSuffix}
                    onChange={(e) => patchNiePart("suffix", e.target.value)}
                  />
                </div>
                <Label>Pasaporte (опционально)</Label>
                <Input
                  value={payload.identificacion.pasaporte || ""}
                  onChange={(e) =>
                    patchPayload("identificacion", "pasaporte", e.target.value)
                  }
                />
                <div className="grid grid-cols-1 gap-2 pt-1 md:grid-cols-3">
                  <div>
                    <Label>Primer apellido</Label>
                    <Input
                      value={primerApellido}
                      onChange={(e) =>
                        patchSplitNameAndCompose(
                          "primer_apellido",
                          e.target.value,
                        )
                      }
                    />
                  </div>
                  <div>
                    <Label>Segundo apellido</Label>
                    <Input
                      value={segundoApellido}
                      onChange={(e) =>
                        patchSplitNameAndCompose(
                          "segundo_apellido",
                          e.target.value,
                        )
                      }
                    />
                  </div>
                  <div>
                    <Label>Nombre</Label>
                    <Input
                      value={nombreSolo}
                      onChange={(e) =>
                        patchSplitNameAndCompose("nombre", e.target.value)
                      }
                    />
                  </div>
                </div>
              </section>

              <Separator />

              <section className="space-y-2">
                <h3 className="text-sm font-semibold">Domicilio</h3>
                <div className="rounded-md border bg-muted/30 p-3">
                  <Label>Адрес одной строкой</Label>
                  <div className="mt-1 flex flex-col gap-2 md:flex-row">
                    <Input
                      className="md:flex-1"
                      placeholder="Calle Enrique Monsonis Domingo, 5, 2 B, Alicante, 03013"
                      value={addressLineInput}
                      onChange={(e) => setAddressLineInput(e.target.value)}
                    />
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      className="md:w-auto"
                      onClick={() => void autofillAddressFromStreet()}
                      disabled={addressAutofillLoading || !documentId.trim()}
                    >
                      {addressAutofillLoading ? (
                        <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />
                      ) : null}
                      Дозаполнить адрес из строки
                    </Button>
                  </div>
                  <p className="mt-1 text-xs text-muted-foreground">
                    Парсинг + геокод сверят адрес и заполнят поля ниже.
                  </p>
                </div>
                <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                  <div>
                    <Label>Tipo vía</Label>
                    <Input
                      value={payload.domicilio.tipo_via}
                      onChange={(e) =>
                        patchPayload("domicilio", "tipo_via", e.target.value)
                      }
                    />
                  </div>
                  <div className="md:col-span-2">
                    <Label>Nombre vía</Label>
                    <Input
                      className="mt-1"
                      value={payload.domicilio.nombre_via}
                      onChange={(e) =>
                        patchPayload("domicilio", "nombre_via", e.target.value)
                      }
                    />
                  </div>
                  <div>
                    <Label>Número</Label>
                    <Input
                      value={payload.domicilio.numero}
                      onChange={(e) =>
                        patchPayload("domicilio", "numero", e.target.value)
                      }
                    />
                  </div>
                  <div>
                    <Label>Escalera</Label>
                    <Input
                      value={payload.domicilio.escalera}
                      onChange={(e) =>
                        patchPayload("domicilio", "escalera", e.target.value)
                      }
                    />
                  </div>
                  <div>
                    <Label>Piso</Label>
                    <Input
                      value={payload.domicilio.piso}
                      onChange={(e) =>
                        patchPayload("domicilio", "piso", e.target.value)
                      }
                    />
                  </div>
                  <div>
                    <Label>Puerta</Label>
                    <Input
                      value={payload.domicilio.puerta}
                      onChange={(e) =>
                        patchPayload("domicilio", "puerta", e.target.value)
                      }
                    />
                  </div>
                  <div className="grid grid-cols-1 gap-2 md:col-span-2 md:grid-cols-3">
                    <div>
                      <Label>Municipio</Label>
                      <Input
                        value={payload.domicilio.municipio}
                        onChange={(e) =>
                          patchPayload("domicilio", "municipio", e.target.value)
                        }
                      />
                    </div>
                    <div>
                      <Label>Provincia</Label>
                      <Input
                        value={payload.domicilio.provincia}
                        onChange={(e) =>
                          patchPayload("domicilio", "provincia", e.target.value)
                        }
                      />
                    </div>
                    <div>
                      <Label>CP</Label>
                      <Input
                        value={payload.domicilio.cp}
                        onChange={(e) =>
                          patchPayload("domicilio", "cp", e.target.value)
                        }
                      />
                    </div>
                  </div>
                </div>
              </section>

              <Separator />

              <section className="space-y-2">
                <h3 className="text-sm font-semibold">
                  Дополнительные персональные поля (CRM)
                </h3>
                <div className="space-y-2 rounded-md border p-3">
                  <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                    Контакты
                  </h4>
                  <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                    <div className="md:col-span-2">
                      <Label>Email</Label>
                      <Input
                        value={payload.extra?.email || ""}
                        onChange={(e) => patchExtra("email", e.target.value)}
                      />
                    </div>
                    <div className="md:col-span-2">
                      <Label>Teléfono</Label>
                      <div className="mt-1 flex flex-col gap-2 md:flex-row">
                        <select
                          className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring md:w-[260px] md:flex-none"
                          value={telefonoCountryIso}
                          onChange={(e) =>
                            patchPhonePart("countryIso", e.target.value)
                          }
                        >
                          {PHONE_COUNTRIES.map((item) => (
                            <option key={item.iso} value={item.iso}>
                              {item.flag} {item.iso} ({item.dialCode}){" "}
                              {item.label}
                            </option>
                          ))}
                        </select>
                        <Input
                          className="md:flex-1"
                          placeholder={
                            phoneCountry.iso === "RU"
                              ? "9123456789"
                              : "624731544"
                          }
                          value={telefonoLocalNumber}
                          onChange={(e) =>
                            patchPhonePart("localNumber", e.target.value)
                          }
                        />
                      </div>
                      <p
                        className={`mt-1 text-xs ${phoneValidation.valid ? "text-muted-foreground" : "text-red-700"}`}
                      >
                        {phoneValidation.valid
                          ? `Формат: ${phoneCountry.dialCode} + ${phoneCountry.minDigits === phoneCountry.maxDigits ? phoneCountry.minDigits : `${phoneCountry.minDigits}-${phoneCountry.maxDigits}`} цифр`
                          : phoneValidation.message}
                      </p>
                    </div>
                  </div>
                </div>

                <div className="space-y-2 rounded-md border p-3">
                  <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                    Рождение и гражданство
                  </h4>
                  <Label>Дата рождения (DD/MM/YYYY)</Label>
                  <div className="grid grid-cols-3 gap-2">
                    <Input
                      placeholder="dd"
                      value={fechaNacimientoDia}
                      onChange={(e) =>
                        patchNacimientoDatePart("day", e.target.value)
                      }
                    />
                    <Input
                      placeholder="mm"
                      value={fechaNacimientoMes}
                      onChange={(e) =>
                        patchNacimientoDatePart("month", e.target.value)
                      }
                    />
                    <Input
                      placeholder="yyyy"
                      value={fechaNacimientoAnio}
                      onChange={(e) =>
                        patchNacimientoDatePart("year", e.target.value)
                      }
                    />
                  </div>
                  <Input
                    type="date"
                    value={ddmmyyyyToIso(
                      composeDdmmyyyy(
                        fechaNacimientoDia,
                        fechaNacimientoMes,
                        fechaNacimientoAnio,
                      ) ||
                        payload.extra?.fecha_nacimiento ||
                        "",
                    )}
                    onChange={(e) => {
                      const next = isoToDdmmyyyy(e.target.value);
                      const parts = splitDdmmyyyy(next);
                      patchNacimientoDate(parts.day, parts.month, parts.year);
                    }}
                  />
                  <Label>Nacionalidad (гражданство)</Label>
                  <Input
                    value={payload.extra?.nacionalidad || ""}
                    onChange={(e) => patchExtra("nacionalidad", e.target.value)}
                  />
                  <Label>País de nacimiento (страна рождения)</Label>
                  <Input
                    value={payload.extra?.pais_nacimiento || ""}
                    onChange={(e) =>
                      patchExtra("pais_nacimiento", e.target.value)
                    }
                  />
                  <Label>Lugar de nacimiento (место рождения)</Label>
                  <Input
                    value={payload.extra?.lugar_nacimiento || ""}
                    onChange={(e) =>
                      patchExtra("lugar_nacimiento", e.target.value)
                    }
                  />
                </div>

                <div className="space-y-2 rounded-md border p-3">
                  <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                    Личные данные
                  </h4>
                  <Label>Sexo</Label>
                  <select
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    value={payload.extra?.sexo || ""}
                    onChange={(e) => patchExtra("sexo", e.target.value)}
                  >
                    <option value="">--</option>
                    <option value="H">H (Hombre)</option>
                    <option value="M">M (Mujer)</option>
                    <option value="X">X</option>
                  </select>
                  <Label>Estado civil</Label>
                  <select
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    value={payload.extra?.estado_civil || ""}
                    onChange={(e) => patchExtra("estado_civil", e.target.value)}
                  >
                    <option value="">--</option>
                    <option value="S">S (Soltero/a)</option>
                    <option value="C">C (Casado/a)</option>
                    <option value="V">V (Viudo/a)</option>
                    <option value="D">D (Divorciado/a)</option>
                    <option value="Sp">Sp (Separado/a)</option>
                  </select>
                  <Label>Nombre del padre</Label>
                  <Input
                    value={payload.extra?.nombre_padre || ""}
                    onChange={(e) => patchExtra("nombre_padre", e.target.value)}
                  />
                  <Label>Nombre de la madre</Label>
                  <Input
                    value={payload.extra?.nombre_madre || ""}
                    onChange={(e) => patchExtra("nombre_madre", e.target.value)}
                  />
                </div>

                <div className="space-y-2 rounded-md border p-3">
                  <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                    Представитель
                  </h4>
                  <Label>Representante legal</Label>
                  <Input
                    value={payload.extra?.representante_legal || ""}
                    onChange={(e) =>
                      patchExtra("representante_legal", e.target.value)
                    }
                  />
                  <Label>DNI/NIE/PAS representante</Label>
                  <Input
                    value={payload.extra?.representante_documento || ""}
                    onChange={(e) =>
                      patchExtra("representante_documento", e.target.value)
                    }
                  />
                  <Label>Título representante</Label>
                  <Input
                    value={payload.extra?.titulo_representante || ""}
                    onChange={(e) =>
                      patchExtra("titulo_representante", e.target.value)
                    }
                  />
                  <Label>Hijas/os escolarización en España (SI/NO)</Label>
                  <div className="flex items-center gap-6 rounded-md border p-3">
                    <label className="flex items-center gap-2 text-sm">
                      <input
                        type="checkbox"
                        checked={
                          (
                            payload.extra?.hijos_escolarizacion_espana || ""
                          ).toUpperCase() === "SI"
                        }
                        onChange={(e) =>
                          patchExtra(
                            "hijos_escolarizacion_espana",
                            e.target.checked ? "SI" : "",
                          )
                        }
                      />
                      SI
                    </label>
                    <label className="flex items-center gap-2 text-sm">
                      <input
                        type="checkbox"
                        checked={
                          (
                            payload.extra?.hijos_escolarizacion_espana || ""
                          ).toUpperCase() === "NO"
                        }
                        onChange={(e) =>
                          patchExtra(
                            "hijos_escolarizacion_espana",
                            e.target.checked ? "NO" : "",
                          )
                        }
                      />
                      NO
                    </label>
                  </div>
                </div>

                <div className="space-y-2 rounded-md border p-3">
                  <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                    Declarante / Ingreso
                  </h4>
                  <Label>Localidad declaración</Label>
                  <Input
                    value={
                      payload.declarante.localidad ||
                      payload.domicilio.municipio ||
                      ""
                    }
                    onChange={(e) =>
                      patchPayload("declarante", "localidad", e.target.value)
                    }
                  />
                  <Label>Fecha (dd/mm/yyyy)</Label>
                  <div className="grid grid-cols-3 gap-2">
                    <Input
                      placeholder="dd"
                      value={fechaDia}
                      onChange={(e) =>
                        patchDeclaranteDatePart("day", e.target.value)
                      }
                    />
                    <Input
                      placeholder="mm"
                      value={fechaMes}
                      onChange={(e) =>
                        patchDeclaranteDatePart("month", e.target.value)
                      }
                    />
                    <Input
                      placeholder="yyyy"
                      value={fechaAnio}
                      onChange={(e) =>
                        patchDeclaranteDatePart("year", e.target.value)
                      }
                    />
                  </div>
                  <Input
                    type="date"
                    value={ddmmyyyyToIso(
                      composeDdmmyyyy(fechaDia, fechaMes, fechaAnio) ||
                        payload.declarante.fecha,
                    )}
                    onChange={(e) => {
                      const next = isoToDdmmyyyy(e.target.value);
                      const parts = splitDdmmyyyy(next);
                      patchDeclaranteDate(parts.day, parts.month, parts.year);
                    }}
                  />
                  <Label>Forma de pago</Label>
                  <select
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    value={payload.ingreso.forma_pago || "efectivo"}
                    onChange={(e) =>
                      patchPayload("ingreso", "forma_pago", e.target.value)
                    }
                  >
                    <option value="efectivo">efectivo</option>
                    <option value="adeudo">adeudo</option>
                  </select>
                  {(payload.ingreso.forma_pago || "efectivo") !== "efectivo" ? (
                    <>
                      <Label>IBAN</Label>
                      <Input
                        value={payload.ingreso.iban}
                        onChange={(e) =>
                          patchPayload("ingreso", "iban", e.target.value)
                        }
                      />
                    </>
                  ) : null}
                </div>
              </section>
              </CardContent>
            </Card>
            <Card className="border-dashed">
              <CardContent className="space-y-2 p-3">
                <Button
                  onClick={confirmData}
                  disabled={saving || !phoneValidation.valid}
                  className="w-full"
                >
                  {saving ? (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  ) : (
                    <CheckCircle2 className="mr-2 h-4 w-4" />
                  )}
                  Подтвердить данные
                </Button>
                <p className="text-xs text-muted-foreground">
                  Сохранит форму и переведёт документ на следующий шаг.
                </p>
              </CardContent>
            </Card>
          </div>

          <Card className="overflow-hidden xl:h-[calc(100vh-220px)]">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <FileSearch className="h-5 w-5 text-primary" />
                Превью исходного документа
              </CardTitle>
              <CardDescription>
                Проверьте соответствие OCR-данных исходнику
                {effectivePreviewItems.length > 1
                  ? ` · ${effectivePreviewItems.length} документа`
                  : ""}
              </CardDescription>
            </CardHeader>
            <CardContent className="flex min-h-[420px] flex-col gap-3 xl:h-[calc(100%-80px)]">
              {effectivePreviewItems.length > 1 ? (
                <div className="flex flex-wrap gap-2">
                  {effectivePreviewItems.map((item) => (
                    <Button
                      key={item.documentId}
                      type="button"
                      size="sm"
                      variant={
                        item.documentId === activePreviewItem?.documentId
                          ? "default"
                          : "outline"
                      }
                      className="max-w-full"
                      onClick={() =>
                        setActivePreviewDocumentId(item.documentId)
                      }
                    >
                      <span className="truncate">
                        {item.isCurrent ? "Текущий" : "Связанный"} ·{" "}
                        {item.label}
                      </span>
                    </Button>
                  ))}
                </div>
              ) : null}

              {activePreviewIsImage ? (
                <div className="flex items-center justify-between gap-2">
                  <div className="text-xs text-muted-foreground">
                    Масштаб: {Math.round(previewZoom * 100)}%
                  </div>
                  <div className="flex items-center gap-1">
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        setPreviewZoom((value) => Math.max(0.5, value - 0.25))
                      }
                    >
                      -
                    </Button>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() => setPreviewZoom(1)}
                    >
                      100%
                    </Button>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        setPreviewZoom((value) => Math.min(3, value + 0.25))
                      }
                    >
                      +
                    </Button>
                  </div>
                </div>
              ) : null}

              {activePreviewUrl ? (
                activePreviewIsPdf ? (
                  <iframe
                    src={activePreviewUrl}
                    className="h-full min-h-0 w-full rounded-md border"
                    title="Document preview"
                  />
                ) : (
                  <div className="h-full min-h-0 overflow-auto rounded-md border bg-muted/20 p-2">
                    <img
                      src={activePreviewUrl}
                      alt="Uploaded preview"
                      className="mx-auto block h-full min-h-0 w-auto max-w-none object-contain"
                      style={{ transform: `scale(${previewZoom})` }}
                    />
                  </div>
                )
              ) : (
                <div className="rounded-md border p-4 text-sm text-muted-foreground">
                  Превью недоступно для выбранного документа.
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      ) : null}

      {step === "prepare" ? (
        <div className="mx-auto grid max-w-[1600px] grid-cols-1 gap-4 px-4 pb-6 lg:px-6">
          <Card className="overflow-auto xl:h-[calc(100vh-220px)]">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Globe className="h-5 w-5 text-primary" />
                Готов к заполнению
              </CardTitle>
              <CardDescription>
                Вставьте адрес страницы/формы и запустите autofill.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <Label>Адрес страницы/PDF</Label>
                <div className="flex flex-col gap-2 md:flex-row">
                  <Input
                    className="md:flex-1"
                    placeholder="https://..."
                    value={targetUrl}
                    onChange={(e) => setTargetUrl(e.target.value)}
                  />
                  {!isPdfTarget ? (
                    <Button
                      className="md:w-auto"
                      onClick={() => void openManagedSession()}
                      disabled={saving || !targetUrl.trim()}
                    >
                      {saving ? (
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      ) : null}
                      Перейти по адресу
                    </Button>
                  ) : null}
                </div>
                <div className="rounded-md border border-dashed p-2 text-xs text-muted-foreground">
                  Сессия браузера:{" "}
                  {browserSessionAlive ? "активна" : "не запущена"}
                  {browserSessionId ? ` · ${browserSessionId}` : ""}
                  {browserCurrentUrl ? ` · ${browserCurrentUrl}` : ""}
                </div>
              </div>
              <div className="space-y-2">
                <Label>Быстрый выбор документа/тасы</Label>
                <div className="space-y-3">
                  {[
                    { label: "Tasas", items: tasaPresets },
                    { label: "Formularios", items: formPresets },
                  ].map((group) => (
                    <div key={group.label} className="space-y-2">
                      <div className="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted-foreground">
                        {group.label}
                      </div>
                      <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-3">
                        {group.items
                          .slice()
                          .sort((left, right) => {
                            if (left.key === preferredPresetKey) return -1;
                            if (right.key === preferredPresetKey) return 1;
                            return left.label.localeCompare(right.label);
                          })
                          .map((preset) => {
                            const selected = targetPresetKey === preset.key;
                            const isPreferred =
                              preset.key === preferredPresetKey;
                            const shortCode =
                              preset.label.match(
                                /(790-\d{3}|Doc\s+\d+)/i,
                              )?.[1] || preset.label;
                            return (
                              <button
                                key={preset.key}
                                type="button"
                                className={cn(
                                  "rounded-xl border-[1.5px] p-3 text-left transition-all",
                                  selected
                                    ? "border-primary bg-primary/10 shadow-[0_0_0_3px_hsl(var(--primary)/0.14)]"
                                    : "border-border bg-card shadow-sm hover:-translate-y-0.5 hover:border-primary hover:shadow-[0_0_0_3px_hsl(var(--primary)/0.12)]",
                                )}
                                onClick={() => {
                                  setTargetPresetKey(preset.key);
                                  setTargetUrl(preset.url);
                                }}
                                disabled={saving}
                              >
                                {isPreferred ? (
                                  <div className="mb-1 text-[10px] font-semibold uppercase tracking-[0.06em] text-primary">
                                    Рекомендуется
                                  </div>
                                ) : null}
                                <div className="text-base font-semibold">
                                  {shortCode}
                                </div>
                                <div className="line-clamp-2 text-xs text-muted-foreground">
                                  {preset.label}
                                </div>
                              </button>
                            );
                          })}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
              <div className="flex flex-wrap gap-2">
                <Button
                  onClick={() => {
                    if (isPdfTarget) {
                      void downloadFilledPdfForPdfTarget();
                      return;
                    }
                    void runAutofillFromManagedSession();
                  }}
                  disabled={
                    saving ||
                    !targetUrl.trim() ||
                    (!isPdfTarget && !browserSessionId)
                  }
                >
                  {saving ? (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  ) : (
                    <ArrowRight className="mr-2 h-4 w-4" />
                  )}
                  {isPdfTarget
                    ? "Скачать заполненный PDF"
                    : "Заполнить, когда готов"}
                </Button>
                <Button
                  variant="outline"
                  onClick={() => setStepAndSync("review")}
                  disabled={saving || !payload}
                >
                  Редактировать данные
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      ) : null}

      {step === "autofill" && autofill ? (
        <div className="mx-auto grid max-w-[1600px] grid-cols-1 gap-4 px-4 pb-6 lg:px-6">
          <Card className="overflow-auto xl:h-[calc(100vh-220px)]">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <CheckCircle2 className="h-5 w-5 text-emerald-600" />
                Manual Handoff
              </CardTitle>
              <CardDescription>
                Заполнение завершено. Проверьте результат и передайте оператору.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="rounded-xl border border-emerald-300 bg-emerald-50 p-4 text-sm text-emerald-800">
                Документ успешно обработан. Обязательные поля:{" "}
                {missingFields.length === 0 ? "заполнены" : "есть пропуски"}.
              </div>
              <div className="grid gap-3 sm:grid-cols-3">
                <div className="rounded-lg border p-3">
                  <div className="text-xs text-muted-foreground">Статус</div>
                  <div className="text-sm font-semibold">Готов к handoff</div>
                </div>
                <div className="rounded-lg border p-3">
                  <div className="text-xs text-muted-foreground">Проблемы</div>
                  <div className="text-sm font-semibold">{totalProblems}</div>
                </div>
                <div className="rounded-lg border p-3">
                  <div className="text-xs text-muted-foreground">Документ</div>
                  <div className="truncate text-sm font-semibold">
                    {documentId || "—"}
                  </div>
                </div>
              </div>
              {autofill.filled_pdf_url ? (
                <Button asChild variant="outline">
                  <a
                    href={`${autofill.filled_pdf_url}${autofill.filled_pdf_url.includes("?") ? "&" : "?"}v=${filledPdfNonce || Date.now()}`}
                    target="_blank"
                    rel="noreferrer"
                  >
                    <FileSearch className="mr-2 h-4 w-4" />
                    Открыть заполненный PDF
                  </a>
                </Button>
              ) : null}
              <div className="flex flex-col gap-2 md:flex-row">
                <Button
                  variant="outline"
                  onClick={goToPrepareForAnotherDocument}
                  disabled={saving}
                >
                  <Globe className="mr-2 h-4 w-4" />
                  Заполнить другой документ
                </Button>
                <Button
                  variant="outline"
                  onClick={() => setStepAndSync("review")}
                  disabled={saving}
                >
                  Редактировать данные
                </Button>
                <Button onClick={resetWorkflow} disabled={saving}>
                  <ArrowRight className="mr-2 h-4 w-4" />
                  Вернуться к загрузке
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      ) : null}
    </main>
  );
}
