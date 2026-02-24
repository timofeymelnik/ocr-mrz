"use client";

import { useEffect, useState } from "react";
import { AlertCircle, CheckCircle2, FileUp, Loader2 } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
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
import { extractAddressHints } from "@/lib/address";
import { PHONE_COUNTRIES, composePhone, parsePhoneParts, type PhoneCountryIso, validatePhone } from "@/lib/phone";
import type {
  AutofillPreviewResponse,
  EnrichByIdentityResponse,
  MergeCandidate,
  Payload,
  SavedCrmDocument,
  UploadResponse,
} from "@/lib/types";

export default function HomePage() {
  const [step, setStep] = useState<Step>("upload");
  const [file, setFile] = useState<File | null>(null);
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
  const [autofill, setAutofill] = useState<AutofillPreviewResponse | null>(null);
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
  const [telefonoCountryIso, setTelefonoCountryIso] = useState<PhoneCountryIso>("ES");
  const [telefonoLocalNumber, setTelefonoLocalNumber] = useState("");
  const [savedDocs, setSavedDocs] = useState<SavedCrmDocument[]>([]);
  const [savedDocsFilter, setSavedDocsFilter] = useState("");
  const [loadingSavedDocs, setLoadingSavedDocs] = useState(false);
  const [deletingDocumentId, setDeletingDocumentId] = useState("");
  const [uploadSourceKind, setUploadSourceKind] = useState<UploadSourceKind>("");
  const [mergeCandidates, setMergeCandidates] = useState<MergeCandidate[]>([]);
  const [selectedMergeSourceId, setSelectedMergeSourceId] = useState("");
  const [mergePreview, setMergePreview] = useState<EnrichByIdentityResponse["enrichment_preview"]>([]);
  const [mergeAppliedFields, setMergeAppliedFields] = useState<string[]>([]);
  const [mergeSkippedFields, setMergeSkippedFields] = useState<string[]>([]);
  const [mergeLoading, setMergeLoading] = useState(false);
  const [error, setError] = useState("");
  const [dragOver, setDragOver] = useState(false);

  useEffect(() => {
    const timer = setTimeout(() => {
      void loadSavedDocuments(savedDocsFilter);
    }, 250);
    return () => clearTimeout(timer);
  }, [savedDocsFilter]);

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
    if (selectedMergeSourceId && next.some((row) => row.document_id === selectedMergeSourceId)) {
      return;
    }
    setSelectedMergeSourceId(next[0]?.document_id || "");
  }

  function resetWorkflow() {
    setStep("upload");
    setFile(null);
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
    setMergePreview([]);
    setMergeAppliedFields([]);
    setMergeSkippedFields([]);
    setMergeLoading(false);
    setError("");
    setDragOver(false);
  }

  function goToPrepareForAnotherDocument() {
    setAutofill(null);
    setFormUrl("");
    setTargetUrl("");
    setTargetPresetKey("");
    setBrowserSessionId("");
    setBrowserSessionAlive(false);
    setBrowserCurrentUrl("");
    setError("");
    setStep("prepare");
  }

  async function loadSavedDocuments(query: string) {
    setLoadingSavedDocs(true);
    try {
      const params = new URLSearchParams();
      if (query.trim()) params.set("query", query.trim());
      params.set("limit", "100");
      const resp = await fetch(`${API_BASE}/api/crm/documents?${params.toString()}`);
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as { items?: SavedCrmDocument[] };
      setSavedDocs(data.items || []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed loading saved documents");
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
    const ident = nextPayload.identificacion || ({ nif_nie: "", nombre_apellidos: "" } as Payload["identificacion"]);
    const nie = parseNieParts(ident.nif_nie || "");
    setNiePrefix(nie.prefix);
    setNieNumber(nie.number);
    setNieSuffix(nie.suffix);
    const split = splitFullName(ident.nombre_apellidos || "");
    setPrimerApellido((ident.primer_apellido || split.primer_apellido || "").trim());
    setSegundoApellido((ident.segundo_apellido || split.segundo_apellido || "").trim());
    setNombreSolo((ident.nombre || split.nombre || "").trim());
    const decl = splitDdmmyyyy(nextPayload.declarante?.fecha || "");
    setFechaDia((nextPayload.declarante?.fecha_dia || decl.day || "").trim());
    setFechaMes((nextPayload.declarante?.fecha_mes || decl.month || "").trim());
    setFechaAnio((nextPayload.declarante?.fecha_anio || decl.year || "").trim());
    const birth = splitDdmmyyyy(nextPayload.extra?.fecha_nacimiento || "");
    setFechaNacimientoDia((nextPayload.extra?.fecha_nacimiento_dia || birth.day || "").trim());
    setFechaNacimientoMes((nextPayload.extra?.fecha_nacimiento_mes || birth.month || "").trim());
    setFechaNacimientoAnio((nextPayload.extra?.fecha_nacimiento_anio || birth.year || "").trim());
    const phone = parsePhoneParts(nextPayload.domicilio?.telefono || "");
    const explicitIso = ((nextPayload.extra?.telefono_country_iso || "").toUpperCase() as PhoneCountryIso) || phone.countryIso;
    setTelefonoCountryIso(PHONE_COUNTRIES.some((item) => item.iso === explicitIso) ? explicitIso : phone.countryIso || "ES");
    setTelefonoLocalNumber(phone.localNumber || "");
  }

  async function openSavedDocument(documentIdToOpen: string) {
    setSaving(true);
    setError("");
    try {
      const resp = await fetch(`${API_BASE}/api/crm/documents/${documentIdToOpen}`);
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as UploadResponse;
      setDocumentId(data.document_id);
      setPayload(data.payload);
      syncNamePartsFromPayload(data.payload);
      setPreviewUrl(toUrl(data.preview_url || "", API_BASE));
      setFormUrl(data.form_url);
      setTargetUrl(data.target_url || data.form_url);
      setBrowserSessionId("");
      setBrowserSessionAlive(false);
      setBrowserCurrentUrl("");
      setMissingFields(data.missing_fields || []);
      applyMergeStateFromCandidates(data.merge_candidates, data.identity_source_document_id || "");
      setMergePreview(data.enrichment_preview || []);
      setMergeAppliedFields((data.enrichment_preview || []).map((row) => row.field));
      setMergeSkippedFields([]);
      setAutofill(null);
      setStep("review");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed opening saved document");
    } finally {
      setSaving(false);
    }
  }

  async function deleteSavedDocument(documentIdToDelete: string) {
    const approved = window.confirm("Удалить документ из CRM? Это действие нельзя отменить.");
    if (!approved) return;
    setDeletingDocumentId(documentIdToDelete);
    setError("");
    try {
      const resp = await fetch(`${API_BASE}/api/crm/documents/${documentIdToDelete}`, {
        method: "DELETE",
      });
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
    setPayload({
      ...payload,
      [section]: {
        ...(payload[section] as Record<string, unknown>),
        [key]: value,
      },
    });
  }

  function patchNiePart(kind: "prefix" | "number" | "suffix", value: string) {
    const nextPrefix = kind === "prefix" ? value : niePrefix;
    const nextNumber = kind === "number" ? value : nieNumber;
    const nextSuffix = kind === "suffix" ? value : nieSuffix;
    if (kind === "prefix") setNiePrefix(value.toUpperCase().replace(/[^XYZ]/g, "").slice(0, 1));
    if (kind === "number") setNieNumber(value.replace(/\D/g, "").slice(0, 7));
    if (kind === "suffix") setNieSuffix(value.toUpperCase().replace(/[^A-Z]/g, "").slice(0, 1));
    const composed = composeNie(nextPrefix, nextNumber, nextSuffix);
    if (composed) patchPayload("identificacion", "nif_nie", composed);
  }

  function patchPhonePart(kind: "countryIso" | "localNumber", value: string) {
    const nextCountry = kind === "countryIso" ? (value as PhoneCountryIso) : telefonoCountryIso;
    const nextLocal = kind === "localNumber" ? value.replace(/\D/g, "").slice(0, 15) : telefonoLocalNumber;
    if (kind === "countryIso") setTelefonoCountryIso(nextCountry);
    if (kind === "localNumber") setTelefonoLocalNumber(nextLocal);
    patchPayload("domicilio", "telefono", composePhone(nextCountry, nextLocal));
    patchExtra("telefono_country_iso", nextCountry);
  }

  function autofillAddressFromStreet() {
    if (!payload) return;
    const hints = extractAddressHints(payload.domicilio.nombre_via || "");
    const hasHints = Boolean(hints.numero || hints.piso || hints.puerta || hints.cp || hints.municipio || hints.streetName);
    if (!hasHints) {
      setError("Не удалось извлечь подсказки из строки адреса.");
      return;
    }
    const domicilio = payload.domicilio;
    setPayload({
      ...payload,
      domicilio: {
        ...domicilio,
        nombre_via: domicilio.nombre_via || hints.streetName,
        numero: domicilio.numero || hints.numero,
        piso: domicilio.piso || hints.piso,
        puerta: domicilio.puerta || hints.puerta,
        cp: domicilio.cp || hints.cp,
        municipio: domicilio.municipio || hints.municipio,
      },
    });
  }

  function patchSplitNameAndCompose(kind: "primer_apellido" | "segundo_apellido" | "nombre", value: string) {
    const nextPrimer = kind === "primer_apellido" ? value : primerApellido;
    const nextSegundo = kind === "segundo_apellido" ? value : segundoApellido;
    const nextNombre = kind === "nombre" ? value : nombreSolo;
    if (kind === "primer_apellido") setPrimerApellido(value);
    if (kind === "segundo_apellido") setSegundoApellido(value);
    if (kind === "nombre") setNombreSolo(value);
    patchPayload("identificacion", "primer_apellido", nextPrimer);
    patchPayload("identificacion", "segundo_apellido", nextSegundo);
    patchPayload("identificacion", "nombre", nextNombre);
    patchPayload("identificacion", "nombre_apellidos", composeFullName(nextPrimer, nextSegundo, nextNombre));
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

  function patchDeclaranteDatePart(kind: "day" | "month" | "year", value: string) {
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

  function patchNacimientoDatePart(kind: "day" | "month" | "year", value: string) {
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

  function onFileSelected(next: File | null) {
    setFile(next);
    setError("");
  }

  async function uploadDocument() {
    if (!file) {
      setError("Выберите файл .jpg/.jpeg/.png/.pdf");
      return;
    }
    if (!uploadSourceKind) {
      setError("Выберите тип документа перед запуском OCR.");
      return;
    }
    setUploading(true);
    setError("");
    try {
      const formData = new FormData();
      formData.append("file", file);
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
      setDocumentId(data.document_id);
      setPayload(data.payload);
      syncNamePartsFromPayload(data.payload);
      setPreviewUrl(toUrl(data.preview_url, API_BASE));
      setFormUrl(data.form_url);
      setTargetUrl(data.target_url || data.form_url);
      setBrowserSessionId("");
      setBrowserSessionAlive(false);
      setBrowserCurrentUrl("");
      setMissingFields(data.missing_fields || []);
      applyMergeStateFromCandidates(data.merge_candidates, data.identity_source_document_id || "");
      setMergePreview(data.enrichment_preview || []);
      setMergeAppliedFields((data.enrichment_preview || []).map((row) => row.field));
      setMergeSkippedFields([]);
      setStep("review");
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
          nombre_apellidos: composeFullName(primerApellido, segundoApellido, nombreSolo),
        },
        domicilio: {
          ...payload.domicilio,
          telefono: composePhone(telefonoCountryIso, telefonoLocalNumber) || payload.domicilio.telefono,
        },
        declarante: {
          ...payload.declarante,
          fecha: composeDdmmyyyy(fechaDia, fechaMes, fechaAnio) || payload.declarante.fecha,
          fecha_dia: fechaDia,
          fecha_mes: fechaMes,
          fecha_anio: fechaAnio,
        },
        extra: {
          ...(payload.extra || {}),
          telefono_country_iso: telefonoCountryIso,
          fecha_nacimiento:
            composeDdmmyyyy(fechaNacimientoDia, fechaNacimientoMes, fechaNacimientoAnio) ||
            payload.extra?.fecha_nacimiento ||
            "",
          fecha_nacimiento_dia: fechaNacimientoDia,
          fecha_nacimiento_mes: fechaNacimientoMes,
          fecha_nacimiento_anio: fechaNacimientoAnio,
        },
      };
      const confirmResp = await fetch(`${API_BASE}/api/documents/${documentId}/confirm`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ payload: normalizedPayload }),
      });
      if (!confirmResp.ok) {
        throw new Error(await readErrorResponse(confirmResp));
      }
      const confirmed = await confirmResp.json();
      const confirmedPayload = (confirmed.payload as Payload) || normalizedPayload;
      setPayload(confirmedPayload);
      syncNamePartsFromPayload(confirmedPayload);
      setMissingFields(confirmed.missing_fields || []);
      applyMergeStateFromCandidates(confirmed.merge_candidates, confirmed.identity_source_document_id || "");
      setMergePreview(confirmed.enrichment_preview || []);
      setMergeAppliedFields((confirmed.enrichment_preview || []).map((row: { field: string }) => row.field));
      setMergeSkippedFields([]);
      setStep("prepare");
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
      const resp = await fetch(`${API_BASE}/api/documents/${documentId}/merge-candidates`);
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as { merge_candidates?: MergeCandidate[] };
      applyMergeStateFromCandidates(data.merge_candidates);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed loading merge suggestions");
    } finally {
      setMergeLoading(false);
    }
  }

  async function runMerge(apply: boolean) {
    if (!documentId) return;
    if (!selectedMergeSourceId) {
      setError("Выберите источник данных для merge.");
      return;
    }
    if (apply) {
      const ok = window.confirm("Применить merge предложенных данных в текущий документ?");
      if (!ok) return;
    }
    setMergeLoading(true);
    setError("");
    try {
      const resp = await fetch(`${API_BASE}/api/documents/${documentId}/enrich-by-identity`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          apply,
          source_document_id: selectedMergeSourceId,
        }),
      });
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as EnrichByIdentityResponse;
      setMergePreview(data.enrichment_preview || []);
      setMergeAppliedFields(data.applied_fields || []);
      setMergeSkippedFields(data.skipped_fields || []);
      setMissingFields(data.missing_fields || []);
      applyMergeStateFromCandidates(data.merge_candidates, data.identity_source_document_id || selectedMergeSourceId);
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
  ): Promise<{ session_id: string; current_url: string; target_url: string } | null> {
    if (!documentId) return null;
    const resolvedTargetUrl = (targetUrlOverride ?? targetUrl).trim();
    if (!resolvedTargetUrl) {
      setError("Укажите адрес страницы или PDF.");
      return null;
    }
    setSaving(true);
    setError("");
    try {
      const resp = await fetch(`${CLIENT_AGENT_BASE}/api/browser-session/open`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          target_url: resolvedTargetUrl,
          headless: options?.headless ?? false,
          slowmo: options?.slowmo ?? 40,
          timeout_ms: 30000,
        }),
      });
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
      setError(e instanceof Error ? e.message : "Failed to open browser session");
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
      setError("Сначала нажмите 'Перейти по адресу' и откройте управляемую сессию.");
      return "";
    }
    setSaving(true);
    setError("");
    try {
      let currentUrl = (currentUrlOverride || "").trim();
      if (currentUrl === "about:blank") currentUrl = "";
      if (!currentUrl) {
        const stateResp = await fetch(`${CLIENT_AGENT_BASE}/api/browser-session/${sessionId}/state`);
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

      const templateResp = await fetch(`${API_BASE}/api/documents/${documentId}/browser-session/template`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          current_url: currentUrl,
          payload,
          fill_strategy: "strict_template",
        }),
      });
      if (!templateResp.ok) {
        throw new Error(await readErrorResponse(templateResp));
      }
      const templateData = await templateResp.json();

      const autoResp = await fetch(`${CLIENT_AGENT_BASE}/api/browser-session/${sessionId}/fill`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          payload,
          timeout_ms: 25000,
          explicit_mappings: templateData.effective_mappings || [],
          fill_strategy: templateData.fill_strategy || "strict_template",
          document_id: documentId,
        }),
      });
      if (!autoResp.ok) {
        throw new Error(await readErrorResponse(autoResp));
      }
      const autoData: AutofillPreviewResponse = await autoResp.json();
      const filledPdfUrl = autoData.filled_pdf_url ? toUrl(autoData.filled_pdf_url, CLIENT_AGENT_BASE) : "";
      setAutofill({
        ...autoData,
        filled_pdf_url: filledPdfUrl,
      });
      setFilledPdfNonce(Date.now());
      setFormUrl(currentUrl);
      setBrowserCurrentUrl(currentUrl);
      setStep("autofill");
      if (openFilledPdfAfter && filledPdfUrl) {
        const sep = filledPdfUrl.includes("?") ? "&" : "?";
        window.open(`${filledPdfUrl}${sep}v=${Date.now()}`, "_blank", "noopener,noreferrer");
      }
      return filledPdfUrl;
    } catch (e) {
      setError(e instanceof Error ? e.message : "Autofill in opened browser session failed");
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
    const opened = await openManagedSession(targetUrl.trim(), { headless: true, slowmo: 0 });
    if (!opened?.session_id) return;
    const currentUrl = opened.target_url || (opened.current_url === "about:blank" ? "" : opened.current_url) || targetUrl.trim();
    const filledPdfUrl = await runAutofillFromManagedSession(opened.session_id, currentUrl, true);
    if (!filledPdfUrl) {
      setError("Заполненный PDF не был сформирован. Проверьте маппинг полей для этого документа.");
    }
    try {
      await fetch(`${CLIENT_AGENT_BASE}/api/browser-session/${opened.session_id}/close`, {
        method: "POST",
      });
    } catch {
      // best-effort close
    }
    setBrowserSessionId("");
    setBrowserSessionAlive(false);
    setBrowserCurrentUrl("");
  }

  const phoneCountry = PHONE_COUNTRIES.find((item) => item.iso === telefonoCountryIso) || PHONE_COUNTRIES[0];
  const phoneValidation = validatePhone(telefonoCountryIso, telefonoLocalNumber);
  const isPdfTarget = isPdfTargetUrl(targetUrl);

  return (
    <main className="mx-auto min-h-screen max-w-[1400px] p-6">
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold">Tasa OCR Workspace</h1>
          <p className="text-sm text-muted-foreground">
            Загрузка документа, верификация данных, autofill полей заявителя и ручной handoff.
          </p>
        </div>
        <div className="flex items-center gap-2">
          {step !== "upload" ? (
            <Button variant="secondary" onClick={resetWorkflow} disabled={saving || uploading}>
              Начать сначала
            </Button>
          ) : null}
          <Badge variant="secondary">{step.toUpperCase()}</Badge>
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

      {step === "upload" ? (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[1fr_420px]">
          <Card>
            <CardHeader>
              <CardTitle>Загрузка исходника</CardTitle>
              <CardDescription>Поддерживаются изображения и PDF. Можно перетащить файл в зону ниже.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div
                className={`rounded-lg border-2 border-dashed p-10 text-center transition ${
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
                  const next = e.dataTransfer.files?.[0] || null;
                  onFileSelected(next);
                }}
              >
                <FileUp className="mx-auto mb-3 h-8 w-8 text-muted-foreground" />
                <p className="text-sm text-muted-foreground">Перетащите файл сюда или выберите через input</p>
                <Input
                  type="file"
                  accept=".jpg,.jpeg,.png,.pdf"
                  className="mx-auto mt-4 max-w-md"
                  onChange={(e) => onFileSelected(e.target.files?.[0] || null)}
                />
                {file ? <p className="mt-3 text-sm font-medium">{file.name}</p> : null}
              </div>
              <div className="space-y-2">
                <Label>Источник данных</Label>
                <select
                  className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  value={uploadSourceKind}
                  onChange={(e) => setUploadSourceKind(e.target.value as UploadSourceKind)}
                >
                  <option value="">-- выберите тип документа --</option>
                  <option value="anketa">Анкета</option>
                  <option value="fmiliar">Анкета familiar</option>
                  <option value="passport">Паспорт</option>
                  <option value="nie_tie">NIE/TIE/DNI</option>
                  <option value="visa">Виза</option>
                </select>
              </div>
              <Button onClick={uploadDocument} disabled={!file || !uploadSourceKind || uploading}>
                {uploading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                Запустить OCR и парсинг
              </Button>
            </CardContent>
          </Card>

          <Card className="h-[calc(100vh-180px)] overflow-auto">
            <CardHeader>
              <CardTitle>Сохраненные документы (CRM)</CardTitle>
              <CardDescription>Поиск по имени или номеру документа.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-3">
              <Input
                placeholder="Фильтр: имя или номер документа"
                value={savedDocsFilter}
                onChange={(e) => setSavedDocsFilter(e.target.value)}
              />
              <Button variant="outline" onClick={() => loadSavedDocuments(savedDocsFilter)} disabled={loadingSavedDocs}>
                {loadingSavedDocs ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                Обновить список
              </Button>
              <div className="space-y-2">
                {(savedDocs || []).map((item) => (
                  <div key={item.document_id} className="rounded-md border p-3">
                    <div className="text-sm font-medium">{item.name || "Без имени"}</div>
                    <div className="text-xs text-muted-foreground">{item.document_number || "Без номера"}</div>
                    <div className="text-xs text-muted-foreground">{item.updated_at || ""}</div>
                    <div className="mt-2 flex gap-2">
                      <Button size="sm" onClick={() => openSavedDocument(item.document_id)} disabled={saving}>
                        Открыть
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => deleteSavedDocument(item.document_id)}
                        disabled={Boolean(deletingDocumentId) || saving}
                      >
                        {deletingDocumentId === item.document_id ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                        Удалить
                      </Button>
                    </div>
                  </div>
                ))}
                {!loadingSavedDocs && savedDocs.length === 0 ? (
                  <div className="rounded-md border p-3 text-sm text-muted-foreground">Нет сохраненных документов.</div>
                ) : null}
              </div>
            </CardContent>
          </Card>
        </div>
      ) : null}

      {step === "review" && payload ? (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[520px_1fr]">
          <Card className="h-[calc(100vh-180px)] overflow-auto">
            <CardHeader>
              <CardTitle>Проверка и правка данных</CardTitle>
              <CardDescription>
                Подтвердите данные заявителя. Trámite, CAPTCHA и скачивание останутся на человеке.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-5">
              <section className="space-y-2 rounded-md border p-3">
                <h3 className="text-sm font-semibold">Merge данных из других документов</h3>
                <p className="text-xs text-muted-foreground">
                  Система только предлагает поля для дозаполнения. Данные применяются только после вашего подтверждения.
                </p>
                <div className="flex gap-2">
                  <Button variant="outline" size="sm" onClick={refreshMergeCandidates} disabled={mergeLoading || saving}>
                    {mergeLoading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                    Обновить кандидатов
                  </Button>
                </div>
                <Label>Источник для merge</Label>
                <select
                  className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  value={selectedMergeSourceId}
                  onChange={(e) => setSelectedMergeSourceId(e.target.value)}
                >
                  <option value="">-- выбрать документ --</option>
                  {mergeCandidates.map((candidate) => (
                    <option key={candidate.document_id} value={candidate.document_id}>
                      {candidate.name || "Без имени"} | {candidate.document_number || "без номера"} | score {candidate.score}
                    </option>
                  ))}
                </select>
                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => runMerge(false)}
                    disabled={mergeLoading || saving || !selectedMergeSourceId}
                  >
                    {mergeLoading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                    Показать предложения
                  </Button>
                  <Button size="sm" onClick={() => runMerge(true)} disabled={mergeLoading || saving || !selectedMergeSourceId}>
                    {mergeLoading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                    Применить merge
                  </Button>
                </div>
                {mergePreview.length > 0 ? (
                  <div className="max-h-40 overflow-auto rounded-md border p-2 text-xs">
                    <div>Будет заполнено: {mergeAppliedFields.length}</div>
                    <div>Пропущено: {mergeSkippedFields.length}</div>
                    {mergePreview.map((row) => (
                      <div key={`${row.field}-${row.suggested_value}`}>
                        {row.field}: {row.suggested_value}
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="text-xs text-muted-foreground">Нет предложений по merge.</div>
                )}
              </section>

              <Separator />

              <section className="space-y-2">
                <h3 className="text-sm font-semibold">Identificación</h3>
                <Label>NIE (буква + 7 цифр + буква)</Label>
                <div className="grid grid-cols-3 gap-2">
                  <Input placeholder="Y" value={niePrefix} onChange={(e) => patchNiePart("prefix", e.target.value)} />
                  <Input placeholder="1234567" value={nieNumber} onChange={(e) => patchNiePart("number", e.target.value)} />
                  <Input placeholder="X" value={nieSuffix} onChange={(e) => patchNiePart("suffix", e.target.value)} />
                </div>
                <Label>Pasaporte (опционально)</Label>
                <Input
                  value={payload.identificacion.pasaporte || ""}
                  onChange={(e) => patchPayload("identificacion", "pasaporte", e.target.value)}
                />
                <div className="grid grid-cols-1 gap-2 pt-1 md:grid-cols-3">
                  <div>
                    <Label>Primer apellido</Label>
                    <Input value={primerApellido} onChange={(e) => patchSplitNameAndCompose("primer_apellido", e.target.value)} />
                  </div>
                  <div>
                    <Label>Segundo apellido</Label>
                    <Input value={segundoApellido} onChange={(e) => patchSplitNameAndCompose("segundo_apellido", e.target.value)} />
                  </div>
                  <div>
                    <Label>Nombre</Label>
                    <Input value={nombreSolo} onChange={(e) => patchSplitNameAndCompose("nombre", e.target.value)} />
                  </div>
                </div>
              </section>

              <Separator />

              <section className="space-y-2">
                <h3 className="text-sm font-semibold">Domicilio</h3>
                <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                  <div>
                    <Label>Tipo vía</Label>
                    <Input value={payload.domicilio.tipo_via} onChange={(e) => patchPayload("domicilio", "tipo_via", e.target.value)} />
                  </div>
                  <div className="md:col-span-2">
                    <Label>Nombre vía</Label>
                    <div className="mt-1 flex flex-col gap-2 md:flex-row">
                      <Input
                        className="md:flex-1"
                        value={payload.domicilio.nombre_via}
                        onChange={(e) => patchPayload("domicilio", "nombre_via", e.target.value)}
                      />
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        className="md:w-auto"
                        onClick={autofillAddressFromStreet}
                      >
                        Дозаполнить адрес из строки
                      </Button>
                    </div>
                  </div>
                  <div>
                    <Label>Número</Label>
                    <Input value={payload.domicilio.numero} onChange={(e) => patchPayload("domicilio", "numero", e.target.value)} />
                  </div>
                  <div>
                    <Label>Escalera</Label>
                    <Input value={payload.domicilio.escalera} onChange={(e) => patchPayload("domicilio", "escalera", e.target.value)} />
                  </div>
                  <div>
                    <Label>Piso</Label>
                    <Input value={payload.domicilio.piso} onChange={(e) => patchPayload("domicilio", "piso", e.target.value)} />
                  </div>
                  <div>
                    <Label>Puerta</Label>
                    <Input value={payload.domicilio.puerta} onChange={(e) => patchPayload("domicilio", "puerta", e.target.value)} />
                  </div>
                  <div className="md:col-span-2">
                    <Label>Teléfono</Label>
                    <div className="mt-1 flex flex-col gap-2 md:flex-row">
                      <select
                        className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring md:w-[260px] md:flex-none"
                        value={telefonoCountryIso}
                        onChange={(e) => patchPhonePart("countryIso", e.target.value)}
                      >
                        {PHONE_COUNTRIES.map((item) => (
                          <option key={item.iso} value={item.iso}>
                            {item.flag} {item.iso} ({item.dialCode}) {item.label}
                          </option>
                        ))}
                      </select>
                      <Input
                        className="md:flex-1"
                        placeholder={phoneCountry.iso === "RU" ? "9123456789" : "624731544"}
                        value={telefonoLocalNumber}
                        onChange={(e) => patchPhonePart("localNumber", e.target.value)}
                      />
                    </div>
                    <p className={`mt-1 text-xs ${phoneValidation.valid ? "text-muted-foreground" : "text-red-700"}`}>
                      {phoneValidation.valid
                        ? `Формат: ${phoneCountry.dialCode} + ${phoneCountry.minDigits === phoneCountry.maxDigits ? phoneCountry.minDigits : `${phoneCountry.minDigits}-${phoneCountry.maxDigits}`} цифр`
                        : phoneValidation.message}
                    </p>
                  </div>
                  <div className="grid grid-cols-1 gap-2 md:col-span-2 md:grid-cols-3">
                    <div>
                      <Label>Municipio</Label>
                      <Input
                        value={payload.domicilio.municipio}
                        onChange={(e) => patchPayload("domicilio", "municipio", e.target.value)}
                      />
                    </div>
                    <div>
                      <Label>Provincia</Label>
                      <Input
                        value={payload.domicilio.provincia}
                        onChange={(e) => patchPayload("domicilio", "provincia", e.target.value)}
                      />
                    </div>
                    <div>
                      <Label>CP</Label>
                      <Input value={payload.domicilio.cp} onChange={(e) => patchPayload("domicilio", "cp", e.target.value)} />
                    </div>
                  </div>
                </div>
              </section>

              <Separator />

              <section className="space-y-2">
                <h3 className="text-sm font-semibold">Declarante / Ingreso</h3>
                <Label>Localidad declaración</Label>
                <Input value={payload.declarante.localidad} onChange={(e) => patchPayload("declarante", "localidad", e.target.value)} />
                <Label>Fecha (dd/mm/yyyy)</Label>
                <div className="grid grid-cols-3 gap-2">
                  <Input placeholder="dd" value={fechaDia} onChange={(e) => patchDeclaranteDatePart("day", e.target.value)} />
                  <Input placeholder="mm" value={fechaMes} onChange={(e) => patchDeclaranteDatePart("month", e.target.value)} />
                  <Input placeholder="yyyy" value={fechaAnio} onChange={(e) => patchDeclaranteDatePart("year", e.target.value)} />
                </div>
                <Input
                  type="date"
                  value={ddmmyyyyToIso(composeDdmmyyyy(fechaDia, fechaMes, fechaAnio) || payload.declarante.fecha)}
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
                  onChange={(e) => patchPayload("ingreso", "forma_pago", e.target.value)}
                >
                  <option value="efectivo">efectivo</option>
                  <option value="adeudo">adeudo</option>
                </select>
                <Label>IBAN</Label>
                <Input value={payload.ingreso.iban} onChange={(e) => patchPayload("ingreso", "iban", e.target.value)} />
              </section>

              <Separator />

              <section className="space-y-2">
                <h3 className="text-sm font-semibold">Дополнительные персональные поля (CRM)</h3>
                <div className="space-y-2 rounded-md border p-3">
                  <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Контакты</h4>
                  <Label>Email</Label>
                  <Input value={payload.extra?.email || ""} onChange={(e) => patchExtra("email", e.target.value)} />
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
                      onChange={(e) => patchNacimientoDatePart("day", e.target.value)}
                    />
                    <Input
                      placeholder="mm"
                      value={fechaNacimientoMes}
                      onChange={(e) => patchNacimientoDatePart("month", e.target.value)}
                    />
                    <Input
                      placeholder="yyyy"
                      value={fechaNacimientoAnio}
                      onChange={(e) => patchNacimientoDatePart("year", e.target.value)}
                    />
                  </div>
                  <Input
                    type="date"
                    value={ddmmyyyyToIso(
                      composeDdmmyyyy(fechaNacimientoDia, fechaNacimientoMes, fechaNacimientoAnio) ||
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
                  <Input value={payload.extra?.nacionalidad || ""} onChange={(e) => patchExtra("nacionalidad", e.target.value)} />
                  <Label>País de nacimiento (страна рождения)</Label>
                  <Input value={payload.extra?.pais_nacimiento || ""} onChange={(e) => patchExtra("pais_nacimiento", e.target.value)} />
                  <Label>Lugar de nacimiento (место рождения)</Label>
                  <Input
                    value={payload.extra?.lugar_nacimiento || ""}
                    onChange={(e) => patchExtra("lugar_nacimiento", e.target.value)}
                  />
                </div>

                <div className="space-y-2 rounded-md border p-3">
                  <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Личные данные</h4>
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
                  <Input value={payload.extra?.nombre_padre || ""} onChange={(e) => patchExtra("nombre_padre", e.target.value)} />
                  <Label>Nombre de la madre</Label>
                  <Input value={payload.extra?.nombre_madre || ""} onChange={(e) => patchExtra("nombre_madre", e.target.value)} />
                </div>

                <div className="space-y-2 rounded-md border p-3">
                  <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Представитель</h4>
                  <Label>Representante legal</Label>
                  <Input
                    value={payload.extra?.representante_legal || ""}
                    onChange={(e) => patchExtra("representante_legal", e.target.value)}
                  />
                  <Label>DNI/NIE/PAS representante</Label>
                  <Input
                    value={payload.extra?.representante_documento || ""}
                    onChange={(e) => patchExtra("representante_documento", e.target.value)}
                  />
                  <Label>Título representante</Label>
                  <Input
                    value={payload.extra?.titulo_representante || ""}
                    onChange={(e) => patchExtra("titulo_representante", e.target.value)}
                  />
                  <Label>Hijas/os escolarización en España (SI/NO)</Label>
                  <div className="flex items-center gap-6 rounded-md border p-3">
                    <label className="flex items-center gap-2 text-sm">
                      <input
                        type="checkbox"
                        checked={(payload.extra?.hijos_escolarizacion_espana || "").toUpperCase() === "SI"}
                        onChange={(e) => patchExtra("hijos_escolarizacion_espana", e.target.checked ? "SI" : "")}
                      />
                      SI
                    </label>
                    <label className="flex items-center gap-2 text-sm">
                      <input
                        type="checkbox"
                        checked={(payload.extra?.hijos_escolarizacion_espana || "").toUpperCase() === "NO"}
                        onChange={(e) => patchExtra("hijos_escolarizacion_espana", e.target.checked ? "NO" : "")}
                      />
                      NO
                    </label>
                  </div>
                </div>
              </section>

              <Button onClick={confirmData} disabled={saving || !phoneValidation.valid}>
                {saving ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <CheckCircle2 className="mr-2 h-4 w-4" />}
                Подтвердить данные
              </Button>
            </CardContent>
          </Card>

          <Card className="h-[calc(100vh-180px)]">
            <CardHeader>
              <CardTitle>Превью исходного документа</CardTitle>
              <CardDescription>Проверьте соответствие OCR-данных исходнику</CardDescription>
            </CardHeader>
            <CardContent className="h-[calc(100%-80px)]">
              {previewUrl ? (
                previewUrl.toLowerCase().includes(".pdf") ? (
                  <iframe src={previewUrl} className="h-full w-full rounded-md border" title="Document preview" />
                ) : (
                  <img src={previewUrl} alt="Uploaded preview" className="h-full w-full rounded-md border object-contain" />
                )
              ) : (
                <div className="rounded-md border p-4 text-sm text-muted-foreground">Превью недоступно для этого документа.</div>
              )}
            </CardContent>
          </Card>
        </div>
      ) : null}

      {step === "prepare" ? (
        <div className="grid grid-cols-1 gap-4">
          <Card className="h-[calc(100vh-180px)] overflow-auto">
            <CardHeader>
              <CardTitle>Готов к заполнению</CardTitle>
              <CardDescription>
                Вставьте адрес страницы или PDF, который нужно заполнить, затем нажмите запуск.
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
                      {saving ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                      Перейти по адресу
                    </Button>
                  ) : null}
                </div>
              </div>
              <div className="space-y-2">
                <Label>Быстрый выбор документа/тасы</Label>
                <select
                  className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  value={targetPresetKey}
                  onChange={(e) => {
                    const nextKey = e.target.value;
                    setTargetPresetKey(nextKey);
                    const preset = TARGET_URL_PRESETS.find((item) => item.key === nextKey);
                    if (!preset) return;
                    setTargetUrl(preset.url);
                  }}
                  disabled={saving}
                >
                  <option value="">-- выбрать из частых --</option>
                  {TARGET_URL_PRESETS.map((preset) => (
                    <option key={preset.key} value={preset.key}>
                      {preset.label}
                    </option>
                  ))}
                </select>
              </div>
              <Button
                onClick={() => {
                  if (isPdfTarget) {
                    void downloadFilledPdfForPdfTarget();
                    return;
                  }
                  void runAutofillFromManagedSession();
                }}
                disabled={saving || !targetUrl.trim() || (!isPdfTarget && !browserSessionId)}
              >
                {saving ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <CheckCircle2 className="mr-2 h-4 w-4" />}
                {isPdfTarget ? "Скачать заполненный PDF" : "Заполнить, когда готов"}
              </Button>
            </CardContent>
          </Card>
        </div>
      ) : null}

      {step === "autofill" && autofill ? (
        <div className="grid grid-cols-1 gap-4">
          <Card className="h-[calc(100vh-180px)] overflow-auto">
            <CardHeader>
              <CardTitle>Manual Handoff</CardTitle>
              <CardDescription>Заполнение завершено. Проверьте результат и продолжите вручную.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              {autofill.filled_pdf_url ? (
                <Button asChild variant="outline">
                  <a
                    href={`${autofill.filled_pdf_url}${autofill.filled_pdf_url.includes("?") ? "&" : "?"}v=${filledPdfNonce || Date.now()}`}
                    target="_blank"
                    rel="noreferrer"
                  >
                    Открыть заполненный PDF
                  </a>
                </Button>
              ) : null}
              <div className="flex flex-col gap-2 md:flex-row">
                <Button variant="outline" onClick={goToPrepareForAnotherDocument} disabled={saving}>
                  Заполнить другой документ
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      ) : null}
    </main>
  );
}
