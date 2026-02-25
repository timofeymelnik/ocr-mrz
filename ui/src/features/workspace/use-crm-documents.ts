"use client";

import { useEffect, useState } from "react";

import { API_BASE } from "@/features/workspace/constants";
import { readErrorResponse } from "@/features/workspace/utils";
import type { SavedCrmDocument } from "@/lib/types";

type UseCrmDocumentsResult = {
  deletingDocumentId: string;
  error: string;
  loadingSavedDocs: boolean;
  savedDocs: SavedCrmDocument[];
  savedDocsFilter: string;
  setSavedDocsFilter: (value: string) => void;
  loadSavedDocuments: (query: string) => Promise<void>;
  deleteSavedDocument: (documentIdToDelete: string) => Promise<boolean>;
};

type UseCrmDocumentsOptions = {
  includeDuplicates?: boolean;
};

export function useCrmDocuments(
  options: UseCrmDocumentsOptions = {},
): UseCrmDocumentsResult {
  const includeDuplicates = Boolean(options.includeDuplicates);
  const [savedDocs, setSavedDocs] = useState<SavedCrmDocument[]>([]);
  const [savedDocsFilter, setSavedDocsFilter] = useState("");
  const [loadingSavedDocs, setLoadingSavedDocs] = useState(false);
  const [deletingDocumentId, setDeletingDocumentId] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    const timer = setTimeout(() => {
      void loadSavedDocuments(savedDocsFilter);
    }, 250);
    return () => clearTimeout(timer);
  }, [savedDocsFilter]);

  async function loadSavedDocuments(query: string): Promise<void> {
    setLoadingSavedDocs(true);
    try {
      const params = new URLSearchParams();
      if (query.trim()) params.set("query", query.trim());
      params.set("limit", "100");
      if (includeDuplicates) params.set("include_duplicates", "1");
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

  async function deleteSavedDocument(documentIdToDelete: string): Promise<boolean> {
    const approved = window.confirm(
      "Удалить документ из CRM? Это действие нельзя отменить.",
    );
    if (!approved) return false;
    setDeletingDocumentId(documentIdToDelete);
    setError("");
    try {
      const resp = await fetch(`${API_BASE}/api/crm/documents/${documentIdToDelete}`, {
        method: "DELETE",
      });
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      await loadSavedDocuments(savedDocsFilter);
      return true;
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed deleting CRM document");
      return false;
    } finally {
      setDeletingDocumentId("");
    }
  }

  return {
    deletingDocumentId,
    error,
    loadingSavedDocs,
    savedDocs,
    savedDocsFilter,
    setSavedDocsFilter,
    loadSavedDocuments,
    deleteSavedDocument,
  };
}
