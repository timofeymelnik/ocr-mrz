"use client";

import { useEffect, useState } from "react";

import { API_BASE } from "@/features/workspace/constants";
import { readErrorResponse } from "@/features/workspace/utils";
import type { UploadResponse } from "@/lib/types";

type UseCrmDocumentResult = {
  error: string;
  loading: boolean;
  record: UploadResponse | null;
  reload: () => Promise<void>;
};

export function useCrmDocument(documentId: string): UseCrmDocumentResult {
  const [record, setRecord] = useState<UploadResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function reload(): Promise<void> {
    const id = documentId.trim();
    if (!id) {
      setRecord(null);
      setError("Не указан documentId");
      return;
    }

    setLoading(true);
    setError("");
    try {
      const resp = await fetch(`${API_BASE}/api/crm/documents/${id}`);
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as UploadResponse;
      setRecord(data);
    } catch (e) {
      setRecord(null);
      setError(e instanceof Error ? e.message : "Failed loading CRM document");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, [documentId]);

  return { error, loading, record, reload };
}
