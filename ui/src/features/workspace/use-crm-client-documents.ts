"use client";

import { useEffect, useState } from "react";

import { API_BASE } from "@/features/workspace/constants";
import { readErrorResponse } from "@/features/workspace/utils";
import type { SavedCrmDocument } from "@/lib/types";

type UseCrmClientDocumentsResult = {
  clientDocs: SavedCrmDocument[];
  loadingClientDocs: boolean;
  clientDocsError: string;
  reloadClientDocs: () => Promise<void>;
};

export function useCrmClientDocuments(
  clientId: string,
): UseCrmClientDocumentsResult {
  const [clientDocs, setClientDocs] = useState<SavedCrmDocument[]>([]);
  const [loadingClientDocs, setLoadingClientDocs] = useState(false);
  const [clientDocsError, setClientDocsError] = useState("");

  async function reloadClientDocs(): Promise<void> {
    const key = clientId.trim();
    if (!key) {
      setClientDocs([]);
      setClientDocsError("");
      return;
    }

    setLoadingClientDocs(true);
    setClientDocsError("");
    try {
      const params = new URLSearchParams({
        include_merged: "1",
        limit: "500",
      });
      const resp = await fetch(
        `${API_BASE}/api/crm/clients/${encodeURIComponent(key)}/documents?${params.toString()}`,
      );
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as { items?: SavedCrmDocument[] };
      setClientDocs(data.items || []);
    } catch (e) {
      setClientDocs([]);
      setClientDocsError(
        e instanceof Error
          ? e.message
          : "Failed loading client related documents",
      );
    } finally {
      setLoadingClientDocs(false);
    }
  }

  useEffect(() => {
    void reloadClientDocs();
  }, [clientId]);

  return {
    clientDocs,
    loadingClientDocs,
    clientDocsError,
    reloadClientDocs,
  };
}
