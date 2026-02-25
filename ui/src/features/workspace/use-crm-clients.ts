"use client";

import { useEffect, useState } from "react";

import { API_BASE } from "@/features/workspace/constants";
import { readErrorResponse } from "@/features/workspace/utils";
import type { SavedCrmDocument } from "@/lib/types";

type UseCrmClientsResult = {
  error: string;
  loadingClients: boolean;
  clients: SavedCrmDocument[];
  clientsFilter: string;
  setClientsFilter: (value: string) => void;
  loadClients: (query: string) => Promise<void>;
};

export function useCrmClients(): UseCrmClientsResult {
  const [clients, setClients] = useState<SavedCrmDocument[]>([]);
  const [clientsFilter, setClientsFilter] = useState("");
  const [loadingClients, setLoadingClients] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    const timer = setTimeout(() => {
      void loadClients(clientsFilter);
    }, 250);
    return () => clearTimeout(timer);
  }, [clientsFilter]);

  async function loadClients(query: string): Promise<void> {
    setLoadingClients(true);
    try {
      const params = new URLSearchParams();
      if (query.trim()) params.set("query", query.trim());
      params.set("limit", "100");
      const resp = await fetch(`${API_BASE}/api/crm/clients?${params.toString()}`);
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as { items?: SavedCrmDocument[] };
      setClients(data.items || []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed loading CRM clients");
    } finally {
      setLoadingClients(false);
    }
  }

  return {
    error,
    loadingClients,
    clients,
    clientsFilter,
    setClientsFilter,
    loadClients,
  };
}
