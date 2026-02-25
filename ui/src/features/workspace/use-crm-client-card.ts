"use client";

import { useEffect, useState } from "react";

import { API_BASE } from "@/features/workspace/constants";
import { readErrorResponse } from "@/features/workspace/utils";
import type { ClientCardResponse } from "@/lib/types";

type UseCrmClientCardResult = {
  error: string;
  loading: boolean;
  card: ClientCardResponse | null;
  reload: () => Promise<void>;
};

export function useCrmClientCard(clientId: string): UseCrmClientCardResult {
  const [card, setCard] = useState<ClientCardResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function reload(): Promise<void> {
    const id = clientId.trim();
    if (!id) {
      setCard(null);
      setError("Не указан clientId");
      return;
    }
    setLoading(true);
    setError("");
    try {
      const resp = await fetch(`${API_BASE}/api/crm/clients/${encodeURIComponent(id)}`);
      if (!resp.ok) {
        throw new Error(await readErrorResponse(resp));
      }
      const data = (await resp.json()) as ClientCardResponse;
      setCard(data);
    } catch (e) {
      setCard(null);
      setError(e instanceof Error ? e.message : "Failed loading CRM client");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, [clientId]);

  return { error, loading, card, reload };
}

