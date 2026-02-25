"use client";

import { useEffect } from "react";
import { useRouter, useSearchParams } from "next/navigation";

export default function WorkspaceIndexPage() {
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    const query = searchParams.toString();
    router.replace(`/workspace/upload${query ? `?${query}` : ""}`);
  }, [router, searchParams]);

  return <main className="min-h-screen bg-gradient-to-b from-background to-muted/30" />;
}
