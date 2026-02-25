"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function RootPageRedirect() {
  const router = useRouter();

  useEffect(() => {
    const query = window.location.search || "";
    router.replace(`/workspace/upload${query}`);
  }, [router]);

  return <main className="min-h-screen bg-gradient-to-b from-background to-muted/30" />;
}
