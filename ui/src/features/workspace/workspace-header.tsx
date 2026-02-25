"use client";

import Link from "next/link";
import { Search, UserCircle2 } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";

type WorkspaceHeaderProps = {
  activeTab: "upload" | "crm";
  workspaceBadge: string;
};

export function WorkspaceHeader({
  activeTab,
  workspaceBadge,
}: WorkspaceHeaderProps) {
  return (
    <header className="sticky top-0 z-30 border-b border-border/70 bg-background/95 backdrop-blur">
      <div className="mx-auto flex h-14 max-w-[1600px] items-center gap-4 px-4 lg:px-6">
        <div className="flex items-center gap-2">
          <div className="h-2 w-2 rounded-full bg-primary" />
          <span className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
            OCR Workspace
          </span>
        </div>
        <Separator orientation="vertical" className="h-5" />
        <nav className="flex items-center gap-1">
          <Button asChild size="sm" variant={activeTab === "upload" ? "secondary" : "outline"}>
            <Link href="/">Загрузка</Link>
          </Button>
          <Button asChild size="sm" variant={activeTab === "crm" ? "secondary" : "outline"}>
            <Link href="/crm">CRM</Link>
          </Button>
          <Button size="sm" variant="outline" disabled>
            Дашборд
          </Button>
        </nav>
        <div className="ml-auto flex items-center gap-2">
          <div className="relative hidden sm:block">
            <Search className="pointer-events-none absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
            <Input className="h-9 w-64 pl-8" placeholder="Поиск клиента..." />
          </div>
          <Badge variant={workspaceBadge === "В работе" ? "default" : "secondary"}>
            {workspaceBadge}
          </Badge>
          <div className="flex h-8 w-8 items-center justify-center rounded-full bg-primary/10 text-primary">
            <UserCircle2 className="h-4 w-4" />
          </div>
        </div>
      </div>
    </header>
  );
}
