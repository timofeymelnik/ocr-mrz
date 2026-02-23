import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Tasa OCR Workspace",
  description: "Upload document, review extracted fields, and run manual handoff autofill.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}

