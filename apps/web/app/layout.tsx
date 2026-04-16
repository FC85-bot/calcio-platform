import type { Metadata } from "next";
import type { PropsWithChildren } from "react";
import { AppShell } from "@/components/app-shell";
import "./globals.css";

export const metadata: Metadata = {
  title: "Calcio Platform Dashboard",
  description: "Dashboard dati per competitions, matches, standings, odds e freshness del layer ingestione.",
};

export default function RootLayout({ children }: PropsWithChildren) {
  return (
    <html lang="it">
      <body>
        <AppShell>{children}</AppShell>
      </body>
    </html>
  );
}
