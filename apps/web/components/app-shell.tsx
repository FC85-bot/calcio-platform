import Link from "next/link";
import type { PropsWithChildren } from "react";

const navigation = [
  { href: "/", label: "Home" },
  { href: "/competitions", label: "Competitions" },
  { href: "/matches", label: "Matches" },
  { href: "/admin/freshness", label: "Admin freshness" },
];

export function AppShell({ children }: PropsWithChildren) {
  const appName = process.env.NEXT_PUBLIC_APP_NAME ?? "Calcio Platform Dashboard";

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <Link href="/" className="brand-link">
            {appName}
          </Link>
          <p className="muted">Data dashboard consultabile collegata alle API reali.</p>
        </div>
        <nav className="main-nav" aria-label="Main navigation">
          {navigation.map((item) => (
            <Link key={item.href} href={item.href} className="nav-link">
              {item.label}
            </Link>
          ))}
        </nav>
      </header>
      <main className="page-container">{children}</main>
    </div>
  );
}
