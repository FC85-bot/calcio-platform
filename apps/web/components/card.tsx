import type { PropsWithChildren, ReactNode } from "react";

export function Card({ children, title, description }: PropsWithChildren<{ title?: string; description?: ReactNode }>) {
  return (
    <section className="card">
      {(title || description) && (
        <div className="card-header">
          {title ? <h2>{title}</h2> : null}
          {description ? <p className="muted">{description}</p> : null}
        </div>
      )}
      {children}
    </section>
  );
}
