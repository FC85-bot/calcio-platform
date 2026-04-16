import { formatStatusLabel } from "@/lib/format";

export function StatusPill({ value }: { value: string | null | undefined }) {
  const normalized = value?.toLowerCase() ?? "unknown";
  return <span className={`status-pill status-${normalized}`}>{formatStatusLabel(value)}</span>;
}
