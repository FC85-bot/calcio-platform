export function EmptyState({ title, message }: { title: string; message: string }) {
  return (
    <div className="state-block">
      <strong>{title}</strong>
      <p>{message}</p>
    </div>
  );
}

export function ErrorState({ title, message }: { title: string; message: string }) {
  return (
    <div className="state-block state-block-error">
      <strong>{title}</strong>
      <p>{message}</p>
    </div>
  );
}
