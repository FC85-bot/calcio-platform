export type ApiResult<T> = {
  data: T | null;
  error: string | null;
  status: number;
};

const DEFAULT_TIMEOUT_MS = 15000;
const DEFAULT_LOCAL_API_BASE_URL = "http://127.0.0.1:8000/api/v1";

function buildUrl(path: string): string {
  const baseUrl = process.env.CALCIO_API_BASE_URL || DEFAULT_LOCAL_API_BASE_URL;
  return `${baseUrl.replace(/\/$/, "")}${path.startsWith("/") ? path : `/${path}`}`;
}

export async function fetchJson<T>(path: string): Promise<ApiResult<T>> {
  try {
    const response = await fetch(buildUrl(path), {
      cache: "no-store",
      signal: AbortSignal.timeout(DEFAULT_TIMEOUT_MS),
      headers: {
        Accept: "application/json",
      },
    });

    if (!response.ok) {
      const text = await response.text();
      return {
        data: null,
        error: text || `Request failed with status ${response.status}`,
        status: response.status,
      };
    }

    const data = (await response.json()) as T;
    return { data, error: null, status: response.status };
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unexpected fetch error";
    return {
      data: null,
      error: message,
      status: 0,
    };
  }
}
