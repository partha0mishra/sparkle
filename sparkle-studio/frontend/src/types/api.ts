/**
 * API response type definitions.
 */

export interface APIResponse<T> {
  success: boolean;
  data?: T;
  message?: string;
  error?: string;
}

export interface HealthResponse {
  status: string;
  version: string;
  environment: string;
  spark_available: boolean;
  git_repo_path: string;
  git_repo_exists: boolean;
}
