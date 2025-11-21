/**
 * Git type definitions matching backend schemas.
 */

export interface GitBranch {
  name: string;
  is_current: boolean;
  commit_sha?: string;
  last_commit_message?: string;
  last_commit_date?: string;
}

export interface GitStatus {
  current_branch: string;
  is_clean: boolean;
  modified_files: string[];
  untracked_files: string[];
  staged_files: string[];
  ahead: number;
  behind: number;
  remote_url?: string;
}

export interface GitCommitRequest {
  message: string;
  files?: string[];
  push: boolean;
}

export interface GitCommitResponse {
  commit_sha: string;
  message: string;
  author: string;
  timestamp: string;
  files_changed: string[];
  pushed: boolean;
}

export interface GitPRRequest {
  title: string;
  description: string;
  source_branch: string;
  target_branch: string;
  draft: boolean;
}

export interface GitPRResponse {
  pr_number: number;
  pr_url: string;
  title: string;
  source_branch: string;
  target_branch: string;
}
