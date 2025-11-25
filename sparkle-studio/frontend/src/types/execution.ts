/**
 * Execution type definitions for Phase 4.
 */

export type ExecutionStatus = 'pending' | 'running' | 'success' | 'failed' | 'cancelled';
export type NodeExecutionStatus = 'pending' | 'running' | 'success' | 'failed' | 'skipped';
export type ExecutionMode = 'dry_run' | 'full' | 'backfill';

export interface ExecutionRequest {
  pipeline_name: string;
  mode: ExecutionMode;
  start_date?: string;
  end_date?: string;
  parallelism?: number;
  sample_size?: number;
  config_overrides?: Record<string, any>;
}

export interface NodeExecutionInfo {
  node_id: string;
  node_name: string;
  component_type: string;
  status: NodeExecutionStatus;
  start_time?: string;
  end_time?: string;
  duration_seconds?: number;
  rows_processed?: number;
  rows_written?: number;
  error?: string;
  progress_percent: number;
}

export interface ExecutionMetrics {
  total_rows_processed: number;
  total_rows_written: number;
  peak_memory_mb: number;
  total_cpu_seconds: number;
  data_read_mb: number;
  data_written_mb: number;
  num_tasks: number;
  num_stages: number;
}

export interface ExecutionResponse {
  run_id: string;
  pipeline_name: string;
  status: ExecutionStatus;
  submitted_at: string;
  started_at?: string;
  completed_at?: string;
  duration_seconds?: number;
  error_message?: string;
}

export interface ExecutionProgress {
  run_id: string;
  pipeline_name: string;
  status: ExecutionStatus;
  progress_percent: number;
  current_node?: string;
  nodes: NodeExecutionInfo[];
  elapsed_seconds: number;
  estimated_remaining_seconds?: number;
}

export interface ExecutionListItem {
  run_id: string;
  pipeline_name: string;
  status: ExecutionStatus;
  submitted_at: string;
  started_at?: string;
  completed_at?: string;
  duration_seconds?: number;
  dry_run: boolean;
  error_message?: string;
}

export interface WebSocketMessage {
  type: 'log' | 'progress' | 'status' | 'error' | 'complete';
  run_id: string;
  timestamp: string;
  data: any;
}
