/**
 * Pipeline type definitions matching backend schemas.
 */

export interface Position {
  x: number;
  y: number;
}

export interface NodeData {
  component_type: string; // ingestor, transformer, ml, connection
  component_name: string;
  label: string;
  config: Record<string, any>;
  description?: string;
}

export interface PipelineNode {
  id: string;
  type: string;
  position: Position;
  data: NodeData;
}

export interface PipelineEdge {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string;
  targetHandle?: string;
  type?: string;
  animated?: boolean;
  label?: string;
}

export interface PipelineMetadata {
  name: string;
  display_name?: string;
  description?: string;
  author?: string;
  created_at?: string;
  updated_at?: string;
  version: string;
  tags: string[];
}

export interface PipelineConfig {
  schedule?: string;
  timezone: string;
  parallelism: number;
  retry_policy: Record<string, any>;
  notifications: Record<string, any>;
  environment: Record<string, string>;
}

export interface Pipeline {
  metadata: PipelineMetadata;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
  config: PipelineConfig;
}

export interface PipelineListItem {
  name: string;
  display_name?: string;
  description?: string;
  author?: string;
  updated_at?: string;
  node_count: number;
  edge_count: number;
  tags: string[];
}
