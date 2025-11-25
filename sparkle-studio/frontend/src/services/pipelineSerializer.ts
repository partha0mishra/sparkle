/**
 * Pipeline serializer for converting between React Flow and backend formats.
 */
import type { Node, Edge } from 'reactflow';
import type {
  Pipeline,
  PipelineNode,
  PipelineEdge,
  PipelineMetadata,
  PipelineConfig,
  NodeData,
} from '@/types/pipeline';

/**
 * Convert React Flow nodes/edges to backend pipeline format.
 */
export function toBackendFormat(
  nodes: Node[],
  edges: Edge[],
  metadata: PipelineMetadata,
  config: PipelineConfig
): Pipeline {
  const pipelineNodes: PipelineNode[] = nodes.map((node) => ({
    id: node.id,
    type: node.type || 'sparkle_component',
    position: node.position,
    data: node.data as NodeData,
  }));

  const pipelineEdges: PipelineEdge[] = edges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    target: edge.target,
    sourceHandle: edge.sourceHandle || undefined,
    targetHandle: edge.targetHandle || undefined,
    type: edge.type || 'default',
    animated: edge.animated || false,
    label: edge.label as string | undefined,
  }));

  return {
    metadata,
    nodes: pipelineNodes,
    edges: pipelineEdges,
    config,
  };
}

/**
 * Convert backend pipeline format to React Flow nodes/edges.
 */
export function fromBackendFormat(pipeline: Pipeline): {
  nodes: Node[];
  edges: Edge[];
  metadata: PipelineMetadata;
  config: PipelineConfig;
} {
  const nodes: Node[] = pipeline.nodes.map((node) => ({
    id: node.id,
    type: node.type,
    position: node.position,
    data: node.data,
  }));

  const edges: Edge[] = pipeline.edges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    target: edge.target,
    sourceHandle: edge.sourceHandle,
    targetHandle: edge.targetHandle,
    type: edge.type || 'smoothstep',
    animated: edge.animated !== undefined ? edge.animated : true,
    label: edge.label,
    markerEnd: 'arrowclosed',
    style: { stroke: '#6b7280' },
  }));

  return {
    nodes,
    edges,
    metadata: pipeline.metadata,
    config: pipeline.config,
  };
}

/**
 * Create default pipeline metadata.
 */
export function createDefaultMetadata(name: string): PipelineMetadata {
  return {
    name,
    display_name: name.replace(/_/g, ' ').replace(/\b\w/g, (l) => l.toUpperCase()),
    description: `Pipeline: ${name}`,
    author: 'Sparkle Studio',
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    version: '1.0.0',
    tags: [],
  };
}

/**
 * Create default pipeline config.
 */
export function createDefaultConfig(): PipelineConfig {
  return {
    timezone: 'UTC',
    parallelism: 4,
    retry_policy: {},
    notifications: {},
    environment: {},
  };
}

/**
 * Create a new empty pipeline.
 */
export function createEmptyPipeline(name: string): Pipeline {
  return {
    metadata: createDefaultMetadata(name),
    nodes: [],
    edges: [],
    config: createDefaultConfig(),
  };
}
