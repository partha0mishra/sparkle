/**
 * Custom node types for React Flow.
 */
import type { NodeTypes } from 'reactflow';
import { CustomNode } from './CustomNode';

export const nodeTypes: NodeTypes = {
  sparkle_component: CustomNode,
  ingestor: CustomNode,
  transformer: CustomNode,
  ml: CustomNode,
  sink: CustomNode,
  connection: CustomNode,
};
