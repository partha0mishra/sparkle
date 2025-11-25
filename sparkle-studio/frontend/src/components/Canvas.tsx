/**
 * Main canvas component with React Flow.
 */
import React, { useCallback } from 'react';
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  addEdge,
  type Connection,
  type Edge,
} from 'reactflow';
import 'reactflow/dist/style.css';

import { nodeTypes } from './NodeTypes';
import { usePipelineStore } from '@/store/pipelineStore';
import { usePipeline } from '@/hooks/usePipeline';

export function Canvas() {
  const { selectedNode, setSelectedNode, saveToHistory } = usePipelineStore();
  const { nodes, edges, onNodesChange, onEdgesChange, addEdge: addEdgeToStore } = usePipeline();

  const onConnect = useCallback(
    (connection: Connection) => {
      const newEdge = {
        ...connection,
        type: 'smoothstep',
        animated: true,
        markerEnd: {
          type: 'arrowclosed' as const,
          color: '#6b7280',
        },
      };
      addEdgeToStore(newEdge);
    },
    [addEdgeToStore]
  );

  const onNodeClick = useCallback(
    (_event: React.MouseEvent, node: any) => {
      setSelectedNode(node);
    },
    [setSelectedNode]
  );

  const onPaneClick = useCallback(() => {
    setSelectedNode(null);
  }, [setSelectedNode]);

  return (
    <div className="flex-1 h-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        nodeTypes={nodeTypes}
        fitView
        className="bg-background"
      >
        <Background />
        <Controls />
        <MiniMap
          nodeColor={(node) => {
            const colors: Record<string, string> = {
              ingestor: '#22c55e',
              transformer: '#a855f7',
              ml: '#f97316',
              sink: '#ef4444',
              connection: '#3b82f6',
            };
            return colors[node.data.component_type] || '#6b7280';
          }}
        />
      </ReactFlow>
    </div>
  );
}
