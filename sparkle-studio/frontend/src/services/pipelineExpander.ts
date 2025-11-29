/**
 * Utility to expand orchestrator pipeline components into DAGs of constituent tasks.
 */
import type { Node, Edge } from 'reactflow';
import { MarkerType } from 'reactflow';
import api from '@/lib/api';
import type { APIResponse } from '@/types/api';
import type { ComponentManifest } from '@/types/component';
import { generateId } from '@/lib/utils';

interface PipelineGraph {
  nodes: {
    id: string;
    type: string;
    label: string;
    config: Record<string, any>;
  }[];
  edges: {
    source: string;
    target: string;
  }[];
}

interface ExpandedPipeline {
  nodes: Node[];
  edges: Edge[];
}

/**
 * Map task types to component categories for visual representation.
 */
const taskTypeToCategory: Record<string, string> = {
  // Ingest tasks
  'IngestTask': 'ingestor',

  // Transform tasks
  'TransformTask': 'transformer',

  // Write tasks
  'WriteDeltaTask': 'sink',
  'WriteFeatureTableTask': 'ml',
  'CreateUnityCatalogTableTask': 'sink',

  // ML tasks
  'TrainMLModelTask': 'ml',
  'BatchScoreTask': 'ml',
  'RegisterModelTask': 'ml',
  'PromoteModelChampionTask': 'ml',

  // Quality tasks
  'RunGreatExpectationsTask': 'transformer',
  'RunDbtCoreTask': 'transformer',
  'RunSQLFileTask': 'transformer',

  // Governance tasks
  'GrantPermissionsTask': 'sink',
  'WaitForTableFreshnessTask': 'transformer',

  // Notification tasks
  'SendSlackAlertTask': 'sink',
  'SendEmailAlertTask': 'sink',
  'NotifyOnFailureTask': 'sink',

  // Orchestration tasks
  'RunNotebookTask': 'transformer',
  'TriggerDownstreamPipelineTask': 'orchestrator',
  'AutoRecoverTask': 'transformer',

  // Observability tasks
  'MonteCarloDataIncidentTask': 'sink',
  'LightupDataSLAAlertTask': 'sink',
};

/**
 * Get icons for different task types.
 */
const taskTypeToIcon: Record<string, string> = {
  'IngestTask': 'download',
  'TransformTask': 'zap',
  'WriteDeltaTask': 'upload',
  'WriteFeatureTableTask': 'brain',
  'TrainMLModelTask': 'brain',
  'BatchScoreTask': 'brain',
  'RunGreatExpectationsTask': 'check-circle',
  'SendSlackAlertTask': 'bell',
  'SendEmailAlertTask': 'mail',
};

/**
 * Calculate auto-layout positions for DAG nodes using a simple layered approach.
 */
function calculateLayout(graph: PipelineGraph): Record<string, { x: number; y: number }> {
  const positions: Record<string, { x: number; y: number }> = {};

  // Build adjacency list for topological sort
  const inDegree: Record<string, number> = {};
  const adjacency: Record<string, string[]> = {};

  graph.nodes.forEach(node => {
    inDegree[node.id] = 0;
    adjacency[node.id] = [];
  });

  graph.edges.forEach(edge => {
    adjacency[edge.source].push(edge.target);
    inDegree[edge.target] = (inDegree[edge.target] || 0) + 1;
  });

  // Topological sort to determine layers
  const layers: string[][] = [];
  const queue: string[] = [];
  const layerMap: Record<string, number> = {};

  // Start with nodes that have no incoming edges
  graph.nodes.forEach(node => {
    if (inDegree[node.id] === 0) {
      queue.push(node.id);
      layerMap[node.id] = 0;
    }
  });

  while (queue.length > 0) {
    const current = queue.shift()!;
    const currentLayer = layerMap[current];

    if (!layers[currentLayer]) {
      layers[currentLayer] = [];
    }
    layers[currentLayer].push(current);

    adjacency[current].forEach(neighbor => {
      inDegree[neighbor]--;
      if (inDegree[neighbor] === 0) {
        layerMap[neighbor] = currentLayer + 1;
        queue.push(neighbor);
      }
    });
  }

  // Position nodes
  const HORIZONTAL_SPACING = 300;
  const VERTICAL_SPACING = 120;

  layers.forEach((layer, layerIndex) => {
    const layerHeight = layer.length * VERTICAL_SPACING;
    const startY = -layerHeight / 2;

    layer.forEach((nodeId, index) => {
      positions[nodeId] = {
        x: layerIndex * HORIZONTAL_SPACING,
        y: startY + index * VERTICAL_SPACING,
      };
    });
  });

  return positions;
}

/**
 * Check if a component is an orchestrator pipeline that should be expanded.
 * Pipeline components are those ending with "Pipeline" (not adapters, tasks, or schedules).
 */
export function isPipelineComponent(componentCategory: string, componentName: string): boolean {
  if (componentCategory !== 'orchestrator') {
    return false;
  }

  // Expand only pipeline templates (not adapters, tasks, or schedules)
  // Pipeline templates end with "Pipeline"
  const isPipelineTemplate = componentName.endsWith('Pipeline');

  // Exclude adapters (contain "Adapter"), tasks (contain "Task"), and schedules (contain "Schedule")
  const isAdapter = componentName.includes('Adapter');
  const isTask = componentName.includes('Task');
  const isSchedule = componentName.includes('Schedule') || componentName.includes('Monitor') ||
                     componentName.includes('Builder') || componentName.includes('Visualizer') ||
                     componentName.includes('Trigger');

  return isPipelineTemplate && !isAdapter && !isTask && !isSchedule;
}

/**
 * Fetch component details and expand pipeline into DAG.
 */
export async function expandPipeline(
  category: string,
  name: string,
  dropPosition: { x: number; y: number }
): Promise<ExpandedPipeline | null> {
  try {
    // For orchestrator pipelines, the API expects "pipeline_" prefix
    const apiName = category === 'orchestrator' ? `pipeline_${name}` : name;

    // Fetch component details to get the graph
    const response = await api.get<APIResponse<{ component: ComponentManifest }>>(
      `/components/${category}/${apiName}`
    );

    if (!response.data.success || !response.data.data) {
      return null;
    }

    const manifest = response.data.data.component;
    const configSchema = manifest.config_schema;

    // Check if this is a pipeline with a graph
    if (!configSchema || configSchema.type !== 'pipeline' || !configSchema.graph) {
      return null;
    }

    const graph: PipelineGraph = configSchema.graph;

    // Calculate layout positions
    const positions = calculateLayout(graph);

    // Convert graph nodes to React Flow nodes
    const nodes: Node[] = graph.nodes.map((graphNode) => {
      const position = positions[graphNode.id];
      const category = taskTypeToCategory[graphNode.type] || 'transformer';
      const icon = taskTypeToIcon[graphNode.type] || 'component';

      return {
        id: `${name}_${graphNode.id}_${generateId()}`,
        type: 'sparkle_component',
        position: {
          x: dropPosition.x + position.x,
          y: dropPosition.y + position.y,
        },
        data: {
          // For task nodes from pipeline expansion, use orchestrator category
          // and task_ prefix to match API structure
          component_type: 'orchestrator',
          component_name: `task_${graphNode.type}`,
          label: graphNode.label,
          description: `${graphNode.type} - ${graphNode.label}`,
          config: graphNode.config || {},
          task_type: graphNode.type,
          icon: icon,
          // Visual category for color coding (ingestor, transformer, ml, sink)
          visual_category: category,
          // Mark as task node from pipeline expansion
          is_task_node: true,
        },
      };
    });

    // Create a mapping from old IDs to new IDs
    const idMap: Record<string, string> = {};
    graph.nodes.forEach((graphNode, index) => {
      idMap[graphNode.id] = nodes[index].id;
    });

    // Convert graph edges to React Flow edges
    const edges: Edge[] = graph.edges.map((graphEdge, index) => {
      return {
        id: `${name}_edge_${index}_${generateId()}`,
        source: idMap[graphEdge.source],
        target: idMap[graphEdge.target],
        type: 'smoothstep',
        animated: true,
        markerEnd: {
          type: MarkerType.ArrowClosed,
          width: 20,
          height: 20,
          color: '#6b7280',
        },
        style: {
          stroke: '#6b7280',
          strokeWidth: 2,
        },
      };
    });

    return { nodes, edges };
  } catch (error) {
    console.error('Error expanding pipeline:', error);
    return null;
  }
}
