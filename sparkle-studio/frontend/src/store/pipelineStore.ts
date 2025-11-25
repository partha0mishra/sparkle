import { create } from 'zustand';
import {
  type Node,
  type Edge,
  type OnNodesChange,
  type OnEdgesChange,
  type NodeChange,
  type EdgeChange,
  type Connection,
  applyNodeChanges,
  applyEdgeChanges,
  addEdge as reactFlowAddEdge,
} from 'reactflow';
import type { Pipeline, PipelineMetadata, PipelineConfig } from '@/types/pipeline';

interface PipelineState {
  // Current pipeline
  pipeline: Pipeline | null;
  pipelineName: string | null;

  // React Flow state
  nodes: Node[];
  edges: Edge[];

  // Selected node for properties panel
  selectedNode: Node | null;

  // Undo/Redo
  history: { nodes: Node[]; edges: Edge[] }[];
  historyIndex: number;

  // Loading states
  isLoading: boolean;
  isSaving: boolean;

  // Actions
  setPipeline: (pipeline: Pipeline, name: string) => void;
  setNodes: (nodes: Node[]) => void;
  setEdges: (edges: Edge[]) => void;
  addEdge: (connection: Connection | Edge) => void;
  onNodesChange: OnNodesChange;
  onEdgesChange: OnEdgesChange;
  setSelectedNode: (node: Node | null) => void;
  addNode: (node: Node) => void;
  updateNode: (id: string, data: Partial<Node['data']>) => void;
  deleteNode: (id: string) => void;
  undo: () => void;
  redo: () => void;
  canUndo: () => boolean;
  canRedo: () => boolean;
  saveToHistory: () => void;
  setLoading: (loading: boolean) => void;
  setSaving: (saving: boolean) => void;
  reset: () => void;
}

export const usePipelineStore = create<PipelineState>((set, get) => ({
  pipeline: null,
  pipelineName: null,
  nodes: [],
  edges: [],
  selectedNode: null,
  history: [],
  historyIndex: -1,
  isLoading: false,
  isSaving: false,

  setPipeline: (pipeline, name) => {
    set({
      pipeline,
      pipelineName: name,
      nodes: [],
      edges: [],
      history: [],
      historyIndex: -1,
    });
  },

  setNodes: (nodes) => {
    set({ nodes });
  },

  setEdges: (edges) => {
    set({ edges });
  },

  addEdge: (connection) => {
    const { edges } = get();
    set({ edges: reactFlowAddEdge(connection, edges) });
    get().saveToHistory();
  },

  onNodesChange: (changes: NodeChange[]) => {
    set({
      nodes: applyNodeChanges(changes, get().nodes),
    });
  },

  onEdgesChange: (changes: EdgeChange[]) => {
    set({
      edges: applyEdgeChanges(changes, get().edges),
    });
  },

  setSelectedNode: (node) => {
    set({ selectedNode: node });
  },

  addNode: (node) => {
    const { nodes } = get();
    set({ nodes: [...nodes, node] });
    get().saveToHistory();
  },

  updateNode: (id, data) => {
    const { nodes } = get();
    set({
      nodes: nodes.map((n) =>
        n.id === id ? { ...n, data: { ...n.data, ...data } } : n
      ),
    });
    get().saveToHistory();
  },

  deleteNode: (id) => {
    const { nodes, edges } = get();
    set({
      nodes: nodes.filter((n) => n.id !== id),
      edges: edges.filter((e) => e.source !== id && e.target !== id),
      selectedNode: null,
    });
    get().saveToHistory();
  },

  saveToHistory: () => {
    const { nodes, edges, history, historyIndex } = get();

    // Remove any history after current index (for redo)
    const newHistory = history.slice(0, historyIndex + 1);

    // Add current state
    newHistory.push({ nodes: [...nodes], edges: [...edges] });

    // Limit history to 50 items
    if (newHistory.length > 50) {
      newHistory.shift();
    }

    set({
      history: newHistory,
      historyIndex: newHistory.length - 1,
    });
  },

  undo: () => {
    const { history, historyIndex } = get();
    if (historyIndex > 0) {
      const prevState = history[historyIndex - 1];
      set({
        nodes: prevState.nodes,
        edges: prevState.edges,
        historyIndex: historyIndex - 1,
      });
    }
  },

  redo: () => {
    const { history, historyIndex } = get();
    if (historyIndex < history.length - 1) {
      const nextState = history[historyIndex + 1];
      set({
        nodes: nextState.nodes,
        edges: nextState.edges,
        historyIndex: historyIndex + 1,
      });
    }
  },

  canUndo: () => {
    const { historyIndex } = get();
    return historyIndex > 0;
  },

  canRedo: () => {
    const { history, historyIndex } = get();
    return historyIndex < history.length - 1;
  },

  setLoading: (loading) => set({ isLoading: loading }),
  setSaving: (saving) => set({ isSaving: saving }),

  reset: () => {
    set({
      pipeline: null,
      pipelineName: null,
      nodes: [],
      edges: [],
      selectedNode: null,
      history: [],
      historyIndex: -1,
      isLoading: false,
      isSaving: false,
    });
  },
}));
