/**
 * Hook for managing pipelines - load, save, commit.
 */
import { useState, useCallback } from 'react';
import { useNodesState, useEdgesState } from 'reactflow';
import api from '@/lib/api';
import type { APIResponse } from '@/types/api';
import type { Pipeline, PipelineListItem } from '@/types/pipeline';
import { toBackendFormat, fromBackendFormat, createEmptyPipeline } from '@/services/pipelineSerializer';
import { usePipelineStore } from '@/store/pipelineStore';

export function usePipeline() {
  const {
    pipeline,
    pipelineName,
    setPipeline,
    setLoading,
    setSaving,
    isLoading,
    isSaving,
  } = usePipelineStore();

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  /**
   * List all pipelines.
   */
  const listPipelines = useCallback(async (): Promise<PipelineListItem[]> => {
    try {
      const response = await api.get<APIResponse<PipelineListItem[]>>('/pipelines');
      return response.data.data || [];
    } catch (error) {
      console.error('Error listing pipelines:', error);
      return [];
    }
  }, []);

  /**
   * Load a pipeline by name.
   */
  const loadPipeline = useCallback(async (name: string) => {
    setLoading(true);
    try {
      const response = await api.get<APIResponse<Pipeline>>(`/pipelines/${name}`);
      if (response.data.success && response.data.data) {
        const pipelineData = response.data.data;
        const { nodes: flowNodes, edges: flowEdges } = fromBackendFormat(pipelineData);

        setPipeline(pipelineData, name);
        setNodes(flowNodes);
        setEdges(flowEdges);

        return pipelineData;
      }
    } catch (error) {
      console.error('Error loading pipeline:', error);
      throw error;
    } finally {
      setLoading(false);
    }
  }, [setPipeline, setNodes, setEdges, setLoading]);

  /**
   * Save current pipeline.
   */
  const savePipeline = useCallback(async (
    commitMessage?: string,
    autoCommit = true
  ): Promise<void> => {
    if (!pipelineName || !pipeline) {
      throw new Error('No pipeline loaded');
    }

    setSaving(true);
    try {
      const pipelineData = toBackendFormat(
        nodes,
        edges,
        pipeline.metadata,
        pipeline.config
      );

      const response = await api.post<APIResponse<Pipeline>>(
        `/pipelines/${pipelineName}`,
        {
          pipeline: pipelineData,
          commit_message: commitMessage || `Update pipeline: ${pipelineName}`,
        }
      );

      if (response.data.success) {
        console.log('Pipeline saved successfully');
      }
    } catch (error) {
      console.error('Error saving pipeline:', error);
      throw error;
    } finally {
      setSaving(false);
    }
  }, [pipelineName, pipeline, nodes, edges, setSaving]);

  /**
   * Create a new pipeline.
   */
  const createPipeline = useCallback(async (name: string): Promise<Pipeline> => {
    setLoading(true);
    try {
      const newPipeline = createEmptyPipeline(name);

      const response = await api.post<APIResponse<Pipeline>>(
        `/pipelines`,
        {
          pipeline_name: name,
        }
      );

      if (response.data.success && response.data.data) {
        setPipeline(response.data.data, name);
        setNodes([]);
        setEdges([]);
        return response.data.data;
      }

      // Fallback to empty pipeline
      setPipeline(newPipeline, name);
      setNodes([]);
      setEdges([]);
      return newPipeline;
    } catch (error) {
      console.error('Error creating pipeline:', error);
      throw error;
    } finally {
      setLoading(false);
    }
  }, [setPipeline, setNodes, setEdges, setLoading]);

  /**
   * Delete a pipeline.
   */
  const deletePipeline = useCallback(async (name: string): Promise<void> => {
    try {
      await api.delete(`/pipelines/${name}`);
    } catch (error) {
      console.error('Error deleting pipeline:', error);
      throw error;
    }
  }, []);

  return {
    // State
    pipeline,
    pipelineName,
    nodes,
    edges,
    isLoading,
    isSaving,

    // React Flow handlers
    onNodesChange,
    onEdgesChange,
    setNodes,
    setEdges,

    // Actions
    listPipelines,
    loadPipeline,
    savePipeline,
    createPipeline,
    deletePipeline,
  };
}
