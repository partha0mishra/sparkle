/**
 * Hook for fetching and managing components.
 */
import { useState, useCallback, useEffect } from 'react';
import api from '@/lib/api';
import type { APIResponse } from '@/types/api';
import type {
  ComponentListResponse,
  ComponentManifest,
  ComponentValidationResponse,
} from '@/types/component';

export function useComponents() {
  const [components, setComponents] = useState<ComponentListResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  /**
   * Fetch all components.
   */
  const fetchComponents = useCallback(async () => {
    setIsLoading(true);
    try {
      const response = await api.get<APIResponse<ComponentListResponse>>('/components');
      if (response.data.success && response.data.data) {
        setComponents(response.data.data);
        return response.data.data;
      }
      return null;
    } catch (error) {
      console.error('Error fetching components:', error);
      return null;
    } finally {
      setIsLoading(false);
    }
  }, []);

  /**
   * Get component detail.
   */
  const getComponent = useCallback(async (
    category: string,
    name: string
  ): Promise<ComponentManifest | null> => {
    try {
      const response = await api.get<APIResponse<{ component: ComponentManifest }>>(
        `/components/${category}/${name}`
      );
      if (response.data.success && response.data.data) {
        return response.data.data.component;
      }
      return null;
    } catch (error) {
      console.error('Error fetching component:', error);
      return null;
    }
  }, []);

  /**
   * Validate component config.
   */
  const validateConfig = useCallback(async (
    category: string,
    name: string,
    config: Record<string, any>
  ): Promise<ComponentValidationResponse | null> => {
    try {
      const response = await api.post<APIResponse<ComponentValidationResponse>>(
        `/components/${category}/${name}/validate`,
        { config }
      );
      if (response.data.data) {
        return response.data.data;
      }
      return null;
    } catch (error) {
      console.error('Error validating config:', error);
      return null;
    }
  }, []);

  /**
   * Search components.
   */
  const searchComponents = useCallback(async (query: string) => {
    try {
      const response = await api.get(`/components/search`, {
        params: { q: query },
      });
      return response.data.data || [];
    } catch (error) {
      console.error('Error searching components:', error);
      return [];
    }
  }, []);

  // Auto-fetch on mount
  useEffect(() => {
    fetchComponents();
  }, [fetchComponents]);

  return {
    components,
    isLoading,
    fetchComponents,
    getComponent,
    validateConfig,
    searchComponents,
  };
}
