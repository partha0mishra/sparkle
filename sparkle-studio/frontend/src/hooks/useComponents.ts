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
  const [error, setError] = useState<string | null>(null);

  /**
   * Fetch all components.
   */
  const fetchComponents = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      console.log('[useComponents] Fetching from:', api.defaults.baseURL);
      const response = await api.get<APIResponse<ComponentListResponse>>('/components');
      console.log('[useComponents] Response:', response.data);

      if (response.data.success && response.data.data) {
        setComponents(response.data.data);
        console.log('[useComponents] Loaded components:', response.data.data);
        return response.data.data;
      }

      const errorMsg = response.data.message || 'Failed to load components';
      setError(errorMsg);
      console.error('[useComponents] Error in response:', errorMsg);
      return null;
    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to connect to backend';
      console.error('[useComponents] Error fetching components:', err);
      console.error('[useComponents] Error details:', {
        status: err.response?.status,
        data: err.response?.data,
        message: err.message,
      });
      setError(errorMessage);
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
    error,
    fetchComponents,
    getComponent,
    validateConfig,
    searchComponents,
  };
}
