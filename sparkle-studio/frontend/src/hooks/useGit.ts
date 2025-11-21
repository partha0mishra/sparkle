/**
 * Hook for Git operations - branches, status, commit, PR.
 */
import { useState, useCallback, useEffect } from 'react';
import api from '@/lib/api';
import type { APIResponse } from '@/types/api';
import type {
  GitBranch,
  GitStatus,
  GitCommitRequest,
  GitCommitResponse,
  GitPRRequest,
  GitPRResponse,
} from '@/types/git';

export function useGit() {
  const [branches, setBranches] = useState<GitBranch[]>([]);
  const [status, setStatus] = useState<GitStatus | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  /**
   * Fetch all branches.
   */
  const fetchBranches = useCallback(async () => {
    try {
      const response = await api.get<APIResponse<GitBranch[]>>('/git/branches');
      if (response.data.success && response.data.data) {
        setBranches(response.data.data);
        return response.data.data;
      }
      return [];
    } catch (error) {
      console.error('Error fetching branches:', error);
      return [];
    }
  }, []);

  /**
   * Fetch Git status.
   */
  const fetchStatus = useCallback(async () => {
    try {
      const response = await api.get<APIResponse<GitStatus>>('/git/status');
      if (response.data.success && response.data.data) {
        setStatus(response.data.data);
        return response.data.data;
      }
      return null;
    } catch (error) {
      console.error('Error fetching status:', error);
      return null;
    }
  }, []);

  /**
   * Commit changes.
   */
  const commit = useCallback(async (
    message: string,
    files?: string[],
    push = false
  ): Promise<GitCommitResponse | null> => {
    setIsLoading(true);
    try {
      const request: GitCommitRequest = {
        message,
        files,
        push,
      };

      const response = await api.post<APIResponse<GitCommitResponse>>(
        '/git/commit',
        request
      );

      if (response.data.success && response.data.data) {
        // Refresh status after commit
        await fetchStatus();
        return response.data.data;
      }
      return null;
    } catch (error) {
      console.error('Error committing:', error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  }, [fetchStatus]);

  /**
   * Pull from remote.
   */
  const pull = useCallback(async (branch?: string): Promise<void> => {
    setIsLoading(true);
    try {
      await api.post('/git/pull', { branch });
      await fetchStatus();
    } catch (error) {
      console.error('Error pulling:', error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  }, [fetchStatus]);

  /**
   * Create pull request.
   */
  const createPR = useCallback(async (
    request: GitPRRequest
  ): Promise<GitPRResponse | null> => {
    setIsLoading(true);
    try {
      const response = await api.post<APIResponse<GitPRResponse>>(
        '/git/create-pr',
        request
      );

      if (response.data.success && response.data.data) {
        return response.data.data;
      }
      return null;
    } catch (error) {
      console.error('Error creating PR:', error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  }, []);

  /**
   * Get current branch.
   */
  const getCurrentBranch = useCallback((): GitBranch | null => {
    return branches.find((b) => b.is_current) || null;
  }, [branches]);

  // Auto-fetch on mount
  useEffect(() => {
    fetchBranches();
    fetchStatus();
  }, [fetchBranches, fetchStatus]);

  return {
    branches,
    status,
    isLoading,
    fetchBranches,
    fetchStatus,
    commit,
    pull,
    createPR,
    getCurrentBranch,
  };
}
