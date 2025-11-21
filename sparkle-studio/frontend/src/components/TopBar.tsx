/**
 * Top bar with Git controls and actions.
 */
import React, { useState } from 'react';
import {
  GitBranch,
  GitCommit,
  GitPullRequest,
  Download,
  Play,
  Save,
  Undo,
  Redo,
  Calendar,
} from 'lucide-react';
import { useGit } from '@/hooks/useGit';
import { usePipeline } from '@/hooks/usePipeline';
import { usePipelineStore } from '@/store/pipelineStore';
import { cn } from '@/lib/utils';

export function TopBar() {
  const { status, branches, commit, pull, isLoading } = useGit();
  const { savePipeline, pipelineName, isSaving } = usePipeline();
  const { canUndo, canRedo, undo, redo } = usePipelineStore();
  const [commitMessage, setCommitMessage] = useState('');
  const [showCommitDialog, setShowCommitDialog] = useState(false);

  const currentBranch = branches.find((b) => b.is_current);

  const handleSave = async () => {
    try {
      await savePipeline(`Update pipeline: ${pipelineName}`);
    } catch (error) {
      console.error('Save failed:', error);
    }
  };

  const handleCommit = async () => {
    if (!commitMessage.trim()) return;
    try {
      await commit(commitMessage, undefined, false);
      setCommitMessage('');
      setShowCommitDialog(false);
    } catch (error) {
      console.error('Commit failed:', error);
    }
  };

  const handlePull = async () => {
    try {
      await pull();
    } catch (error) {
      console.error('Pull failed:', error);
    }
  };

  return (
    <div className="h-14 border-b border-border bg-card px-4 flex items-center justify-between">
      {/* Left: Pipeline name and Git status */}
      <div className="flex items-center gap-4">
        <h1 className="text-lg font-semibold">{pipelineName || 'Sparkle Studio'}</h1>

        {status && (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <GitBranch className="w-4 h-4" />
            <span>{status.current_branch}</span>
            {!status.is_clean && (
              <span className="text-orange-500">● {status.modified_files.length} modified</span>
            )}
            {status.ahead > 0 && <span className="text-green-500">↑{status.ahead}</span>}
            {status.behind > 0 && <span className="text-red-500">↓{status.behind}</span>}
          </div>
        )}
      </div>

      {/* Right: Actions */}
      <div className="flex items-center gap-2">
        {/* Undo/Redo */}
        <button
          onClick={undo}
          disabled={!canUndo()}
          className={cn(
            'p-2 rounded hover:bg-accent',
            !canUndo() && 'opacity-50 cursor-not-allowed'
          )}
          title="Undo (Ctrl+Z)"
        >
          <Undo className="w-4 h-4" />
        </button>
        <button
          onClick={redo}
          disabled={!canRedo()}
          className={cn(
            'p-2 rounded hover:bg-accent',
            !canRedo() && 'opacity-50 cursor-not-allowed'
          )}
          title="Redo (Ctrl+Shift+Z)"
        >
          <Redo className="w-4 h-4" />
        </button>

        <div className="w-px h-6 bg-border mx-2" />

        {/* Git Actions */}
        <button
          onClick={handlePull}
          disabled={isLoading}
          className="flex items-center gap-2 px-3 py-1.5 text-sm rounded hover:bg-accent"
          title="Pull from remote"
        >
          <Download className="w-4 h-4" />
          Pull
        </button>

        <button
          onClick={() => setShowCommitDialog(true)}
          className="flex items-center gap-2 px-3 py-1.5 text-sm rounded hover:bg-accent"
          title="Commit changes"
        >
          <GitCommit className="w-4 h-4" />
          Commit
        </button>

        <div className="w-px h-6 bg-border mx-2" />

        {/* Save */}
        <button
          onClick={handleSave}
          disabled={isSaving}
          className="flex items-center gap-2 px-3 py-1.5 text-sm rounded bg-primary text-primary-foreground hover:bg-primary/90"
          title="Save (Ctrl+S)"
        >
          <Save className="w-4 h-4" />
          {isSaving ? 'Saving...' : 'Save'}
        </button>

        {/* Run */}
        <button
          className="flex items-center gap-2 px-3 py-1.5 text-sm rounded border border-green-500 text-green-500 hover:bg-green-500/10"
          title="Run pipeline"
        >
          <Play className="w-4 h-4" />
          Run
        </button>
      </div>

      {/* Commit Dialog (Simple) */}
      {showCommitDialog && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-card p-6 rounded-lg shadow-lg w-96 border border-border">
            <h3 className="text-lg font-semibold mb-4">Commit Changes</h3>
            <input
              type="text"
              value={commitMessage}
              onChange={(e) => setCommitMessage(e.target.value)}
              placeholder="Commit message..."
              className="w-full px-3 py-2 border border-input rounded-md mb-4"
              autoFocus
              onKeyDown={(e) => {
                if (e.key === 'Enter') handleCommit();
                if (e.key === 'Escape') setShowCommitDialog(false);
              }}
            />
            <div className="flex gap-2 justify-end">
              <button
                onClick={() => setShowCommitDialog(false)}
                className="px-4 py-2 text-sm rounded hover:bg-accent"
              >
                Cancel
              </button>
              <button
                onClick={handleCommit}
                disabled={!commitMessage.trim()}
                className="px-4 py-2 text-sm rounded bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              >
                Commit
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
