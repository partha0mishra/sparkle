/**
 * Execution Panel for running pipelines and monitoring progress (Phase 4).
 */
import React, { useState, useEffect, useRef } from 'react';
import { Play, Square, X, Loader2, CheckCircle, XCircle, Clock, AlertCircle } from 'lucide-react';
import type {
  ExecutionStatus,
  ExecutionProgress,
  NodeExecutionInfo,
  WebSocketMessage,
} from '@/types/execution';

interface ExecutionPanelProps {
  pipelineName: string;
  onClose: () => void;
}

export function ExecutionPanel({ pipelineName, onClose }: ExecutionPanelProps) {
  const [isRunning, setIsRunning] = useState(false);
  const [runId, setRunId] = useState<string | null>(null);
  const [status, setStatus] = useState<ExecutionStatus>('pending');
  const [progress, setProgress] = useState<ExecutionProgress | null>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const logsEndRef = useRef<HTMLDivElement>(null);
  const wsRef = useRef<WebSocket | null>(null);

  // Auto-scroll logs to bottom
  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs]);

  // Cleanup WebSocket on unmount
  useEffect(() => {
    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, []);

  const connectWebSocket = (executionRunId: string) => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.host;
    const wsUrl = `${protocol}//${host}/api/v1/execute/${executionRunId}/logs`;

    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = () => {
      addLog('Connected to execution stream');
    };

    ws.onmessage = (event) => {
      const message: WebSocketMessage = JSON.parse(event.data);

      switch (message.type) {
        case 'status':
          setStatus(message.data.status);
          addLog(`Status: ${message.data.message}`);
          break;

        case 'progress':
          setProgress((prev) => ({
            ...prev!,
            progress_percent: message.data.progress_percent,
            current_node: message.data.current_node,
            nodes: message.data.nodes,
          }));
          break;

        case 'log':
          addLog(message.data.message);
          break;

        case 'error':
          setError(message.data.message || message.message);
          addLog(`ERROR: ${message.data.message || message.message}`);
          break;

        case 'complete':
          setStatus(message.data.status);
          setIsRunning(false);
          addLog(`Execution completed with status: ${message.data.status}`);
          if (message.data.error_message) {
            setError(message.data.error_message);
          }
          break;
      }
    };

    ws.onerror = (error) => {
      console.error('WebSocket error:', error);
      addLog('WebSocket connection error');
    };

    ws.onclose = () => {
      addLog('Disconnected from execution stream');
      setIsRunning(false);
    };
  };

  const addLog = (message: string) => {
    const timestamp = new Date().toLocaleTimeString();
    setLogs((prev) => [...prev, `[${timestamp}] ${message}`]);
  };

  const handleRun = async () => {
    setIsRunning(true);
    setLogs([]);
    setError(null);
    setStatus('pending');

    try {
      addLog(`Starting execution of pipeline: ${pipelineName}`);

      const response = await fetch('/api/v1/execute/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pipeline_name: pipelineName,
          parameters: {},
          environment: {},
          dry_run: false,
        }),
      });

      const result = await response.json();

      if (result.success && result.data) {
        const executionRunId = result.data.run_id;
        setRunId(executionRunId);
        addLog(`Execution submitted: ${executionRunId}`);

        // Connect WebSocket for real-time updates
        connectWebSocket(executionRunId);
      } else {
        throw new Error(result.error || 'Failed to start execution');
      }
    } catch (err: any) {
      setError(err.message);
      addLog(`Error: ${err.message}`);
      setIsRunning(false);
    }
  };

  const handleStop = async () => {
    if (!runId) return;

    try {
      addLog('Stopping execution...');
      await fetch(`/api/v1/execute/${runId}/stop`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ run_id: runId, force: false }),
      });

      if (wsRef.current) {
        wsRef.current.close();
      }

      setIsRunning(false);
      addLog('Execution stopped');
    } catch (err: any) {
      setError(err.message);
      addLog(`Error stopping execution: ${err.message}`);
    }
  };

  const getStatusIcon = () => {
    switch (status) {
      case 'running':
        return <Loader2 className="w-5 h-5 text-blue-500 animate-spin" />;
      case 'success':
        return <CheckCircle className="w-5 h-5 text-green-500" />;
      case 'failed':
        return <XCircle className="w-5 h-5 text-red-500" />;
      case 'cancelled':
        return <AlertCircle className="w-5 h-5 text-yellow-500" />;
      default:
        return <Clock className="w-5 h-5 text-gray-500" />;
    }
  };

  return (
    <div className="fixed right-0 top-0 h-full w-[600px] bg-card border-l border-border shadow-xl flex flex-col z-50">
      {/* Header */}
      <div className="p-4 border-b border-border flex items-center justify-between">
        <div className="flex items-center gap-3">
          {getStatusIcon()}
          <div>
            <h2 className="font-semibold">Pipeline Execution</h2>
            <p className="text-sm text-muted-foreground">{pipelineName}</p>
          </div>
        </div>
        <button
          onClick={onClose}
          className="p-2 hover:bg-accent rounded transition-colors"
        >
          <X className="w-5 h-5" />
        </button>
      </div>

      {/* Progress */}
      {progress && (
        <div className="p-4 border-b border-border">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium">Progress</span>
            <span className="text-sm text-muted-foreground">
              {progress.progress_percent.toFixed(0)}%
            </span>
          </div>
          <div className="w-full h-2 bg-secondary rounded-full overflow-hidden">
            <div
              className="h-full bg-primary transition-all duration-300"
              style={{ width: `${progress.progress_percent}%` }}
            />
          </div>
          {progress.current_node && (
            <p className="text-xs text-muted-foreground mt-2">
              Current: {progress.current_node}
            </p>
          )}
        </div>
      )}

      {/* Node Status */}
      {progress?.nodes && progress.nodes.length > 0 && (
        <div className="p-4 border-b border-border">
          <h3 className="text-sm font-medium mb-2">Node Status</h3>
          <div className="space-y-1 max-h-32 overflow-y-auto">
            {progress.nodes.map((node) => (
              <div key={node.node_id} className="flex items-center gap-2 text-sm">
                {node.status === 'running' && (
                  <Loader2 className="w-4 h-4 text-blue-500 animate-spin" />
                )}
                {node.status === 'success' && (
                  <CheckCircle className="w-4 h-4 text-green-500" />
                )}
                {node.status === 'failed' && (
                  <XCircle className="w-4 h-4 text-red-500" />
                )}
                {node.status === 'pending' && (
                  <Clock className="w-4 h-4 text-gray-500" />
                )}
                <span className="flex-1 truncate">{node.node_name}</span>
                {node.rows_processed !== undefined && (
                  <span className="text-xs text-muted-foreground">
                    {node.rows_processed.toLocaleString()} rows
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Logs */}
      <div className="flex-1 overflow-hidden flex flex-col">
        <div className="p-4 border-b border-border">
          <h3 className="text-sm font-medium">Execution Logs</h3>
        </div>
        <div className="flex-1 overflow-y-auto p-4 font-mono text-xs bg-black/5">
          {logs.map((log, index) => (
            <div key={index} className="mb-1">
              {log}
            </div>
          ))}
          <div ref={logsEndRef} />
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="p-4 border-t border-border bg-red-500/10">
          <div className="flex items-start gap-2">
            <XCircle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />
            <div className="flex-1">
              <p className="text-sm font-medium text-red-500">Error</p>
              <p className="text-xs text-red-500/80 mt-1">{error}</p>
            </div>
          </div>
        </div>
      )}

      {/* Actions */}
      <div className="p-4 border-t border-border flex gap-2">
        {!isRunning ? (
          <button
            onClick={handleRun}
            className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Play className="w-4 h-4" />
            Run Pipeline
          </button>
        ) : (
          <button
            onClick={handleStop}
            className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-red-500 text-white rounded-md hover:bg-red-600 transition-colors"
          >
            <Square className="w-4 h-4" />
            Stop Execution
          </button>
        )}
      </div>
    </div>
  );
}
