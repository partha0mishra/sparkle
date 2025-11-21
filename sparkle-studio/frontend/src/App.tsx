/**
 * Main App component with layout.
 */
import React, { useCallback, useRef } from 'react';
import { ReactFlowProvider } from 'reactflow';
import { Sidebar } from './components/Sidebar';
import { Canvas } from './components/Canvas';
import { PropertiesPanel } from './components/PropertiesPanel';
import { TopBar } from './components/TopBar';
import { usePipelineStore } from './store/pipelineStore';
import { usePipeline } from './hooks/usePipeline';
import { generateId } from './lib/utils';

function App() {
  const reactFlowWrapper = useRef<HTMLDivElement>(null);
  const { addNode } = usePipelineStore();
  const { setNodes } = usePipeline();

  // Handle drop for drag & drop from sidebar
  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();

      const reactFlowBounds = reactFlowWrapper.current?.getBoundingClientRect();
      if (!reactFlowBounds) return;

      const data = event.dataTransfer.getData('application/reactflow');
      if (!data) return;

      const nodeData = JSON.parse(data);

      // Calculate position
      const position = {
        x: event.clientX - reactFlowBounds.left - 100,
        y: event.clientY - reactFlowBounds.top - 20,
      };

      const newNode = {
        id: generateId(),
        type: 'sparkle_component',
        position,
        data: nodeData,
      };

      addNode(newNode);
    },
    [addNode]
  );

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  // Keyboard shortcuts
  React.useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      // Ctrl+S or Cmd+S to save
      if ((event.ctrlKey || event.metaKey) && event.key === 's') {
        event.preventDefault();
        // Trigger save from TopBar
        document.dispatchEvent(new CustomEvent('save-pipeline'));
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, []);

  return (
    <div className="h-screen flex flex-col bg-background text-foreground">
      <TopBar />

      <div className="flex-1 flex overflow-hidden">
        <Sidebar />

        <div
          ref={reactFlowWrapper}
          className="flex-1"
          onDrop={onDrop}
          onDragOver={onDragOver}
        >
          <ReactFlowProvider>
            <Canvas />
          </ReactFlowProvider>
        </div>

        <PropertiesPanel />
      </div>
    </div>
  );
}

export default App;
