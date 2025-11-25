/**
 * Component library sidebar with drag & drop.
 */
import React, { useMemo } from 'react';
import { Database, Download, Zap, Brain, Upload, Search } from 'lucide-react';
import { useComponents } from '@/hooks/useComponents';
import { generateId } from '@/lib/utils';
import { usePipelineStore } from '@/store/pipelineStore';

const categoryIcons = {
  connection: Database,
  ingestor: Download,
  transformer: Zap,
  ml: Brain,
  sink: Upload,
};

export function Sidebar() {
  const { components, isLoading, error } = useComponents();
  const [searchQuery, setSearchQuery] = React.useState('');
  const { addNode } = usePipelineStore();

  const filteredGroups = useMemo(() => {
    if (!components) return [];
    if (!searchQuery) return components.groups;

    return components.groups
      .map((group) => ({
        ...group,
        components: group.components.filter(
          (c) =>
            c.display_name.toLowerCase().includes(searchQuery.toLowerCase()) ||
            c.description?.toLowerCase().includes(searchQuery.toLowerCase()) ||
            c.tags.some((t) => t.toLowerCase().includes(searchQuery.toLowerCase()))
        ),
      }))
      .filter((group) => group.components.length > 0);
  }, [components, searchQuery]);

  const onDragStart = (event: React.DragEvent, component: any) => {
    event.dataTransfer.setData(
      'application/reactflow',
      JSON.stringify({
        component_type: component.category,
        component_name: component.name,
        label: component.display_name,
        config: {},
        description: component.description,
      })
    );
    event.dataTransfer.effectAllowed = 'move';
  };

  if (isLoading) {
    return (
      <div className="w-80 border-r border-border bg-card p-4">
        <div className="text-sm text-muted-foreground">Loading components...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="w-80 border-r border-border bg-card p-4">
        <div className="text-sm text-red-500">
          <p className="font-semibold mb-2">Error loading components:</p>
          <p className="text-xs">{error}</p>
          <p className="text-xs mt-2 text-muted-foreground">
            Check browser console for details
          </p>
        </div>
      </div>
    );
  }

  if (!components || filteredGroups.length === 0) {
    return (
      <div className="w-80 border-r border-border bg-card p-4">
        <h2 className="text-lg font-semibold mb-3">Components</h2>
        <div className="text-sm text-muted-foreground">
          {searchQuery ? 'No components match your search.' : 'No components available.'}
          <p className="text-xs mt-2">
            Backend: {import.meta.env.VITE_API_URL || 'http://localhost:8000'}
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="w-80 border-r border-border bg-card flex flex-col h-full">
      {/* Header */}
      <div className="p-4 border-b border-border">
        <h2 className="text-lg font-semibold mb-3">Components</h2>
        <div className="relative">
          <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
          <input
            type="text"
            placeholder="Search components..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-8 pr-3 py-2 text-sm border border-input rounded-md bg-background"
          />
        </div>
      </div>

      {/* Component List */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {filteredGroups.map((group) => {
          const Icon = categoryIcons[group.category as keyof typeof categoryIcons] || Zap;

          return (
            <div key={group.category}>
              <div className="flex items-center gap-2 mb-2">
                <Icon className="w-4 h-4 text-muted-foreground" />
                <h3 className="font-medium text-sm">{group.display_name}</h3>
                <span className="text-xs text-muted-foreground">({group.count})</span>
              </div>

              <div className="space-y-1">
                {group.components.map((component) => (
                  <div
                    key={component.name}
                    draggable
                    onDragStart={(e) => onDragStart(e, component)}
                    className="p-3 rounded-md border border-border bg-background hover:bg-accent cursor-move transition-colors"
                  >
                    <div className="font-medium text-sm">{component.display_name}</div>
                    {component.description && (
                      <div className="text-xs text-muted-foreground mt-1 line-clamp-2">
                        {component.description}
                      </div>
                    )}
                    <div className="flex gap-1 mt-2">
                      {component.is_streaming && (
                        <span className="text-xs px-1.5 py-0.5 rounded bg-blue-500/20 text-blue-500">
                          Streaming
                        </span>
                      )}
                      {component.supports_incremental && (
                        <span className="text-xs px-1.5 py-0.5 rounded bg-green-500/20 text-green-500">
                          Incremental
                        </span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
