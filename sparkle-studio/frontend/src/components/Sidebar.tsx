/**
 * Component library sidebar with drag & drop and collapsible sections.
 */
import React, { useMemo, useState } from 'react';
import { Database, Download, Zap, Brain, Upload, Search, ChevronDown, ChevronRight } from 'lucide-react';
import { useComponents } from '@/hooks/useComponents';
import { usePipelineStore } from '@/store/pipelineStore';
import type { ComponentMetadata } from '@/types/component';

const categoryIcons = {
  sources: Download,
  transformers: Zap,
  ml: Brain,
  sinks: Upload,
};

const categoryColors = {
  sources: 'text-green-500',
  transformers: 'text-purple-500',
  ml: 'text-orange-500',
  sinks: 'text-red-500',
};

interface CollapsibleSectionProps {
  title: string;
  icon: React.ElementType;
  count: number;
  color: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
}

function CollapsibleSection({ title, icon: Icon, count, color, children, defaultOpen = true }: CollapsibleSectionProps) {
  const [isOpen, setIsOpen] = useState(defaultOpen);

  return (
    <div>
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="w-full flex items-center gap-2 mb-2 hover:bg-accent px-2 py-1 rounded transition-colors"
      >
        {isOpen ? (
          <ChevronDown className="w-4 h-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="w-4 h-4 text-muted-foreground" />
        )}
        <Icon className={`w-4 h-4 ${color}`} />
        <h3 className="font-medium text-sm flex-1 text-left">{title}</h3>
        <span className="text-xs text-muted-foreground">({count})</span>
      </button>

      {isOpen && <div className="space-y-1 ml-2">{children}</div>}
    </div>
  );
}

export function Sidebar() {
  const { components, isLoading, error } = useComponents();
  const [searchQuery, setSearchQuery] = React.useState('');
  const { addNode } = usePipelineStore();

  // Reorganize components into Sources, Transformers, ML, and Sinks
  const organizedSections = useMemo(() => {
    if (!components) return { sources: [], transformers: [], ml: [], sinks: [] };

    const sources: ComponentMetadata[] = [];
    const transformers: ComponentMetadata[] = [];
    const ml: ComponentMetadata[] = [];
    const sinks: ComponentMetadata[] = [];

    components.groups.forEach((group) => {
      group.components.forEach((component) => {
        // Filter by search query
        if (searchQuery) {
          const searchLower = searchQuery.toLowerCase();
          const matches =
            component.display_name.toLowerCase().includes(searchLower) ||
            component.description?.toLowerCase().includes(searchLower) ||
            component.tags.some((t) => t.toLowerCase().includes(searchLower));

          if (!matches) return;
        }

        // Categorize components
        if (group.category === 'connection' || group.category === 'ingestor') {
          sources.push(component);
        } else if (group.category === 'transformer') {
          transformers.push(component);
        } else if (group.category === 'ml') {
          ml.push(component);
        } else if (group.category === 'sink') {
          sinks.push(component);
        }
      });
    });

    return { sources, transformers, ml, sinks };
  }, [components, searchQuery]);

  const onDragStart = (event: React.DragEvent, component: ComponentMetadata) => {
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

  const totalComponents =
    organizedSections.sources.length +
    organizedSections.transformers.length +
    organizedSections.ml.length +
    organizedSections.sinks.length;

  if (!components || totalComponents === 0) {
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

      {/* Component List with Collapsible Sections */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {/* Sources Section */}
        {organizedSections.sources.length > 0 && (
          <CollapsibleSection
            title="Sources"
            icon={categoryIcons.sources}
            count={organizedSections.sources.length}
            color={categoryColors.sources}
            defaultOpen={true}
          >
            {organizedSections.sources.map((component) => (
              <div
                key={component.name}
                draggable
                onDragStart={(e) => onDragStart(e, component)}
                className="p-3 rounded-md border border-border bg-background hover:bg-accent cursor-move transition-colors"
              >
                <div className="flex items-center gap-2">
                  <Database className="w-4 h-4 text-green-500" />
                  <div className="font-medium text-sm">{component.display_name}</div>
                </div>
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
          </CollapsibleSection>
        )}

        {/* Transformers Section */}
        {organizedSections.transformers.length > 0 && (
          <CollapsibleSection
            title="Transformers"
            icon={categoryIcons.transformers}
            count={organizedSections.transformers.length}
            color={categoryColors.transformers}
            defaultOpen={true}
          >
            {organizedSections.transformers.map((component) => (
              <div
                key={component.name}
                draggable
                onDragStart={(e) => onDragStart(e, component)}
                className="p-3 rounded-md border border-border bg-background hover:bg-accent cursor-move transition-colors"
              >
                <div className="flex items-center gap-2">
                  <Zap className="w-4 h-4 text-purple-500" />
                  <div className="font-medium text-sm">{component.display_name}</div>
                </div>
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
          </CollapsibleSection>
        )}

        {/* Machine Learning Section */}
        {organizedSections.ml.length > 0 && (
          <CollapsibleSection
            title="Machine Learning"
            icon={categoryIcons.ml}
            count={organizedSections.ml.length}
            color={categoryColors.ml}
            defaultOpen={true}
          >
            {organizedSections.ml.map((component) => (
              <div
                key={component.name}
                draggable
                onDragStart={(e) => onDragStart(e, component)}
                className="p-3 rounded-md border border-border bg-background hover:bg-accent cursor-move transition-colors"
              >
                <div className="flex items-center gap-2">
                  <Brain className="w-4 h-4 text-orange-500" />
                  <div className="font-medium text-sm">{component.display_name}</div>
                </div>
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
          </CollapsibleSection>
        )}

        {/* Sinks Section */}
        {organizedSections.sinks.length > 0 && (
          <CollapsibleSection
            title="Sinks"
            icon={categoryIcons.sinks}
            count={organizedSections.sinks.length}
            color={categoryColors.sinks}
            defaultOpen={true}
          >
            {organizedSections.sinks.map((component) => (
              <div
                key={component.name}
                draggable
                onDragStart={(e) => onDragStart(e, component)}
                className="p-3 rounded-md border border-border bg-background hover:bg-accent cursor-move transition-colors"
              >
                <div className="flex items-center gap-2">
                  <Upload className="w-4 h-4 text-red-500" />
                  <div className="font-medium text-sm">{component.display_name}</div>
                </div>
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
          </CollapsibleSection>
        )}
      </div>
    </div>
  );
}
