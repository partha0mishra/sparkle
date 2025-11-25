/**
 * Component library sidebar with drag & drop and collapsible sections.
 */
import React, { useMemo, useState } from 'react';
import { Database, Download, Zap, Brain, Upload, Search, ChevronDown, ChevronRight, Plug } from 'lucide-react';
import { useComponents } from '@/hooks/useComponents';
import { usePipelineStore } from '@/store/pipelineStore';
import type { ComponentMetadata } from '@/types/component';

const categoryIcons = {
  source: Plug,
  ingestor: Download,
  transformer: Zap,
  ml: Brain,
  destination: Upload,
};

const categoryColors = {
  source: 'text-blue-500',
  ingestor: 'text-green-500',
  transformer: 'text-purple-500',
  ml: 'text-orange-500',
  destination: 'text-red-500',
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

  // Reorganize components into Source, Ingestor, Transformer, ML, and Destination
  const organizedSections = useMemo(() => {
    if (!components) return { source: [], ingestor: [], transformer: [], ml: [], destination: [] };

    const source: ComponentMetadata[] = [];
    const ingestor: ComponentMetadata[] = [];
    const transformer: ComponentMetadata[] = [];
    const ml: ComponentMetadata[] = [];
    const destination: ComponentMetadata[] = [];

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

        // Categorize components into 5 sections
        if (group.category === 'connection') {
          source.push(component);
        } else if (group.category === 'ingestor') {
          ingestor.push(component);
        } else if (group.category === 'transformer') {
          transformer.push(component);
        } else if (group.category === 'ml') {
          ml.push(component);
        } else if (group.category === 'sink') {
          destination.push(component);
        }
      });
    });

    return { source, ingestor, transformer, ml, destination };
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
    organizedSections.source.length +
    organizedSections.ingestor.length +
    organizedSections.transformer.length +
    organizedSections.ml.length +
    organizedSections.destination.length;

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
        {/* Source Section */}
        {organizedSections.source.length > 0 && (
          <CollapsibleSection
            title="Source"
            icon={categoryIcons.source}
            count={organizedSections.source.length}
            color={categoryColors.source}
            defaultOpen={true}
          >
            {organizedSections.source.map((component) => (
              <div
                key={component.name}
                draggable
                onDragStart={(e) => onDragStart(e, component)}
                className="p-3 rounded-md border border-border bg-background hover:bg-accent cursor-move transition-colors"
              >
                <div className="flex items-center gap-2">
                  <Plug className="w-4 h-4 text-blue-500" />
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

        {/* Ingestor Section */}
        {organizedSections.ingestor.length > 0 && (
          <CollapsibleSection
            title="Ingestor"
            icon={categoryIcons.ingestor}
            count={organizedSections.ingestor.length}
            color={categoryColors.ingestor}
            defaultOpen={true}
          >
            {organizedSections.ingestor.map((component) => (
              <div
                key={component.name}
                draggable
                onDragStart={(e) => onDragStart(e, component)}
                className="p-3 rounded-md border border-border bg-background hover:bg-accent cursor-move transition-colors"
              >
                <div className="flex items-center gap-2">
                  <Download className="w-4 h-4 text-green-500" />
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

        {/* Transformer Section */}
        {organizedSections.transformer.length > 0 && (
          <CollapsibleSection
            title="Transformer"
            icon={categoryIcons.transformer}
            count={organizedSections.transformer.length}
            color={categoryColors.transformer}
            defaultOpen={true}
          >
            {organizedSections.transformer.map((component) => (
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

        {/* Destination Section */}
        {organizedSections.destination.length > 0 && (
          <CollapsibleSection
            title="Destination"
            icon={categoryIcons.destination}
            count={organizedSections.destination.length}
            color={categoryColors.destination}
            defaultOpen={true}
          >
            {organizedSections.destination.map((component) => (
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
