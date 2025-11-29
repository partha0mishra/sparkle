/**
 * Component library sidebar with drag & drop and collapsible sections.
 */
import React, { useMemo, useState } from 'react';
import { Database, Download, Zap, Brain, Upload, Search, ChevronDown, ChevronRight, Plug } from 'lucide-react';
import { useComponents } from '@/hooks/useComponents';
import { usePipelineStore } from '@/store/pipelineStore';
import type { ComponentMetadata } from '@/types/component';
import { SidebarIcon } from '@/components/IconDisplay';

const categoryIcons = {
  source: Plug,
  ingestor: Download,
  transformer: Zap,
  ml: Brain,
  orchestrator: Database,
  destination: Upload,
};

const categoryColors = {
  source: 'text-blue-500',
  ingestor: 'text-green-500',
  transformer: 'text-purple-500',
  ml: 'text-orange-500',
  orchestrator: 'text-cyan-500',
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
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          setIsOpen(!isOpen);
        }}
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

interface SubGroupSectionProps {
  title: string;
  count: number;
  children: React.ReactNode;
  defaultOpen?: boolean;
}

function SubGroupSection({ title, count, children, defaultOpen = false }: SubGroupSectionProps) {
  const [isOpen, setIsOpen] = useState(defaultOpen);

  return (
    <div className="mb-3">
      <button
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          setIsOpen(!isOpen);
        }}
        className="w-full flex items-center gap-2 mb-2 px-2 py-1 rounded hover:bg-accent/50 transition-colors"
      >
        {isOpen ? (
          <ChevronDown className="w-3 h-3 text-muted-foreground" />
        ) : (
          <ChevronRight className="w-3 h-3 text-muted-foreground" />
        )}
        <div className="text-xs font-semibold text-muted-foreground flex-1 text-left">
          {title} ({count})
        </div>
      </button>

      {isOpen && <div className="space-y-1">{children}</div>}
    </div>
  );
}

export function Sidebar() {
  const { components, isLoading, error } = useComponents();
  const [searchQuery, setSearchQuery] = React.useState('');
  const { addNode, setSelectedComponent } = usePipelineStore();

  // Reorganize components into Source, Ingestor, Transformer, ML, Orchestrator, and Destination with sub-groups
  const organizedSections = useMemo(() => {
    if (!components) return { source: {}, ingestor: {}, transformer: {}, ml: {}, orchestrator: {}, destination: {} };

    const source: Record<string, ComponentMetadata[]> = {};
    const ingestor: Record<string, ComponentMetadata[]> = {};
    const transformer: Record<string, ComponentMetadata[]> = {};
    const ml: Record<string, ComponentMetadata[]> = {};
    const orchestrator: Record<string, ComponentMetadata[]> = {};
    const destination: Record<string, ComponentMetadata[]> = {};

    // Helper to add component with deduplication
    const addComponent = (targetSection: Record<string, any[]>, subGroupName: string, component: ComponentMetadata) => {
      if (!targetSection[subGroupName]) {
        targetSection[subGroupName] = [];
      }

      // Check for existing component with same display name
      const existingIndex = targetSection[subGroupName].findIndex(
        (c) => c.display_name === component.display_name
      );

      if (existingIndex >= 0) {
        // Add as alias to existing component
        const existing = targetSection[subGroupName][existingIndex];
        if (!existing.aliases) {
          existing.aliases = [];
        }
        existing.aliases.push(component);
      } else {
        // Add new component
        targetSection[subGroupName].push({ ...component });
      }
    };

    components.groups.forEach((group) => {
      // Use sub_groups if available, otherwise organize components directly
      const subGroups = group.sub_groups || {};
      const hasSubGroups = Object.keys(subGroups).length > 0;

      if (hasSubGroups) {
        // Process sub-grouped components
        Object.entries(subGroups).forEach(([subGroupName, subGroupComponents]) => {
          subGroupComponents.forEach((component) => {
            // Filter by search query
            if (searchQuery) {
              const searchLower = searchQuery.toLowerCase();
              const matches =
                component.display_name.toLowerCase().includes(searchLower) ||
                component.description?.toLowerCase().includes(searchLower) ||
                component.tags.some((t) => t.toLowerCase().includes(searchLower));

              if (!matches) return;
            }

            // Categorize into sections
            let targetSection: Record<string, ComponentMetadata[]> | null = null;
            if (group.category === 'connection') targetSection = source;
            else if (group.category === 'ingestor') targetSection = ingestor;
            else if (group.category === 'transformer') targetSection = transformer;
            else if (group.category === 'ml') targetSection = ml;
            else if (group.category === 'orchestrator') targetSection = orchestrator;
            else if (group.category === 'sink') targetSection = destination;

            if (targetSection) {
              addComponent(targetSection, subGroupName, component);
            }
          });
        });
      } else {
        // No sub-groups - organize flat with "Other" as default sub-group
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

          const subGroupName = component.sub_group || 'Other';
          let targetSection: Record<string, ComponentMetadata[]> | null = null;
          if (group.category === 'connection') targetSection = source;
          else if (group.category === 'ingestor') targetSection = ingestor;
          else if (group.category === 'transformer') targetSection = transformer;
          else if (group.category === 'ml') targetSection = ml;
          else if (group.category === 'orchestrator') targetSection = orchestrator;
          else if (group.category === 'sink') targetSection = destination;

          if (targetSection) {
            addComponent(targetSection, subGroupName, component);
          }
        });
      }
    });

    return { source, ingestor, transformer, ml, orchestrator, destination };
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
        icon: component.icon, // Include icon for display
      })
    );
    event.dataTransfer.effectAllowed = 'move';
  };

  // Helper function to render component card
  const renderComponentCard = (component: ComponentMetadata & { aliases?: ComponentMetadata[] }, iconComponent: React.ElementType, iconColor: string) => (
    <div
      key={component.name}
      draggable
      onDragStart={(e) => onDragStart(e, component)}
      onClick={() => setSelectedComponent(component)}
      className="p-3 rounded-md border border-border bg-background hover:bg-accent cursor-pointer transition-colors"
    >
      <div className="flex items-center gap-2">
        {/* Use component-specific icon if available (for connections), otherwise use category icon */}
        {component.icon ? (
          <SidebarIcon icon={component.icon} className={`w-4 h-4 ${iconColor}`} />
        ) : (
          React.createElement(iconComponent, { className: `w-4 h-4 ${iconColor}` })
        )}
        <div className="font-medium text-sm flex-1">
          {component.display_name}
          {component.aliases && component.aliases.length > 0 && (
            <span className="text-xs text-muted-foreground ml-1" title={`Aliases: ${component.aliases.map(c => c.name).join(', ')}`}>
              (+{component.aliases.length})
            </span>
          )}
        </div>
      </div>

      {
        component.description && (
          <div className="text-xs text-muted-foreground mt-1 line-clamp-2">
            {component.description}
          </div>
        )
      }
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
    </div >
  );

  // Helper to render components grouped by sub-group
  const renderComponentsWithSubGroups = (
    subGroups: Record<string, ComponentMetadata[]>,
    iconComponent: React.ElementType,
    iconColor: string
  ) => {
    const subGroupNames = Object.keys(subGroups).sort();

    // If only one sub-group called "Other", render flat without sub-group headers
    if (subGroupNames.length === 1 && subGroupNames[0] === 'Other') {
      return subGroups['Other'].map((component) => renderComponentCard(component, iconComponent, iconColor));
    }

    // Render with collapsible sub-group sections
    return subGroupNames.map((subGroupName, index) => (
      <SubGroupSection
        key={subGroupName}
        title={subGroupName}
        count={subGroups[subGroupName].length}
        defaultOpen={index === 0} // First sub-group open by default
      >
        {subGroups[subGroupName].map((component) => renderComponentCard(component, iconComponent, iconColor))}
      </SubGroupSection>
    ));
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
    Object.values(organizedSections.source).flat().length +
    Object.values(organizedSections.ingestor).flat().length +
    Object.values(organizedSections.transformer).flat().length +
    Object.values(organizedSections.ml).flat().length +
    Object.values(organizedSections.orchestrator).flat().length +
    Object.values(organizedSections.destination).flat().length;

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
        {Object.keys(organizedSections.source).length > 0 && (
          <CollapsibleSection
            title="Source/ Destinations"
            icon={categoryIcons.source}
            count={Object.values(organizedSections.source).flat().length}
            color={categoryColors.source}
            defaultOpen={true}
          >
            {renderComponentsWithSubGroups(organizedSections.source, Plug, 'text-blue-500')}
          </CollapsibleSection>
        )}

        {/* Ingestor Section */}
        {Object.keys(organizedSections.ingestor).length > 0 && (
          <CollapsibleSection
            title="Ingestor"
            icon={categoryIcons.ingestor}
            count={Object.values(organizedSections.ingestor).flat().length}
            color={categoryColors.ingestor}
            defaultOpen={true}
          >
            {renderComponentsWithSubGroups(organizedSections.ingestor, Download, 'text-green-500')}
          </CollapsibleSection>
        )}

        {/* Transformer Section */}
        {Object.keys(organizedSections.transformer).length > 0 && (
          <CollapsibleSection
            title="Transformer"
            icon={categoryIcons.transformer}
            count={Object.values(organizedSections.transformer).flat().length}
            color={categoryColors.transformer}
            defaultOpen={true}
          >
            {renderComponentsWithSubGroups(organizedSections.transformer, Zap, 'text-purple-500')}
          </CollapsibleSection>
        )}

        {/* Machine Learning Section */}
        {Object.keys(organizedSections.ml).length > 0 && (
          <CollapsibleSection
            title="Machine Learning"
            icon={categoryIcons.ml}
            count={Object.values(organizedSections.ml).flat().length}
            color={categoryColors.ml}
            defaultOpen={true}
          >
            {renderComponentsWithSubGroups(organizedSections.ml, Brain, 'text-orange-500')}
          </CollapsibleSection>
        )}

        {/* Orchestrators Section */}
        {Object.keys(organizedSections.orchestrator).length > 0 && (
          <CollapsibleSection
            title="Orchestrators"
            icon={categoryIcons.orchestrator}
            count={Object.values(organizedSections.orchestrator).flat().length}
            color={categoryColors.orchestrator}
            defaultOpen={true}
          >
            {renderComponentsWithSubGroups(organizedSections.orchestrator, Database, 'text-cyan-500')}
          </CollapsibleSection>
        )}

        {/* Destination Section */}
        {Object.keys(organizedSections.destination).length > 0 && (
          <CollapsibleSection
            title="Destination"
            icon={categoryIcons.destination}
            count={Object.values(organizedSections.destination).flat().length}
            color={categoryColors.destination}
            defaultOpen={true}
          >
            {renderComponentsWithSubGroups(organizedSections.destination, Upload, 'text-red-500')}
          </CollapsibleSection>
        )}
      </div>
    </div>
  );
}
