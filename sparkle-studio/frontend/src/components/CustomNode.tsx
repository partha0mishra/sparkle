/**
 * Custom node component for React Flow.
 */
import React from 'react';
import { Handle, Position, type NodeProps } from 'reactflow';
import { Database, Download, Zap, Brain, Upload, Code } from 'lucide-react';
import { cn } from '@/lib/utils';
import { IconDisplay } from '@/components/IconDisplay';

const categoryIcons = {
  connection: Database,
  ingestor: Download,
  transformer: Zap,
  ml: Brain,
  sink: Upload,
};

const categoryColors = {
  connection: 'bg-blue-500',
  ingestor: 'bg-green-500',
  transformer: 'bg-purple-500',
  ml: 'bg-orange-500',
  sink: 'bg-red-500',
};

export function CustomNode({ data, selected }: NodeProps) {
  // Use visual_category for color if available (for task nodes from pipeline expansion)
  // Otherwise use component_type (for regular nodes)
  const categoryForVisual = data.visual_category || data.component_type;
  const Icon = categoryIcons[categoryForVisual as keyof typeof categoryIcons] || Zap;
  const colorClass = categoryColors[categoryForVisual as keyof typeof categoryColors] || 'bg-gray-500';

  return (
    <div
      className={cn(
        'px-4 py-3 shadow-lg rounded-lg border-2 bg-card min-w-[200px]',
        selected ? 'border-primary' : 'border-border'
      )}
    >
      <Handle type="target" position={Position.Top} className="w-3 h-3" />

      <div className="flex items-center gap-2 mb-2">
        {/* Use component-specific icon if available, otherwise use category icon */}
        {data.icon ? (
          <IconDisplay icon={data.icon} className="w-4 h-4" bgColor={colorClass} showBackground={true} />
        ) : (
          <div className={cn('p-1.5 rounded', colorClass)}>
            <Icon className="w-4 h-4 text-white" />
          </div>
        )}
        <div className="flex-1">
          <div className="font-semibold text-sm text-foreground">{data.label}</div>
          {data.component_name && (
            <div className="text-xs text-muted-foreground">{data.component_name}</div>
          )}
        </div>
        {data.has_code_editor && (
          <Code className="w-4 h-4 text-muted-foreground" />
        )}
      </div>

      {data.description && (
        <div className="text-xs text-muted-foreground mt-1 line-clamp-2">
          {data.description}
        </div>
      )}

      <Handle type="source" position={Position.Bottom} className="w-3 h-3" />
    </div>
  );
}
