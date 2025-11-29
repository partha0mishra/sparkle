/**
 * Icon display component that supports both Lucide icons and Simple Icons.
 *
 * For connections with brand logos (postgres, snowflake, etc.), we use Simple Icons from CDN.
 * For generic category icons, we use Lucide React icons.
 */
import React from 'react';
import {
  Database,
  Download,
  Zap,
  Brain,
  Upload,
  Workflow,
  Plug,
  Cloud,
  Globe,
  Send,
  FileText,
  TrendingUp,
  Building,
  Activity,
  Folder,
  LucideIcon,
} from 'lucide-react';
import { cn } from '@/lib/utils';

// Mapping of simple-icons names to Lucide icons (where overlap exists)
const LUCIDE_ICON_MAP: Record<string, LucideIcon> = {
  'database': Database,
  'download': Download,
  'zap': Zap,
  'transform': Zap,
  'brain': Brain,
  'upload': Upload,
  'workflow': Workflow,
  'plug': Plug,
  'component': Database,
  // Generic API/Protocol icons (no Simple Icons equivalent)
  'api': Globe,
  'rest': Globe,
  'soap': Send,
  'grpc': Send,
  'webhook': Send,
  'http': Globe,
  'https': Globe,
  'cloud': Cloud,
  // Generic category icons
  'file-text': FileText,
  'trending-up': TrendingUp,
  'bank': Building,
  'hl7': Activity,
  'fhir': Activity,
  'folder': Folder,
};

interface IconDisplayProps {
  /**
   * Icon name (from backend: simple-icons name or lucide name)
   */
  icon?: string;

  /**
   * Size class (e.g., "w-4 h-4", "w-6 h-6")
   */
  className?: string;

  /**
   * Background color class (e.g., "bg-blue-500")
   */
  bgColor?: string;

  /**
   * Whether to show background
   */
  showBackground?: boolean;
}

export function IconDisplay({
  icon = 'database',
  className = 'w-4 h-4',
  bgColor = 'bg-blue-500',
  showBackground = true,
}: IconDisplayProps) {
  // Check if this is a Lucide icon
  const LucideIconComponent = LUCIDE_ICON_MAP[icon.toLowerCase()];

  if (LucideIconComponent) {
    // Render Lucide icon
    return showBackground ? (
      <div className={cn('p-1.5 rounded', bgColor)}>
        <LucideIconComponent className={cn(className, 'text-white')} />
      </div>
    ) : (
      <LucideIconComponent className={className} />
    );
  }

  // Otherwise, use Simple Icons from CDN
  const simpleIconUrl = `https://cdn.simpleicons.org/${icon}`;

  return showBackground ? (
    <div className={cn('p-1.5 rounded flex items-center justify-center', bgColor)}>
      <img
        src={simpleIconUrl}
        alt={icon}
        className={cn(className, 'invert')}
        onError={(e) => {
          // Fallback to database emoji on error
          e.currentTarget.style.display = 'none';
          const parent = e.currentTarget.parentElement;
          if (parent) {
            const fallback = document.createElement('span');
            fallback.textContent = '🗄️';
            fallback.className = className;
            parent.appendChild(fallback);
          }
        }}
      />
    </div>
  ) : (
    <img
      src={simpleIconUrl}
      alt={icon}
      className={className}
      onError={(e) => {
        // Fallback to generic icon
        const fallback = document.createElement('span');
        fallback.textContent = '🔌';
        e.currentTarget.replaceWith(fallback);
      }}
    />
  );
}

/**
 * Icon display for sidebar items (no background, smaller)
 */
export function SidebarIcon({ icon, className = 'w-4 h-4' }: { icon?: string; className?: string }) {
  const iconLower = icon?.toLowerCase() || 'database';
  const LucideIconComponent = LUCIDE_ICON_MAP[iconLower];

  if (LucideIconComponent) {
    return <LucideIconComponent className={className} />;
  }

  // For simple-icons in sidebar, show colored version
  const simpleIconUrl = `https://cdn.simpleicons.org/${icon}`;

  return (
    <img
      src={simpleIconUrl}
      alt={icon || 'icon'}
      className={className}
      style={{ maxWidth: '1rem', maxHeight: '1rem', objectFit: 'contain' }}
      onError={(e) => {
        // On error, replace with a generic plug emoji
        const target = e.currentTarget;
        const span = document.createElement('span');
        span.textContent = '🔌';
        span.className = className;
        target.replaceWith(span);
      }}
    />
  );
}
