/**
 * Component type definitions matching backend Phase 2 schemas.
 */

export type ComponentCategory = 'connection' | 'ingestor' | 'transformer' | 'ml' | 'sink';

export interface ComponentMetadata {
  name: string;
  category: ComponentCategory;
  display_name: string;
  description?: string;
  icon?: string;
  tags: string[];
  is_streaming: boolean;
  supports_incremental: boolean;
}

export interface ComponentManifest {
  name: string;
  category: ComponentCategory;
  display_name: string;
  description?: string;
  icon?: string;
  tags: string[];
  config_schema: Record<string, any>;
  sample_config: Record<string, any>;
  has_code_editor: boolean;
  is_streaming: boolean;
  supports_incremental: boolean;
  module_path?: string;
  class_name?: string;
}

export interface ComponentGroup {
  category: ComponentCategory;
  display_name: string;
  icon?: string;
  count: number;
  components: ComponentMetadata[];
}

export interface ComponentListResponse {
  groups: ComponentGroup[];
  total_count: number;
  stats: Record<string, number>;
}

export interface FieldError {
  field: string;
  message: string;
  error_type: string;
}

export interface ComponentValidationResponse {
  valid: boolean;
  errors: FieldError[];
  warnings: string[];
}
