/**
 * Enhanced Properties panel with support for task nodes and pipeline config.
 */
import React, { useEffect, useState } from 'react';
import { useForm } from 'react-hook-form';
import { X, Settings, Code, CheckCircle, XCircle, Loader2 } from 'lucide-react';
import { usePipelineStore } from '@/store/pipelineStore';
import { useComponents } from '@/hooks/useComponents';
import { cn } from '@/lib/utils';
import api from '@/lib/api';

export function PropertiesPanel() {
  const { selectedNode, selectedComponent, updateNode, setSelectedNode, setSelectedComponent, addNode, addEdges } = usePipelineStore();
  const { getComponent } = useComponents();
  const [schema, setSchema] = useState<any>(null);
  const [jsonConfig, setJsonConfig] = useState<string>('');
  const [editMode, setEditMode] = useState<'form' | 'json'>('form');
  const [testStatus, setTestStatus] = useState<'idle' | 'testing' | 'success' | 'failed'>('idle');
  const [testMessage, setTestMessage] = useState<string>('');

  const { register, handleSubmit, reset, formState: { errors } } = useForm();

  useEffect(() => {
    const loadSchema = async () => {
      if (selectedNode) {
        // Load component schema for selected node
        const component = await getComponent(
          selectedNode.data.component_type,
          selectedNode.data.component_name
        );

        if (component) {
          setSchema(component.config_schema);
          const currentConfig = selectedNode.data.config || {};
          reset(currentConfig);
          setJsonConfig(JSON.stringify(currentConfig, null, 2));

          // Default to JSON mode if schema is empty (for task nodes)
          if (!component.config_schema || Object.keys(component.config_schema).length === 0) {
            setEditMode('json');
          } else {
            setEditMode('form');
          }
        }
      } else if (selectedComponent) {
        // Load schema for selected component from sidebar
        const component = await getComponent(
          selectedComponent.category,
          selectedComponent.name
        );
        if (component) {
          setSchema(component.config_schema);
          reset(component.sample_config || {});
          setJsonConfig(JSON.stringify(component.sample_config || {}, null, 2));
          setEditMode('form');
        }
      } else {
        setSchema(null);
        setJsonConfig('{}');
      }
    };
    loadSchema();
  }, [selectedNode, selectedComponent, getComponent, reset]);

  const onSubmit = (data: any) => {
    if (selectedNode) {
      updateNode(selectedNode.id, { config: data });
      setSelectedNode(null); // Close panel after update
    } else if (selectedComponent) {
      // Check if this is a pipeline (orchestrator)
      if (selectedComponent.category === 'orchestrator' && schema?.type === 'pipeline' && schema?.graph) {
        // This is handled by drag-and-drop in App.tsx now
        setSelectedComponent(null);
      } else {
        // Add single node to canvas
        addNode({
          id: `${selectedComponent.name}_${Date.now()}`,
          type: 'sparkle_component',
          position: { x: 100, y: 100 }, // Default position
          data: {
            label: selectedComponent.display_name,
            component_type: selectedComponent.category,
            component_name: selectedComponent.name,
            config: data,
            description: selectedComponent.description,
          },
        });
      }
      setSelectedComponent(null);
    }
  };

  const onJsonSubmit = () => {
    try {
      const parsedConfig = JSON.parse(jsonConfig);
      if (selectedNode) {
        updateNode(selectedNode.id, { config: parsedConfig });
        setSelectedNode(null); // Close panel after update
      }
    } catch (error) {
      alert('Invalid JSON: ' + error);
    }
  };

  const onTestConnection = async () => {
    setTestStatus('testing');
    setTestMessage('');

    try {
      const config = editMode === 'json' ? JSON.parse(jsonConfig) : (selectedNode?.data.config || {});
      const componentName = selectedNode?.data.component_name || selectedComponent?.name;

      const response = await api.post('/connections/test', {
        name: componentName,
        config: config,
      });

      if (response.data.success && response.data.status === 'success') {
        setTestStatus('success');
        setTestMessage(response.data.message);
      } else {
        setTestStatus('failed');
        setTestMessage(response.data.message + (response.data.error ? `: ${response.data.error}` : ''));
      }
    } catch (error: any) {
      setTestStatus('failed');
      setTestMessage(error.response?.data?.message || error.message || 'Connection test failed');
    }

    // Auto-clear status after 5 seconds
    setTimeout(() => {
      setTestStatus('idle');
      setTestMessage('');
    }, 5000);
  };

  const getTaskConfigSchema = (taskType: string) => {
    // Define schemas for common task types
    const taskSchemas: Record<string, any> = {
      'IngestTask': {
        ingestor: { type: 'string', label: 'Ingestor Name', placeholder: 'e.g., jdbc_postgres_ingestor' },
        ingestor_config: { type: 'object', label: 'Ingestor Configuration' },
      },
      'WriteDeltaTask': {
        destination_catalog: { type: 'string', label: 'Destination Catalog', placeholder: 'e.g., sparkle_prod' },
        destination_schema: { type: 'string', label: 'Destination Schema', placeholder: 'e.g., bronze' },
        destination_table: { type: 'string', label: 'Destination Table', placeholder: 'e.g., customers' },
        write_mode: { type: 'string', label: 'Write Mode', enum: ['append', 'overwrite', 'merge'], default: 'append' },
        partition_columns: { type: 'array', label: 'Partition Columns', placeholder: 'e.g., year,month,day' },
        zorder_columns: { type: 'array', label: 'Z-Order Columns', placeholder: 'e.g., customer_id' },
      },
      'TransformTask': {
        transformers: { type: 'array', label: 'Transformers', placeholder: 'List of transformer names' },
      },
    };

    return taskSchemas[taskType] || {};
  };

  const renderJsonEditor = () => (
    <div className="space-y-4">
      <div className="flex items-center justify-between mb-2">
        <label className="block text-sm font-medium">Configuration (JSON)</label>
        <button
          type="button"
          onClick={() => setEditMode(editMode === 'json' ? 'form' : 'json')}
          className="text-xs text-muted-foreground hover:text-foreground"
        >
          {editMode === 'json' ? 'Switch to Form' : 'Switch to JSON'}
        </button>
      </div>

      <textarea
        value={jsonConfig}
        onChange={(e) => setJsonConfig(e.target.value)}
        className="w-full px-3 py-2 border border-input rounded-md bg-background text-sm font-mono"
        rows={15}
        placeholder='{\n  "key": "value"\n}'
      />

      <button
        onClick={onJsonSubmit}
        className="w-full px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90"
      >
        Apply Changes
      </button>
    </div>
  );

  const renderSimpleForm = (taskType: string, config: any) => {
    const fieldSchemas = getTaskConfigSchema(taskType);

    return (
      <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
        <div className="text-xs text-muted-foreground mb-4">
          Configure the parameters for this {taskType}
        </div>

        {Object.entries(fieldSchemas).map(([key, field]: [string, any]) => (
          <div key={key}>
            <label className="block text-sm font-medium mb-1">
              {field.label || key}
            </label>

            {field.type === 'array' ? (
              <input
                type="text"
                {...register(key)}
                defaultValue={Array.isArray(config[key]) ? config[key].join(',') : ''}
                className="w-full px-3 py-2 border border-input rounded-md bg-background text-sm"
                placeholder={field.placeholder}
              />
            ) : field.enum ? (
              <select
                {...register(key)}
                defaultValue={config[key] || field.default}
                className="w-full px-3 py-2 border border-input rounded-md bg-background text-sm"
              >
                <option value="">Select...</option>
                {field.enum.map((option: string) => (
                  <option key={option} value={option}>{option}</option>
                ))}
              </select>
            ) : field.type === 'object' ? (
              <textarea
                {...register(key)}
                defaultValue={JSON.stringify(config[key] || {}, null, 2)}
                className="w-full px-3 py-2 border border-input rounded-md bg-background text-sm font-mono"
                rows={3}
                placeholder="{}"
              />
            ) : (
              <input
                type="text"
                {...register(key)}
                defaultValue={config[key] || ''}
                className="w-full px-3 py-2 border border-input rounded-md bg-background text-sm"
                placeholder={field.placeholder}
              />
            )}
          </div>
        ))}

        <div className="flex gap-2">
          <button
            type="submit"
            className="flex-1 px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90"
          >
            Apply Changes
          </button>
          <button
            type="button"
            onClick={() => setEditMode('json')}
            className="px-4 py-2 border border-input rounded-md hover:bg-accent"
            title="Edit as JSON"
          >
            <Code className="w-4 h-4" />
          </button>
        </div>
      </form>
    );
  };

  if (!selectedNode && !selectedComponent) {
    return (
      <div className="w-96 border-l border-border bg-card p-4">
        <div className="text-sm text-muted-foreground text-center">
          <Settings className="w-8 h-8 mx-auto mb-2 opacity-50" />
          <p>Select a node or component to edit its properties</p>
        </div>
      </div>
    );
  }

  const title = selectedNode ? selectedNode.data.label : selectedComponent?.display_name;
  const subtitle = selectedNode ? selectedNode.data.component_name : selectedComponent?.name;
  const description = selectedNode?.data.description || selectedComponent?.description;
  const isTaskNode = selectedNode?.data.is_task_node;
  const taskType = selectedNode?.data.task_type;

  return (
    <div className="w-96 border-l border-border bg-card flex flex-col h-full">
      {/* Header */}
      <div className="p-4 border-b border-border">
        <div className="flex items-center justify-between mb-2">
          <div className="flex-1">
            <h3 className="font-semibold">{title}</h3>
            <p className="text-xs text-muted-foreground">{subtitle}</p>
          </div>
          <button
            onClick={() => {
              setSelectedNode(null);
              setSelectedComponent(null);
            }}
            className="p-1 rounded hover:bg-accent"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {description && (
          <p className="text-xs text-muted-foreground mt-2 p-2 bg-muted rounded">
            {description}
          </p>
        )}

        {/* Test Connection Button (for connection components only) */}
        {(selectedNode?.data.component_type === 'connection' || selectedComponent?.category === 'connection') && (
          <div className="mt-3">
            <button
              onClick={onTestConnection}
              disabled={testStatus === 'testing'}
              className={cn(
                'w-full px-4 py-2 rounded-md font-medium transition-colors flex items-center justify-center gap-2',
                testStatus === 'idle' && 'bg-blue-500 hover:bg-blue-600 text-white',
                testStatus === 'testing' && 'bg-gray-400 text-white cursor-not-allowed',
                testStatus === 'success' && 'bg-green-500 text-white',
                testStatus === 'failed' && 'bg-red-500 text-white'
              )}
            >
              {testStatus === 'testing' && (
                <>
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Testing...
                </>
              )}
              {testStatus === 'success' && (
                <>
                  <CheckCircle className="w-4 h-4" />
                  Success
                </>
              )}
              {testStatus === 'failed' && (
                <>
                  <XCircle className="w-4 h-4" />
                  Failed
                </>
              )}
              {testStatus === 'idle' && 'Test Connection'}
            </button>

            {testMessage && (
              <p className={cn(
                'text-xs mt-2 p-2 rounded',
                testStatus === 'success' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
              )}>
                {testMessage}
              </p>
            )}
          </div>
        )}
      </div>

      {/* Form */}
      <div className="flex-1 overflow-y-auto p-4">
        {isTaskNode && editMode === 'json' ? (
          renderJsonEditor()
        ) : isTaskNode && taskType ? (
          renderSimpleForm(taskType, selectedNode.data.config || {})
        ) : schema && schema.properties && Object.keys(schema.properties).length > 0 ? (
          <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
            {Object.entries(schema.properties || {}).map(([key, field]: [string, any]) => (
              <div key={key}>
                <label className="block text-sm font-medium mb-1">
                  {field.title || key}
                  {schema.required?.includes(key) && (
                    <span className="text-red-500 ml-1">*</span>
                  )}
                </label>

                {field.description && (
                  <p className="text-xs text-muted-foreground mb-2">{field.description}</p>
                )}

                {field.ui?.widget === 'textarea' ? (
                  <textarea
                    {...register(key, { required: schema.required?.includes(key) })}
                    className="w-full px-3 py-2 border border-input rounded-md bg-background text-sm"
                    rows={4}
                    placeholder={field.ui?.placeholder || field.default}
                  />
                ) : field.ui?.widget === 'password' ? (
                  <input
                    type="password"
                    {...register(key, { required: schema.required?.includes(key) })}
                    className="w-full px-3 py-2 border border-input rounded-md bg-background text-sm"
                    placeholder={field.ui?.placeholder}
                  />
                ) : field.enum ? (
                  <select
                    {...register(key, { required: schema.required?.includes(key) })}
                    className="w-full px-3 py-2 border border-input rounded-md bg-background text-sm"
                  >
                    <option value="">Select...</option>
                    {field.enum.map((option: string) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </select>
                ) : field.type === 'boolean' ? (
                  <input
                    type="checkbox"
                    {...register(key)}
                    className="w-4 h-4"
                  />
                ) : field.type === 'integer' || field.type === 'number' ? (
                  <input
                    type="number"
                    {...register(key, {
                      required: schema.required?.includes(key),
                      min: field.minimum,
                      max: field.maximum,
                    })}
                    className="w-full px-3 py-2 border border-input rounded-md bg-background text-sm"
                    placeholder={field.ui?.placeholder || field.default?.toString()}
                  />
                ) : (
                  <input
                    type="text"
                    {...register(key, { required: schema.required?.includes(key) })}
                    className="w-full px-3 py-2 border border-input rounded-md bg-background text-sm"
                    placeholder={field.ui?.placeholder || field.default}
                  />
                )}

                {errors[key] && (
                  <p className="text-xs text-red-500 mt-1">This field is required</p>
                )}

                {field.ui?.help && (
                  <p className="text-xs text-muted-foreground mt-1">{field.ui.help}</p>
                )}
              </div>
            ))}

            <button
              type="submit"
              className="w-full px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90"
            >
              {selectedComponent ? 'Add to Canvas' : 'Apply Changes'}
            </button>
          </form>
        ) : (
          // Fallback to JSON editor if no schema
          renderJsonEditor()
        )}
      </div>
    </div>
  );
}
