/**
 * Properties panel with auto-generated forms from JSON Schema.
 */
import React, { useEffect, useState } from 'react';
import { useForm } from 'react-hook-form';
import { X } from 'lucide-react';
import { usePipelineStore } from '@/store/pipelineStore';
import { useComponents } from '@/hooks/useComponents';
import { cn } from '@/lib/utils';

export function PropertiesPanel() {
  const { selectedNode, updateNode, setSelectedNode } = usePipelineStore();
  const { getComponent } = useComponents();
  const [schema, setSchema] = useState<any>(null);

  const { register, handleSubmit, reset, formState: { errors } } = useForm();

  useEffect(() => {
    if (selectedNode) {
      // Load component schema
      const loadSchema = async () => {
        const component = await getComponent(
          selectedNode.data.component_type,
          selectedNode.data.component_name
        );
        if (component) {
          setSchema(component.config_schema);
          reset(selectedNode.data.config || {});
        }
      };
      loadSchema();
    } else {
      setSchema(null);
    }
  }, [selectedNode, getComponent, reset]);

  const onSubmit = (data: any) => {
    if (selectedNode) {
      updateNode(selectedNode.id, { config: data });
    }
  };

  if (!selectedNode) {
    return (
      <div className="w-96 border-l border-border bg-card p-4">
        <div className="text-sm text-muted-foreground text-center">
          Select a node to edit its properties
        </div>
      </div>
    );
  }

  return (
    <div className="w-96 border-l border-border bg-card flex flex-col h-full">
      {/* Header */}
      <div className="p-4 border-b border-border flex items-center justify-between">
        <div>
          <h3 className="font-semibold">{selectedNode.data.label}</h3>
          <p className="text-xs text-muted-foreground">{selectedNode.data.component_name}</p>
        </div>
        <button
          onClick={() => setSelectedNode(null)}
          className="p-1 rounded hover:bg-accent"
        >
          <X className="w-4 h-4" />
        </button>
      </div>

      {/* Form */}
      <div className="flex-1 overflow-y-auto p-4">
        {schema && (
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

                {/* Render input based on type and UI hints */}
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
              Apply Changes
            </button>
          </form>
        )}
      </div>
    </div>
  );
}
