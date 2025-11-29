# Configuration Enhancements - Task & Pipeline Configuration

**Status:** ✅ IMPLEMENTED
**Build:** `index-DVleIwwh.js`
**Date:** November 27, 2024

## Issues Identified and Fixed

### Issue 1: Task Node Configurations Not Showing ❌

**Problem:**
- When clicking on expanded task nodes (IngestTask, WriteDeltaTask), the Properties Panel was empty
- The backend returns empty `config_schema: {}` for task components
- PropertiesPanel only renders forms when schema has properties

**Root Cause:**
- Task building blocks in backend don't have JSON schemas defined
- Frontend expected schemas to exist for all components
- No fallback mechanism for showing configs without schemas

**Solution: ✅**
- Enhanced PropertiesPanel to show task configurations from pipeline graph
- Added intelligent form generation based on task type
- Fallback to JSON editor when no schema exists
- Configurations are editable and persist to node data

### Issue 2: No Pipeline-Level Configuration ❌

**Problem:**
- After dropping a pipeline and it expanding into DAG, no way to configure the entire pipeline
- Each task node is independent with no parent pipeline config
- Users can't set pipeline-wide parameters (schedule, environment, tags, etc.)

**Current Limitation:**
- Pipeline-level configuration not yet implemented (requires deeper architecture changes)
- For now, configure each task independently

**Future Enhancement:**
- Will add pipeline wrapper nodes that can be configured
- Pipeline metadata will be stored separately from task configs

### Issue 3: Components Not Reused from Existing Library ⚠️

**Question:** Are task nodes using real component implementations?

**Answer:**
- **Task nodes ARE using real task implementations** from `orchestrators/tasks/`
- Each task (IngestTask, WriteDeltaTask, etc.) has actual Python code that will execute
- However, task nodes don't reference the full component library (connections, ingestors, transformers, ml)
- Task configs are pipeline-specific, not pulling from component registry

**Current Behavior:**
- `IngestTask` config specifies which ingestor to use by name (e.g., "jdbc_postgres_ingestor")
- `Transform Task` config specifies which transformers to apply
- The actual ingestor/transformer components are loaded at **execution time**, not design time

**This is by design:**
- Pipelines are configuration-driven
- Tasks reference components by name, not by drag-and-drop
- Allows pipelines to be portable and version-controlled as JSON/YAML

## New Features

### Feature 1: Task Configuration Forms

When you click on a task node, you now see:

#### IngestTask Configuration:
```
┌─────────────────────────────────────┐
│ ingest_source                       │
│ task_IngestTask                     │
├─────────────────────────────────────┤
│                                     │
│ Configure the parameters for this   │
│ IngestTask                          │
│                                     │
│ Ingestor Name:                      │
│ [jdbc_postgres_ingestor_____]       │
│                                     │
│ Ingestor Configuration:             │
│ {                                   │
│   "table": "customers",             │
│   "connection": "prod_db"           │
│ }                                   │
│                                     │
│ [Apply Changes] [</>]               │
└─────────────────────────────────────┘
```

#### WriteDeltaTask Configuration:
```
┌─────────────────────────────────────┐
│ write_bronze                        │
│ task_WriteDeltaTask                 │
├─────────────────────────────────────┤
│                                     │
│ Destination Catalog:                │
│ [sparkle_prod______________]        │
│                                     │
│ Destination Schema:                 │
│ [bronze____________________]        │
│                                     │
│ Destination Table:                  │
│ [customers_________________]        │
│                                     │
│ Write Mode:                         │
│ [append ▼]                          │
│                                     │
│ Partition Columns:                  │
│ [year,month,day____________]        │
│                                     │
│ Z-Order Columns:                    │
│ [customer_id_______________]        │
│                                     │
│ [Apply Changes] [</>]               │
└─────────────────────────────────────┘
```

### Feature 2: JSON Editor Mode

Click the `</>` button to switch to JSON editor mode:

```
┌─────────────────────────────────────┐
│ Configuration (JSON)                │
│                      [Switch to Form]│
├─────────────────────────────────────┤
│ {                                   │
│   "destination_catalog": "sparkle_prod",│
│   "destination_schema": "bronze",   │
│   "destination_table": "customers", │
│   "write_mode": "append",           │
│   "partition_columns": ["year","month"],│
│   "zorder_columns": ["customer_id"] │
│ }                                   │
│                                     │
│                                     │
│ [Apply Changes]                     │
└─────────────────────────────────────┘
```

### Feature 3: Smart Mode Detection

The PropertiesPanel automatically chooses the best mode:

- **Task nodes with known schemas** → Form mode
- **Task nodes without schemas** → JSON mode (default)
- **Regular components** → Form mode with full schema support
- **Switch button** → Toggle between form and JSON anytime

## Supported Task Types

### Currently Supported (22 Tasks):

#### Ingestion (1)
- **IngestTask** - Form fields: `ingestor`, `ingestor_config`

#### Transformation (1)
- **TransformTask** - Form fields: `transformers`

#### Write (3)
- **WriteDeltaTask** - Form fields: `destination_catalog`, `destination_schema`, `destination_table`, `write_mode`, `partition_columns`, `zorder_columns`
- **WriteFeatureTableTask** - JSON editor (advanced)
- **CreateUnityCatalogTableTask** - JSON editor (advanced)

#### ML (4)
- **TrainMLModelTask** - JSON editor
- **BatchScoreTask** - JSON editor
- **RegisterModelTask** - JSON editor
- **PromoteModelChampionTask** - JSON editor

#### Quality (3)
- **RunGreatExpectationsTask** - JSON editor
- **RunDbtCoreTask** - JSON editor
- **RunSQLFileTask** - JSON editor

#### Governance (2)
- **GrantPermissionsTask** - JSON editor
- **WaitForTableFreshnessTask** - JSON editor

#### Notification (3)
- **SendSlackAlertTask** - JSON editor
- **SendEmailAlertTask** - JSON editor
- **NotifyOnFailureTask** - JSON editor

#### Orchestration (3)
- **RunNotebookTask** - JSON editor
- **TriggerDownstreamPipelineTask** - JSON editor
- **AutoRecoverTask** - JSON editor

#### Observability (2)
- **MonteCarloDataIncidentTask** - JSON editor
- **LightupDataSLAAlertTask** - JSON editor

## Usage Guide

### Step 1: Expand a Pipeline

1. **Drag** "Bronze Raw Ingestion Pipeline" onto canvas
2. **Pipeline expands** into 2 task nodes
3. Nodes appear with default configs from pipeline template

### Step 2: Configure IngestTask

1. **Click** on "IngestTask" (green node)
2. **Properties Panel opens** on the right
3. **Fill in fields:**
   - Ingestor Name: `jdbc_postgres_ingestor`
   - Ingestor Configuration:
     ```json
     {
       "table": "customers",
       "connection": "prod_postgres"
     }
     ```
4. **Click** "Apply Changes"
5. **Panel closes**, configuration saved to node

### Step 3: Configure WriteDeltaTask

1. **Click** on "WriteDeltaTask" (red node)
2. **Properties Panel opens**
3. **Fill in fields:**
   - Destination Catalog: `sparkle_prod`
   - Destination Schema: `bronze`
   - Destination Table: `customers`
   - Write Mode: `append`
   - Partition Columns: `year,month,day`
   - Z-Order Columns: `customer_id`
4. **Click** "Apply Changes"

### Step 4: Switch to JSON Mode (Optional)

1. **While editing** a task node
2. **Click** the `</>` button (bottom right of form)
3. **JSON editor appears** with current config
4. **Edit JSON** directly
5. **Click** "Apply Changes"

### Step 5: Save Pipeline

1. **Click** "Save" in top bar
2. **Pipeline saved** with all task configurations
3. **Configurations persist** across sessions

## Testing Instructions

**IMPORTANT: Clear browser cache!**

New bundle: `index-DVleIwwh.js`

### Test 1: Task Configuration Form

1. **Clear cache**: Ctrl+Shift+R
2. **Navigate to**: http://localhost:3000
3. **Expand**: "Bronze Raw Ingestion Pipeline"
4. **Click**: IngestTask node
5. **Verify**: Properties panel shows form fields

**Expected:**
- ✅ Panel title: "ingest_source"
- ✅ Subtitle: "task_IngestTask"
- ✅ Form field: "Ingestor Name"
- ✅ Form field: "Ingestor Configuration"
- ✅ Button: "Apply Changes"
- ✅ Button: Code icon `</>` for JSON mode

### Test 2: Configuration Updates Persist

1. **Click**: IngestTask
2. **Enter**: Ingestor Name = "test_ingestor"
3. **Click**: "Apply Changes"
4. **Panel closes**
5. **Click**: IngestTask again
6. **Verify**: "test_ingestor" is still there

**Expected:**
- ✅ Configuration persists in node data
- ✅ Re-opening shows saved values

### Test 3: JSON Editor Mode

1. **Click**: WriteDeltaTask
2. **Click**: `</>` button (code icon)
3. **Verify**: JSON editor appears with current config
4. **Edit** JSON (change table name)
5. **Click**: "Apply Changes"
6. **Re-open** node
7. **Verify**: Changes persisted

**Expected:**
- ✅ Switch to JSON mode works
- ✅ Can edit JSON directly
- ✅ Changes save correctly
- ✅ Can switch back to form mode

### Test 4: Empty Config Handling

1. **Drag**: "ML Training Champion Challenger Pipeline"
2. **Click**: on any ML task node
3. **Verify**: JSON editor appears (since no form schema defined)

**Expected:**
- ✅ JSON editor shows by default for unknown schemas
- ✅ Can edit config as JSON
- ✅ No errors or blank panels

## Architecture Notes

### Configuration Flow

```
Pipeline Graph (Backend)
    ↓
Contains task configs with values
    ↓
Expanded into React Flow nodes
    ↓
Each node has data.config = {...}
    ↓
Click node → Load config into PropertiesPanel
    ↓
Edit in form or JSON mode
    ↓
Apply Changes → Update node.data.config
    ↓
Save pipeline → Configs serialized to backend
```

### Data Structure

**Node Data:**
```typescript
{
  component_type: 'orchestrator',
  component_name: 'task_IngestTask',
  label: 'ingest_source',
  config: {
    ingestor: 'jdbc_postgres_ingestor',
    ingestor_config: {
      table: 'customers',
      connection: 'prod_postgres'
    }
  },
  task_type: 'IngestTask',
  visual_category: 'ingestor',
  is_task_node: true
}
```

### Component Reuse Explanation

**Design Philosophy:**

Sparkle uses a **declarative configuration** approach, not a **visual wiring** approach.

**Instead of:**
```
Drag PostgreSQL → Drag Ingestor → Wire together
```

**We use:**
```
Configure task: { ingestor: "jdbc_postgres_ingestor" }
```

**At execution time:**
1. Pipeline reads config
2. IngestTask loads ingestor by name: `get_ingestor("jdbc_postgres_ingestor")`
3. Ingestor component executes with provided config
4. Result passes to next task

**Benefits:**
- ✅ Pipelines are portable (just JSON files)
- ✅ Version control friendly
- ✅ Environment-specific configs (dev/qa/prod)
- ✅ Can generate pipelines programmatically
- ✅ Reuse across orchestrators (Airflow, Databricks, Dagster)

**Future Enhancement:**
- Add "Browse Components" button in task config
- Opens modal showing available ingestors/transformers
- Click to select → Auto-fills name in config
- Best of both worlds: visual selection + config storage

## Known Limitations

### 1. No Pipeline-Level Configuration

**Current:**
- No way to configure the entire pipeline after expansion
- Each task is independent

**Workaround:**
- Configure individual tasks with consistent settings
- Use naming conventions (e.g., all use same catalog)

**Future:**
- Add pipeline wrapper node
- Pipeline-level configs (schedule, env, tags, SLA)
- Cascade configs to child tasks

### 2. Limited Form Schemas

**Current:**
- Only 3 tasks have custom forms (IngestTask, WriteDeltaTask, TransformTask)
- Other 19 tasks use JSON editor

**Future:**
- Add form schemas for all 22 tasks
- Generate forms from backend task class signatures
- Auto-detect parameter types and generate UI

### 3. No Component Browser

**Current:**
- Must type ingestor/transformer names manually
- No autocomplete or validation

**Future:**
- Add component picker modal
- Search/filter available components
- Show descriptions and schemas
- One-click to select

## Summary

✅ **Fixed Issues:**
1. Task configurations now visible and editable
2. Smart mode detection (form vs JSON)
3. Configurations persist correctly
4. Clear explanation of component reuse model

✅ **New Features:**
1. Task-specific configuration forms
2. JSON editor mode with toggle
3. Enhanced PropertiesPanel UI
4. Better error handling

✅ **Components ARE Reused:**
- Tasks reference components by name
- Components loaded at execution time
- Declarative vs visual wiring approach

**Just clear your browser cache and test!** 🎯

Bundle: `index-DVleIwwh.js`
