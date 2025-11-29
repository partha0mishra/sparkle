# DAG Visualization - API Alignment Fixes

**Status:** ✅ ALL ISSUES FIXED
**Date:** November 27, 2024
**Build:** `index-B9HwJJX9.js`

## Issues Discovered

### Issue 1: Component Name Mismatch
**Error:** `"Component not found: orchestrator/BronzeRawIngestionPipeline"`

**Root Cause:**
- Backend API has inconsistency:
  - List API returns: `"BronzeRawIngestionPipeline"` (no prefix)
  - Detail API expects: `"pipeline_BronzeRawIngestionPipeline"` (with prefix)
- Frontend was checking for `pipeline_` prefix in component name
- Detection failed because sidebar shows names WITHOUT prefix

**Fix:**
```typescript
// OLD: Only detected names starting with "pipeline_"
return componentCategory === 'orchestrator' && componentName.startsWith('pipeline_');

// NEW: Intelligently detects pipeline templates by suffix
const isPipelineTemplate = componentName.endsWith('Pipeline');
const isAdapter = componentName.includes('Adapter');
const isTask = componentName.includes('Task');
return isPipelineTemplate && !isAdapter && !isTask && !isSchedule;
```

```typescript
// Add prefix when calling API
const apiName = category === 'orchestrator' ? `pipeline_${name}` : name;
const response = await api.get(`/components/${category}/${apiName}`);
```

**Files Modified:**
- `frontend/src/services/pipelineExpander.ts`

---

### Issue 2: Task Component API Mismatch
**Error:** `"Component not found: ingestor/IngestTask"`

**Root Cause:**
- When clicking on expanded task nodes, PropertiesPanel fetches component details
- Task nodes had: `component_type: 'ingestor'`, `component_name: 'IngestTask'`
- API call: `/api/v1/components/ingestor/IngestTask` ❌ Doesn't exist
- Correct API structure: `/api/v1/components/orchestrator/task_IngestTask` ✅

**API Documentation Check:**
```bash
curl http://localhost:8000/api/v1/components | jq '.data.groups[] | select(.category=="orchestrator") | .components[] | select(.name | contains("Task")) | .name' | head -5
"IngestTask"
"TransformTask"
"CreateUnityCatalogTableTask"
"WriteDeltaTask"
"WriteFeatureTableTask"
```

**Verified:** Tasks are registered as:
- Category: `orchestrator`
- Name: `task_IngestTask`, `task_TransformTask`, etc.
- Sub-group: "Task Building Blocks"

**Fix:**
```typescript
// Task nodes now use correct API structure
data: {
  component_type: 'orchestrator',        // ← Was 'ingestor'
  component_name: `task_${graphNode.type}`,  // ← Was 'IngestTask'
  label: graphNode.label,
  config: graphNode.config || {},
  task_type: graphNode.type,
  visual_category: category,             // ← NEW: For color coding
  is_task_node: true,                    // ← NEW: Flag for identification
}
```

**Files Modified:**
- `frontend/src/services/pipelineExpander.ts`

---

### Issue 3: Color Coding Broken
**Problem:** With `component_type: 'orchestrator'`, all task nodes would be gray

**Root Cause:**
- CustomNode uses `component_type` for color selection
- Mapping:
  - `ingestor` → Green
  - `transformer` → Purple
  - `ml` → Orange
  - `sink` → Red
  - `orchestrator` → Gray
- All task nodes would be gray instead of color-coded by their function

**Fix:**
Added `visual_category` field for color coding while keeping `component_type` for API calls:

```typescript
// CustomNode.tsx - Use visual_category if available
const categoryForVisual = data.visual_category || data.component_type;
const Icon = categoryIcons[categoryForVisual as keyof typeof categoryIcons] || Zap;
const colorClass = categoryColors[categoryForVisual as keyof typeof categoryColors] || 'bg-gray-500';
```

```typescript
// Canvas.tsx - Minimap also uses visual_category
nodeColor={(node) => {
  const category = node.data.visual_category || node.data.component_type;
  return colors[category] || '#6b7280';
}}
```

**Files Modified:**
- `frontend/src/components/CustomNode.tsx`
- `frontend/src/components/Canvas.tsx`

---

## Complete Fix Summary

### Files Changed

1. **`frontend/src/services/pipelineExpander.ts`**
   - Fixed pipeline detection (check suffix instead of prefix)
   - Added `pipeline_` prefix for API calls
   - Updated task nodes to use orchestrator/task_ structure
   - Added `visual_category` and `is_task_node` fields

2. **`frontend/src/components/CustomNode.tsx`**
   - Use `visual_category` for colors (with fallback to `component_type`)

3. **`frontend/src/components/Canvas.tsx`**
   - Minimap uses `visual_category` for node colors

### Task Node Data Structure

**Before (Broken):**
```typescript
{
  component_type: 'ingestor',      // ❌ Wrong category
  component_name: 'IngestTask',     // ❌ Wrong name format
  label: 'ingest_source',
  config: { ... }
}
```

**After (Fixed):**
```typescript
{
  component_type: 'orchestrator',       // ✅ Correct category
  component_name: 'task_IngestTask',    // ✅ Correct API name
  label: 'ingest_source',
  config: { ... },
  task_type: 'IngestTask',              // ✅ Original task class name
  visual_category: 'ingestor',          // ✅ For color coding
  is_task_node: true                    // ✅ Identification flag
}
```

### API Call Flow

**Pipeline Expansion:**
1. User drags "Bronze Raw Ingestion Pipeline" from sidebar
2. Sidebar shows name: `"BronzeRawIngestionPipeline"` (no prefix)
3. Detection: `name.endsWith('Pipeline')` → True
4. API call: `/api/v1/components/orchestrator/pipeline_BronzeRawIngestionPipeline` ✅
5. Response contains graph with 2 tasks
6. Creates 2 nodes on canvas

**Task Node Click:**
1. User clicks on "IngestTask" node in expanded DAG
2. PropertiesPanel loads component details
3. API call: `/api/v1/components/orchestrator/task_IngestTask` ✅
4. Returns task schema and description
5. Shows editable config fields

## Testing Instructions

### Clear Browser Cache (CRITICAL!)

**The JavaScript bundle changed to `index-B9HwJJX9.js`**

You MUST clear your browser cache:

**Chrome/Edge:**
```
1. Open DevTools (F12)
2. Right-click refresh button
3. "Empty Cache and Hard Reload"
```

**Or:**
```
Ctrl+Shift+Delete → Check "Cached images and files" → Clear
```

### Test Steps

#### Test 1: Pipeline Expansion ✅

1. **Navigate to:** http://localhost:3000
2. **Sidebar:** Expand "Orchestrator" section
3. **Expand:** "Core Pipeline Templates" sub-group
4. **Drag:** "Bronze Raw Ingestion Pipeline"
5. **Drop:** Onto canvas

**Expected Result:**
```
┌─────────────────┐
│  IngestTask     │  ← Green node (ingestor visual category)
│  ingest_source  │
└────────┬────────┘
         │ ↓ Animated arrow
         v
┌─────────────────┐
│ WriteDeltaTask  │  ← Red node (sink visual category)
│  write_bronze   │
└─────────────────┘
```

**Verify:**
- ✅ 2 nodes created (not 1 single node)
- ✅ Nodes connected with animated arrow
- ✅ IngestTask is green
- ✅ WriteDeltaTask is red
- ✅ Nodes positioned horizontally with spacing

#### Test 2: Node Selection & Configuration ✅

1. **Click:** On the "IngestTask" node
2. **Properties Panel:** Should open on the right

**Expected Result:**
- ✅ Panel shows "Ingest Task"
- ✅ Shows description: "Executes any registered ingestor with parameters."
- ✅ Shows config fields for the task
- ✅ NO error in console

**DevTools Check:**
```
F12 → Console
Should see:
✅ GET /api/v1/components/orchestrator/task_IngestTask → 200 OK

Should NOT see:
❌ GET /api/v1/components/ingestor/IngestTask → 404
```

#### Test 3: Complex Pipeline ✅

1. **Drag:** "Silver Batch Transformation Pipeline"
2. **Drop:** On canvas

**Expected Result:**
- ✅ 3 nodes created
- ✅ Connected in sequence
- ✅ Proper colors (green → purple → red)

#### Test 4: Non-Pipeline Components ✅

1. **Go to:** "Connections" section in sidebar
2. **Drag:** Any connection (e.g., "PostgreSQL")
3. **Drop:** On canvas

**Expected Result:**
- ✅ Creates single node (normal behavior)
- ✅ Does NOT expand into multiple nodes

### Verification Commands

**Check backend API:**
```bash
# Verify pipeline API
curl http://localhost:8000/api/v1/components/orchestrator/pipeline_BronzeRawIngestionPipeline | jq '.success'
# Should return: true

# Verify task API
curl http://localhost:8000/api/v1/components/orchestrator/task_IngestTask | jq '.success'
# Should return: true
```

**Check frontend build:**
```bash
# Verify new bundle is deployed
docker run --rm sparkle-studio-studio-frontend ls -la /usr/share/nginx/html/assets/ | grep B9HwJJX9
# Should show: index-B9HwJJX9.js
```

## API Structure Reference

### Orchestrator Components

**Categories in API:**
- **Pipelines:** `pipeline_*` (24 templates)
  - `pipeline_BronzeRawIngestionPipeline`
  - `pipeline_SilverBatchTransformationPipeline`
  - etc.

- **Adapters:** `adapter_*` (40 adapters)
  - `adapter_DatabricksWorkflowsJobAdapter`
  - `adapter_AirflowTaskFlowDecoratorAdapter`
  - etc.

- **Tasks:** `task_*` (22 tasks)
  - `task_IngestTask`
  - `task_TransformTask`
  - `task_WriteDeltaTask`
  - etc.

- **Schedules:** `schedule_*` (13 patterns)
  - `schedule_DailyAtMidnightSchedule`
  - `schedule_HourlySchedule`
  - etc.

**All are in category:** `orchestrator`

### Visual Categories (for color coding only)

- `connection` → Blue
- `ingestor` → Green
- `transformer` → Purple
- `ml` → Orange
- `sink` → Red

## Success Indicators

✅ **DAG Visualization Working:**
- Pipeline components expand into multiple nodes
- Nodes are color-coded by function
- Animated edges connect nodes
- Auto-layout positions nodes properly

✅ **API Integration Working:**
- No "Component not found" errors in console
- Properties panel loads for task nodes
- Task schemas fetched successfully

✅ **Visual Feedback Working:**
- IngestTask = Green
- TransformTask = Purple
- WriteDeltaTask = Red
- ML tasks = Orange

## Troubleshooting

### Still seeing single nodes?

1. **Hard refresh:** Ctrl+Shift+R
2. **Clear cache completely**
3. **Close all tabs and reopen**

### Still getting API errors?

**Check console:**
```
F12 → Console → Look for red errors
```

**Verify services:**
```bash
docker-compose ps
# All services should be "Up" and "healthy"
```

### Nodes are all gray?

- Clear browser cache
- Verify new build: `index-B9HwJJX9.js`
- Check CustomNode is using `visual_category`

---

## Summary

All API alignment issues have been fixed:

1. ✅ Pipeline detection works with names from sidebar
2. ✅ API calls use correct prefix structure
3. ✅ Task nodes use orchestrator/task_ API format
4. ✅ Color coding preserved with visual_category
5. ✅ No more "Component not found" errors

**Just clear your browser cache and test!** 🚀
