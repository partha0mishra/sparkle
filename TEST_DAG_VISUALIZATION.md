# Testing DAG Visualization - Step by Step Guide

**Services Status:** ✅ All services rebuilt and running
**Date:** November 27, 2024

## Step 1: Clear Browser Cache (IMPORTANT!)

Since Docker was caching the old frontend, your browser is likely also caching it. You MUST clear your browser cache.

### Chrome/Edge:
1. Open **DevTools** (F12 or Cmd+Option+I on Mac)
2. **Right-click** on the refresh button
3. Select **"Empty Cache and Hard Reload"**
4. OR: Settings → Privacy → Clear browsing data → Check "Cached images and files" → Clear data

### Firefox:
1. Press **Ctrl+Shift+Delete** (Cmd+Shift+Delete on Mac)
2. Select **"Cached Web Content"**
3. Click **"Clear Now"**

### Safari:
1. **Develop** menu → **Empty Caches**
2. OR: Safari → Preferences → Advanced → Check "Show Develop menu"
3. Then: Develop → Empty Caches

## Step 2: Open Sparkle Studio

1. Navigate to: **http://localhost:3000**
2. Wait for the page to fully load
3. You should see the Sparkle Studio interface with:
   - Top bar (Sparkle Studio logo, New/Open/Save buttons)
   - Left sidebar (Components)
   - Center canvas (empty grid)
   - Right properties panel

## Step 3: Test DAG Visualization

### Test 1: Simple 2-Node Pipeline (Bronze Raw Ingestion)

1. **In the left sidebar**, scroll to find **"Orchestrator"** section
2. **Click to expand** "Orchestrator"
3. **Find and expand** "Core Pipeline Templates" sub-group
4. **Locate** "Bronze Raw Ingestion Pipeline"
5. **Drag it** onto the canvas (center area)
6. **Drop it** anywhere on the canvas

**Expected Result:**
- Pipeline should automatically expand into **2 nodes**:
  - **Node 1**: "IngestTask" (ingest_source) - Green color
  - **Node 2**: "WriteDeltaTask" (write_bronze) - Red color
- **One animated edge** connecting Node 1 → Node 2
- Nodes should be positioned horizontally with proper spacing

**If you see:**
- ✅ **2 separate nodes with an arrow** = SUCCESS!
- ❌ **Single node** = Cache issue, see troubleshooting below

### Test 2: Complex Multi-Node Pipeline (Silver Transformation)

1. **Locate** "Silver Batch Transformation Pipeline" in the same sub-group
2. **Drag and drop** onto a different area of the canvas

**Expected Result:**
- Should expand into **3 nodes**:
  - IngestTask → TransformTask → WriteDeltaTask
- Multiple edges connecting them in sequence

### Test 3: ML Pipeline (Training Champion Challenger)

1. **Locate** "ML Training Champion Challenger Pipeline"
2. **Drag and drop** onto canvas

**Expected Result:**
- More complex DAG with **4-5 nodes**
- Multiple ML tasks
- Branching edges

## Step 4: Test Node Configuration

1. **Click on any node** in the expanded DAG
2. **Properties panel** on the right should open
3. **You should see**:
   - Node label/name
   - Task type
   - Configuration fields specific to that task
4. **Try changing** a configuration value
5. **The node** should update

## Step 5: Test Non-Pipeline Components (Should NOT Expand)

1. **Go to** "Connections" or "Ingestors" section
2. **Drag** any regular component (not a pipeline)
3. **Drop** on canvas

**Expected Result:**
- Should create **single node** (normal behavior)
- Should NOT expand into multiple nodes

## Verification Checklist

Check each item:

- [ ] Browser cache cleared completely
- [ ] Page loaded at http://localhost:3000
- [ ] Orchestrator section visible in sidebar
- [ ] "Core Pipeline Templates" sub-group exists
- [ ] Can drag "Bronze Raw Ingestion Pipeline"
- [ ] Pipeline expands into 2 nodes when dropped
- [ ] Nodes are connected with animated arrow
- [ ] Can click individual nodes
- [ ] Properties panel shows node config
- [ ] Regular components create single nodes

## Troubleshooting

### Issue: Pipeline still creates single node instead of expanding

**Cause:** Browser cache not cleared

**Solution:**
```bash
# Force clear and reload:
1. Close all Sparkle Studio tabs
2. Clear browser cache (see Step 1)
3. Close browser completely
4. Open new browser window
5. Navigate to http://localhost:3000
6. Try again
```

### Issue: "TypeError" or console errors

**Check:**
1. Open browser **DevTools** (F12)
2. Go to **Console** tab
3. Look for errors (red messages)
4. If you see errors mentioning "expandPipeline" or "isPipelineComponent", copy them

**Quick fix:**
```bash
# In terminal:
cd sparkle-studio
docker-compose restart studio-frontend
# Wait 15 seconds, then refresh browser with Ctrl+Shift+R
```

### Issue: Canvas doesn't accept drops

**Check:**
1. Is the canvas area visible and taking up space?
2. Try dropping in the center of the canvas
3. Make sure you're dragging from the Orchestrator section

### Issue: Only see single orchestrator node, no sub-components

**This means the DAG expansion is NOT working. Try:**

1. **Hard refresh**: Ctrl+Shift+R (Cmd+Shift+R on Mac)
2. **Check console**: F12 → Console tab → Look for errors
3. **Verify API**: Open http://localhost:8000/api/v1/components/orchestrator/pipeline_BronzeRawIngestionPipeline
   - Should see `"config_schema": { "type": "pipeline", "graph": { ... } }`
4. **Rebuild frontend**:
   ```bash
   cd sparkle-studio
   docker-compose down
   docker-compose build --no-cache studio-frontend
   docker-compose up -d
   ```

## Debug Mode

To see what's happening:

1. **Open DevTools** (F12)
2. **Go to Console tab**
3. **Drag and drop** a pipeline
4. **Watch for messages**:
   - Should see API calls to `/api/v1/components/...`
   - May see "Expanding pipeline..." messages
   - Look for errors in red

## API Verification

Test the backend is providing graph data:

```bash
# Test API endpoint:
curl http://localhost:8000/api/v1/components/orchestrator/pipeline_BronzeRawIngestionPipeline | python3 -m json.tool
```

**Look for:**
```json
{
  "config_schema": {
    "type": "pipeline",
    "graph": {
      "nodes": [ ... ],
      "edges": [ ... ]
    }
  }
}
```

If you see this, the backend is working correctly.

## What Success Looks Like

When DAG visualization is working correctly:

### Visual Confirmation:
```
Canvas View After Dropping "Bronze Raw Ingestion Pipeline":

┌─────────────────────────────┐
│     IngestTask              │  ← Green node
│     ingest_source           │
└──────────┬──────────────────┘
           │ ↓ Animated arrow
           │
┌──────────┴──────────────────┐
│    WriteDeltaTask           │  ← Red node
│    write_bronze             │
└─────────────────────────────┘
```

### Properties Panel (when node clicked):
```
Properties
─────────────────────
IngestTask
ingest_source

Configuration:
┌─────────────────────┐
│ ingestor: [____]    │
│                     │
│ ingestor_config:    │
│ { ... }             │
└─────────────────────┘
```

## Success Indicators

✅ **It's Working If:**
- Pipeline name shown in sidebar
- Drag creates visual "ghost" image
- Drop on canvas creates multiple nodes
- Nodes appear in a horizontal line
- Animated arrows connect nodes
- Node colors match their types (green/purple/red/orange)
- Click node → properties panel opens
- Properties show specific task config

❌ **It's NOT Working If:**
- Drop creates single node
- No arrows between components
- Console shows errors
- Properties panel shows pipeline config instead of task config

## Next Steps After Confirmation

Once DAG visualization works:

1. **Try all 24 pipeline templates**
2. **Configure individual task nodes**
3. **Save pipeline with configured DAG**
4. **Create custom pipelines** by mixing task nodes
5. **Report any issues** or unexpected behavior

---

**Need Help?**

If you're still having issues after:
- Clearing cache
- Rebuilding without cache
- Hard refreshing browser

Please share:
1. Screenshot of what you see
2. Browser console errors (F12 → Console)
3. Output of: `curl http://localhost:8000/api/v1/components/orchestrator/pipeline_BronzeRawIngestionPipeline | jq .data.component.config_schema`
