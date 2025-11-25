# Troubleshooting Sparkle Studio Frontend

## Issue: No components showing in sidebar

If you see an empty sidebar with "No components available", follow these steps:

### 1. Check Browser Console

Open your browser's Developer Tools (F12) and check the Console tab for errors. Look for:

- **Network errors**: `ERR_CONNECTION_REFUSED`, `Failed to fetch`
- **API errors**: 401, 403, 404, 500 status codes
- **CORS errors**: `Access-Control-Allow-Origin` messages
- **Component loading logs**: `[useComponents]` prefixed messages

### 2. Verify Backend is Running

The frontend needs the backend API to be accessible. Check if the backend is running:

**If using Docker:**
```bash
cd sparkle-studio
docker compose ps

# You should see services running:
# - studio-backend
# - studio-frontend
# - spark-master
# - spark-worker
```

**Check backend health:**
```bash
curl http://localhost:8000/health
# Should return: {"status":"healthy"}
```

**Check components endpoint:**
```bash
curl http://localhost:8000/api/v1/components
# Should return JSON with components list
```

### 3. Check Environment Variables

The frontend needs to know where the backend is:

**When running in Docker:**
- The environment variable `VITE_API_URL` should be set to `http://studio-backend:8000`
- This is configured in `docker-compose.yml`

**When running locally with `npm run dev`:**
- Create a `.env` file in the `frontend/` directory
- Add: `VITE_API_URL=http://localhost:8000`
- Or the Vite proxy will default to `http://localhost:8000`

### 4. Check CORS Configuration

The backend must allow requests from the frontend origin.

In `sparkle-studio/docker-compose.yml`, verify the `studio-backend` service has:
```yaml
environment:
  - BACKEND_CORS_ORIGINS=["http://localhost:3000", "http://localhost:8000", "http://localhost"]
```

### 5. Verify Components are Registered

The backend needs to discover components on startup. Check backend logs:

**Docker:**
```bash
docker compose logs studio-backend | grep -i component
```

You should see logs like:
```
Discovered 6 Sparkle components
Registered component: salesforce_incremental (ingestor)
Registered component: jdbc_incremental (ingestor)
Registered component: kafka_raw (ingestor)
Registered component: scd_type2 (transformer)
Registered component: data_quality (transformer)
Registered component: xgboost_trainer (ml)
```

### 6. Common Issues and Fixes

#### Issue: "Failed to connect to backend"

**Symptom:** Sidebar shows error message, console shows network errors

**Fix:**
1. Ensure backend is running on port 8000
2. Check `VITE_API_URL` environment variable
3. Try accessing http://localhost:8000/docs directly in browser

#### Issue: "401 Unauthorized" or "403 Forbidden"

**Symptom:** API requests fail with auth errors

**Fix:**
1. Check backend logs for auth errors
2. Verify `SPARKLE_ENV=local` and `DEBUG=true` in backend environment
3. In local/dev mode, auth should be bypassed automatically

#### Issue: CORS errors

**Symptom:** Console shows "Access-Control-Allow-Origin" errors

**Fix:**
1. Update `BACKEND_CORS_ORIGINS` in backend environment
2. Ensure it includes your frontend origin (e.g., `http://localhost:3000`)
3. Restart backend after changing CORS settings

#### Issue: Components endpoint returns empty list

**Symptom:** API call succeeds but returns `{"groups": [], "total_count": 0}`

**Fix:**
1. Check that Sparkle components exist in `/opt/sparkle/sparkle/`
2. Verify components have `@config_schema` decorator
3. Check backend startup logs for component discovery errors
4. Ensure `/opt/sparkle` is mounted correctly in Docker

#### Issue: TypeScript errors in browser

**Symptom:** Blank page, errors in console about undefined imports

**Fix:**
1. Ensure all dependencies are installed: `npm install`
2. Check `tsconfig.json` has correct `paths` configuration
3. Restart Vite dev server: `npm run dev`

### 7. Debug Mode

The updated Sidebar component now shows helpful debugging information:

- **Loading state**: "Loading components..."
- **Error state**: Shows error message and backend URL
- **Empty state**: Shows "No components available" with backend URL

Check the message on screen to identify the issue.

### 8. Manual Testing

Test the API manually to isolate the issue:

**Test health endpoint:**
```bash
curl http://localhost:8000/health
```

**Test components endpoint:**
```bash
curl -v http://localhost:8000/api/v1/components
```

**Expected response:**
```json
{
  "success": true,
  "data": {
    "groups": [
      {
        "category": "ingestor",
        "display_name": "Ingestors",
        "count": 3,
        "components": [...]
      },
      ...
    ],
    "total_count": 6,
    "stats": {...}
  },
  "message": "Retrieved 6 components"
}
```

### 9. Reset and Rebuild

If all else fails, try a clean rebuild:

**Docker:**
```bash
cd sparkle-studio
docker compose down -v
docker compose up --build
```

**Local development:**
```bash
cd frontend
rm -rf node_modules package-lock.json
npm install
npm run dev
```

### 10. Get Help

If you're still having issues:

1. Check the browser console for errors (copy exact error messages)
2. Check backend logs: `docker compose logs studio-backend`
3. Verify the API URL shown in the sidebar matches your backend
4. Take a screenshot of the error state
5. Report the issue with:
   - Error message from console
   - Backend logs
   - Environment (Docker / local dev)
   - Steps to reproduce

## Quick Checklist

- [ ] Backend is running on port 8000
- [ ] Frontend is running on port 3000
- [ ] `curl http://localhost:8000/health` returns healthy
- [ ] `curl http://localhost:8000/api/v1/components` returns components
- [ ] Browser console shows no errors
- [ ] `VITE_API_URL` is set correctly
- [ ] CORS origins include frontend URL
- [ ] Backend logs show "Discovered N components"
- [ ] No TypeScript errors in console
- [ ] Sidebar shows loading → components (not error)

## Debugging Checklist Output

The updated Sidebar component will show one of these states:

1. **Loading:** "Loading components..." (brief)
2. **Error:** Red error message with details
3. **Empty:** "No components available" with backend URL
4. **Success:** List of draggable components grouped by category

Check which state you're seeing and follow the corresponding fix above.

---

**Note**: All Phase 3 components include console.log debugging. Open browser DevTools (F12) → Console to see detailed logs of what's happening.
