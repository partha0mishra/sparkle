# Sparkle Studio Frontend

React + TypeScript frontend for Sparkle Studio with React Flow canvas, auto-generated forms, and Git integration.

## Tech Stack

- **React 18** - UI library
- **TypeScript** - Type safety
- **Vite** - Build tool
- **React Flow** - Visual canvas
- **Tailwind CSS** - Styling
- **Zustand** - State management
- **React Hook Form** - Form handling
- **Zod** - Schema validation
- **Axios** - API client

## Features

✅ **Visual Canvas**
- Drag & drop components from sidebar
- React Flow with mini-map and controls
- Custom nodes with icons and badges
- Real-time edge connections

✅ **Auto-Generated Forms**
- JSON Schema → React forms
- Field types: text, textarea, password, number, dropdown, checkbox
- UI hints: widget, group, order, placeholder, help text
- Validation with error messages

✅ **Git Integration**
- Branch selector
- Commit with message
- Pull from remote
- Create pull requests
- Real-time status display

✅ **Pipeline Management**
- Load/save pipelines
- Undo/redo support
- Auto-save on Ctrl+S
- Export to Git

## Development

### Prerequisites

- Node.js 20+
- npm or yarn

### Install Dependencies

```bash
npm install
```

### Start Development Server

```bash
npm run dev
```

Open http://localhost:3000

### Build for Production

```bash
npm run build
```

### Preview Production Build

```bash
npm run preview
```

## Environment Variables

Create a `.env` file:

```env
VITE_API_URL=http://localhost:8000
```

## Docker

### Build

```bash
docker build -t sparkle-studio-frontend .
```

### Run

```bash
docker run -p 3000:80 sparkle-studio-frontend
```

## Project Structure

```
src/
├── components/        # React components
│   ├── Canvas.tsx
│   ├── Sidebar.tsx
│   ├── TopBar.tsx
│   ├── PropertiesPanel.tsx
│   ├── CustomNode.tsx
│   └── NodeTypes.ts
├── hooks/            # Custom hooks
│   ├── usePipeline.ts
│   ├── useGit.ts
│   └── useComponents.ts
├── store/            # Zustand stores
│   └── pipelineStore.ts
├── types/            # TypeScript types
│   ├── component.ts
│   ├── pipeline.ts
│   ├── git.ts
│   └── api.ts
├── services/         # Services
│   └── pipelineSerializer.ts
├── lib/             # Utilities
│   ├── api.ts
│   └── utils.ts
└── App.tsx          # Main app component
```

## Keyboard Shortcuts

- `Ctrl+S` / `Cmd+S` - Save pipeline
- `Ctrl+Z` - Undo
- `Ctrl+Shift+Z` - Redo

## API Integration

The frontend communicates with the Sparkle Studio backend API at `/api/v1`:

- `GET /components` - List all components
- `GET /pipelines/{name}` - Load pipeline
- `POST /pipelines/{name}` - Save pipeline
- `GET /git/status` - Git status
- `POST /git/commit` - Commit changes

See `src/lib/api.ts` for the Axios configuration.

## Contributing

1. Follow TypeScript strict mode
2. Use Tailwind CSS for styling
3. Keep components small and focused
4. Add types for all props and state
5. Handle errors gracefully

## License

Apache License 2.0
