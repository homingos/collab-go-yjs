import { ReactFlow, ReactFlowProvider, addEdge, applyEdgeChanges, applyNodeChanges, useReactFlow } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { useCallback, useEffect, useRef, useState } from 'react';
import { WebsocketProvider } from 'y-websocket';
import * as Y from 'yjs';

const initialNodes = [
  { id: 'n1', position: { x: 0, y: 0 }, data: { label: 'Start' } },
  { id: 'n2', position: { x: 150, y: 100 }, data: { label: 'Process 1' } },
  { id: 'n3', position: { x: 300, y: 0 }, data: { label: 'Process 2' } },
  { id: 'n4', position: { x: 300, y: 200 }, data: { label: 'Process 3' } },
  { id: 'n5', position: { x: 450, y: 100 }, data: { label: 'Decision' } },
  { id: 'n6', position: { x: 600, y: 0 }, data: { label: 'Output 1' } },
  { id: 'n7', position: { x: 600, y: 200 }, data: { label: 'Output 2' } },
];

const initialEdges = [
  { id: 'n1-n2', source: 'n1', target: 'n2' },
  { id: 'n2-n3', source: 'n2', target: 'n3' },
  { id: 'n2-n4', source: 'n2', target: 'n4' },
  { id: 'n3-n5', source: 'n3', target: 'n5' },
  { id: 'n4-n5', source: 'n4', target: 'n5' },
  { id: 'n5-n6', source: 'n5', target: 'n6' },
  { id: 'n5-n7', source: 'n5', target: 'n7' },
];

const generateUsername = () => {
  const adjectives = ['Yash', 'Vishal', 'Deba', 'Naveen'];
  const adj = adjectives[Math.floor(Math.random() * adjectives.length)];
  return `${adj}`;
};

function FlowCanvas() {
  const { screenToFlowPosition, flowToScreenPosition } = useReactFlow();
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const [connected, setConnected] = useState(false);
  const [synced, setSynced] = useState(false);
  const [error, setError] = useState(null);
  const [username] = useState(() => generateUsername());
  const [cursors, setCursors] = useState({});

  // Yjs refs
  const ydocRef = useRef(null);
  const providerRef = useRef(null);
  const yNodesRef = useRef(null);
  const yEdgesRef = useRef(null);
  const reactFlowWrapper = useRef(null);

  const roomName = 'flow-document';
  const httpBase = 'http://localhost:8080';
  const wsBase = 'ws://localhost:8080';

  // -------- reload via HTTP (INSIDE component so it sees refs) --------
  const reloadDocumentState = useCallback(() => {
    const ydoc = ydocRef.current;
    if (!ydoc) return;
    const docStateUrl = `${httpBase}/doc/${roomName}`;

    fetch(docStateUrl)
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        return response.arrayBuffer();
      })
      .then((buffer) => {
        if (buffer && buffer.byteLength > 0) {
          const update = new Uint8Array(buffer);
          Y.applyUpdate(ydoc, update);
        }
      })
      .catch((err) => setError('Reload failed: ' + (err?.message ?? String(err))));
  }, []);

  useEffect(() => {
    if (ydocRef.current) {
      // already initialized
      return;
    }

    const ydoc = new Y.Doc();
    const yNodes = ydoc.getArray('nodes');
    const yEdges = ydoc.getArray('edges');

    ydocRef.current = ydoc;
    yNodesRef.current = yNodes;
    yEdgesRef.current = yEdges;

    if (yNodes.length === 0 && yEdges.length === 0) {
      yNodes.insert(0, initialNodes);
      yEdges.insert(0, initialEdges);
    }

    const docStateUrl = `${httpBase}/doc/${roomName}`;

    let cleanup = null;
    let provider = null;

    const setupWebSocketProvider = () => {
      try {
        provider = new WebsocketProvider(wsBase, roomName, ydoc);
        providerRef.current = provider;

        // connection lifecycle
        const handleStatus = ({ status }) => {
          setConnected(status === 'connected');
          if (status === 'connected') setError(null);
        };
        provider.on('status', handleStatus);

        const handleSync = (isSynced) => setSynced(!!isSynced);
        provider.on('sync', handleSync);

        provider.on('connection-error', (err) => {
          setError(`Connection error: ${err?.message || 'Unknown error'}`);
        });
        provider.on('connection-close', () => setError('Connection closed'));

        // awareness
        const awareness = provider.awareness;
        awareness.setLocalStateField('user', {
          name: username,
          color: `hsl(${Math.random() * 360}, 70%, 60%)`,
        });
        const awarenessChangeHandler = () => {
          const states = awareness.getStates(); // Map<clientId, state>
          const newCursors = {};
          states.forEach((state, clientId) => {
            if (clientId !== awareness.clientID && state?.cursor) {
              newCursors[clientId] = { ...state.cursor, user: state.user };
            }
          });
          setCursors(newCursors);
        };
        awareness.on('change', awarenessChangeHandler);
        provider.awarenessChangeHandler = awarenessChangeHandler;

        // y-array observers
        const nodesObserver = () => setNodes(yNodes.toArray());
        const edgesObserver = () => setEdges(yEdges.toArray());
        yNodes.observe(nodesObserver);
        yEdges.observe(edgesObserver);

        // seed react state
        if (yNodes.length > 0) setNodes(yNodes.toArray());
        if (yEdges.length > 0) setEdges(yEdges.toArray());

        cleanup = () => {
          yNodes.unobserve(nodesObserver);
          yEdges.unobserve(edgesObserver);
          provider.off('status', handleStatus);
          provider.off('sync', handleSync);
          if (provider.awarenessChangeHandler) {
            provider.awareness.off('change', provider.awarenessChangeHandler);
            delete provider.awarenessChangeHandler;
          }
          provider.destroy();
          ydoc.destroy();
        };
      } catch (e) {
        setError('Failed to create WebsocketProvider');
      }
      return cleanup;
    };

    // First. try to hydrate from server snapshot. then connect WS
    fetch(docStateUrl)
      .then((response) => {
        if (response.status === 204 || response.status === 404) return null;
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        return response.arrayBuffer();
      })
      .then((buffer) => {
        if (buffer && buffer.byteLength > 0) {
          const update = new Uint8Array(buffer);
          Y.applyUpdate(ydoc, update);
        }
        cleanup = setupWebSocketProvider();
      })
      .catch(() => {
        // even if HTTP fails, still bring up WS
        cleanup = setupWebSocketProvider();
      });

    return () => {
      if (cleanup) cleanup();
    };
  }, [username]);

  // ReactFlow handlers -> mirror into Yjs arrays atomically
  const onNodesChange = useCallback(
    (changes) => {
      const yArr = yNodesRef.current;
      if (!yArr) return;
      const updated = applyNodeChanges(changes, yArr.toArray());
      yArr.delete(0, yArr.length);
      yArr.insert(0, updated);
    },
    []
  );

  const onEdgesChange = useCallback(
    (changes) => {
      const yArr = yEdgesRef.current;
      if (!yArr) return;
      const updated = applyEdgeChanges(changes, yArr.toArray());
      yArr.delete(0, yArr.length);
      yArr.insert(0, updated);
    },
    []
  );

  const onConnect = useCallback((params) => {
    const yArr = yEdgesRef.current;
    if (!yArr) return;
    const updated = addEdge(params, yArr.toArray());
    yArr.delete(0, yArr.length);
    yArr.insert(0, updated);
  }, []);

  const handleMouseMove = useCallback(
    (event) => {
      window.lastMouseEvent = event;
      if (!providerRef.current || !reactFlowWrapper.current) return;
      const rect = reactFlowWrapper.current.getBoundingClientRect();
      const screenX = event.clientX - rect.left;
      const screenY = event.clientY - rect.top;
      const flowPosition = screenToFlowPosition({ x: screenX, y: screenY });
      const now = Date.now();
      if (!window.lastAwarenessUpdate || now - window.lastAwarenessUpdate > 50) {
        window.lastAwarenessUpdate = now;
        providerRef.current.awareness.setLocalStateField('cursor', {
          x: flowPosition.x,
          y: flowPosition.y,
          timestamp: now,
        });
      }
    },
    [screenToFlowPosition]
  );

  const handleMouseLeave = useCallback(() => {
    if (!providerRef.current) return;
    providerRef.current.awareness.setLocalStateField('cursor', null);
  }, []);

  const onNodeDrag = useCallback(
    (event, node) => {
      if (!providerRef.current || !reactFlowWrapper.current) return;
      const rect = reactFlowWrapper.current.getBoundingClientRect();
      const screenX = event.clientX - rect.left;
      const screenY = event.clientY - rect.top;
      const flowPosition = screenToFlowPosition({ x: screenX, y: screenY });
      const now = Date.now();
      if (!window.lastDragAwarenessUpdate || now - window.lastDragAwarenessUpdate > 50) {
        window.lastDragAwarenessUpdate = now;
        providerRef.current.awareness.setLocalStateField('cursor', {
          x: flowPosition.x,
          y: flowPosition.y,
          timestamp: now,
          dragging: true,
          nodeId: node.id,
        });
      }
    },
    [screenToFlowPosition]
  );

  const addNode = useCallback((position) => {
    const yArr = yNodesRef.current;
    if (!yArr) return;
    const nodeId = `node-${Date.now()}-${Math.random().toString(36).slice(2, 11)}`;
    const newNode = {
      id: nodeId,
      position: position || { x: Math.random() * 400, y: Math.random() * 400 },
      data: { label: `Node ${nodeId.split('-')[1]}` },
    };
    const current = yArr.toArray();
    yArr.delete(0, yArr.length);
    yArr.insert(0, [...current, newNode]);
  }, []);

  const handlePaneDoubleClick = useCallback(
    (event) => {
      if (!reactFlowWrapper.current) return;
      const rect = reactFlowWrapper.current.getBoundingClientRect();
      const screenX = event.clientX - rect.left;
      const screenY = event.clientY - rect.top;
      const flowPosition = screenToFlowPosition({ x: screenX, y: screenY });
      addNode(flowPosition);
    },
    [screenToFlowPosition, addNode]
  );

  return (
    <div
      ref={reactFlowWrapper}
      style={{ width: '100vw', height: '100vh', position: 'relative' }}
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
    >
      <div
        style={{
          position: 'absolute',
          top: 10,
          right: 10,
          zIndex: 10,
          background: 'white',
          padding: '12px 16px',
          borderRadius: '8px',
          boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
          fontSize: '14px',
          minWidth: '180px',
        }}
      >
        <div style={{ fontWeight: 600, marginBottom: 8, color: '#333' }}>{username}</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4, color: connected ? 'green' : 'red' }}>
          <span>{connected ? 'Connected' : 'Disconnected'}</span>
        </div>
        <div style={{ fontSize: 12, color: '#666', marginBottom: 8 }}>{synced ? 'Synced' : 'Syncing...'}</div>
        <button
          onClick={() => addNode()}
          style={{
            width: '100%',
            padding: '8px 12px',
            background: '#007bff',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '13px',
            fontWeight: 500,
            transition: 'background 0.2s',
          }}
          onMouseOver={(e) => (e.currentTarget.style.background = '#0056b3')}
          onMouseOut={(e) => (e.currentTarget.style.background = '#007bff')}
        >
          Add Node
        </button>
        <button
          onClick={reloadDocumentState}
          style={{
            width: '100%',
            marginTop: 8,
            padding: '8px 12px',
            background: '#28a745',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '13px',
            fontWeight: 500,
            transition: 'background 0.2s',
          }}
          onMouseOver={(e) => (e.currentTarget.style.background = '#218838')}
          onMouseOut={(e) => (e.currentTarget.style.background = '#28a745')}
        >
          Reload Document
        </button>
        <div style={{ fontSize: 11, color: '#999', marginTop: 8, textAlign: 'center' }}>Double-click canvas to add node</div>
        {error && (
          <div
            style={{
              marginTop: 8,
              padding: '6px 8px',
              background: '#fee',
              border: '1px solid #fcc',
              borderRadius: '4px',
              fontSize: 11,
              color: '#c33',
            }}
          >
            {error}
          </div>
        )}
      </div>

      {Object.entries(cursors).map(([clientId, cursor]) => {
        if (!cursor || cursor.x === undefined || cursor.y === undefined) return null;
        const screenPosition = flowToScreenPosition({ x: cursor.x, y: cursor.y });
        return (
          <div
            key={clientId}
            style={{
              position: 'absolute',
              left: screenPosition.x,
              top: screenPosition.y,
              pointerEvents: 'none',
              zIndex: 1000,
              transition: 'left 0.1s ease-out, top 0.1s ease-out',
            }}
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" viewBox="0 0 24 24">
              <path
                fill={cursor.user?.color || '#000'}
                stroke="#000"
                strokeWidth="2"
                d="M5.5 3.21V20.8c0 .45.54.67.85.35l4.86-4.86a.5.5 0 0 1 .35-.15h6.87a.5.5 0 0 0 .35-.85L6.35 2.85a.5.5 0 0 0-.85.35Z"
              />
            </svg>
            <div
              style={{
                marginLeft: 20,
                marginTop: -20,
                padding: '4px 8px',
                background: cursor.user?.color || '#000',
                color: 'white',
                borderRadius: '4px',
                fontSize: 12,
                fontWeight: 500,
                whiteSpace: 'nowrap',
                boxShadow: '0 2px 4px rgba(0,0,0,0.2)',
              }}
            >
              {cursor.user?.name || 'Anonymous'}
            </div>
          </div>
        );
      })}

      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeDrag={onNodeDrag}
        onPaneDoubleClick={handlePaneDoubleClick}
        fitView
      />
    </div>
  );
}

export default function App() {
  return (
    <ReactFlowProvider>
      <FlowCanvas />
    </ReactFlowProvider>
  );
}
