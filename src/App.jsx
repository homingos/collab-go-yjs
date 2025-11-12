import { ReactFlow, addEdge, applyEdgeChanges, applyNodeChanges, useReactFlow, ReactFlowProvider } from '@xyflow/react';
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

  // Use refs to store Yjs objects
  const ydocRef = useRef(null);
  const providerRef = useRef(null);
  const yNodesRef = useRef(null);
  const yEdgesRef = useRef(null);
  const reactFlowWrapper = useRef(null);

  useEffect(() => {
    if (ydocRef.current) {
      console.log('Yjs already initialized, skipping...');
      return;
    }

    console.log('Initializing Yjs document and provider');
    const ydoc = new Y.Doc();
    const yNodes = ydoc.getArray('nodes');
    const yEdges = ydoc.getArray('edges');

    ydocRef.current = ydoc;
    yNodesRef.current = yNodes;
    yEdgesRef.current = yEdges;

    // Fetch initial document state from HTTP endpoint before connecting WebSocket
    const roomName = 'flow-document';
    const docStateUrl = `http://localhost:8080/doc/${roomName}`;
    
    let cleanup = null;
    
    fetch(docStateUrl)
      .then(response => {
        if (response.status === 204 || response.status === 404) {
          console.log('No existing document state, starting fresh');
          return null;
        }
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.arrayBuffer();
      })
      .then(buffer => {
        if (buffer && buffer.byteLength > 0) {
          console.log(`Loading document state (${buffer.byteLength} bytes)`);
          const update = new Uint8Array(buffer);
          Y.applyUpdate(ydoc, update);
          console.log('Document state loaded successfully');
        }
        
        // Now connect WebSocket after loading initial state
        return setupWebSocketProvider(roomName, ydoc, yNodes, yEdges);
      })
      .catch(error => {
        console.error('Failed to fetch initial document state:', error);
        // Still try to connect WebSocket even if fetch fails
        return setupWebSocketProvider(roomName, ydoc, yNodes, yEdges);
      })
      .then(cleanupFn => {
        cleanup = cleanupFn;
      });

    function setupWebSocketProvider(roomName, ydoc, yNodes, yEdges) {
      let provider;
      try {
        provider = new WebsocketProvider(
          'ws://localhost:8080',
          roomName,
          ydoc
        );
        providerRef.current = provider;

        provider.on('connection-error', (err) => {
          console.error('WebSocket connection error:', err);
          setError(`Connection error: ${err.message || 'Unknown error'}`);
        });

        provider.on('connection-close', (event) => {
          console.log('WebSocket connection closed:', event);
          setError('Connection closed');
        });

        const awareness = provider.awareness;
        awareness.setLocalStateField('user', {
          name: username,
          color: `hsl(${Math.random() * 360}, 70%, 60%)`,
        });

        const awarenessChangeHandler = () => {
          const states = awareness.getStates();
          const newCursors = {};

          states.forEach((state, clientId) => {
            if (clientId !== awareness.clientID && state.cursor) {
              newCursors[clientId] = {
                ...state.cursor,
                user: state.user,
              };
            }
          });

          setCursors(newCursors);
        };

        awareness.on('change', awarenessChangeHandler);
        provider.awarenessChangeHandler = awarenessChangeHandler;

        let isInitialized = false;

        const handleStatus = ({ status }) => {
          console.log('WebSocket status:', status);
          setConnected(status === 'connected');
          if (status === 'connected') {
            setError(null);
          }
        };

        const handleSync = (isSynced) => {
          console.log('Sync status:', isSynced);
          setSynced(isSynced);

          if (isSynced && !isInitialized) {
            isInitialized = true;
            console.log('Synced. Document state:', {
              nodes: yNodes.length,
              edges: yEdges.length
            });
          }
        };

        const nodesObserver = () => {
          const newNodes = yNodes.toArray();
          console.log('Nodes updated:', newNodes.length);
          setNodes(newNodes);
        };

        const edgesObserver = () => {
          const newEdges = yEdges.toArray();
          console.log('Edges updated:', newEdges.length);
          setEdges(newEdges);
        };

        yNodes.observe(nodesObserver);
        yEdges.observe(edgesObserver);
        provider.on('status', handleStatus);
        provider.on('sync', handleSync);

        if (yNodes.length > 0) {
          setNodes(yNodes.toArray());
        }
        if (yEdges.length > 0) {
          setEdges(yEdges.toArray());
        }

        return () => {
          console.log('Cleaning up listeners...');
          yNodes.unobserve(nodesObserver);
          yEdges.unobserve(edgesObserver);
          provider.off('status', handleStatus);
          provider.off('sync', handleSync);
          if (provider.awarenessChangeHandler) {
            provider.awareness.off('change', provider.awarenessChangeHandler);
          }
        };
      } catch (error) {
        console.error('Failed to create WebsocketProvider:', error);
        return null;
      }
    }

    return () => {
      if (cleanup) {
        cleanup();
      }
    };
  }, [username]);

  const onNodesChange = useCallback(
    (changes) => {
      if (!yNodesRef.current) return;
      const updatedNodes = applyNodeChanges(changes, nodes);
      yNodesRef.current.delete(0, yNodesRef.current.length);
      yNodesRef.current.insert(0, updatedNodes);
    },
    [nodes]
  );

  const onEdgesChange = useCallback(
    (changes) => {
      if (!yEdgesRef.current) return;
      const updatedEdges = applyEdgeChanges(changes, edges);
      yEdgesRef.current.delete(0, yEdgesRef.current.length);
      yEdgesRef.current.insert(0, updatedEdges);
    },
    [edges]
  );

  const onConnect = useCallback(
    (params) => {
      if (!yEdgesRef.current) return;
      const updatedEdges = addEdge(params, edges);
      yEdgesRef.current.delete(0, yEdgesRef.current.length);
      yEdgesRef.current.insert(0, updatedEdges);
    },
    [edges]
  );

  const handleMouseMove = useCallback((event) => {
    // storing the last mouse event globally so we can access it during node drag
    window.lastMouseEvent = event;

    if (!providerRef.current || !reactFlowWrapper.current) return;

    const rect = reactFlowWrapper.current.getBoundingClientRect();
    const screenX = event.clientX - rect.left;
    const screenY = event.clientY - rect.top;

    // Convert screen coordinates to flow coordinates
    const flowPosition = screenToFlowPosition({ x: screenX, y: screenY });

    // Throttle awareness updates to avoid flooding the server
    const now = Date.now();
    if (!window.lastAwarenessUpdate || now - window.lastAwarenessUpdate > 50) {
      window.lastAwarenessUpdate = now;
      providerRef.current.awareness.setLocalStateField('cursor', {
        x: flowPosition.x,
        y: flowPosition.y,
        timestamp: now,
      });
    }
  }, [screenToFlowPosition]);

  const handleMouseLeave = useCallback(() => {
    if (!providerRef.current) return;
    providerRef.current.awareness.setLocalStateField('cursor', null);
  }, []);

  const onNodeDrag = useCallback((event, node) => {
    if (!providerRef.current || !reactFlowWrapper.current) return;

    const rect = reactFlowWrapper.current.getBoundingClientRect();
    const screenX = event.clientX - rect.left;
    const screenY = event.clientY - rect.top;

    // Convert screen coordinates to flow coordinates
    const flowPosition = screenToFlowPosition({ x: screenX, y: screenY });

    // Throttle drag awareness updates
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
  }, [screenToFlowPosition]);

  const addNode = useCallback((position) => {
    if (!yNodesRef.current) return;

    // Generate unique node ID
    const nodeId = `node-${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
    const newNode = {
      id: nodeId,
      position: position || { x: Math.random() * 400, y: Math.random() * 400 },
      data: { label: `Node ${nodeId.split('-')[1]}` },
    };

    // Add to Yjs array
    const currentNodes = yNodesRef.current.toArray();
    yNodesRef.current.delete(0, yNodesRef.current.length);
    yNodesRef.current.insert(0, [...currentNodes, newNode]);
  }, []);

  const handlePaneDoubleClick = useCallback((event) => {
    if (!reactFlowWrapper.current) return;

    const rect = reactFlowWrapper.current.getBoundingClientRect();
    const screenX = event.clientX - rect.left;
    const screenY = event.clientY - rect.top;

    // Convert screen coordinates to flow coordinates
    const flowPosition = screenToFlowPosition({ x: screenX, y: screenY });
    addNode(flowPosition);
  }, [screenToFlowPosition, addNode]);

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
        <div style={{
          fontWeight: '600',
          marginBottom: '8px',
          color: '#333',
          fontStyle: 'strong',
        }}>
          {username}
        </div>
        <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: '6px',
          marginBottom: '4px',
          color: connected ? 'green' : 'red',
        }}>
          <span>{connected ? 'Connected' : 'Disconnected'}</span>
        </div>
        <div style={{ fontSize: '12px', color: '#666', marginBottom: '8px' }}>
          {synced ? 'Synced' : 'Syncing...'}
        </div>
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
            fontWeight: '500',
            transition: 'background 0.2s',
          }}
          onMouseOver={(e) => e.target.style.background = '#0056b3'}
          onMouseOut={(e) => e.target.style.background = '#007bff'}
        >
          Add Node
        </button>
        <div style={{ fontSize: '11px', color: '#999', marginTop: '8px', textAlign: 'center' }}>
          Double-click canvas to add node
        </div>
        {error && (
          <div
            style={{
              marginTop: '8px',
              padding: '6px 8px',
              background: '#fee',
              border: '1px solid #fcc',
              borderRadius: '4px',
              fontSize: '11px',
              color: '#c33',
            }}
          >
            {error}
          </div>
        )}
      </div>

      {
        Object.entries(cursors).map(([clientId, cursor]) => {
          // Skip if cursor doesn't have coordinates
          if (!cursor || cursor.x === undefined || cursor.y === undefined) return null;
          
          // Convert flow coordinates back to screen coordinates for rendering
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
            {/* Cursor pointer */}
            {/* <svg
              width="24"
              height="24"
              viewBox="0 0 24 24"
              fill="none"
              style={{ transform: 'translate(-2px, -2px)' }}
            >
              <path
                d="M5.65376 12.3673L8.96375 15.6773L11.6297 19.3423C11.8617 19.6863 12.3577 19.6863 12.5897 19.3423L19.3457 9.3833C19.5777 9.0393 19.3307 8.5623 18.9167 8.5623H8.49375C8.19975 8.5623 7.90575 8.6763 7.69375 8.8883L5.65376 10.9283C5.22776 11.3543 5.22776 12.0553 5.65376 12.3673Z"
                fill={cursor.user?.color || '#000'}
                stroke="white"
                strokeWidth="1.5"
              />
            </svg> */}
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="48"
              height="48"
              viewBox="0 0 24 24"
            >
              <path
                fill={cursor.user?.color || '#000'}
                stroke="#000"
                strokeWidth="2" d="M5.5 3.21V20.8c0 .45.54.67.85.35l4.86-4.86a.5.5 0 0 1 .35-.15h6.87a.5.5 0 0 0 .35-.85L6.35 2.85a.5.5 0 0 0-.85.35Z">
              </path>
            </svg>
            {/* Username label */}
            <div
              style={{
                marginLeft: '20px',
                marginTop: '-20px',
                padding: '4px 8px',
                background: cursor.user?.color || '#000',
                color: 'white',
                borderRadius: '4px',
                fontSize: '12px',
                fontWeight: '500',
                whiteSpace: 'nowrap',
                boxShadow: '0 2px 4px rgba(0,0,0,0.2)',
              }}
            >
              {cursor.user?.name || 'Anonymous'}
            </div>
          </div>
          );
        })
      }

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
    </div >
  );
}

export default function App() {
  return (
    <ReactFlowProvider>
      <FlowCanvas />
    </ReactFlowProvider>
  );
}