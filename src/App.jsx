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
  const [roomName, setRoomName] = useState("");
  const [showRoomModal, setShowRoomModal] = useState(true);
  const [joinOrCreate, setJoinOrCreate] = useState(null); // 'join' or 'create'

  // Use refs to store Yjs objects
  const ydocRef = useRef(null);
  const providerRef = useRef(null);
  const yNodesRef = useRef(null);
  const yEdgesRef = useRef(null);
  const reactFlowWrapper = useRef(null);

  useEffect(() => {
    if (!roomName || !joinOrCreate) return;
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
    const docStateUrl = `http://localhost:8080/doc/${roomName}`;

    let cleanup = null;

    // If creating, POST to create room, then GET document state
    const fetchDocState = async () => {
      if (joinOrCreate === 'create') {
        const createRes = await fetch(docStateUrl, { method: 'POST' });
        if (!createRes.ok && createRes.status !== 201) {
          throw new Error(`Room creation failed: ${createRes.status}`);
        }
      }
      // Always fetch document state after create or join
      return fetch(docStateUrl);
    };

    Promise.resolve(fetchDocState())
      .then(response => {
        if (!response || response.status === 204 || response.status === 404) {
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
        } else {
          // If new room, initialize with default nodes/edges
          yNodes.insert(0, initialNodes);
          yEdges.insert(0, initialEdges);
          console.log('Initialized new room with default nodes/edges');
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
  }, [username, roomName, joinOrCreate]);

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
      {/* Room Modal */}
      {showRoomModal && (
        <div style={{
          position: 'fixed',
          top: 0,
          left: 0,
          width: '100vw',
          height: '100vh',
          background: 'rgba(0,0,0,0.25)',
          zIndex: 9999,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
        }}>
          <div style={{
            background: 'white',
            padding: '32px 40px',
            borderRadius: '12px',
            boxShadow: '0 4px 24px rgba(0,0,0,0.18)',
            minWidth: '320px',
            textAlign: 'center',
          }}>
            <h2 style={{ marginBottom: '18px', fontWeight: 700 }}>Enter Room Name</h2>
            <input
              type="text"
              value={roomName}
              onChange={e => setRoomName(e.target.value.replace(/\s+/g, '-'))}
              placeholder="Room name"
              style={{
                width: '80%',
                padding: '10px',
                fontSize: '16px',
                borderRadius: '6px',
                border: '1px solid #ccc',
                marginBottom: '18px',
              }}
              autoFocus
            />
            <div style={{ display: 'flex', gap: '16px', justifyContent: 'center', marginBottom: '8px' }}>
              <button
                style={{
                  padding: '10px 18px',
                  background: '#007bff',
                  color: 'white',
                  border: 'none',
                  borderRadius: '6px',
                  fontWeight: 600,
                  fontSize: '15px',
                  cursor: roomName ? 'pointer' : 'not-allowed',
                  opacity: roomName ? 1 : 0.6,
                }}
                disabled={!roomName}
                onClick={() => {
                  setJoinOrCreate('create');
                  setShowRoomModal(false);
                }}
              >Create Room</button>
              <button
                style={{
                  padding: '10px 18px',
                  background: '#28a745',
                  color: 'white',
                  border: 'none',
                  borderRadius: '6px',
                  fontWeight: 600,
                  fontSize: '15px',
                  cursor: roomName ? 'pointer' : 'not-allowed',
                  opacity: roomName ? 1 : 0.6,
                }}
                disabled={!roomName}
                onClick={() => {
                  setJoinOrCreate('join');
                  setShowRoomModal(false);
                }}
              >Join Room</button>
            </div>
            <div style={{ fontSize: '13px', color: '#888', marginTop: '8px' }}>
              Room name must not be empty
            </div>
          </div>
        </div>
      )}
      {/* ...existing code... */}
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
        {/* ...existing code... */}
      </div>
      {/* ...existing code... */}
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