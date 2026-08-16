import React, { useEffect, useState, useRef } from "react";
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer
} from "recharts";

const GATEWAY_WS = import.meta.env.VITE_GATEWAY_WS || "ws://localhost:8080";

export default function App() {
  const [events, setEvents] = useState([]);
  const [status, setStatus] = useState("connecting");
  const [gatewayStats, setGatewayStats] = useState({ uptime: 0, connections: 0, memoryUsage: 0 });
  
  const [rateData, setRateData] = useState([]);
  const [lastCount, setLastCount] = useState(0);

  // Lifecycle Test State
  const [testTraceId, setTestTraceId] = useState(null);
  const testTraceIdRef = useRef(null);
  const [testFlowState, setTestFlowState] = useState("IDLE"); // IDLE, SENT, DELIVERED, ACKED, DELIVERED_NACK, DLQ
  const [testRetries, setTestRetries] = useState(0);

  const wsRef = useRef(null);

  // ---------------------------- SOCKET ----------------------------
  useEffect(() => {
    let reconnectTimer;
    let isUnmounted = false;

    function connect() {
      if (isUnmounted) return;
      
      const ws = new WebSocket(GATEWAY_WS);
      wsRef.current = ws;

      ws.onopen = () => {
        if (isUnmounted) { ws.close(); return; }
        setStatus("connected");
        // DO NOT subscribe to '*' to prevent crashing the browser during 20k+ benchmarks!
        const topics = ['$SYS.stats', 'order.*', 'payment.*', 'notification.*', '$DLQ.*', 'test.*'];
        topics.forEach((topic) => {
          ws.send(JSON.stringify({ type: 'subscribe', topic }));
        });
      };

      ws.onclose = () => {
        if (isUnmounted) return;
        setStatus("disconnected");
        reconnectTimer = setTimeout(connect, 2000);
      };

      ws.onmessage = (msgEvent) => {
        if (isUnmounted) return;
        let e;
        try { e = JSON.parse(msgEvent.data); } catch { return; }
        
        if (e.type === 'status') {
          console.log('Broker status:', e.status);
          return;
        }

        if (e.type !== 'event') return;

        // Handle Gateway Stats
        if (e.topic === '$SYS.stats') {
          setGatewayStats(e.body);
          setRateData(p => [...p.slice(-40), { 
            t: new Date().toLocaleTimeString(), 
            msgPerSec: e.body.msgPerSec || 0 
          }]);
          return; // don't clutter event stream
        }

        // Handle Lifecycle Visualizer Tracking
        if (e.topic === 'test.lifecycle' && e.body?.traceId === testTraceIdRef.current) {
          setTestFlowState("DELIVERED");
          setTimeout(() => { if (!isUnmounted) setTestFlowState("ACKED"); }, 500); // visualize the ack delay
        }

        if (e.topic === 'test.dlq' && e.body?.traceId === testTraceIdRef.current) {
          setTestFlowState(prev => prev === "SENT" ? "DELIVERED_NACK" : prev);
          setTestRetries(prev => prev + 1);
        }

        if (e.topic === '$DLQ.test.dlq' && e.body?.traceId === testTraceIdRef.current) {
          setTestFlowState("DLQ");
        }

        // Auto-ack everything EXCEPT test.dlq messages
        if (e.topic !== 'test.dlq') {
          ws.send(JSON.stringify({ type: 'ack', msgId: e.msgId }));
        }

        const formatted = {
          id: e.msgId,
          topic: e.topic,
          timestamp: e.body?.timestamp || Date.now(),
          payload: typeof e.body === "object" ? JSON.stringify(e.body) : e.body,
        };

        setEvents(prev => [formatted, ...prev].slice(0, 100)); // Keep stream fast
      };
    }

    connect();

    return () => {
      isUnmounted = true;
      clearTimeout(reconnectTimer);
      if (wsRef.current) wsRef.current.close();
    };
  }, []); // Empty dependency array ensures WebSocket stays open permanently!

  // ---------------------------- ACTIONS ----------------------------
  const startTestAck = () => {
    const tid = Math.random().toString(36).substring(7);
    setTestTraceId(tid);
    testTraceIdRef.current = tid;
    setTestFlowState("SENT");
    setTestRetries(0);
    wsRef.current?.send(JSON.stringify({
      type: 'publish',
      topic: 'test.lifecycle',
      body: { traceId: tid, message: "Hello Reliable Queue!" }
    }));
  };

  const startTestDlq = () => {
    const tid = Math.random().toString(36).substring(7);
    setTestTraceId(tid);
    testTraceIdRef.current = tid;
    setTestFlowState("SENT");
    setTestRetries(0);
    wsRef.current?.send(JSON.stringify({
      type: 'publish',
      topic: 'test.dlq',
      body: { traceId: tid, message: "Poison Pill!" }
    }));
  };

  // ---------------------------- UI ----------------------------
  return (
    <div className="min-h-screen bg-[#0d1117] text-gray-200 p-6 font-sans antialiased">
      <div className="max-w-7xl mx-auto space-y-6">

        {/* HEADER */}
        <header className="flex items-center justify-between pb-4 border-b border-gray-800">
          <div>
            <h1 className="text-3xl font-bold tracking-tight text-white">Observability</h1>
            <p className="text-gray-500 text-sm mt-1">Real-time metrics & stream monitoring</p>
          </div>
          <div className="flex gap-4">
            <HealthBadge label="Gateway" status={status === "connected"} />
            <div className="flex flex-col text-right">
              <span className="text-xs text-gray-500 uppercase tracking-wider">Gateway Uptime</span>
              <span className="text-sm font-mono text-gray-300">{gatewayStats.uptime.toFixed(0)}s</span>
            </div>
            <div className="flex flex-col text-right pl-4 border-l border-gray-800">
              <span className="text-xs text-gray-500 uppercase tracking-wider">Active WS</span>
              <span className="text-sm font-mono text-gray-300">{gatewayStats.connections} clients</span>
            </div>
          </div>
        </header>

        {/* TOP METRICS & LIFECYCLE GRID */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          
          {/* THROUGHPUT CHART */}
          <div className="lg:col-span-2 bg-[#161b22] border border-gray-800 rounded-xl p-5 shadow-sm">
            <h2 className="text-sm font-semibold text-gray-400 mb-4 uppercase tracking-wider">Throughput (msg/s)</h2>
            <div className="h-[200px] w-full">
              <ResponsiveContainer>
                <AreaChart data={rateData}>
                  <defs>
                    <linearGradient id="colorRate" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#238636" stopOpacity={0.4}/>
                      <stop offset="95%" stopColor="#238636" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                  <Tooltip 
                    contentStyle={{ backgroundColor: '#0d1117', borderColor: '#30363d', color: '#c9d1d9' }}
                    itemStyle={{ color: '#2ea043' }}
                  />
                  <Area type="monotone" dataKey="msgPerSec" stroke="#2ea043" strokeWidth={2} fillOpacity={1} fill="url(#colorRate)" isAnimationActive={false} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* LIFECYCLE PLAYGROUND */}
          <div className="bg-[#161b22] border border-gray-800 rounded-xl p-5 shadow-sm flex flex-col">
            <h2 className="text-sm font-semibold text-gray-400 mb-4 uppercase tracking-wider">Lifecycle Playground</h2>
            
            <div className="flex gap-2 mb-6">
              <button onClick={startTestAck} className="flex-1 bg-[#238636] hover:bg-[#2ea043] text-white py-2 rounded text-sm font-medium transition-colors">
                Test ACK Flow
              </button>
              <button onClick={startTestDlq} className="flex-1 bg-[#da3633] hover:bg-[#f85149] text-white py-2 rounded text-sm font-medium transition-colors">
                Test DLQ Flow
              </button>
            </div>

            <div className="flex-1 flex flex-col justify-center space-y-4">
              <FlowStep label="Published to Gateway" active={testFlowState !== "IDLE"} />
              <FlowStep label="Broker Delivered" active={["DELIVERED", "ACKED", "DELIVERED_NACK", "DLQ"].includes(testFlowState)} 
                meta={testRetries > 0 ? `(Attempt ${testRetries}/3)` : null} />
              
              {testFlowState === "DELIVERED_NACK" || testFlowState === "DLQ" ? (
                <FlowStep label="Moved to Dead Letter Queue" active={testFlowState === "DLQ"} color="red" />
              ) : (
                <FlowStep label="Acknowledged (ACK)" active={testFlowState === "ACKED"} color="green" />
              )}
            </div>
          </div>
        </div>

        {/* LIVE STREAM TERMINAL */}
        <div className="bg-[#161b22] border border-gray-800 rounded-xl shadow-sm overflow-hidden flex flex-col h-[500px]">
          <div className="bg-[#0d1117] border-b border-gray-800 px-4 py-3 flex justify-between items-center">
            <div className="flex items-center gap-2">
              <div className="w-2.5 h-2.5 rounded-full bg-red-500"></div>
              <div className="w-2.5 h-2.5 rounded-full bg-yellow-500"></div>
              <div className="w-2.5 h-2.5 rounded-full bg-green-500"></div>
              <span className="ml-3 text-xs font-mono text-gray-500">broker_stream_tail</span>
            </div>
            <span className="text-xs font-mono text-gray-500">{events.length} events buffered</span>
          </div>
          
          <div className="flex-1 p-4 overflow-y-auto font-mono text-[13px] leading-relaxed space-y-1">
            {events.length === 0 && <div className="text-gray-600">Waiting for messages...</div>}
            {events.map((e, i) => (
              <div key={i} className="flex gap-4 group hover:bg-[#0d1117] px-2 py-0.5 rounded transition-colors">
                <span className="text-gray-600 shrink-0">{new Date(e.timestamp).toISOString().split('T')[1].replace('Z','')}</span>
                <span className={`shrink-0 w-32 truncate ${e.topic.startsWith('$DLQ') ? 'text-red-400' : 'text-blue-400'}`}>
                  [{e.topic}]
                </span>
                <span className="text-gray-400 truncate group-hover:text-gray-200 transition-colors">
                  {e.payload}
                </span>
              </div>
            ))}
          </div>
        </div>

      </div>
    </div>
  );
}

// --- Micro Components ---

function HealthBadge({ label, status }) {
  return (
    <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-[#161b22] border border-gray-800">
      <div className={`w-2 h-2 rounded-full ${status ? 'bg-green-500 shadow-[0_0_8px_rgba(34,197,94,0.6)]' : 'bg-red-500'}`}></div>
      <span className="text-xs font-medium text-gray-300">{label}</span>
    </div>
  );
}

function FlowStep({ label, active, color = "blue", meta }) {
  const dotColor = active 
    ? (color === "green" ? "bg-green-500" : color === "red" ? "bg-red-500" : "bg-blue-500")
    : "bg-gray-700";
    
  return (
    <div className="flex items-center gap-4">
      <div className={`w-3 h-3 rounded-full ${dotColor} transition-colors duration-500 relative`}>
        {active && <div className={`absolute inset-0 rounded-full animate-ping opacity-75 ${dotColor}`}></div>}
      </div>
      <div className={`text-sm ${active ? 'text-white font-medium' : 'text-gray-500'} transition-colors duration-500 flex gap-2 items-center`}>
        {label}
        {meta && <span className="text-xs text-yellow-500 font-mono">{meta}</span>}
      </div>
    </div>
  );
}
