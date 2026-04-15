import React, { useEffect, useMemo, useState } from "react";
import { io } from "socket.io-client";
import {
  LineChart, Line, XAxis, YAxis, Tooltip,
  BarChart, Bar
} from "recharts";

const SOCKET_URL = "http://localhost:5000";

export default function App() {
  const [events, setEvents] = useState([]);
  const [topics, setTopics] = useState({});
  const [status, setStatus] = useState("connecting");

  const [topicFilter, setTopicFilter] = useState([]);
  const [paused, setPaused] = useState(false);
  const [search, setSearch] = useState("");

  const [rateData, setRateData] = useState([]);
  const [lastCount, setLastCount] = useState(0);

  const [selectedEvent, setSelectedEvent] = useState(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);

  // ---------------------------- SOCKET ----------------------------
  useEffect(() => {
    const socket = io(SOCKET_URL, { transports: ["websocket"] });

    socket.on("connect", () => setStatus("connected"));
    socket.on("disconnect", () => setStatus("disconnected"));

    socket.on("event", (evt) => {
      const evts = Array.isArray(evt) ? evt : [evt];

      evts.forEach(e => {
        if (!e || !e.topic) return;

        const formatted = {
          ...e,
          timestamp: e.timestamp || Date.now(),
          payload: typeof e.payload === "object"
            ? JSON.stringify(e.payload)
            : e.payload,
        };

        if (!paused) {
          setEvents(prev => [formatted, ...prev].slice(0, 200));
        }

        setTopics(prev => ({
          ...prev,
          [e.topic]: (prev[e.topic] || 0) + 1,
        }));
      });
    });

    return () => socket.disconnect();
  }, [paused]);

  // ---------------------------- METRICS ----------------------------
  useEffect(() => {
    const timer = setInterval(() => {
      const diff = events.length - lastCount;
      setLastCount(events.length);

      setRateData(p => [...p.slice(-20), { t: Date.now(), v: Math.max(diff, 0) }]);
    }, 1000);

    return () => clearInterval(timer);
  }, [events, lastCount]);

  // ---------------------------- HELPERS ----------------------------
  const sortedTopics = useMemo(
    () => Object.entries(topics).sort((a, b) => b[1] - a[1]),
    [topics]
  );

  const searchLower = search.toLowerCase().trim();

  const topTopics = sortedTopics.slice(0, 3).map(([t]) => t);
  const otherTopics = sortedTopics.slice(3)
    .filter(([t]) => t.toLowerCase().includes(searchLower));

  const toggleTopic = (t) => {
    setTopicFilter(prev =>
      prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t]
    );
  };

  const filteredEvents = events.filter(e => {
    const topicMatch = topicFilter.length === 0 || topicFilter.includes(e.topic);
    const searchMatch = e.topic.toLowerCase().includes(searchLower);
    return topicMatch && searchMatch;
  });

  const clearAll = () => {
    setEvents([]);
    setTopics({});
    setTopicFilter([]);
    setLastCount(0);
    setRateData([]);
  };

  const download = (blob, filename) => {
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = filename; a.click();
    URL.revokeObjectURL(url);
  };

  const exportJSON = () =>
    download(new Blob([JSON.stringify(events, null, 2)], { type: "application/json" }), "events.json");

  const exportCSV = () => {
    const header = "id,topic,payload\n";
    const rows = events
      .map(e => `${e.id},${e.topic},${JSON.stringify(e.payload)}`)
      .join("\n");
    download(new Blob([header + rows], { type: "text/csv" }), "events.csv");
  };

  // ---------------------------- UI ----------------------------
  return (
    <div className="p-6 max-w-7xl mx-auto text-white space-y-6">

      {/* HEADER */}
      <header className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">EventBus Dashboard</h1>
        <span className={`px-3 py-1 rounded text-sm ${
          status === "connected" ? "bg-emerald-600" : "bg-red-600"
        }`}>
          {status}
        </span>
      </header>

      {/* CONTROLS */}
      <div className="flex gap-3 items-center flex-wrap">

        <button onClick={() => setPaused(p=>!p)}
          className={`px-3 py-1 rounded text-sm ${
            paused ? "bg-yellow-600" : "bg-emerald-600"
          }`}>
          {paused ? "Resume" : "Pause"}
        </button>

        <button onClick={clearAll} className="px-3 py-1 rounded text-sm bg-red-600">
          Clear
        </button>

        <button onClick={exportJSON} className="px-3 py-1 rounded bg-blue-600 text-sm">
          Export JSON
        </button>
        <button onClick={exportCSV} className="px-3 py-1 rounded bg-purple-600 text-sm">
          Export CSV
        </button>

        <input
          value={search}
          onChange={(e)=>setSearch(e.target.value)}
          placeholder="Search topics..."
          className="px-3 py-1 bg-gray-800 border border-gray-700 rounded text-sm w-56"
        />

        <button
          onClick={()=>setSidebarOpen(true)}
          className="px-3 py-1 rounded bg-indigo-600 text-sm"
        >
          Stats →
        </button>
      </div>

      {/* CHARTS */}
      <section className="grid grid-cols-2 gap-4">
        <ChartCard title="Messages / Sec">
          <LineChart width={300} height={150} data={rateData}>
            <Line dataKey="v" stroke="#4ade80" strokeWidth={2} dot={false}/>
            <XAxis hide/><YAxis hide/><Tooltip/>
          </LineChart>
        </ChartCard>

        <ChartCard title="Topic Frequency">
          <BarChart width={300} height={150} data={sortedTopics.map(([name,count])=>({name,count}))}>
            <Bar dataKey="count" fill="#6366f1"/>
            <XAxis dataKey="name"/><YAxis/><Tooltip/>
          </BarChart>
        </ChartCard>
      </section>

      {/* STATS */}
      <section className="grid grid-cols-3 gap-4">
        <StatCard title="Events">{filteredEvents.length}</StatCard>
        <StatCard title="Topics">{sortedTopics.length}</StatCard>
        <StatCard title="Top Topic">{sortedTopics[0]?.[0] || "-"}</StatCard>
      </section>

      {/* TOPICS + EVENTS */}
      <section className="grid grid-cols-3 gap-4">

        {/* TOPICS */}
        <div className="col-span-1 space-y-3">
          <h2 className="text-lg font-medium">Topics</h2>

          <div className="bg-gray-900 rounded-xl p-3 border border-gray-700 max-h-[380px] overflow-auto">
            {[...topTopics, ...otherTopics.map(x=>x[0])].map(name => (
              <div key={name}
                onClick={()=>toggleTopic(name)}
                className={`flex justify-between py-1 px-2 text-sm border-b border-gray-800 cursor-pointer ${
                  topicFilter.includes(name)
                    ? "bg-emerald-700/40 text-emerald-300"
                    : "text-gray-300"
                }`}>
                <span>{name}</span><span>{topics[name]}</span>
              </div>
            ))}
          </div>
        </div>

        {/* EVENTS LIST */}
        <div className="col-span-2">
          <h2 className="text-lg font-medium mb-2">Events</h2>

          <div className="bg-gray-900 rounded-xl p-3 border border-gray-700 max-h-[400px] overflow-auto">
            {filteredEvents.map((e,i)=>{
              let parsed;
              try { parsed = JSON.parse(e.payload); } catch { parsed = e.payload; }

              return (
                <div key={i}
                  onClick={()=>setSelectedEvent(e)}
                  className="relative p-4 mb-3 bg-gray-900/80 border border-gray-800 rounded-lg cursor-pointer hover:bg-gray-800/80 transition">

                  <div className="absolute left-3 top-2 text-[10px] text-gray-500">
                    {new Date(e.timestamp).toLocaleString()}
                  </div>

                  <div className="flex items-center gap-2 mt-4 mb-2">
                    <span className="px-2 py-0.5 text-xs rounded bg-indigo-600 font-medium">
                      {e.topic}
                    </span>

                    <span className="px-2 py-0.5 text-[10px] rounded bg-emerald-700 text-white">
                      PUB: {e.service || "unknown"}
                    </span>

                    <span className="px-2 py-0.5 text-[10px] rounded bg-gray-700 text-gray-300">
                      SUB: {e.topic.startsWith("inventory") ? "ecomm" : "ALL"}
                    </span>
                  </div>

                  <pre className="text-xs text-green-300 whitespace-pre-wrap leading-5 bg-black/30 p-2 rounded">
                    {typeof parsed === "object"
                      ? JSON.stringify(parsed, null, 2)
                      : parsed}
                  </pre>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      <JsonModal event={selectedEvent} onClose={()=>setSelectedEvent(null)}/>
      <Sidebar open={sidebarOpen} onClose={()=>setSidebarOpen(false)} topics={sortedTopics} events={events} topicFilter={topicFilter}/>
    </div>
  );
}

/* ---------------- COMPONENTS ---------------- */

function StatCard({title,children}) {
  return (
    <div className="bg-gray-900 rounded-xl p-4 border border-gray-700">
      <div className="text-gray-400 text-sm">{title}</div>
      <div className="text-2xl font-semibold">{children}</div>
    </div>
  );
}

function ChartCard({title,children}) {
  return (
    <div className="bg-gray-900 rounded-xl p-4 border border-gray-700 space-y-2">
      <div className="text-gray-400 text-sm">{title}</div>
      {children}
    </div>
  );
}

function JsonModal({event,onClose}) {
  if (!event) return null;
  let parsed;
  try { parsed = JSON.parse(event.payload); } catch { parsed = event.payload; }

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
      <div className="bg-gray-900 p-4 rounded-xl border border-gray-700 shadow-xl w-[450px] max-h-[80vh] overflow-auto">
        <div className="flex justify-between mb-2">
          <h3 className="font-semibold text-lg">Event Details</h3>
          <button onClick={onClose} className="text-red-400">✕</button>
        </div>
        <pre className="bg-black/40 p-3 rounded text-green-300 text-xs">
          <b className="text-emerald-400">Topic:</b> {event.topic}
          {"\n"}<b className="text-blue-400">Timestamp:</b> {new Date(event.timestamp).toLocaleString()}
          {"\n"}<b className="text-purple-400">Publisher:</b> {event.service || "unknown"}
          {"\n\n"}<b className="text-emerald-300">Payload:</b>{"\n"}
          {JSON.stringify(parsed, null, 2)}
        </pre>
      </div>
    </div>
  );
}

function Sidebar({open,onClose,topics,events,topicFilter}) {
  return (
    <div className={`fixed right-0 top-0 h-full w-80 bg-gray-900 border-l border-gray-700 p-4 z-40 transition-transform duration-300 ${
      open ? "translate-x-0" : "translate-x-full"
    }`}>
      <button onClick={onClose} className="mb-3 text-red-400">✕ Close</button>
      <h2 className="font-semibold text-lg mb-2">Live Stats</h2>
      <div className="text-sm space-y-2">
        <div>Events: <b>{events.length}</b></div>
        <div>Topics: <b>{topics.length}</b></div>
        <div>Filters: <b>{topicFilter.join(", ") || "-"}</b></div>
      </div>
    </div>
  );
}
