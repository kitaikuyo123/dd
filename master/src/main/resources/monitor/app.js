const { createApp, reactive, ref, computed, onMounted, onBeforeUnmount, nextTick } = Vue;

/* ========== Utilities ========== */

function formatNumber(value) {
  if (value == null) return "-";
  return Number(value).toLocaleString();
}

function formatTime(timestamp) {
  if (!timestamp) return "-";
  return new Date(timestamp).toLocaleTimeString();
}

async function fetchJson(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error("HTTP " + r.status);
  return r.json();
}

async function postJson(url, body) {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": typeof body === "string" ? "text/plain; charset=UTF-8" : "application/json" },
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
  return r.json();
}

function resolveStatus(s) {
  return s.status || (s.online !== false ? 'online' : 'offline');
}

/* ========== Animated Counter Directive ========== */

const vCounter = {
  mounted(el, binding) {
    el._target = Number(binding.value) || 0;
    el.textContent = formatNumber(el._target);
  },
  updated(el, binding) {
    const from = el._target || 0;
    const to = Number(binding.value) || 0;
    if (from === to) return;
    el._target = to;
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
      el.textContent = formatNumber(to);
      return;
    }
    const start = performance.now();
    const dur = 500;
    (function tick(now) {
      const p = Math.min((now - start) / dur, 1);
      const eased = 1 - Math.pow(1 - p, 3);
      el.textContent = formatNumber(Math.round(from + (to - from) * eased));
      if (p < 1) requestAnimationFrame(tick);
    })(start);
  },
};

/* ========== SSE Connection ========== */

function createSseConnection(state) {
  let es = null;
  let delay = 1000;
  let timer = null;

  function connect() {
    es = new EventSource("/monitor/api/stream");

    es.addEventListener("snapshot", (e) => {
      delay = 1000;
      try {
        const d = JSON.parse(e.data);
        if (d.overview) Object.assign(state.overview, d.overview);
        if (d.servers) {
          state.servers.splice(0, state.servers.length, ...d.servers);
          console.log("[SSE] servers updated:", d.servers.map(s => s.serverId));
        }
        state.lastUpdated = formatTime(d.timestamp);
        state.connected = true;
      } catch (_) {}
    });

    es.addEventListener("event", (e) => {
      try {
        const ev = JSON.parse(e.data);
        state.events.unshift(ev);
        if (state.events.length > 100) state.events.length = 100;

        // Optimistic update: immediately show offline state in topology
        if (ev.type === "SERVER_OFFLINE" && ev.sourceServer) {
          const server = state.servers.find(s => s.serverId === ev.sourceServer);
          if (server) {
            server.status = "offline";
            server.online = false;
          }
        }
      } catch (_) {}
    });

    es.onerror = () => {
      state.connected = false;
      if (es) { es.close(); es = null; }
      timer = setTimeout(connect, delay);
      delay = Math.min(delay * 2, 30000);
    };
  }

  connect();
  return {
    close() {
      if (es) es.close();
      if (timer) clearTimeout(timer);
    },
  };
}

/* ========== Components ========== */

const OverviewCards = {
  props: ["overview"],
  template: `
    <section class="cards" aria-label="Cluster overview">
      <article class="card" v-for="item in items" :key="item.label">
        <div class="card-label">{{ item.label }}</div>
        <div class="card-value" v-counter="item.value">{{ item.value }}</div>
      </article>
    </section>
  `,
  computed: {
    items() {
      const o = this.overview || {};
      return [
        { label: "Active Nodes", value: Number(o.activeServers || 0) },
        { label: "Warning", value: Number(o.warningServers || 0) },
        { label: "Offline", value: Number(o.offlineServers || 0) },
        { label: "Regions", value: Number(o.regionCount || 0) },
        { label: "Tables", value: Number(o.tableCount || 0) },
        { label: "Replica Alerts", value: Number(o.replicaAlerts || 0) },
        { label: "QPS", value: Number(o.currentQps || 0).toFixed(2) },
        { label: "24h Errors", value: Number(o.errors24h || 0) },
      ];
    },
  },
};

const ClusterTopology = {
  props: ["servers"],
  template: `
    <section class="panel" aria-label="Cluster topology">
      <div class="panel-header"><h2>Cluster Topology</h2></div>
      <svg :viewBox="'0 0 ' + width + ' ' + height" class="topology-svg" role="img" aria-label="Cluster topology diagram">
        <g class="master-node" :transform="'translate(' + (width/2) + ',50)'">
          <rect x="-55" y="-22" width="110" height="44" rx="10" class="node-rect master" />
          <text text-anchor="middle" y="5" class="node-text">Master</text>
        </g>
        <line v-for="(s, i) in positions" :key="'l'+i"
              :x1="width/2" y1="72" :x2="s.x" :y2="s.y - 22" :class="['topology-link', s.status || 'online']" />
        <g v-for="(s, i) in positions" :key="'s'+i" :transform="'translate(' + s.x + ',' + s.y + ')'">
          <rect x="-65" y="-22" width="130" height="44" rx="10" :class="['node-rect', s.status || 'online']" />
          <circle cx="-45" cy="0" r="5" :class="['status-dot', s.status || 'online']" />
          <text x="8" text-anchor="middle" y="5" :class="['node-text', s.status === 'offline' ? 'offline-text' : '']">{{ s.name }}</text>
        </g>
      </svg>
      <div class="topology-stats">
        <span>Total: <b>{{ (servers || []).length }}</b></span>
        <span>Online: <b>{{ onlineCount }}</b></span>
        <span>Warning: <b>{{ warningCount }}</b></span>
        <span>Offline: <b>{{ offlineCount }}</b></span>
      </div>
    </section>
  `,
  computed: {
    width() { return 600; },
    height() {
      const n = (this.servers || []).length;
      return Math.max(160, 80 + Math.ceil(n / 3) * 90 + 40);
    },
    positions() {
      const list = (this.servers || []).map(s => ({
        name: (s.serverId || "").split(":").pop(),
        status: s.status || (s.online !== false ? 'online' : 'offline'),
        online: s.online !== false,
      }));
      const cols = Math.min(3, list.length || 1);
      return list.map((s, i) => ({
        ...s,
        x: (this.width / (cols + 1)) * ((i % cols) + 1),
        y: 150 + Math.floor(i / cols) * 80,
      }));
    },
    onlineCount() { return (this.servers || []).filter(s => resolveStatus(s) === 'online').length; },
    warningCount() { return (this.servers || []).filter(s => resolveStatus(s) === 'warning').length; },
    offlineCount() { return (this.servers || []).filter(s => resolveStatus(s) === 'offline').length; },
  },
};

const SqlPanel = {
  props: ["summary", "sqlWindow"],
  emits: ["change-window"],
  template: `
    <section class="panel">
      <div class="panel-header">
        <h2>SQL Metrics</h2>
        <div class="pill-row">
          <button v-for="w in ['5m','1h','24h']" :key="w" class="pill" :class="{active: sqlWindow===w}"
                  @click="$emit('change-window',w)" :aria-label="'Window '+w">{{ w }}</button>
        </div>
      </div>
      <div class="metrics-grid">
        <div class="metric-chip" v-for="item in items" :key="item.label">
          <span class="muted">{{ item.label }}</span>
          <b>{{ item.value }}</b>
        </div>
      </div>
      <div class="chart">
        <div class="bar-wrap" v-for="pt in chartPoints" :key="pt.key">
          <div class="bar" :style="{height: pt.height + 'px'}"></div>
          <div class="bar-label">{{ pt.label }}</div>
        </div>
      </div>
    </section>
  `,
  computed: {
    items() {
      const s = this.summary || {};
      return [
        { label: "Requests", value: formatNumber(s.requestCount) },
        { label: "Success", value: formatNumber(s.successCount) },
        { label: "Errors", value: formatNumber(s.errorCount) },
        { label: "QPS", value: Number(s.qps || 0).toFixed(2) },
        { label: "Avg Latency", value: Number(s.avgLatencyMs || 0).toFixed(1) + " ms" },
        { label: "P95", value: Number(s.p95LatencyMs || 0).toFixed(1) + " ms" },
        { label: "Reads", value: formatNumber(s.readCount) },
        { label: "Writes", value: formatNumber(s.writeCount) },
      ];
    },
    chartPoints() {
      const pts = (this.summary || {}).points || [];
      const last = pts.slice(-24);
      const max = Math.max(1, ...last.map(p => Number(p.requestCount || 0)));
      return last.map(p => ({
        key: p.minuteBucket,
        height: Math.max(4, (Number(p.requestCount || 0) / max) * 140),
        label: new Date(p.minuteBucket).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
      }));
    },
  },
};

const DataTable = {
  props: ["title", "rows", "columns"],
  template: `
    <section class="panel">
      <div class="panel-header"><h2>{{ title }}</h2></div>
      <div v-if="!rows || !rows.length" class="empty-state">No data</div>
      <table v-else>
        <thead><tr><th v-for="c in columns" :key="c.key">{{ c.label }}</th></tr></thead>
        <tbody>
          <tr v-for="(row, i) in rows" :key="i">
            <td v-for="c in columns" :key="c.key" v-html="c.render(row)"></td>
          </tr>
        </tbody>
      </table>
    </section>
  `,
};

const EventTimeline = {
  props: ["events"],
  template: `
    <section class="panel" aria-label="Event timeline">
      <div class="panel-header"><h2>Event Timeline</h2></div>
      <div v-if="!events || !events.length" class="empty-state">No events</div>
      <div class="timeline" v-else>
        <div class="event" :class="severityClass(e.severity)" v-for="(e, i) in events" :key="i">
          <div class="event-top">
            <span class="event-type">{{ e.type }}</span>
            <span class="muted">{{ formatTime(e.timestamp) }}</span>
          </div>
          <div class="muted" style="margin-top:2px">{{ e.message || "-" }}</div>
          <div class="muted" v-if="e.details" style="margin-top:2px">{{ e.details }}</div>
        </div>
      </div>
    </section>
  `,
  methods: {
    formatTime,
    severityClass(s) { return String(s || "").toLowerCase(); },
  },
};

const SqlConsole = {
  template: `
    <section class="panel sql-console" aria-label="SQL Console">
      <div class="panel-header">
        <h2>SQL Console</h2>
        <button class="pill" :disabled="running" @click="run" aria-label="Run SQL">{{ running ? "Running..." : "Run" }}</button>
      </div>
      <textarea v-model="sql" class="sql-editor" spellcheck="false"
                placeholder="SELECT * FROM demo_users;&#10;SHOW TABLES;" aria-label="SQL input"></textarea>
      <div class="muted" v-if="message">{{ message }}</div>
      <div class="sql-error" v-if="error">{{ error }}</div>
      <div v-if="results.length">
        <div v-for="(r, idx) in results" :key="idx" style="margin-bottom:10px">
          <div class="sql-meta" v-if="r.success===false" style="color:var(--danger)">
            <code>{{ r.sql }}</code> — {{ r.error }}
          </div>
          <template v-else>
            <div class="sql-meta" v-if="r.columns">{{ (r.rows||[]).length }} rows, {{ (r.columns||[]).length }} cols</div>
            <div class="sql-meta" v-else>Done, {{ r.updateCount ?? 0 }} rows affected</div>
            <div class="sql-result-wrap" v-if="r.columns">
              <table class="sql-result-table">
                <thead><tr><th v-for="c in r.columns" :key="c">{{ c }}</th></tr></thead>
                <tbody>
                  <tr v-for="(row, ri) in r.rows" :key="ri">
                    <td v-for="(v, ci) in row" :key="ci">{{ v == null ? "NULL" : v }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </template>
        </div>
      </div>
    </section>
  `,
  setup() {
    const sql = ref("SHOW TABLES;");
    const running = ref(false);
    const results = ref([]);
    const error = ref("");
    const message = ref("");
    async function run() {
      if (!sql.value.trim() || running.value) return;
      running.value = true;
      error.value = "";
      results.value = [];
      message.value = "";
      try {
        const r = await fetch("/monitor/api/sql/execute", {
          method: "POST",
          headers: { "Content-Type": "text/plain; charset=UTF-8" },
          body: sql.value,
        });
        const data = await r.json();
        if (!r.ok) throw new Error(data.error || "HTTP " + r.status);
        results.value = data;
        message.value = "";
      } catch (e) {
        error.value = e.message;
      } finally {
        running.value = false;
      }
    }
    return { sql, running, results, error, message, run };
  },
};

/* ========== Demo Mode ========== */

const DEMO_STEPS = [
  { id: "setup", label: "Create Tables + Insert Data", tag: "DDL / DML" },
  { id: "query", label: "SELECT + Aggregation + GROUP BY", tag: "Query" },
  { id: "join", label: "JOIN (Broadcast Hash Join)", tag: "Complex Query" },
  { id: "failover", label: "Kill Random RS -> Auto Failover", tag: "Fault Tolerance" },
  { id: "recover", label: "Restart RS -> Data Catch-up", tag: "Recovery" },
  { id: "scaleout", label: "Add New RS-4 Node", tag: "Cluster Scaling" },
  { id: "balance", label: "Trigger Load Balance", tag: "Load Balance" },
];

const DEMO_SQL = {
  query: [
    "SELECT * FROM demo_users;",
    "SELECT id, name, age FROM demo_users WHERE age > 25;",
    "SELECT COUNT(*), SUM(amount), AVG(amount) FROM demo_orders;",
    "SELECT user_id, SUM(amount) AS total FROM demo_orders GROUP BY user_id HAVING total > 100;",
  ].join("\n"),
  join: [
    "SELECT u.name, o.amount FROM demo_users u JOIN demo_orders o ON u.id = o.user_id;",
    "SELECT u.name, SUM(o.amount) AS total FROM demo_users u JOIN demo_orders o ON u.id = o.user_id GROUP BY u.name ORDER BY total DESC;",
  ].join("\n"),
  verify: "SELECT * FROM demo_users;",
};

const DemoMode = {
  emits: ["fill-sql"],
  template: `
    <aside class="demo-sidebar" role="complementary" aria-label="Demo mode">
      <div class="demo-title">Demo Mode</div>
      <div v-for="(step, idx) in steps" :key="step.id"
           class="demo-step" :class="stepClass(idx)"
           @click="runStep(idx)" role="button" :aria-label="step.label" tabindex="0"
           @keydown.enter="runStep(idx)" @keydown.space.prevent="runStep(idx)">
        <span class="demo-step-num">{{ idx + 1 }}</span>
        <span class="demo-step-label">{{ step.label }}</span>
        <span class="demo-step-tag">{{ step.tag }}</span>
      </div>
      <button class="demo-reset" @click="reset" aria-label="Reset demo">Reset</button>
    </aside>
  `,
  setup(props, { emit }) {
    const current = ref(-1);
    const killedInstance = ref(parseInt(localStorage.getItem("killedInstance") || "0"));
    const statuses = reactive(DEMO_STEPS.map(() => "pending"));

    function stepClass(idx) {
      if (statuses[idx] === "running") return "running";
      if (statuses[idx] === "done") return "done";
      if (statuses[idx] === "error") return "error";
      return idx === current.value ? "active" : "";
    }

    async function runStep(idx) {
      if (statuses[idx] === "running") return;
      current.value = idx;
      statuses[idx] = "running";
      try {
        const step = DEMO_STEPS[idx];
        if (step.id === "setup") {
          await postJson("/monitor/api/demo/setup", "");
        } else if (step.id === "query") {
          emit("fill-sql", DEMO_SQL.query);
        } else if (step.id === "join") {
          emit("fill-sql", DEMO_SQL.join);
        } else if (step.id === "failover") {
          // 随机选一个 RS 实例杀掉
          const instance = Math.floor(Math.random() * 3) + 1;
          killedInstance.value = instance;
          localStorage.setItem("killedInstance", String(instance));
          await postJson("/monitor/api/demo/kill-server", { port: 16019 + instance });
          emit("fill-sql", DEMO_SQL.verify);
        } else if (step.id === "recover") {
          // 重启被杀的 RS
          await postJson("/monitor/api/demo/restart-server", { instance: killedInstance.value || 1 });
        } else if (step.id === "scaleout") {
          // 启动新的 RS-4 节点
          await postJson("/monitor/api/demo/restart-server", { instance: 4 });
        } else if (step.id === "balance") {
          await postJson("/monitor/api/demo/trigger-balance", "");
          emit("fill-sql", DEMO_SQL.verify);
        }
        statuses[idx] = "done";
        current.value = idx + 1 < DEMO_STEPS.length ? idx + 1 : -1;
      } catch (e) {
        statuses[idx] = "error";
      }
    }

    function reset() {
      current.value = -1;
      killedInstance.value = 0;
      localStorage.removeItem("killedInstance");
      statuses.fill("pending");
    }

    return { steps: DEMO_STEPS, current, statuses, stepClass, runStep, reset };
  },
};

/* ========== Main App ========== */

createApp({
  components: { OverviewCards, ClusterTopology, SqlPanel, DataTable, EventTimeline, SqlConsole, DemoMode },
  directives: { counter: vCounter },
  setup() {
    const state = reactive({
      sqlWindow: "5m",
      overview: {},
      sqlSummary: {},
      servers: [],
      tables: [],
      regions: [],
      regionReplicas: [],
      events: [],
      lastUpdated: "Connecting...",
      connected: false,
    });
    const demoOpen = ref(false);
    let sseConn = null;
    let timer = null;
    let sqlRef = null;

    const serverColumns = [
      { key: "serverId", label: "Server", render: row => row.serverId || "-" },
      { key: "lastHeartbeat", label: "Heartbeat", render: row => formatTime(row.lastHeartbeat) },
      { key: "cpuUsage", label: "CPU", render: row => Number(row.cpuUsage || 0).toFixed(2) },
      { key: "memoryUsage", label: "Mem", render: row => Number(row.memoryUsage || 0).toFixed(1) + "%" },
      { key: "regionCount", label: "Regions", render: row => formatNumber(row.regionCount) },
      { key: "loadScore", label: "Load", render: row => Number(row.loadScore || 0).toFixed(1) },
      { key: "readRequests", label: "Reads", render: row => formatNumber(row.readRequests) },
      { key: "writeRequests", label: "Writes", render: row => formatNumber(row.writeRequests) },
    ];

    const tableColumns = [
      { key: "tableName", label: "Table", render: row => row.tableName || "-" },
      { key: "regionCount", label: "Regions", render: row => formatNumber(row.regionCount) },
      { key: "totalReadRequests", label: "Reads", render: row => formatNumber(row.totalReadRequests) },
      { key: "totalWriteRequests", label: "Writes", render: row => formatNumber(row.totalWriteRequests) },
      { key: "totalErrors", label: "Errors", render: row => formatNumber(row.totalErrors) },
      { key: "hotspotScore", label: "Hot", render: row => Number(row.hotspotScore || 0).toFixed(1) },
    ];

    const regionColumns = [
      { key: "regionId", label: "Region", render: row => (row.regionId || "-").substring(0, 12) },
      { key: "tableName", label: "Table", render: row => row.tableName || "-" },
      { key: "serverId", label: "Server", render: row => (row.serverId || "-").split(":").pop() },
      { key: "role", label: "Role", render: row => row.role === "Primary"
        ? '<span class="tag tag-primary">Primary</span>'
        : '<span class="tag tag-replica">Replica</span>' },
      { key: "replicationLag", label: "Lag", render: row => formatNumber(row.replicationLag) },
      { key: "readRequests", label: "Reads", render: row => formatNumber(row.readRequests) },
      { key: "writeRequests", label: "Writes", render: row => formatNumber(row.writeRequests) },
    ];

    const regionReplicaColumns = [
      { key: "regionId", label: "Region", render: row => (row.regionId || "-").substring(0, 12) },
      { key: "serverId", label: "Server", render: row => (row.serverId || "-").split(":").pop() },
      { key: "role", label: "Role", render: row => row.role === "Primary"
        ? '<span class="tag tag-primary">Primary</span>'
        : '<span class="tag tag-replica">Replica</span>' },
      { key: "storeFileSize", label: "Size", render: row => (Number(row.storeFileSize || 0) / 1024 / 1024).toFixed(2) + " MB" },
      { key: "regionLoadScore", label: "Load", render: row => Number(row.regionLoadScore || 0).toFixed(2) },
    ];

    async function refresh() {
      try {
        const [sqlSummary, tables, regions, regionReplicas] = await Promise.all([
          fetchJson("/monitor/api/sql/summary?window=" + state.sqlWindow),
          fetchJson("/monitor/api/tables"),
          fetchJson("/monitor/api/regions"),
          fetchJson("/monitor/api/region-replicas"),
        ]);
        state.sqlSummary = sqlSummary;
        state.tables = tables;
        state.regions = regions;
        state.regionReplicas = regionReplicas;
      } catch (_) {}
    }

    function changeSqlWindow(w) {
      state.sqlWindow = w;
      fetchJson("/monitor/api/sql/summary?window=" + w).then(d => { state.sqlSummary = d; }).catch(() => {});
    }

    function toggleDemo() { demoOpen.value = !demoOpen.value; }

    function fillSql(sql) {
      const textarea = document.querySelector(".sql-editor");
      if (textarea) {
        textarea.value = sql;
        textarea.dispatchEvent(new Event("input"));
      }
    }

    onMounted(async () => {
      try {
        const [overview, sqlSummary, servers, tables, regions, regionReplicas, events] = await Promise.all([
          fetchJson("/monitor/api/overview"),
          fetchJson("/monitor/api/sql/summary?window=5m"),
          fetchJson("/monitor/api/servers"),
          fetchJson("/monitor/api/tables"),
          fetchJson("/monitor/api/regions"),
          fetchJson("/monitor/api/region-replicas"),
          fetchJson("/monitor/api/events?limit=50"),
        ]);
        Object.assign(state.overview, overview);
        state.sqlSummary = sqlSummary;
        state.servers = servers;
        state.tables = tables;
        state.regions = regions;
        state.regionReplicas = regionReplicas;
        state.events = events;
      } catch (_) {}
      sseConn = createSseConnection(state);
      timer = setInterval(refresh, 10000);
    });

    onBeforeUnmount(() => {
      if (sseConn) sseConn.close();
      if (timer) clearInterval(timer);
    });

    const activeServers = computed(() => state.servers.filter(s => resolveStatus(s) !== 'offline'));

    return {
      state, demoOpen, activeServers, serverColumns, tableColumns, regionColumns, regionReplicaColumns,
      changeSqlWindow, toggleDemo, fillSql, formatTime,
    };
  },
  template: `
    <a href="#main" class="skip-link">Skip to main content</a>
    <nav class="dashboard-nav">
      <div class="nav-brand"><span class="brand-accent">MiniSQL</span> Monitor</div>
      <div class="nav-right">
        <span class="connection-dot" :class="state.connected ? 'online' : 'offline'"
              :aria-label="state.connected ? 'Connected' : 'Disconnected'"></span>
        <span class="nav-time">{{ state.lastUpdated }}</span>
        <button class="btn-demo" :class="{active: demoOpen}" @click="toggleDemo"
                aria-label="Toggle demo mode">Demo</button>
      </div>
    </nav>
    <main id="main" class="dashboard" :class="{'demo-active': demoOpen}">
      <demo-mode v-if="demoOpen" @fill-sql="fillSql" />
      <div class="dashboard-content">
        <overview-cards :overview="state.overview" />
        <cluster-topology :servers="state.servers" />
        <sql-panel :summary="state.sqlSummary" :sql-window="state.sqlWindow" @change-window="changeSqlWindow" />
        <sql-console ref="sqlConsole" />
        <section class="grid two">
          <data-table title="Nodes" :rows="activeServers" :columns="serverColumns" />
          <data-table title="Region Distribution" :rows="state.regionReplicas" :columns="regionReplicaColumns" />
        </section>
        <section class="grid two">
          <data-table title="Tables" :rows="state.tables.slice(0, 10)" :columns="tableColumns" />
          <data-table title="Regions" :rows="state.regions.slice(0, 10)" :columns="regionColumns" />
        </section>
        <event-timeline :events="state.events" />
      </div>
    </main>
  `,
}).mount("#app");
