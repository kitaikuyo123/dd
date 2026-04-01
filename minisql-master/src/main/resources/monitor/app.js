const { createApp, reactive, ref, computed, onMounted, onBeforeUnmount } = Vue;

function formatNumber(value) {
  if (value == null) return "-";
  return Number(value).toLocaleString();
}

function formatTime(timestamp) {
  if (!timestamp) return "-";
  return new Date(timestamp).toLocaleString();
}

async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

async function executeSql(sql) {
  const response = await fetch("/monitor/api/sql/execute", {
    method: "POST",
    headers: { "Content-Type": "text/plain; charset=UTF-8" },
    body: sql,
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || `HTTP ${response.status}`);
  }
  return payload;
}

const OverviewCards = {
  props: ["overview"],
  template: `
    <section class="cards">
      <article class="card" v-for="item in items" :key="item.label">
        <div class="card-label">{{ item.label }}</div>
        <div class="card-value">{{ item.value }}</div>
      </article>
    </section>
  `,
  computed: {
    items() {
      return [
        { label: "活跃节点", value: formatNumber(this.overview.activeServers) },
        { label: "离线节点", value: formatNumber(this.overview.offlineServers) },
        { label: "Region 总数", value: formatNumber(this.overview.regionCount) },
        { label: "Table 总数", value: formatNumber(this.overview.tableCount) },
        { label: "副本告警", value: formatNumber(this.overview.replicaAlerts) },
        { label: "当前 QPS", value: Number(this.overview.currentQps || 0).toFixed(2) },
        { label: "24h 错误数", value: formatNumber(this.overview.errors24h) },
      ];
    },
  },
};

const SqlPanel = {
  props: ["summary", "sqlWindow"],
  emits: ["change-window"],
  template: `
    <section class="panel">
      <div class="panel-header">
        <h2>SQL 指标趋势</h2>
        <div class="pill-row">
          <button
            v-for="window in ['5m', '1h', '24h']"
            :key="window"
            class="pill"
            :class="{ active: sqlWindow === window }"
            @click="$emit('change-window', window)">
            {{ window }}
          </button>
        </div>
      </div>
      <div class="metrics-grid">
        <div class="metric-chip" v-for="item in items" :key="item.label">
          <span class="muted">{{ item.label }}</span>
          <b>{{ item.value }}</b>
        </div>
      </div>
      <div class="chart">
        <div class="bar-wrap" v-for="point in chartPoints" :key="point.minuteBucket">
          <div class="bar" :style="{ height: point.height + 'px' }"></div>
          <div class="bar-label">{{ point.label }}</div>
        </div>
      </div>
    </section>
  `,
  computed: {
    items() {
      return [
        { label: "请求总数", value: formatNumber(this.summary.requestCount) },
        { label: "成功数", value: formatNumber(this.summary.successCount) },
        { label: "错误数", value: formatNumber(this.summary.errorCount) },
        { label: "QPS", value: Number(this.summary.qps || 0).toFixed(2) },
        { label: "平均延迟", value: `${Number(this.summary.avgLatencyMs || 0).toFixed(1)} ms` },
        { label: "P95 延迟", value: `${Number(this.summary.p95LatencyMs || 0).toFixed(1)} ms` },
        { label: "读请求", value: formatNumber(this.summary.readCount) },
        { label: "写请求", value: formatNumber(this.summary.writeCount) },
      ];
    },
    chartPoints() {
      const points = (this.summary.points || []).slice(-24);
      const max = Math.max(1, ...points.map(point => Number(point.requestCount || 0)));
      return points.map(point => ({
        minuteBucket: point.minuteBucket,
        height: Math.max(8, (Number(point.requestCount || 0) / max) * 160),
        label: new Date(point.minuteBucket).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
      }));
    },
  },
};

const DataTable = {
  props: ["title", "rows", "columns"],
  template: `
    <section class="panel">
      <div class="panel-header"><h2>{{ title }}</h2></div>
      <table>
        <thead>
          <tr><th v-for="column in columns" :key="column.key">{{ column.label }}</th></tr>
        </thead>
        <tbody>
          <tr v-for="(row, index) in rows" :key="row.id || row.regionId || row.tableName || row.serverId || index">
            <td v-for="column in columns" :key="column.key">
              <span v-html="column.render(row)"></span>
            </td>
          </tr>
        </tbody>
      </table>
    </section>
  `,
};

const EventTimeline = {
  props: ["events"],
  template: `
    <section class="panel">
      <div class="panel-header"><h2>事件时间线</h2></div>
      <div class="timeline">
        <div class="event" :class="severityClass(event.severity)" v-for="event in events" :key="event.timestamp + '-' + event.type">
          <div class="event-top">
            <span class="event-type">{{ event.type }}</span>
            <span class="muted">{{ formatTime(event.timestamp) }}</span>
          </div>
          <div>{{ event.message || '-' }}</div>
          <div class="muted">{{ [event.regionId, event.sourceServer, event.targetServer].filter(Boolean).join(' ') }}</div>
          <div class="muted" v-if="event.details">{{ event.details }}</div>
        </div>
      </div>
    </section>
  `,
  methods: {
    formatTime,
    severityClass(severity) {
      return String(severity || "").toLowerCase();
    },
  },
};

const SqlConsole = {
  template: `
    <section class="panel sql-console">
      <div class="panel-header">
        <h2>SQL Console</h2>
        <button class="pill active" :disabled="running" @click="runSql">{{ running ? '执行中...' : '运行 SQL' }}</button>
      </div>
      <textarea
        v-model="sql"
        class="sql-editor"
        spellcheck="false"
        placeholder="SELECT * FROM products;\\nSHOW TABLES;\\nCREATE TABLE ...;">
      </textarea>
      <div class="muted" v-if="message">{{ message }}</div>
      <div class="sql-error" v-if="error">{{ error }}</div>
      <div v-if="result">
        <div class="sql-meta" v-if="result.hasResultSet">
          返回 {{ (result.rows || []).length }} 行，{{ (result.columns || []).length }} 列
        </div>
        <div class="sql-meta" v-else>
          执行完成，影响 {{ result.updateCount ?? 0 }} 行
        </div>
        <div class="sql-result-wrap" v-if="result.hasResultSet">
          <table class="sql-result-table">
            <thead>
              <tr><th v-for="column in result.columns" :key="column">{{ column }}</th></tr>
            </thead>
            <tbody>
              <tr v-for="(row, rowIndex) in result.rows" :key="rowIndex">
                <td v-for="(value, columnIndex) in row" :key="columnIndex">{{ value == null ? 'NULL' : value }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>
  `,
  setup() {
    const sql = ref("SHOW TABLES;");
    const running = ref(false);
    const result = ref(null);
    const error = ref("");
    const message = ref("浏览器内直接执行 MiniSQL，复用现有 JDBC 客户端链路。");

    async function runSql() {
      if (!sql.value.trim() || running.value) return;
      running.value = true;
      error.value = "";
      try {
        result.value = await executeSql(sql.value);
      } catch (err) {
        result.value = null;
        error.value = err.message;
      } finally {
        running.value = false;
      }
    }

    return { sql, running, result, error, message, runSql };
  },
};

createApp({
  components: {
    OverviewCards,
    SqlPanel,
    DataTable,
    EventTimeline,
    SqlConsole,
  },
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
      lastUpdated: "等待数据",
    });
    const loading = ref(false);
    let timer = null;

    const serverColumns = [
      { key: "serverId", label: "Server", render: row => row.serverId || "-" },
      { key: "lastHeartbeat", label: "Heartbeat", render: row => formatTime(row.lastHeartbeat) },
      { key: "cpuUsage", label: "CPU", render: row => Number(row.cpuUsage || 0).toFixed(2) },
      { key: "memoryUsage", label: "Memory", render: row => `${(Number(row.memoryUsage || 0) * 100).toFixed(1)}%` },
      { key: "regionCount", label: "Regions", render: row => formatNumber(row.regionCount) },
      { key: "readRequests", label: "Reads", render: row => formatNumber(row.readRequests) },
      { key: "writeRequests", label: "Writes", render: row => formatNumber(row.writeRequests) },
    ];

    const tableColumns = [
      { key: "tableName", label: "Table", render: row => row.tableName || "-" },
      { key: "regionCount", label: "Regions", render: row => formatNumber(row.regionCount) },
      { key: "totalReadRequests", label: "Reads", render: row => formatNumber(row.totalReadRequests) },
      { key: "totalWriteRequests", label: "Writes", render: row => formatNumber(row.totalWriteRequests) },
      { key: "totalErrors", label: "Errors", render: row => formatNumber(row.totalErrors) },
      { key: "hotspotScore", label: "Hot Score", render: row => Number(row.hotspotScore || 0).toFixed(1) },
    ];

    const regionColumns = [
      { key: "regionId", label: "Region", render: row => row.regionId || "-" },
      { key: "tableName", label: "Table", render: row => row.tableName || "-" },
      { key: "serverId", label: "Server", render: row => row.serverId || "-" },
      { key: "role", label: "Role", render: row => row.role || "-" },
      { key: "primaryServer", label: "Primary", render: row => row.primaryServer || "-" },
      { key: "replicationLag", label: "Lag", render: row => formatNumber(row.replicationLag) },
      { key: "readRequests", label: "Reads", render: row => formatNumber(row.readRequests) },
      { key: "writeRequests", label: "Writes", render: row => formatNumber(row.writeRequests) },
    ];

    const regionReplicaColumns = [
      { key: "regionId", label: "Region", render: row => row.regionId || "-" },
      { key: "serverId", label: "Server", render: row => row.serverId || "-" },
      { key: "role", label: "Role", render: row => {
          return row.role === 'Primary' 
            ? '<span style="font-size:0.75rem;padding:2px 6px;border-radius:12px;background:#d1fae5;color:#065f46;border:1px solid #10b981;">Primary</span>' 
            : '<span style="font-size:0.75rem;padding:2px 6px;border-radius:12px;background:#f3f4f6;color:#4b5563;border:1px solid #d1d5db;">Replica</span>';
      }},
      { key: "storeFileSize", label: "Size (Split)", render: row => `${(Number(row.storeFileSize || 0) / 1024 / 1024).toFixed(2)} MB` },
      { key: "regionLoadScore", label: "负载权重", render: row => Number(row.regionLoadScore || 0).toFixed(2) },
      { key: "serverLoadScore", label: "节点负载分", render: row => Number(row.serverLoadScore || 0).toFixed(2) },
    ];

    async function refresh() {
      if (loading.value) return;
      loading.value = true;
      try {
        const [overview, sqlSummary, servers, tables, regions, regionReplicas, events] = await Promise.all([
          fetchJson("/monitor/api/overview"),
          fetchJson(`/monitor/api/sql/summary?window=${state.sqlWindow}`),
          fetchJson("/monitor/api/servers"),
          fetchJson("/monitor/api/tables"),
          fetchJson("/monitor/api/regions"),
          fetchJson("/monitor/api/region-replicas"),
          fetchJson("/monitor/api/events?limit=30"),
        ]);
        state.overview = overview;
        state.sqlSummary = sqlSummary;
        state.servers = servers;
        state.tables = tables;
        state.regions = regions;
        state.regionReplicas = regionReplicas;
        state.events = events;
        state.lastUpdated = `更新于 ${new Date().toLocaleTimeString()}`;
      } catch (error) {
        state.lastUpdated = `刷新失败: ${error.message}`;
      } finally {
        loading.value = false;
      }
    }

    function changeSqlWindow(window) {
      state.sqlWindow = window;
      refresh();
    }

    onMounted(() => {
      refresh();
      timer = window.setInterval(refresh, 5000);
    });

    onBeforeUnmount(() => {
      if (timer) {
        clearInterval(timer);
      }
    });

    return {
      state,
      serverColumns,
      tableColumns,
      regionColumns,
      regionReplicaColumns,
      changeSqlWindow,
    };
  },
  template: `
    <div class="page">
      <header class="hero">
        <div>
          <p class="eyebrow">MiniSQL Monitor</p>
          <h1>集群观测面板</h1>
          <p class="subtle">每 5 秒刷新一次，展示运行态、SQL 指标、Split/迁移触发指标和事件时间线。</p>
        </div>
        <div class="stamp">{{ state.lastUpdated }}</div>
      </header>

      <overview-cards :overview="state.overview"></overview-cards>

      <sql-panel
        :summary="state.sqlSummary"
        :sql-window="state.sqlWindow"
        @change-window="changeSqlWindow">
      </sql-panel>

      <sql-console></sql-console>

      <section class="grid two">
        <data-table title="节点状态" :rows="state.servers" :columns="serverColumns"></data-table>
        <data-table title="Region Split & Migration 触发指标" :rows="state.regionReplicas" :columns="regionReplicaColumns"></data-table>
      </section>

      <section class="grid two">
        <data-table title="Table 概览" :rows="state.tables.slice(0, 10)" :columns="tableColumns"></data-table>
        <data-table title="Region 概览" :rows="state.regions.slice(0, 10)" :columns="regionColumns"></data-table>
      </section>

      <event-timeline :events="state.events"></event-timeline>
    </div>
  `,
}).mount("#app");
