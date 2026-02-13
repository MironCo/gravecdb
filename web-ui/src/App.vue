<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import cytoscape from 'cytoscape'

const graphData = ref({ nodes: [], relationships: [] })
const timelineEvents = ref([])
const currentTimeIndex = ref(0)
const selectedNode = ref(null)
const selectedRelationship = ref(null)
const cy = ref(null)
const isPlaying = ref(false)
const playbackSpeed = ref(1)
const playbackInterval = ref(null)
const isLoaded = ref(false)
const isGraphLoading = ref(true)
const loadGraphTimeout = ref(null)

// Query panel state
const isQueryPanelOpen = ref(false)
const queryText = ref('')
const queryResult = ref(null)
const queryError = ref(null)
const isQueryRunning = ref(false)

const currentTimeDisplay = computed(() => {
  if (timelineEvents.value.length === 0) return 'No data'
  const event = timelineEvents.value[currentTimeIndex.value]
  if (!event) return 'Loading...'
  const date = new Date(event.timestamp)
  return date.toLocaleString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  })
})

const currentDateDisplay = computed(() => {
  if (timelineEvents.value.length === 0) return ''
  const event = timelineEvents.value[currentTimeIndex.value]
  if (!event) return ''
  const date = new Date(event.timestamp)
  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric'
  })
})

const selectedNodeRelationships = computed(() => {
  if (!selectedNode.value) return []

  const rels = []
  for (const rel of graphData.value.relationships) {
    if (rel.from === selectedNode.value.id || rel.to === selectedNode.value.id) {
      const otherNodeId = rel.from === selectedNode.value.id ? rel.to : rel.from
      const otherNode = graphData.value.nodes.find(n => n.id === otherNodeId)
      const direction = rel.from === selectedNode.value.id ? 'outgoing' : 'incoming'

      rels.push({
        type: rel.type,
        direction,
        otherNode: otherNode?.properties?.name || 'Unknown',
        properties: rel.properties,
        deleted: !!rel.validTo
      })
    }
  }
  return rels
})

onMounted(async () => {
  await loadTimeline()
  await loadGraph()
  initCytoscape()
  setTimeout(() => {
    isLoaded.value = true
  }, 100)
})

async function loadTimeline() {
  const res = await fetch('/api/timeline')
  timelineEvents.value = await res.json()
  timelineEvents.value.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp))
  currentTimeIndex.value = timelineEvents.value.length - 1
}

async function loadGraph() {
  isGraphLoading.value = true

  if (currentTimeIndex.value >= timelineEvents.value.length) {
    currentTimeIndex.value = timelineEvents.value.length - 1
  }

  if (currentTimeIndex.value === timelineEvents.value.length - 1) {
    const res = await fetch('/api/graph')
    graphData.value = await res.json()
  } else {
    const event = timelineEvents.value[currentTimeIndex.value]
    if (!event) return

    const timestamp = encodeURIComponent(new Date(event.timestamp).toISOString())
    const res = await fetch(`/api/graph/asof?t=${timestamp}`)
    graphData.value = await res.json()
  }

  renderGraph()
}

function initCytoscape() {
  cy.value = cytoscape({
    container: document.getElementById('cy'),
    style: [
      {
        selector: 'node',
        style: {
          'background-color': '#FFFFFF',
          'label': 'data(label)',
          'color': '#000000',
          'text-valign': 'center',
          'text-halign': 'center',
          'font-size': '10px',
          'font-weight': '500',
          'font-family': 'Noto Sans, sans-serif',
          'width': 60,
          'height': 60,
          'border-width': 1,
          'border-color': '#FFFFFF',
          'shape': 'ellipse',
          'text-wrap': 'wrap',
          'text-max-width': '42px'
        }
      },
      {
        selector: 'node[type="Company"]',
        style: {
          'background-color': '#1A1A1A',
          'shape': 'roundrectangle',
          'border-color': '#FFFFFF',
          'color': '#FFFFFF',
          'text-wrap': 'wrap',
          'text-max-width': '45px'
        }
      },
      {
        selector: 'node.deleted',
        style: {
          'background-color': '#404040',
          'opacity': 0.4,
          'border-width': 1,
          'border-color': '#808080',
          'border-style': 'dashed',
          'color': '#808080'
        }
      },
      {
        selector: 'edge',
        style: {
          'width': 1.5,
          'line-color': '#D0D0D0',
          'target-arrow-color': '#D0D0D0',
          'target-arrow-shape': 'triangle',
          'curve-style': 'bezier',
          'label': 'data(label)',
          'font-size': '10px',
          'font-weight': '400',
          'color': '#C0C0C0',
          'font-family': 'JetBrains Mono, monospace',
          'text-background-opacity': 1,
          'text-background-color': '#000000',
          'text-background-padding': '3px',
          'text-background-shape': 'roundrectangle'
        }
      },
      {
        selector: 'edge.deleted',
        style: {
          'line-color': '#707070',
          'target-arrow-color': '#707070',
          'opacity': 0.3,
          'line-style': 'dashed'
        }
      }
    ],
    layout: {
      name: 'cose',
      animate: true,
      animationDuration: 500,
      padding: 50
    }
  })

  cy.value.on('tap', 'node', (evt) => {
    const node = evt.target.data()
    selectedNode.value = graphData.value.nodes.find(n => n.id === node.id)
    selectedRelationship.value = null
  })

  cy.value.on('tap', 'edge', (evt) => {
    const edge = evt.target.data()
    selectedRelationship.value = graphData.value.relationships.find(r => r.id === edge.id)
    selectedNode.value = null
  })

  renderGraph()
}

function renderGraph() {
  if (!cy.value) return

  const elements = []
  const nodeIds = new Set()

  graphData.value.nodes.forEach(node => {
    const label = node.properties.name || node.labels[0] || 'Node'
    nodeIds.add(node.id)
    elements.push({
      group: 'nodes',
      data: {
        id: node.id,
        label: label,
        type: node.labels[0],
        deleted: !!node.validTo
      },
      classes: node.validTo ? 'deleted' : ''
    })
  })

  // Only add edges if both source and target nodes exist
  graphData.value.relationships.forEach(rel => {
    if (nodeIds.has(rel.from) && nodeIds.has(rel.to)) {
      elements.push({
        group: 'edges',
        data: {
          id: rel.id,
          source: rel.from,
          target: rel.to,
          label: rel.type,
          deleted: !!rel.validTo
        },
        classes: rel.validTo ? 'deleted' : ''
      })
    }
  })

  cy.value.elements().remove()
  cy.value.add(elements)

  // If no elements, just clear loading state
  if (elements.length === 0) {
    isGraphLoading.value = false
    return
  }

  // Hide elements while layout calculates
  cy.value.elements().style('opacity', 0)

  const layout = cy.value.layout({
    name: 'cose',
    animate: false
  })
  layout.on('layoutstop', () => {
    // Fade in elements after layout is done
    cy.value.elements().animate({
      style: { opacity: 1 },
      duration: 200,
      easing: 'ease-out'
    })
    isGraphLoading.value = false
  })
  layout.run()
}

function onTimeSliderChange() {
  isGraphLoading.value = true
  if (loadGraphTimeout.value) {
    clearTimeout(loadGraphTimeout.value)
  }
  loadGraphTimeout.value = setTimeout(() => {
    loadGraph()
  }, 150)
}

function togglePlayback() {
  isPlaying.value = !isPlaying.value
  if (isPlaying.value) {
    startPlayback()
  } else {
    stopPlayback()
  }
}

function startPlayback() {
  const interval = 1000 / playbackSpeed.value
  playbackInterval.value = setInterval(() => {
    if (currentTimeIndex.value < timelineEvents.value.length - 1) {
      currentTimeIndex.value++
      loadGraph()
    } else {
      stopPlayback()
    }
  }, interval)
}

function stopPlayback() {
  isPlaying.value = false
  if (playbackInterval.value) {
    clearInterval(playbackInterval.value)
    playbackInterval.value = null
  }
}

function jumpToStart() {
  stopPlayback()
  currentTimeIndex.value = 0
  loadGraph()
}

function jumpToEnd() {
  stopPlayback()
  currentTimeIndex.value = timelineEvents.value.length - 1
  loadGraph()
}

watch(playbackSpeed, () => {
  if (isPlaying.value) {
    // Clear interval without changing isPlaying state
    if (playbackInterval.value) {
      clearInterval(playbackInterval.value)
      playbackInterval.value = null
    }
    startPlayback()
  }
})

// Query panel functions
function toggleQueryPanel() {
  isQueryPanelOpen.value = !isQueryPanelOpen.value
}

async function executeQuery() {
  if (!queryText.value.trim()) return

  isQueryRunning.value = true
  queryError.value = null
  queryResult.value = null

  try {
    const res = await fetch('/api/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: queryText.value })
    })

    const data = await res.json()

    if (!res.ok) {
      queryError.value = data.error || 'Query execution failed'
    } else {
      queryResult.value = data
    }
  } catch (err) {
    queryError.value = 'Network error: ' + err.message
  } finally {
    isQueryRunning.value = false
  }
}

function clearQuery() {
  queryText.value = ''
  queryResult.value = null
  queryError.value = null
}

function formatCellValue(value) {
  if (value === null || value === undefined) return 'null'
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value, null, 2)
    } catch {
      return String(value)
    }
  }
  return String(value)
}

// Check if a value is a Node object
function isNode(value) {
  return value && typeof value === 'object' && 'ID' in value && 'Labels' in value && 'Properties' in value
}

// Check if a value is a Relationship object
function isRelationship(value) {
  return value && typeof value === 'object' && 'ID' in value && 'Type' in value && 'FromNodeID' in value && 'ToNodeID' in value
}

// Get display name for a node
function getNodeDisplayName(node) {
  if (node.Properties?.name) return node.Properties.name
  if (node.Labels?.length) return node.Labels[0]
  return node.ID?.substring(0, 8) || 'Node'
}
</script>

<template>
  <div id="app" :class="{ loaded: isLoaded }">
    <header>
      <div class="header-content">
        <div class="title-group">
          <div class="logo-icon">
            <svg width="40" height="40" viewBox="0 0 40 40" fill="none">
              <circle cx="20" cy="20" r="18" stroke="#FFFFFF" stroke-width="2" fill="none"/>
              <path d="M20 8 L20 20 L28 20" stroke="#FFFFFF" stroke-width="2" stroke-linecap="round"/>
            </svg>
          </div>
          <div>
            <h1>Temporal Graph Database</h1>
            <div class="subtitle">Time-travel through your data</div>
          </div>
        </div>

        <div class="time-display-main">
          <div class="time-value">{{ currentTimeDisplay }}</div>
          <div class="date-value">{{ currentDateDisplay }}</div>
        </div>
      </div>
    </header>

    <div class="controls">
      <div class="controls-inner">
        <div class="playback-controls">
          <button class="control-btn icon-btn" @click="jumpToStart" title="Jump to start">
            <svg width="14" height="14" viewBox="0 0 20 20" fill="currentColor">
              <path d="M3 3h2v14H3V3zm4 7l10-7v14L7 10z"/>
            </svg>
          </button>
          <button class="control-btn play-btn" @click="togglePlayback">
            <svg v-if="isPlaying" width="14" height="14" viewBox="0 0 20 20" fill="currentColor">
              <path d="M6 4h3v12H6V4zm5 0h3v12h-3V4z"/>
            </svg>
            <svg v-else width="14" height="14" viewBox="0 0 20 20" fill="currentColor">
              <path d="M5 3l12 7-12 7V3z"/>
            </svg>
          </button>
          <button class="control-btn icon-btn" @click="jumpToEnd" title="Jump to end">
            <svg width="14" height="14" viewBox="0 0 20 20" fill="currentColor">
              <path d="M15 3h2v14h-2V3zM3 10l10 7V3L3 10z"/>
            </svg>
          </button>
        </div>

        <div class="slider-section">
          <input
            type="range"
            :min="0"
            :max="timelineEvents.length - 1"
            v-model.number="currentTimeIndex"
            @input="onTimeSliderChange"
          />
          <span class="event-counter">{{ currentTimeIndex + 1 }}/{{ timelineEvents.length }}</span>
        </div>

        <div class="speed-control">
          <select v-model="playbackSpeed">
            <option :value="0.5">0.5×</option>
            <option :value="1">1×</option>
            <option :value="2">2×</option>
            <option :value="5">5×</option>
          </select>
        </div>
      </div>
    </div>

    <div class="container">
      <!-- Query Panel (Left Side) -->
      <div class="query-panel" :class="{ open: isQueryPanelOpen }">
        <button class="query-panel-toggle" @click="toggleQueryPanel" :title="isQueryPanelOpen ? 'Collapse' : 'Expand'">
          <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
            <path d="M11.5 1a.5.5 0 01.5.5v1a.5.5 0 01-.5.5H11v1.07a4 4 0 011.555 1.223l.927-.537a.5.5 0 01.683.184l.5.866a.5.5 0 01-.183.683l-.927.536A4 4 0 0114 8a4 4 0 01-.445 1.817l.927.536a.5.5 0 01.183.683l-.5.866a.5.5 0 01-.683.184l-.927-.537A4 4 0 0111 12.93V14h.5a.5.5 0 01.5.5v1a.5.5 0 01-.5.5h-7a.5.5 0 01-.5-.5v-1a.5.5 0 01.5-.5H5v-1.07a4 4 0 01-1.555-1.223l-.927.537a.5.5 0 01-.683-.184l-.5-.866a.5.5 0 01.183-.683l.927-.536A4 4 0 012 8a4 4 0 01.445-1.817l-.927-.536a.5.5 0 01-.183-.683l.5-.866a.5.5 0 01.683-.184l.927.537A4 4 0 015 3.07V2h-.5a.5.5 0 01-.5-.5v-1a.5.5 0 01.5-.5h7zM8 5a3 3 0 100 6 3 3 0 000-6z"/>
          </svg>
          <span class="toggle-label">Query</span>
          <svg class="chevron" width="12" height="12" viewBox="0 0 12 12" fill="currentColor">
            <path d="M4 2l4 4-4 4"/>
          </svg>
        </button>

        <div class="query-panel-content">
          <div class="query-input-section">
            <label class="query-label">Cypher Query</label>
            <textarea
              v-model="queryText"
              class="query-textarea"
              placeholder="MATCH (n:Person) RETURN n"
              rows="6"
              @keydown.ctrl.enter="executeQuery"
              @keydown.meta.enter="executeQuery"
            ></textarea>
            <div class="query-actions">
              <button class="query-btn run-btn" @click="executeQuery" :disabled="isQueryRunning || !queryText.trim()">
                <svg v-if="isQueryRunning" class="spinner-icon" width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                  <circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="2" fill="none" stroke-dasharray="25" stroke-linecap="round"/>
                </svg>
                <svg v-else width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                  <path d="M4 2l10 6-10 6V2z"/>
                </svg>
                <span>{{ isQueryRunning ? 'Running...' : 'Run' }}</span>
              </button>
              <button class="query-btn clear-btn" @click="clearQuery" :disabled="isQueryRunning">
                Clear
              </button>
            </div>
            <span class="query-hint">Ctrl+Enter to run</span>
          </div>

          <div class="query-result-section" v-if="queryError || queryResult">
            <div v-if="queryError" class="query-error">
              <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                <path d="M8 1a7 7 0 100 14A7 7 0 008 1zM7 4h2v5H7V4zm0 6h2v2H7v-2z"/>
              </svg>
              <span>{{ queryError }}</span>
            </div>

            <div v-else-if="queryResult" class="query-results">
              <div class="results-header">
                <span class="results-count">{{ queryResult.Rows?.length || 0 }} row(s)</span>
              </div>

              <div class="results-table-wrapper" v-if="queryResult.Columns && queryResult.Rows?.length">
                <table class="results-table">
                  <thead>
                    <tr>
                      <th v-for="col in queryResult.Columns" :key="col">{{ col }}</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="(row, idx) in queryResult.Rows" :key="idx">
                      <td v-for="col in queryResult.Columns" :key="col">
                        <!-- Node Card -->
                        <div v-if="isNode(row[col])" class="result-node-card">
                          <div class="result-node-header">
                            <span class="result-node-icon">●</span>
                            <span class="result-node-name">{{ getNodeDisplayName(row[col]) }}</span>
                          </div>
                          <div class="result-node-labels">
                            <span class="result-label" v-for="label in row[col].Labels" :key="label">{{ label }}</span>
                          </div>
                          <div class="result-node-props" v-if="row[col].Properties && Object.keys(row[col].Properties).length">
                            <div class="result-prop" v-for="(val, key) in row[col].Properties" :key="key">
                              <span class="result-prop-key">{{ key }}</span>
                              <span class="result-prop-val">{{ val }}</span>
                            </div>
                          </div>
                        </div>
                        <!-- Relationship Card -->
                        <div v-else-if="isRelationship(row[col])" class="result-rel-card">
                          <div class="result-rel-header">
                            <span class="result-rel-icon">→</span>
                            <span class="result-rel-type">{{ row[col].Type }}</span>
                          </div>
                          <div class="result-rel-endpoints">
                            <span class="result-rel-from">{{ row[col].FromNodeID?.substring(0, 8) }}</span>
                            <span class="result-rel-arrow">→</span>
                            <span class="result-rel-to">{{ row[col].ToNodeID?.substring(0, 8) }}</span>
                          </div>
                          <div class="result-node-props" v-if="row[col].Properties && Object.keys(row[col].Properties).length">
                            <div class="result-prop" v-for="(val, key) in row[col].Properties" :key="key">
                              <span class="result-prop-key">{{ key }}</span>
                              <span class="result-prop-val">{{ val }}</span>
                            </div>
                          </div>
                        </div>
                        <!-- Plain Value -->
                        <span v-else class="cell-value">{{ formatCellValue(row[col]) }}</span>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <div v-else class="no-results">
                No results returned.
              </div>
            </div>
          </div>
        </div>
      </div>

      <div class="graph-wrapper">
        <div id="cy"></div>
        <div v-if="isGraphLoading" class="loading-overlay">
          <div class="spinner"></div>
          <span class="loading-text">Loading graph...</span>
        </div>
      </div>

      <div class="sidebar">
        <div class="stats-card">
          <h2 class="card-title">Graph Statistics</h2>
          <div class="stats-grid">
            <div class="stat-item">
              <div class="stat-label">Nodes</div>
              <div class="stat-value">{{ graphData.nodes.length }}</div>
            </div>
            <div class="stat-item">
              <div class="stat-label">Relationships</div>
              <div class="stat-value">{{ graphData.relationships.length }}</div>
            </div>
            <div class="stat-item">
              <div class="stat-label">Events</div>
              <div class="stat-value">{{ timelineEvents.length }}</div>
            </div>
          </div>
        </div>

        <div class="node-details-card" v-if="selectedNode">
          <div class="card-header">
            <h2 class="card-title">Node Details</h2>
            <div class="node-status" :class="{ deleted: selectedNode.validTo }">
              {{ selectedNode.validTo ? 'Deleted' : 'Active' }}
            </div>
          </div>

          <h3 class="node-name">{{ selectedNode.properties.name || 'Unnamed Node' }}</h3>

          <div class="properties-list">
            <div class="property-group">
              <div class="property-label">Labels</div>
              <div class="property-tags">
                <span class="tag" v-for="label in selectedNode.labels" :key="label">
                  {{ label }}
                </span>
              </div>
            </div>

            <div class="property-item" v-for="(value, key) in selectedNode.properties" :key="key">
              <div class="property-label">{{ key }}</div>
              <div class="property-value">{{ value }}</div>
            </div>
          </div>

          <div v-if="selectedNode.embedding" class="embedding-section">
            <h4 class="section-title">Vector Embedding</h4>
            <div class="embedding-info">
              <div class="embedding-item">
                <span class="embedding-label">Model</span>
                <span class="embedding-value">{{ selectedNode.embedding.model }}</span>
              </div>
              <div class="embedding-item">
                <span class="embedding-label">Dimensions</span>
                <span class="embedding-value">{{ selectedNode.embedding.dimensions }}</span>
              </div>
              <div class="embedding-item">
                <span class="embedding-label">Created</span>
                <span class="embedding-value">{{ new Date(selectedNode.embedding.validFrom).toLocaleString() }}</span>
              </div>
            </div>
          </div>

          <div v-if="selectedNodeRelationships.length > 0" class="relationships-section">
            <h4 class="section-title">Relationships</h4>
            <div
              v-for="(rel, idx) in selectedNodeRelationships"
              :key="idx"
              class="relationship-item"
              :class="{ deleted: rel.deleted }"
            >
              <div class="rel-header">
                <span class="rel-direction">{{ rel.direction === 'outgoing' ? '→' : '←' }}</span>
                <span class="rel-type">{{ rel.type }}</span>
              </div>
              <div class="rel-target">{{ rel.otherNode }}</div>
              <div v-if="Object.keys(rel.properties).length > 0" class="rel-properties">
                <div v-for="(value, key) in rel.properties" :key="key" class="rel-property">
                  <span class="property-label">{{ key }}</span>
                  <span class="property-value">{{ value }}</span>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div class="node-details-card" v-else-if="selectedRelationship">
          <div class="card-header">
            <h2 class="card-title">Relationship Details</h2>
            <div class="node-status" :class="{ deleted: selectedRelationship.validTo }">
              {{ selectedRelationship.validTo ? 'Deleted' : 'Active' }}
            </div>
          </div>

          <h3 class="node-name">{{ selectedRelationship.type }}</h3>

          <div class="properties-list">
            <div class="property-group">
              <div class="property-label">Direction</div>
              <div class="connection-info">
                <div class="connection-node">
                  {{ graphData.nodes.find(n => n.id === selectedRelationship.from)?.properties?.name || 'Unknown' }}
                </div>
                <div class="connection-arrow">→</div>
                <div class="connection-node">
                  {{ graphData.nodes.find(n => n.id === selectedRelationship.to)?.properties?.name || 'Unknown' }}
                </div>
              </div>
            </div>

            <div class="property-item" v-for="(value, key) in selectedRelationship.properties" :key="key">
              <div class="property-label">{{ key }}</div>
              <div class="property-value">{{ value }}</div>
            </div>
          </div>
        </div>

        <div class="empty-state" v-else>
          <svg width="64" height="64" viewBox="0 0 64 64" fill="none">
            <circle cx="32" cy="32" r="28" stroke="#808080" stroke-width="2" stroke-dasharray="4 4"/>
            <circle cx="32" cy="32" r="4" fill="#808080"/>
          </svg>
          <p>Select a node to view details</p>
        </div>
      </div>
    </div>
  </div>
</template>

<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans:wght@300;400;500;700&family=JetBrains+Mono:wght@300;400;500;700&display=swap');

:root {
  --primary: #FFFFFF;
  --primary-dark: #E0E0E0;
  --secondary: #808080;
  --accent: #000000;
  --error: #FFFFFF;
  --surface: #000000;
  --surface-elevated: #1A1A1A;
  --background: #0A0A0A;
  --on-surface: #FFFFFF;
  --on-surface-variant: #B0B0B0;
  --outline: #333333;
  --shadow: none;
}

* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

html, body {
  margin: 0;
  padding: 0;
  width: 100%;
  height: 100%;
  overflow: hidden;
}

body {
  font-family: 'Noto Sans', -apple-system, BlinkMacSystemFont, sans-serif;
  background: var(--background);
  color: var(--on-surface);
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}

#app {
  display: flex;
  flex-direction: column;
  width: 100%;
  height: 100vh;
  position: relative;
  opacity: 0;
  animation: fadeIn 0.4s ease forwards;
}

#app.loaded header,
#app.loaded .controls,
#app.loaded .sidebar {
  animation: slideIn 0.3s cubic-bezier(0.4, 0, 0.2, 1) forwards;
}

#app.loaded .controls {
  animation-delay: 0.05s;
}

#app.loaded .sidebar {
  animation-delay: 0.1s;
}

@keyframes fadeIn {
  to { opacity: 1; }
}

@keyframes slideIn {
  from {
    opacity: 0;
    transform: translateY(-8px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

header {
  background: var(--surface);
  box-shadow: 0 1px 0 var(--outline);
  position: relative;
  z-index: 10;
  border-bottom: 1px solid var(--outline);
}

.header-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 20px;
}

.title-group {
  display: flex;
  align-items: center;
  gap: 16px;
}

.logo-icon {
  display: flex;
  align-items: center;
  justify-content: center;
}

.logo-icon svg {
  width: 32px;
  height: 32px;
}

h1 {
  font-size: 20px;
  font-weight: 500;
  color: var(--on-surface);
  margin: 0;
  letter-spacing: 0.25px;
}

.subtitle {
  font-size: 12px;
  color: var(--on-surface-variant);
  margin-top: 2px;
  font-weight: 400;
}

.time-display-main {
  text-align: right;
}

.time-value {
  font-size: 22px;
  font-weight: 400;
  color: var(--primary);
  letter-spacing: 0.5px;
  font-variant-numeric: tabular-nums;
}

.date-value {
  font-size: 11px;
  color: var(--on-surface-variant);
  margin-top: 2px;
  font-weight: 400;
}

.controls {
  background: var(--surface);
  border-bottom: 1px solid var(--outline);
  padding: 6px 20px;
}

.controls-inner {
  max-width: 1400px;
  margin: 0 auto;
  display: flex;
  align-items: center;
  gap: 16px;
}

.slider-section {
  flex: 1;
  display: flex;
  align-items: center;
  gap: 12px;
}

.event-counter {
  color: var(--on-surface-variant);
  font-size: 11px;
  font-weight: 500;
  white-space: nowrap;
  font-family: 'JetBrains Mono', monospace;
}

input[type="range"] {
  width: 100%;
  height: 4px;
  -webkit-appearance: none;
  appearance: none;
  background: var(--outline);
  outline: none;
  border-radius: 2px;
  cursor: pointer;
}

input[type="range"]::-webkit-slider-thumb {
  -webkit-appearance: none;
  appearance: none;
  width: 16px;
  height: 16px;
  background: var(--primary);
  border-radius: 50%;
  cursor: pointer;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}

input[type="range"]::-webkit-slider-thumb:hover {
  transform: scale(1.2);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
}

input[type="range"]::-webkit-slider-thumb:active {
  transform: scale(1.1);
}

input[type="range"]::-moz-range-thumb {
  width: 16px;
  height: 16px;
  background: var(--primary);
  border: none;
  border-radius: 50%;
  cursor: pointer;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}

.controls-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 24px;
}

.playback-controls {
  display: flex;
  gap: 8px;
}

.control-btn {
  background: var(--surface);
  color: var(--primary);
  border: 1px solid var(--outline);
  padding: 6px 12px;
  border-radius: 4px;
  font-family: 'Roboto', sans-serif;
  font-weight: 500;
  font-size: 12px;
  cursor: pointer;
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
  display: flex;
  align-items: center;
  justify-content: center;
  box-shadow: 0 1px 3px var(--shadow);
}

.control-btn:hover {
  background: var(--primary);
  color: var(--surface);
  box-shadow: 0 2px 6px var(--shadow);
  transform: translateY(-1px);
}

.control-btn:active {
  transform: translateY(0);
  box-shadow: 0 1px 3px var(--shadow);
}

.icon-btn {
  padding: 8px 12px;
}

.play-btn {
  background: var(--primary);
  color: var(--surface);
}

.play-btn:hover {
  background: var(--primary-dark);
}

.speed-control {
  display: flex;
  align-items: center;
  gap: 12px;
}

.speed-control label {
  font-size: 14px;
  color: var(--on-surface-variant);
  font-weight: 500;
}

select {
  background: var(--surface);
  color: var(--on-surface);
  border: 1px solid var(--outline);
  padding: 8px 12px;
  border-radius: 4px;
  font-family: 'Roboto', sans-serif;
  font-size: 14px;
  cursor: pointer;
  transition: all 0.2s;
  box-shadow: 0 1px 3px var(--shadow);
}

select:hover {
  border-color: var(--primary);
}

select:focus {
  outline: none;
  border-color: var(--primary);
  box-shadow: 0 0 0 2px rgba(76, 175, 80, 0.2);
}

.container {
  display: flex;
  flex: 1;
  overflow: hidden;
  position: relative;
}

.graph-wrapper {
  flex: 1;
  min-width: 0;
  position: relative;
  display: flex;
}

#cy {
  flex: 1;
  background: #0D0D0D;
  background-image:
    linear-gradient(rgba(255, 255, 255, 0.03) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255, 255, 255, 0.03) 1px, transparent 1px);
  background-size: 20px 20px;
}

.loading-overlay {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(13, 13, 13, 0.85);
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 16px;
  z-index: 5;
}

.spinner {
  width: 40px;
  height: 40px;
  border: 3px solid var(--outline);
  border-top-color: var(--primary);
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.loading-text {
  color: var(--on-surface-variant);
  font-size: 14px;
  font-weight: 400;
}

.sidebar {
  width: 360px;
  min-width: 360px;
  flex-shrink: 0;
  background: var(--surface);
  border-left: 1px solid var(--outline);
  padding: 24px;
  overflow-y: auto;
  box-shadow: -2px 0 8px var(--shadow);
}

.sidebar::-webkit-scrollbar {
  width: 8px;
}

.sidebar::-webkit-scrollbar-track {
  background: var(--background);
}

.sidebar::-webkit-scrollbar-thumb {
  background: var(--outline);
  border-radius: 4px;
}

.sidebar::-webkit-scrollbar-thumb:hover {
  background: #BDBDBD;
}

.stats-card, .node-details-card {
  background: var(--surface-elevated);
  border: 1px solid var(--outline);
  border-radius: 8px;
  padding: 20px;
  margin-bottom: 16px;
  box-shadow: none;
}

.card-title {
  font-size: 14px;
  font-weight: 500;
  text-transform: uppercase;
  color: var(--on-surface-variant);
  margin-bottom: 16px;
  letter-spacing: 0.5px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.node-status {
  font-size: 11px;
  font-weight: 500;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  padding: 4px 12px;
  background: transparent;
  color: var(--primary);
  border-radius: 12px;
  border: 1px solid var(--primary);
}

.node-status.deleted {
  background: transparent;
  color: var(--secondary);
  border-color: var(--secondary);
}

.stats-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 12px;
}

.stat-item {
  text-align: center;
  padding: 16px 8px;
  background: var(--background);
  border-radius: 8px;
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}

.stat-item:hover {
  transform: translateY(-2px);
  box-shadow: 0 4px 8px var(--shadow);
}

.stat-label {
  font-size: 12px;
  color: var(--on-surface-variant);
  margin-bottom: 8px;
  font-weight: 400;
}

.stat-value {
  font-size: 24px;
  font-weight: 300;
  color: var(--primary);
}

.node-name {
  font-size: 20px;
  color: var(--on-surface);
  margin-bottom: 20px;
  font-weight: 500;
}

.properties-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.property-group {
  margin-bottom: 8px;
}

.property-item {
  background: var(--background);
  padding: 12px;
  border-radius: 4px;
  border-left: 3px solid var(--primary);
  transition: all 0.2s;
}

.property-item:hover {
  background: var(--surface-elevated);
  border-left-color: var(--primary);
}

.property-label {
  font-size: 12px;
  color: var(--on-surface-variant);
  margin-bottom: 4px;
  font-weight: 500;
}

.property-value {
  color: var(--on-surface);
  font-size: 14px;
  font-weight: 400;
}

.property-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.tag {
  background: transparent;
  color: var(--primary);
  padding: 4px 12px;
  font-size: 12px;
  font-weight: 500;
  border-radius: 16px;
  border: 1px solid var(--outline);
}

.section-title {
  font-size: 14px;
  font-weight: 500;
  text-transform: uppercase;
  color: var(--on-surface-variant);
  margin: 20px 0 12px;
  padding-bottom: 8px;
  border-bottom: 1px solid var(--outline);
  letter-spacing: 0.5px;
}

.embedding-section {
  margin-top: 20px;
}

.embedding-info {
  background: var(--background);
  padding: 16px;
  border-radius: 4px;
  border-left: 3px solid #4CAF50;
}

.embedding-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 0;
}

.embedding-item:not(:last-child) {
  border-bottom: 1px solid var(--outline);
}

.embedding-label {
  font-size: 12px;
  color: var(--on-surface-variant);
  font-weight: 500;
}

.embedding-value {
  font-size: 13px;
  color: var(--primary);
  font-family: 'JetBrains Mono', monospace;
  font-weight: 500;
}

.relationships-section {
  margin-top: 20px;
}

.relationship-item {
  background: var(--background);
  padding: 12px;
  margin-bottom: 8px;
  border-radius: 4px;
  border-left: 3px solid var(--secondary-light);
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}

.relationship-item:hover {
  background: var(--surface-elevated);
  transform: translateX(4px);
  border-left-color: var(--secondary);
}

.relationship-item.deleted {
  opacity: 0.6;
  border-left-color: var(--error);
}

.rel-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 4px;
}

.rel-direction {
  color: var(--secondary);
  font-size: 18px;
  font-weight: 500;
}

.rel-type {
  font-size: 12px;
  font-weight: 500;
  text-transform: uppercase;
  color: var(--secondary);
  letter-spacing: 0.5px;
}

.rel-target {
  color: var(--on-surface);
  font-size: 14px;
  font-weight: 500;
  margin-bottom: 8px;
}

.rel-properties {
  padding-top: 8px;
  border-top: 1px solid var(--outline);
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.rel-property {
  display: flex;
  justify-content: space-between;
  font-size: 12px;
}

.rel-property .property-label {
  margin: 0;
}

.empty-state {
  text-align: center;
  padding: 48px 16px;
  color: var(--on-surface-variant);
}

.empty-state svg {
  margin-bottom: 16px;
}

.empty-state p {
  font-size: 14px;
}

.connection-info {
  display: flex;
  align-items: center;
  gap: 12px;
  background: var(--background);
  padding: 12px;
  border-radius: 4px;
  margin-top: 8px;
}

.connection-node {
  flex: 1;
  text-align: center;
  padding: 8px;
  background: var(--surface);
  border: 1px solid var(--outline);
  border-radius: 4px;
  font-size: 13px;
  font-weight: 500;
  color: var(--on-surface);
}

.connection-arrow {
  color: var(--secondary);
  font-size: 20px;
  font-weight: 500;
}

/* Query Panel Styles (Left Side) */
.query-panel {
  width: 48px;
  min-width: 48px;
  background: var(--surface);
  border-right: 1px solid var(--outline);
  display: flex;
  flex-direction: column;
  transition: width 0.25s ease, min-width 0.25s ease;
  overflow: hidden;
}

.query-panel.open {
  width: 320px;
  min-width: 320px;
}

.query-panel-toggle {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 12px;
  background: transparent;
  border: none;
  border-bottom: 1px solid var(--outline);
  color: var(--on-surface-variant);
  font-family: 'Noto Sans', sans-serif;
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
  white-space: nowrap;
}

.query-panel-toggle:hover {
  background: var(--surface-elevated);
  color: var(--on-surface);
}

.query-panel-toggle .toggle-label {
  opacity: 0;
  width: 0;
  overflow: hidden;
  transition: opacity 0.2s, width 0.2s;
}

.query-panel.open .query-panel-toggle .toggle-label {
  opacity: 1;
  width: auto;
}

.query-panel-toggle .chevron {
  margin-left: auto;
  transition: transform 0.25s ease;
  flex-shrink: 0;
}

.query-panel.open .query-panel-toggle .chevron {
  transform: rotate(180deg);
}

.query-panel-content {
  flex: 1;
  padding: 16px;
  overflow-y: auto;
  overflow-x: hidden;
  opacity: 0;
  visibility: hidden;
  transition: opacity 0.2s ease;
}

.query-panel.open .query-panel-content {
  opacity: 1;
  visibility: visible;
}

.query-input-section {
  margin-bottom: 16px;
}

.query-label {
  display: block;
  font-size: 11px;
  color: var(--on-surface-variant);
  margin-bottom: 8px;
  font-weight: 500;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.query-textarea {
  width: 100%;
  padding: 10px;
  background: var(--background);
  border: 1px solid var(--outline);
  border-radius: 4px;
  color: var(--on-surface);
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px;
  line-height: 1.5;
  resize: vertical;
  min-height: 120px;
  transition: border-color 0.2s;
}

.query-textarea:focus {
  outline: none;
  border-color: var(--primary);
}

.query-textarea::placeholder {
  color: var(--secondary);
}

.query-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-top: 10px;
}

.query-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  padding: 8px 12px;
  border-radius: 4px;
  font-family: 'Noto Sans', sans-serif;
  font-size: 12px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
  flex: 1;
}

.query-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.run-btn {
  background: var(--primary);
  color: var(--surface);
  border: none;
}

.run-btn:hover:not(:disabled) {
  background: var(--primary-dark);
}

.clear-btn {
  background: transparent;
  color: var(--on-surface-variant);
  border: 1px solid var(--outline);
  flex: 0;
  padding: 8px 14px;
}

.clear-btn:hover:not(:disabled) {
  background: var(--surface-elevated);
  color: var(--on-surface);
}

.query-hint {
  display: block;
  margin-top: 8px;
  font-size: 10px;
  color: var(--secondary);
  text-align: right;
}

.spinner-icon {
  animation: spin 0.8s linear infinite;
}

.query-result-section {
  margin-top: 0;
  flex: 1;
  display: flex;
  flex-direction: column;
  min-height: 0;
}

.query-error {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 10px;
  background: rgba(255, 100, 100, 0.1);
  border: 1px solid rgba(255, 100, 100, 0.3);
  border-radius: 4px;
  color: #ff6b6b;
  font-size: 11px;
  font-family: 'JetBrains Mono', monospace;
}

.query-error svg {
  flex-shrink: 0;
  width: 14px;
  height: 14px;
  margin-top: 1px;
}

.query-results {
  background: var(--background);
  border: 1px solid var(--outline);
  border-radius: 4px;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  flex: 1;
  min-height: 0;
}

.results-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 10px;
  background: var(--surface-elevated);
  border-bottom: 1px solid var(--outline);
  font-size: 11px;
  flex-shrink: 0;
}

.results-count {
  color: var(--primary);
  font-weight: 500;
}

.results-table-wrapper {
  overflow: auto;
  flex: 1;
  min-height: 0;
}

.results-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 11px;
}

.results-table th {
  position: sticky;
  top: 0;
  background: var(--surface-elevated);
  padding: 8px 10px;
  text-align: left;
  font-weight: 500;
  color: var(--on-surface-variant);
  border-bottom: 1px solid var(--outline);
  white-space: nowrap;
}

.results-table td {
  padding: 8px 10px;
  border-bottom: 1px solid var(--outline);
  color: var(--on-surface);
  vertical-align: top;
}

.results-table tbody tr:hover {
  background: var(--surface-elevated);
}

.results-table tbody tr:last-child td {
  border-bottom: none;
}

.cell-value {
  font-family: 'JetBrains Mono', monospace;
  white-space: pre-wrap;
  word-break: break-word;
  max-width: 200px;
  display: block;
  font-size: 11px;
}

.no-results {
  padding: 16px;
  text-align: center;
  color: var(--secondary);
  font-size: 12px;
}

/* Result Node Card */
.result-node-card {
  background: var(--surface-elevated);
  border: 1px solid var(--outline);
  border-radius: 6px;
  padding: 10px;
  min-width: 140px;
}

.result-node-header {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 6px;
}

.result-node-icon {
  color: var(--primary);
  font-size: 10px;
}

.result-node-name {
  font-weight: 500;
  font-size: 12px;
  color: var(--on-surface);
}

.result-node-labels {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  margin-bottom: 8px;
}

.result-label {
  background: var(--background);
  border: 1px solid var(--outline);
  padding: 2px 6px;
  border-radius: 10px;
  font-size: 9px;
  color: var(--on-surface-variant);
  text-transform: uppercase;
  letter-spacing: 0.3px;
}

.result-node-props {
  border-top: 1px solid var(--outline);
  padding-top: 6px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.result-prop {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  font-size: 10px;
}

.result-prop-key {
  color: var(--secondary);
}

.result-prop-val {
  color: var(--on-surface);
  font-family: 'JetBrains Mono', monospace;
  text-align: right;
  word-break: break-word;
}

/* Result Relationship Card */
.result-rel-card {
  background: var(--surface-elevated);
  border: 1px solid var(--outline);
  border-left: 3px solid var(--secondary);
  border-radius: 6px;
  padding: 10px;
  min-width: 140px;
}

.result-rel-header {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 6px;
}

.result-rel-icon {
  color: var(--secondary);
  font-size: 12px;
  font-weight: bold;
}

.result-rel-type {
  font-weight: 500;
  font-size: 11px;
  color: var(--on-surface);
  text-transform: uppercase;
  letter-spacing: 0.3px;
}

.result-rel-endpoints {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 8px;
  font-size: 10px;
  font-family: 'JetBrains Mono', monospace;
}

.result-rel-from,
.result-rel-to {
  color: var(--on-surface-variant);
  background: var(--background);
  padding: 2px 6px;
  border-radius: 4px;
}

.result-rel-arrow {
  color: var(--secondary);
}
</style>
