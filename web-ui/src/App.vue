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
        <div class="slider-section">
          <div class="slider-label">
            <span>Timeline Position</span>
            <span class="event-counter">Event {{ currentTimeIndex + 1 }} of {{ timelineEvents.length }}</span>
          </div>
          <div class="slider-container">
            <input
              type="range"
              :min="0"
              :max="timelineEvents.length - 1"
              v-model.number="currentTimeIndex"
              @input="onTimeSliderChange"
            />
          </div>
        </div>

        <div class="controls-row">
          <div class="playback-controls">
            <button class="control-btn icon-btn" @click="jumpToStart" title="Jump to start">
              <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor">
                <path d="M3 3h2v14H3V3zm4 7l10-7v14L7 10z"/>
              </svg>
            </button>
            <button class="control-btn play-btn" @click="togglePlayback">
              <svg v-if="isPlaying" width="20" height="20" viewBox="0 0 20 20" fill="currentColor">
                <path d="M6 4h3v12H6V4zm5 0h3v12h-3V4z"/>
              </svg>
              <svg v-else width="20" height="20" viewBox="0 0 20 20" fill="currentColor">
                <path d="M5 3l12 7-12 7V3z"/>
              </svg>
            </button>
            <button class="control-btn icon-btn" @click="jumpToEnd" title="Jump to end">
              <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor">
                <path d="M15 3h2v14h-2V3zM3 10l10 7V3L3 10z"/>
              </svg>
            </button>
          </div>

          <div class="speed-control">
            <label>Speed</label>
            <select v-model="playbackSpeed">
              <option :value="0.5">0.5×</option>
              <option :value="1">1×</option>
              <option :value="2">2×</option>
              <option :value="5">5×</option>
            </select>
          </div>
        </div>
      </div>
    </div>

    <div class="container">
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
  padding: 12px 20px;
}

.controls-inner {
  max-width: 1400px;
  margin: 0 auto;
}

.slider-section {
  margin-bottom: 12px;
}

.slider-label {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
  font-size: 12px;
  color: var(--on-surface-variant);
}

.event-counter {
  color: var(--primary);
  font-weight: 500;
}

.slider-container {
  position: relative;
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
  padding: 8px 16px;
  border-radius: 4px;
  font-family: 'Roboto', sans-serif;
  font-weight: 500;
  font-size: 14px;
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
</style>
