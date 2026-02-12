<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import cytoscape from 'cytoscape'

const graphData = ref({ nodes: [], relationships: [] })
const timelineEvents = ref([])
const currentTimeIndex = ref(0)
const selectedNode = ref(null)
const cy = ref(null)
const isPlaying = ref(false)
const playbackSpeed = ref(1)
const playbackInterval = ref(null)

const currentTimeDisplay = computed(() => {
  if (timelineEvents.value.length === 0) return 'No data'
  const event = timelineEvents.value[currentTimeIndex.value]
  if (!event) return 'Loading...'
  const date = new Date(event.timestamp)
  return date.toLocaleTimeString()
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
})

async function loadTimeline() {
  const res = await fetch('/api/timeline')
  timelineEvents.value = await res.json()
  timelineEvents.value.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp))
  currentTimeIndex.value = timelineEvents.value.length - 1
}

async function loadGraph() {
  if (currentTimeIndex.value >= timelineEvents.value.length) {
    currentTimeIndex.value = timelineEvents.value.length - 1
  }

  // If we're at the very end of the timeline, show the current state
  // Otherwise, query at the specific event timestamp
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
          'background-color': '#64b5f6',
          'label': 'data(label)',
          'color': '#fff',
          'text-valign': 'center',
          'text-halign': 'center',
          'font-size': '12px',
          'width': 60,
          'height': 60
        }
      },
      {
        selector: 'node[type="Company"]',
        style: {
          'background-color': '#81c784',
          'shape': 'rectangle'
        }
      },
      {
        selector: 'node.deleted',
        style: {
          'background-color': '#ef5350',
          'opacity': 0.5,
          'border-width': 2,
          'border-color': '#c62828'
        }
      },
      {
        selector: 'edge',
        style: {
          'width': 3,
          'line-color': '#90caf9',
          'target-arrow-color': '#90caf9',
          'target-arrow-shape': 'triangle',
          'curve-style': 'bezier',
          'label': 'data(label)',
          'font-size': '10px',
          'color': '#90caf9',
          'text-background-opacity': 1,
          'text-background-color': '#0f1423',
          'text-background-padding': '3px'
        }
      },
      {
        selector: 'edge.deleted',
        style: {
          'line-color': '#ef5350',
          'target-arrow-color': '#ef5350',
          'opacity': 0.4,
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
  })

  renderGraph()
}

function renderGraph() {
  if (!cy.value) return

  const elements = []

  // Add nodes
  graphData.value.nodes.forEach(node => {
    const label = node.properties.name || node.labels[0] || 'Node'
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

  // Add edges
  graphData.value.relationships.forEach(rel => {
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
  })

  cy.value.elements().remove()
  cy.value.add(elements)
  cy.value.layout({
    name: 'cose',
    animate: true,
    animationDuration: 300
  }).run()
}

async function onTimeSliderChange() {
  await loadGraph()
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
    stopPlayback()
    startPlayback()
  }
})
</script>

<template>
  <div id="app">
    <header>
      <h1>⏱️ Temporal Graph Database</h1>
    </header>

    <div class="controls">
      <div class="time-display">
        {{ currentTimeDisplay }}
      </div>

      <div class="slider-container">
        <input
          type="range"
          :min="0"
          :max="timelineEvents.length - 1"
          v-model="currentTimeIndex"
          @input="onTimeSliderChange"
        />
      </div>

      <div class="playback-controls">
        <button @click="jumpToStart">⏮ Start</button>
        <button @click="togglePlayback">{{ isPlaying ? '⏸ Pause' : '▶ Play' }}</button>
        <button @click="jumpToEnd">⏭ End</button>
      </div>

      <div class="speed-control">
        <select v-model="playbackSpeed">
          <option :value="0.5">0.5x</option>
          <option :value="1">1x</option>
          <option :value="2">2x</option>
          <option :value="5">5x</option>
        </select>
      </div>
    </div>

    <div class="container">
      <div id="cy"></div>

      <div class="sidebar">
        <div class="stats">
          <div class="stat-item">
            <span class="stat-label">Nodes:</span>
            <span class="stat-value">{{ graphData.nodes.length }}</span>
          </div>
          <div class="stat-item">
            <span class="stat-label">Relationships:</span>
            <span class="stat-value">{{ graphData.relationships.length }}</span>
          </div>
          <div class="stat-item">
            <span class="stat-label">Timeline Events:</span>
            <span class="stat-value">{{ timelineEvents.length }}</span>
          </div>
        </div>

        <div class="node-details" v-if="selectedNode">
          <h3>{{ selectedNode.properties.name || 'Node' }}</h3>

          <div class="property" v-for="(value, key) in selectedNode.properties" :key="key">
            <span class="property-key">{{ key }}:</span>
            <span class="property-value">{{ value }}</span>
          </div>

          <div class="property">
            <span class="property-key">Labels:</span>
            <span class="property-value">{{ selectedNode.labels.join(', ') }}</span>
          </div>

          <div class="property" v-if="selectedNode.validTo">
            <span class="property-key">Status:</span>
            <span class="property-value" style="color: #ef5350;">Deleted</span>
          </div>

          <div v-if="selectedNodeRelationships.length > 0" class="relationships-section">
            <h4>Relationships</h4>
            <div
              v-for="(rel, idx) in selectedNodeRelationships"
              :key="idx"
              class="relationship-item"
              :class="{ deleted: rel.deleted }"
            >
              <div class="rel-header">
                <span class="rel-direction">{{ rel.direction === 'outgoing' ? '→' : '←' }}</span>
                <span class="rel-type">{{ rel.type }}</span>
                <span class="rel-other-node">{{ rel.otherNode }}</span>
              </div>
              <div v-if="Object.keys(rel.properties).length > 0" class="rel-properties">
                <div v-for="(value, key) in rel.properties" :key="key" class="rel-property">
                  <span class="property-key">{{ key }}:</span>
                  <span class="property-value">{{ value }}</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style>
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
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: #0a0e27;
  color: #fff;
}

#app {
  display: flex;
  flex-direction: column;
  width: 100%;
  height: 100vh;
}

header {
  background: #1a1f3a;
  padding: 1rem 2rem;
  border-bottom: 2px solid #2a3f5f;
}

h1 {
  font-size: 1.5rem;
  font-weight: 600;
  color: #64b5f6;
}

.container {
  display: flex;
  flex: 1;
  overflow: hidden;
}

#cy {
  flex: 1;
  background: #0f1423;
}

.sidebar {
  width: 320px;
  background: #1a1f3a;
  border-left: 2px solid #2a3f5f;
  padding: 1.5rem;
  overflow-y: auto;
}

.controls {
  background: #151932;
  padding: 1.5rem;
  border-bottom: 2px solid #2a3f5f;
}

.timeline-controls {
  margin-top: 1rem;
}

.slider-container {
  margin: 1rem 0;
}

input[type="range"] {
  width: 100%;
  height: 6px;
  background: #2a3f5f;
  outline: none;
  border-radius: 3px;
}

input[type="range"]::-webkit-slider-thumb {
  appearance: none;
  width: 18px;
  height: 18px;
  background: #64b5f6;
  cursor: pointer;
  border-radius: 50%;
}

.time-display {
  text-align: center;
  font-size: 0.9rem;
  color: #64b5f6;
  margin-bottom: 1rem;
}

.playback-controls {
  display: flex;
  gap: 0.5rem;
  justify-content: center;
  margin-top: 1rem;
}

button {
  background: #64b5f6;
  color: #0a0e27;
  border: none;
  padding: 0.6rem 1.2rem;
  border-radius: 6px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
}

button:hover {
  background: #90caf9;
  transform: translateY(-1px);
}

button:active {
  transform: translateY(0);
}

button.secondary {
  background: #2a3f5f;
  color: #fff;
}

button.secondary:hover {
  background: #3a4f6f;
}

.node-details {
  margin-top: 1.5rem;
}

.node-details h3 {
  font-size: 1rem;
  margin-bottom: 0.8rem;
  color: #64b5f6;
}

.property {
  background: #0f1423;
  padding: 0.6rem;
  margin-bottom: 0.5rem;
  border-radius: 4px;
  font-size: 0.85rem;
}

.property-key {
  color: #90caf9;
  font-weight: 600;
}

.property-value {
  color: #fff;
  margin-left: 0.5rem;
}

.deleted {
  opacity: 0.5;
  border: 1px solid #ef5350;
}

.stats {
  padding: 1rem;
  background: #0f1423;
  border-radius: 6px;
}

.stat-item {
  display: flex;
  justify-content: space-between;
  margin-bottom: 0.5rem;
  font-size: 0.9rem;
}

.stat-label {
  color: #90caf9;
}

.stat-value {
  font-weight: 600;
}

.speed-control {
  margin-top: 0.5rem;
}

select {
  background: #2a3f5f;
  color: #fff;
  border: none;
  padding: 0.5rem;
  border-radius: 4px;
  width: 100%;
  font-size: 0.9rem;
}

.relationships-section {
  margin-top: 1.5rem;
}

.relationships-section h4 {
  font-size: 0.9rem;
  margin-bottom: 0.8rem;
  color: #64b5f6;
}

.relationship-item {
  background: #0f1423;
  padding: 0.8rem;
  margin-bottom: 0.5rem;
  border-radius: 4px;
  font-size: 0.85rem;
}

.rel-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.5rem;
}

.rel-direction {
  color: #90caf9;
  font-size: 1.2rem;
}

.rel-type {
  color: #64b5f6;
  font-weight: 600;
}

.rel-other-node {
  color: #fff;
}

.rel-properties {
  margin-left: 1.5rem;
  padding-top: 0.5rem;
  border-top: 1px solid #2a3f5f;
}

.rel-property {
  margin-bottom: 0.3rem;
}
</style>
