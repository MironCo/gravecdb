package embedding

// MockEmbedder is a test embedder that returns predefined vectors for known texts
type MockEmbedder struct {
	embeddings map[string][]float32
}

// NewMockEmbedder creates a new mock embedder for testing
func NewMockEmbedder() *MockEmbedder {
	return &MockEmbedder{
		embeddings: map[string][]float32{
			"backend engineers":                           {0.8, 0.2, 0.1},
			"frontend developers":                         {0.2, 0.8, 0.1},
			"data scientists":                             {0.1, 0.2, 0.8},
			"Person. name: Alice. role: backend engineer": {0.75, 0.25, 0.1},
			"Person. name: Bob. role: frontend developer": {0.25, 0.75, 0.1},
			"Person. name: Carol. role: data scientist":   {0.1, 0.25, 0.75},
			"backend engineer":                            {0.75, 0.25, 0.1},
			"frontend developer":                          {0.25, 0.75, 0.1},
			"data scientist":                              {0.1, 0.25, 0.75},
		},
	}
}

// Embed returns the predefined vector for known texts or a default vector
func (m *MockEmbedder) Embed(text string) ([]float32, error) {
	if vec, ok := m.embeddings[text]; ok {
		return vec, nil
	}
	// Return a default embedding for unknown text
	return []float32{0.33, 0.33, 0.33}, nil
}

// Model returns the mock model name
func (m *MockEmbedder) Model() string {
	return "mock-embedder"
}
