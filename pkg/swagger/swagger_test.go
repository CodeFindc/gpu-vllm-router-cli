package swagger

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenAPISpecJSONValid(t *testing.T) {
	specBytes := GetOpenAPISpec()
	if len(specBytes) == 0 {
		t.Fatal("expected non-empty OpenAPI spec bytes")
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(specBytes, &parsed); err != nil {
		t.Fatalf("OpenAPI specification is not valid JSON: %v", err)
	}

	if parsed["openapi"] != "3.0.3" {
		t.Errorf("expected openapi version 3.0.3, got %v", parsed["openapi"])
	}

	paths, ok := parsed["paths"].(map[string]interface{})
	if !ok {
		t.Fatal("expected 'paths' object in OpenAPI spec")
	}

	requiredPaths := []string{
		"/v1/chat/completions",
		"/v1/completions",
		"/v1/embeddings",
		"/v1/models",
		"/admin/stats",
		"/admin/supervisor",
		"/health",
		"/metrics",
		"/openapi.json",
	}

	for _, p := range requiredPaths {
		if _, exists := paths[p]; !exists {
			t.Errorf("expected path %q in OpenAPI spec, but was missing", p)
		}
	}
}

func TestSwaggerHandlers(t *testing.T) {
	// 1. Test Swagger UI Handler
	rwUI := httptest.NewRecorder()
	reqUI, _ := http.NewRequest(http.MethodGet, "/swagger/", nil)
	Handler(rwUI, reqUI)

	if rwUI.Code != http.StatusOK {
		t.Errorf("expected /swagger/ to return 200, got %d", rwUI.Code)
	}
	if !strings.Contains(rwUI.Header().Get("Content-Type"), "text/html") {
		t.Errorf("expected text/html for /swagger/, got %s", rwUI.Header().Get("Content-Type"))
	}
	if !strings.Contains(rwUI.Body.String(), "SwaggerUIBundle") {
		t.Errorf("expected SwaggerUIBundle script in /swagger/ HTML")
	}

	// 2. Test Swagger redirect from /swagger
	rwRedirect := httptest.NewRecorder()
	reqRedirect, _ := http.NewRequest(http.MethodGet, "/swagger", nil)
	Handler(rwRedirect, reqRedirect)
	if rwRedirect.Code != http.StatusMovedPermanently {
		t.Errorf("expected 301 for /swagger, got %d", rwRedirect.Code)
	}

	// 3. Test DocJSONHandler
	rwDoc := httptest.NewRecorder()
	reqDoc, _ := http.NewRequest(http.MethodGet, "/openapi.json", nil)
	DocJSONHandler(rwDoc, reqDoc)

	if rwDoc.Code != http.StatusOK {
		t.Errorf("expected /openapi.json to return 200, got %d", rwDoc.Code)
	}
	if !strings.Contains(rwDoc.Header().Get("Content-Type"), "application/json") {
		t.Errorf("expected application/json, got %s", rwDoc.Header().Get("Content-Type"))
	}

	// 4. Test ReDoc Handler
	rwRedoc := httptest.NewRecorder()
	reqRedoc, _ := http.NewRequest(http.MethodGet, "/redoc", nil)
	RedocHandler(rwRedoc, reqRedoc)

	if rwRedoc.Code != http.StatusOK {
		t.Errorf("expected /redoc to return 200, got %d", rwRedoc.Code)
	}
	if !strings.Contains(rwRedoc.Header().Get("Content-Type"), "text/html") {
		t.Errorf("expected text/html for /redoc, got %s", rwRedoc.Header().Get("Content-Type"))
	}

	// 5. Test DocsRedirectHandler
	rwDocs := httptest.NewRecorder()
	reqDocs, _ := http.NewRequest(http.MethodGet, "/docs", nil)
	DocsRedirectHandler(rwDocs, reqDocs)

	if rwDocs.Code != http.StatusFound {
		t.Errorf("expected 302 for /docs, got %d", rwDocs.Code)
	}
	if rwDocs.Header().Get("Location") != "/swagger/" {
		t.Errorf("expected Location /swagger/, got %s", rwDocs.Header().Get("Location"))
	}
}

func TestExportStaticDocs(t *testing.T) {
	docsDir := filepath.Join("..", "..", "docs")
	if err := os.MkdirAll(docsDir, 0755); err != nil {
		t.Fatalf("failed to create docs directory: %v", err)
	}

	jsonPath := filepath.Join(docsDir, "openapi.json")
	if err := os.WriteFile(jsonPath, GetOpenAPISpec(), 0644); err != nil {
		t.Fatalf("failed to write docs/openapi.json: %v", err)
	}

	data, err := os.ReadFile(jsonPath)
	if err != nil || len(data) == 0 {
		t.Fatalf("failed to read written docs/openapi.json: %v", err)
	}
}
