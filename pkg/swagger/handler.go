package swagger

import (
	"net/http"
	"strings"
)

const swaggerUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>GPUStack vLLM Router - Swagger UI</title>
  <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  <link rel="icon" type="image/png" href="https://unpkg.com/swagger-ui-dist@5/favicon-32x32.png" sizes="32x32" />
  <style>
    html {
      box-sizing: border-box;
      overflow: -moz-scrollbars-vertical;
      overflow-y: scroll;
    }
    *, *:before, *:after {
      box-sizing: inherit;
    }
    body {
      margin: 0;
      background: #fafafa;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }
    .custom-topbar {
      background-color: #1a1e24;
      padding: 12px 30px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      color: #ffffff;
      box-shadow: 0 2px 8px rgba(0,0,0,0.15);
    }
    .custom-topbar .brand {
      font-size: 18px;
      font-weight: 700;
      color: #61affe;
      text-decoration: none;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .custom-topbar .nav-links {
      display: flex;
      gap: 18px;
      font-size: 14px;
    }
    .custom-topbar .nav-links a {
      color: #b0c0d0;
      text-decoration: none;
      transition: color 0.2s;
    }
    .custom-topbar .nav-links a:hover {
      color: #ffffff;
      text-decoration: underline;
    }
    .swagger-ui .topbar {
      display: none;
    }
  </style>
</head>
<body>
  <div class="custom-topbar">
    <a href="/docs" class="brand">
      🚀 GPUStack vLLM Router CLI
    </a>
    <div class="nav-links">
      <a href="/redoc" target="_blank">📖 ReDoc View</a>
      <a href="/admin/stats" target="_blank">📊 Cluster Stats</a>
      <a href="/metrics" target="_blank">📈 Prometheus Metrics</a>
      <a href="/health" target="_blank">🟢 Health Probe</a>
      <a href="/openapi.json" target="_blank">📄 OpenAPI JSON</a>
    </div>
  </div>

  <div id="swagger-ui"></div>

  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" charset="UTF-8"></script>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-standalone-preset.js" charset="UTF-8"></script>
  <script>
    window.onload = function() {
      window.ui = SwaggerUIBundle({
        url: "/openapi.json",
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        plugins: [
          SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "BaseLayout",
        defaultModelsExpandDepth: 1,
        defaultModelExpandDepth: 1,
        docExpansion: "list",
        persistAuthorization: true
      });
    };
  </script>
</body>
</html>`

const redocHTML = `<!DOCTYPE html>
<html>
<head>
  <title>GPUStack vLLM Router - ReDoc API Docs</title>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">
  <style>
    body {
      margin: 0;
      padding: 0;
    }
  </style>
</head>
<body>
  <redoc spec-url='/openapi.json' expand-responses="200,404" hide-download-button="false"></redoc>
  <script src="https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js"></script>
</body>
</html>`

// Handler serves the interactive Swagger UI.
func Handler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/swagger" {
		http.Redirect(w, r, "/swagger/", http.StatusMovedPermanently)
		return
	}
	if strings.HasSuffix(r.URL.Path, "doc.json") {
		DocJSONHandler(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(swaggerUIHTML))
}

// DocJSONHandler serves the raw OpenAPI 3.0.3 specification JSON.
func DocJSONHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(GetOpenAPISpec())
}

// RedocHandler serves the ReDoc interface.
func RedocHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(redocHTML))
}

// DocsRedirectHandler redirects /docs to /swagger/.
func DocsRedirectHandler(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/swagger/", http.StatusFound)
}
