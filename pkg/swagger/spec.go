package swagger

// openAPISpecJSON is the complete OpenAPI 3.0.3 specification for gpu-vllm-router.
const openAPISpecJSON = `{
  "openapi": "3.0.3",
  "info": {
    "title": "GPUStack vLLM High-Performance Router & Load Balancer",
    "description": "An intelligent, high-performance OpenAI-compatible router and dynamic load balancer for vLLM / SGLang clusters orchestrated by GPUStack.\n\n### Key Capabilities\n- **OpenAI Compatible API**: Drop-in replacement for OpenAI API endpoints with streaming SSE support.\n- **Session Affinity & Prefix Cache Awareness**: Multi-turn conversation affinity via headers (` + "`" + `X-Session-ID` + "`" + `, ` + "`" + `X-User-ID` + "`" + `) or JSON payload.\n- **Dual-Layer Circuit Breaker & Failover**: Proactive health probing, in-flight request failover retry, and zero-downtime rolling reloads.\n- **Cluster-wide Multi-Model Auto-Sync**: Automatic discovery and management of all models across the GPUStack cluster.\n- **Prometheus Observability**: Native ` + "`" + `/metrics` + "`" + ` endpoint for Grafana dashboard integration.",
    "version": "1.0.0",
    "contact": {
      "name": "CodeFindc / gpu-vllm-router",
      "url": "https://github.com/CodeFindc/gpu-vllm-router"
    }
  },
  "servers": [
    {
      "url": "/",
      "description": "Current Server Gateway"
    }
  ],
  "tags": [
    {
      "name": "OpenAI Compatible Inference",
      "description": "OpenAI-compatible endpoints for LLM chat, completion, embeddings, and model listings"
    },
    {
      "name": "Control Plane & Management",
      "description": "Real-time cluster topology, worker statuses, circuit breaker states, and process management"
    },
    {
      "name": "Observability & Probes",
      "description": "Health check probes and Prometheus metrics"
    }
  ],
  "paths": {
    "/v1/chat/completions": {
      "post": {
        "tags": ["OpenAI Compatible Inference"],
        "summary": "Chat Completions",
        "description": "Generates a model response for the given chat conversation. Supports streaming SSE and session affinity for KV cache reuse.",
        "parameters": [
          {
            "name": "X-Session-ID",
            "in": "header",
            "required": false,
            "description": "Session identifier for consistent hash load balancing and KV cache prefix reuse across multi-turn conversations",
            "schema": {
              "type": "string",
              "example": "session_chat_12345"
            }
          },
          {
            "name": "X-User-ID",
            "in": "header",
            "required": false,
            "description": "User identifier for user-level session stickiness",
            "schema": {
              "type": "string",
              "example": "user_98765"
            }
          },
          {
            "name": "Authorization",
            "in": "header",
            "required": false,
            "description": "Bearer token for authorization (if configured)",
            "schema": {
              "type": "string",
              "example": "Bearer your-api-key"
            }
          }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/ChatCompletionRequest"
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Successful chat completion (or text/event-stream for SSE)",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/ChatCompletionResponse"
                }
              },
              "text/event-stream": {
                "description": "Server-Sent Events stream when stream=true"
              }
            }
          },
          "404": {
            "description": "Model not found or no healthy worker instances in cluster",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/ErrorResponse"
                }
              }
            }
          },
          "502": {
            "description": "Bad Gateway / Backend worker error / Retries exhausted",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/ErrorResponse"
                }
              }
            }
          },
          "503": {
            "description": "Service Temporarily Unavailable / Initializing",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/ErrorResponse"
                }
              }
            }
          }
        }
      }
    },
    "/v1/completions": {
      "post": {
        "tags": ["OpenAI Compatible Inference"],
        "summary": "Text Completions",
        "description": "Legacy text completion endpoint for prompts.",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["model", "prompt"],
                "properties": {
                  "model": {
                    "type": "string",
                    "example": "Qwen3.6-27B"
                  },
                  "prompt": {
                    "type": "string",
                    "example": "Once upon a time in AI,"
                  },
                  "max_tokens": {
                    "type": "integer",
                    "default": 128
                  },
                  "temperature": {
                    "type": "number",
                    "default": 0.7
                  },
                  "stream": {
                    "type": "boolean",
                    "default": false
                  }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Successful completion",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object"
                }
              }
            }
          }
        }
      }
    },
    "/v1/embeddings": {
      "post": {
        "tags": ["OpenAI Compatible Inference"],
        "summary": "Vector Embeddings",
        "description": "Generates embedding vectors representing the input text.",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["model", "input"],
                "properties": {
                  "model": {
                    "type": "string",
                    "example": "bge-large-zh-v1.5"
                  },
                  "input": {
                    "type": "string",
                    "example": "Retrieval Augmented Generation with LLM"
                  }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Successful embeddings",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "object": { "type": "string", "example": "list" },
                    "data": { "type": "array", "items": { "type": "object" } },
                    "model": { "type": "string" },
                    "usage": { "type": "object" }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/v1/models": {
      "get": {
        "tags": ["OpenAI Compatible Inference"],
        "summary": "List Models",
        "description": "Lists all available models currently synchronized and running in the GPUStack cluster.",
        "responses": {
          "200": {
            "description": "List of available model cards",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/ModelListResponse"
                }
              }
            }
          }
        }
      }
    },
    "/admin/stats": {
      "get": {
        "tags": ["Control Plane & Management"],
        "summary": "Proxy Mode Statistics & Circuit Breakers",
        "description": "Returns real-time proxy cluster status, backend targets, active connection counts, circuit breaker states (CLOSED/OPEN/HALF_OPEN), consecutive failure counts, and retry settings.",
        "responses": {
          "200": {
            "description": "Detailed proxy mode statistics",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/AdminStatsResponse"
                }
              }
            }
          }
        }
      }
    },
    "/admin/supervisor": {
      "get": {
        "tags": ["Control Plane & Management"],
        "summary": "Supervisor Mode Process Status",
        "description": "Returns supervisor status in mode: run, including running official vllm-router processes, internal ports, worker URLs, and proactive health breaker statuses.",
        "responses": {
          "200": {
            "description": "Detailed supervisor mode status",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object"
                }
              }
            }
          }
        }
      }
    },
    "/health": {
      "get": {
        "tags": ["Observability & Probes"],
        "summary": "Health Check Probe",
        "description": "Kubernetes liveness and readiness probe endpoint. Returns 200 OK when router has active backends.",
        "responses": {
          "200": {
            "description": "Service is healthy",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "status": {
                      "type": "string",
                      "example": "ok"
                    }
                  }
                }
              }
            }
          },
          "503": {
            "description": "Service is unavailable (no active model backends)",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "status": { "type": "string", "example": "unavailable" },
                    "message": { "type": "string" }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/metrics": {
      "get": {
        "tags": ["Observability & Probes"],
        "summary": "Prometheus Metrics",
        "description": "Prometheus-formatted monitoring metrics for Grafana dashboards, including model counts, backend status, active connections, circuit states, and consecutive failure counts.",
        "responses": {
          "200": {
            "description": "Prometheus text format metrics",
            "content": {
              "text/plain": {
                "schema": {
                  "type": "string",
                  "example": "# HELP gpu_router_models_total Total number of registered active models\n# TYPE gpu_router_models_total gauge\ngpu_router_models_total 2\n"
                }
              }
            }
          }
        }
      }
    },
    "/openapi.json": {
      "get": {
        "tags": ["Observability & Probes"],
        "summary": "OpenAPI Specification JSON",
        "description": "Returns this OpenAPI 3.0.3 specification in machine-readable JSON format for Postman, Apifox, or API gateway import.",
        "responses": {
          "200": {
            "description": "OpenAPI 3.0.3 specification",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object"
                }
              }
            }
          }
        }
      }
    }
  },
  "components": {
    "schemas": {
      "ChatMessage": {
        "type": "object",
        "required": ["role", "content"],
        "properties": {
          "role": {
            "type": "string",
            "enum": ["system", "user", "assistant", "tool"],
            "example": "user"
          },
          "content": {
            "type": "string",
            "example": "Hello! How do you handle circuit breaking in GPU clusters?"
          }
        }
      },
      "ChatCompletionRequest": {
        "type": "object",
        "required": ["model", "messages"],
        "properties": {
          "model": {
            "type": "string",
            "description": "Target model name registered in GPUStack cluster",
            "example": "DeepSeek-V4-Flash-0731-w8a8"
          },
          "messages": {
            "type": "array",
            "items": {
              "$ref": "#/components/schemas/ChatMessage"
            }
          },
          "stream": {
            "type": "boolean",
            "default": false,
            "description": "If set, partial message deltas will be sent as SSE events"
          },
          "temperature": {
            "type": "number",
            "default": 0.7,
            "description": "Sampling temperature between 0 and 2"
          },
          "top_p": {
            "type": "number",
            "default": 1.0,
            "description": "Nucleus sampling parameter"
          },
          "max_tokens": {
            "type": "integer",
            "description": "Maximum number of tokens to generate"
          },
          "presence_penalty": {
            "type": "number",
            "default": 0
          },
          "frequency_penalty": {
            "type": "number",
            "default": 0
          }
        }
      },
      "ChatCompletionResponse": {
        "type": "object",
        "properties": {
          "id": { "type": "string", "example": "chatcmpl-9abc123" },
          "object": { "type": "string", "example": "chat.completion" },
          "created": { "type": "integer", "example": 1725870000 },
          "model": { "type": "string", "example": "DeepSeek-V4-Flash-0731-w8a8" },
          "choices": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "index": { "type": "integer" },
                "message": { "$ref": "#/components/schemas/ChatMessage" },
                "finish_reason": { "type": "string", "example": "stop" }
              }
            }
          },
          "usage": {
            "type": "object",
            "properties": {
              "prompt_tokens": { "type": "integer" },
              "completion_tokens": { "type": "integer" },
              "total_tokens": { "type": "integer" }
            }
          }
        }
      },
      "ModelListResponse": {
        "type": "object",
        "properties": {
          "object": { "type": "string", "example": "list" },
          "data": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "id": { "type": "string", "example": "DeepSeek-V4-Flash-0731-w8a8" },
                "object": { "type": "string", "example": "model" },
                "created": { "type": "integer", "example": 1725870000 },
                "owned_by": { "type": "string", "example": "gpustack" }
              }
            }
          }
        }
      },
      "AdminStatsResponse": {
        "type": "object",
        "properties": {
          "mode": { "type": "string", "example": "multi_model_auto_sync" },
          "policy": { "type": "string", "example": "consistent_hash" },
          "model_count": { "type": "integer", "example": 2 },
          "total_backends": { "type": "integer", "example": 4 },
          "models": { "type": "object" },
          "circuit_breaker_config": {
            "type": "object",
            "properties": {
              "enabled": { "type": "boolean", "example": true },
              "max_failures": { "type": "integer", "example": 3 },
              "cooldown": { "type": "string", "example": "10s" },
              "max_retries": { "type": "integer", "example": 2 },
              "health_check_interval": { "type": "string", "example": "3s" }
            }
          }
        }
      },
      "ErrorResponse": {
        "type": "object",
        "properties": {
          "error": {
            "type": "object",
            "properties": {
              "message": { "type": "string", "example": "The model 'unknown-model' does not exist or has no healthy instances in GPUStack" },
              "type": { "type": "string", "example": "invalid_request_error" },
              "param": { "type": "string", "example": "model" },
              "code": { "type": "string", "example": "model_not_found" }
            }
          }
        }
      }
    }
  }
}`

// GetOpenAPISpec returns the raw OpenAPI 3.0.3 specification JSON bytes.
func GetOpenAPISpec() []byte {
	return []byte(openAPISpecJSON)
}
