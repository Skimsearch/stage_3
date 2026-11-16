# Search gateway: Consul + Consul Template + Nginx

Search instances register themselves in Consul (`CONSUL_URL`) with an HTTP check on
`/health/ready`. Consul Template writes the Nginx upstream with the instances whose
check is passing, and reloads Nginx. Tested with Consul 1.20.1, Consul Template 0.39.1
and Nginx 1.26.2.

| Route | Exposed by the gateway | Backend path |
|---|---|---|
| `GET /search?q=...` | yes (port 8080) | same path, no translation |
| `GET /books/{id}` | yes | same path |
| `GET /health/ready`, `/health/live` | **no** (404) | checked by Consul directly on each Search |

* Retries: a request is retried once on another Search on connection error, timeout
  or HTTP 503 (`proxy_next_upstream error timeout http_503`).
* `logs/gateway.log`: one line per request with `msec`, `req` (X-Request-Id), `upstream`
  and `upstream_status` (a retry shows as `"503, 200"`), `backend` (X-Backend-Id) and
  `version` (X-Index-Version).

## Files

* `nginx.conf`: gateway on port 8080.
* `search-upstream.ctmpl`: template for the upstream (generated `search-upstream.conf`
  is not committed).
* `consul-template.hcl`: renders the template and runs `nginx -s reload`.

## Run on one machine (development)

Consul, consul-template and nginx on the PATH. From this folder (`deploy/gateway`):

```
consul agent -dev
mkdir logs temp
consul-template -config consul-template.hcl -once
nginx -p . -c nginx.conf
consul-template -config consul-template.hcl
```

Then start each Search with `CONSUL_URL=http://127.0.0.1:8500` and
`SEARCH_ADVERTISE_HOST=<this machine's IP>`.

Stop: `nginx -p . -c nginx.conf -s stop`, then Ctrl+C in the other windows.

## Lab (several machines)

* One Consul server, on the gateway machine:
  `consul agent -server -bootstrap-expect=1 -data-dir consul-data -bind <gateway-ip> -client 0.0.0.0 -ui`
* One Consul client on every worker machine:
  `consul agent -data-dir consul-data -bind <worker-ip> -retry-join <gateway-ip>`
* Each Search registers with the Consul agent on its own machine
  (`CONSUL_URL=http://127.0.0.1:8500`) and must set `SEARCH_ADVERTISE_HOST` to its own IP.
* Consul Template and Nginx run only on the gateway machine.

Consul and Nginx are supporting infrastructure, not application workers: if the gateway
machine fails, the public API is down even if every Search is healthy.