# Run from this folder: consul-template -config consul-template.hcl
# Rewrites the Nginx upstream whenever the set of passing "search" instances changes,
# then reloads Nginx (nginx must be on the PATH).

consul {
  address = "127.0.0.1:8500"
}

log_level = "info"

template {
  source          = "search-upstream.ctmpl"
  destination     = "search-upstream.conf"
  command         = ["nginx", "-p", ".", "-c", "nginx.conf", "-s", "reload"]
  command_timeout = "10s"
}