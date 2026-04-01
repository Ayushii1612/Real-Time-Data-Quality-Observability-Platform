terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {}

resource "docker_network" "obs_network" {
  name = "observability-net"
}

resource "docker_volume" "postgres_data" {
  name = "obs-postgres-data"
}

resource "docker_volume" "grafana_data" {
  name = "obs-grafana-data"
}

resource "docker_container" "postgres" {
  name  = "obs-postgres"
  image = "postgres:15"
  networks_advanced { name = docker_network.obs_network.name }
  env = [
    "POSTGRES_DB=observability",
    "POSTGRES_USER=obsuser",
    "POSTGRES_PASSWORD=obspass"
  ]
  ports { internal = 5432; external = 5432 }
  volumes { volume_name = docker_volume.postgres_data.name; container_path = "/var/lib/postgresql/data" }
}

resource "docker_container" "redis" {
  name  = "obs-redis"
  image = "redis:7"
  networks_advanced { name = docker_network.obs_network.name }
  ports { internal = 6379; external = 6379 }
}

resource "docker_container" "grafana" {
  name  = "obs-grafana"
  image = "grafana/grafana:10.2.0"
  networks_advanced { name = docker_network.obs_network.name }
  env   = ["GF_SECURITY_ADMIN_PASSWORD=admin"]
  ports { internal = 3000; external = 3000 }
  volumes { volume_name = docker_volume.grafana_data.name; container_path = "/var/lib/grafana" }
}

output "grafana_url"   { value = "http://localhost:3000" }
output "api_url"       { value = "http://localhost:8000" }
output "airflow_url"   { value = "http://localhost:8080" }