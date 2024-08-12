#!/bin/bash
set -e


function check_docker_compose {
    if ! command -v docker-compose &> /dev/null; then
        echo "docker-compose could not be found. Please install Docker Compose."
        exit 1
    fi
}


function build_images {
    echo "Building Docker images..."
    docker-compose build
}


function start_services {
    echo "Starting Docker Compose services..."
    docker-compose up -d
}


function check_health {
    echo "Waiting for services to be healthy..."

    
    while ! docker inspect -f '{{.State.Health.Status}}' postgres | grep -q "healthy"; do
        echo "Postgres is not healthy yet..."
        sleep 5
    done

    
    while ! docker inspect -f '{{.State.Health.Status}}' redis | grep -q "healthy"; do
        echo "Redis is not healthy yet..."
        sleep 5
    done

    
    while ! curl -s http://localhost:8080/health | grep -q "OK"; do
        echo "Airflow webserver is not healthy yet..."
        sleep 5
    done
}


check_docker_compose
build_images
start_services
check_health

echo "All services are up and running!"
