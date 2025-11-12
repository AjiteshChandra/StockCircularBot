import platform
import subprocess
import json
import requests
import docker
import argparse
import sys
import time
import logging
import uuid
from pathlib import Path
import os
import docker
from docker.errors import NotFound, APIError
import time

class QdrantManager:
    def __init__(self, container_name='qdrant', rest_port=6333, grpc_port=6334):
        self.container_name = container_name
        self.rest_port = rest_port
        self.grpc_port = grpc_port
        self.client = None
    def start_docker_service(self):
            system = platform.system()
            
            if system == "Linux":
                # On Linux with systemd
                try:
                    subprocess.run(['sudo', 'systemctl', 'start', 'docker'], check=True)
                    print("Docker service started")
                    
                except subprocess.CalledProcessError as e:
                    print(f"Failed to start Docker: {e}")
            
            elif system == "Darwin":  # macOS
                # Start Docker Desktop on macOS
                subprocess.Popen(['open', '-a', 'Docker'])
                print("Docker Desktop starting...")
               
            
            elif system == "Windows":
                # Start Docker Desktop on Windows
                subprocess.Popen(['C:\\Program Files\\Docker\\Docker\\Docker Desktop.exe'])
                print("Docker Desktop starting...")
                time.sleep(10)
               

            self.client = docker.from_env()
    def start(self):
        """Start Qdrant container"""
        try:
            # Check if already exists
            try:
                container = self.client.containers.get(self.container_name)
                if container.status == 'running':
                    print("Qdrant is already running")
                    print(f"  Dashboard: http://localhost:{self.rest_port}/dashboard")
                    return container
                else:
                    print("Starting existing container...")
                    print(f"  Dashboard: http://localhost:{self.rest_port}/dashboard")
                    container.start()
                    time.sleep(7)
                    return container
            except NotFound:
                pass
            
            # Create new container
            print("Creating Qdrant container...")
            volume_name = f'{self.container_name}_data'
            
            container = self.client.containers.run(
                image='qdrant/qdrant:latest',
                name=self.container_name,
                detach=True,
                ports={
                    '6333/tcp': self.rest_port,
                    '6334/tcp': self.grpc_port
                },
                volumes={
                    volume_name: {
                        'bind': '/qdrant/storage',
                        'mode': 'rw'
                    }
                },
                restart_policy={"Name": "unless-stopped"}
            )
            
            print(f"Qdrant started")
            print(f"  Dashboard: http://localhost:{self.rest_port}/dashboard")
            time.sleep(5)
            
            return container
            
        except Exception as e:
            print(f"Error starting Qdrant: {e}")
            return None
    
    def stop(self, timeout=30):
        """Stop Qdrant container gracefully"""
        self.client = docker.from_env()
        try:
            container = self.client.containers.get(self.container_name)
            
            if container.status != 'running':
                print(f"Container not running (status: {container.status})")
                return True
            
            print(f"Stopping {self.container_name}...")
            container.stop(timeout=timeout)
            
            # Verify it stopped
            container.reload()
            if container.status == 'exited':
                print("Container stopped successfully")
                return True
            else:
                print(f"Warning: Container status is {container.status}")
                return False
                
        except NotFound:
            print("Container not found")
            return False
        except Exception as e:
            print(f"Error stopping container: {e}")
            return False
    
    def restart(self, timeout=30):
        """Restart Qdrant container"""
        print("Restarting Qdrant...")
        self.stop(timeout=timeout)
        time.sleep(2)
        return self.start()
    
 
    def status(self):
        """Check container status"""
        self.client = docker.from_env()
        try:
            container = self.client.containers.get(self.container_name)
            container.reload()
            print(f"Status: {container.status}")
            print(f"Name: {container.name}")
            print(f"ID: {container.short_id}")
            return container.status
        except NotFound:
            print("Container not found")
            return None
    
    def remove(self, remove_volume=False):
        """Remove container and optionally volume"""
        self.stop(timeout=30)
        
        try:
            container = self.client.containers.get(self.container_name)
            container.remove()
            print("Container removed")
        except NotFound:
            print("Container not found")
        
        if remove_volume:
            try:
                volume = self.client.volumes.get(f'{self.container_name}_data')
                volume.remove()
                print("Volume removed")
            except NotFound:
                print("Volume not found")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--option',choices=['start', 'stop','status','remove'],default='start')
    parser.add_argument("--remove_volume",choices=["yes","no"],default='no')
    args = parser.parse_args()

    manager = QdrantManager()
    if args.option == 'start':
        manager.start_docker_service()
        # Start
        manager.start()
    
    elif args.option == 'status':
        # Check status
        manager.status()
    elif args.option == 'stop' :
        # Stop properly
        manager.stop()
    else:
        manager.remove(remove_volume=args.remove_volumne)
