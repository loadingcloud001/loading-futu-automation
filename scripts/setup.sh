#!/bin/bash
# Setup script for loading-futu-automation
# Installs Docker and deploys futu automation services

set -e

DROPLET_IP="${1:-138.197.126.250}"
SSH_KEY="${HOME}/.ssh/id_ed25519_cicd"

echo "=== Loading Futu Automation Setup ==="
echo "Target: root@${DROPLET_IP}"

# Check if Docker is installed
if ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no root@"$DROPLET_IP" "which docker > /dev/null 2>&1"; then
    echo "[OK] Docker is already installed"
else
    echo "[INFO] Docker not found, installing..."

    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no root@"$DROPLET_IP" << 'INSTALL_DOCKER'
        set -e

        # Install Docker
        apt-get update
        apt-get install -y ca-certificates curl gnupg lsb-release

        mkdir -p /etc/apt/keyrings
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg

        echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

        apt-get update
        apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

        # Enable and start Docker
        systemctl enable docker
        systemctl start docker

        # Add docker group if not exists
        usermod -aG docker root || true

        echo "[OK] Docker installed successfully"
INSTALL_DOCKER
fi

# Pull futu-opend image
echo "[INFO] Pulling futu-opend image..."
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no root@"$DROPLET_IP" "docker pull manhinhang/futu-opend-docker:ubuntu-stable"

# Check if futu-opend container exists and is running
if ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no root@"$DROPLET_IP" "docker ps -a --filter name=futu-opend --format '{{.Names}}'" | grep -q futu-opend; then
    echo "[INFO] futu-opend container exists"
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no root@"$DROPLET_IP" "docker rm -f futu-opend || true"
fi

echo "[OK] Setup complete!"
echo ""
echo "To deploy services:"
echo "  docker-compose -f /opt/loading-futu-automation/docker-compose.yml up -d"
