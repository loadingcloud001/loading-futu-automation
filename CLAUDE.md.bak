# loading-futu-automation

Futu trading automation - deployed to DO002 droplet (138.197.126.250)

## Services

| Service | Description | Port |
|---------|-------------|------|
| futu-opend | Futu OpenD trading API daemon | 11111 |

## Architecture

```
DO002 Droplet (138.197.126.250)
└── futu-opend (Docker container)
    └── Connects to Futu trading API
```

## Deployment

**MANDATORY: Use GitHub Actions CI/CD only. Never manual SSH deploy.**

- SSH only for: debugging, reading logs, health checks
- Resource limits: 512MB memory, 0.5 CPU
- Log rotation: max-size 10m, max-file 3

## Setup

1. Push to main branch
2. GitHub Actions deploys automatically
3. Check logs: `docker logs futu-opend`

## Logs

```bash
# View futu-opend logs
docker logs futu-opend

# Follow logs
docker logs -f futu-opend
```

## Phone Verification

If futu-opend requires phone verification on first run:
1. SSH to DO002
2. Run: `docker attach futu-opend`
3. Enter the verification code when prompted
4. Detach with Ctrl+P, Ctrl+Q
