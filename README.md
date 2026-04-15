# loading-futu-automation

Docker-based Futu OpenD and OptionPython trading automation.

## Overview

This project runs two Docker containers:

1. **futu-opend** - Futu's OpenD gateway (Ubuntu stable image)
2. **option-python** - A Python automation that:
   - Connects to FutuOpenD via the futu-api
   - Fetches option chain data (delta, IV, turnover) for HK/US stocks
   - Writes results to Google Sheets
   - Runs on a configurable schedule (default: 04:00 UTC)

## Components

### FutuOpenD

The official Futu OpenD Docker image (`manhinhang/futu-opend-docker:ubuntu-stable`).

- **API Port**: 11111 (mapped to host)
- **Telnet Port**: 22222 (mapped to host)
- **Login**: Uses `FUTU_ACCOUNT_ID` and `FUTU_ACCOUNT_PWD` (MD5 hashed)
- **Config**: `FutuOpenD.xml` is injected via `start.sh` at container startup
- **Data persistence**: `futu-opend-data` volume stores `/.futu` config

### OptionPython

Custom Python 3.9 image that runs `app.py`.

- Connects to FutuOpenD at `futu-opend:11111`
- Reads stock list from `/app/20241205stocklist.csv`
- Fetches option expiration dates, filters by delta (0.05-0.92), gets market snapshots
- Writes results (turnoverc, turnoverp, ivc, ivp) to Google Sheets
- Also saves results to `/app/20241205optionresults.csv`
- Optional Tkinter GUI if `FUTU_GUI=1` and DISPLAY is set

## Configuration

### Environment Variables

| Variable | Description | Default |
|---|---|---|
| `FUTU_ACCOUNT_ID` | Futu account ID | (required) |
| `FUTU_ACCOUNT_PWD` | Futu account password | (required) |
| `FUTU_OPEND_IP` | Listen address for FutuOpenD | `0.0.0.0` |
| `FUTU_OPEND_PORT` | FutuOpenD API port | `11111` |
| `FUTU_RSA_FILE_PATH` | Path to RSA private key for protocol encryption | (empty = no encryption) |
| `SERVICE_FILE` | Google Sheets service account JSON | `/app/service_account.json` |
| `SHEET_NAME` | Google Sheets workbook name | `LID Risk Management` |
| `WORKSHEET_TITLE` | Worksheet tab name | `Today` |
| `TARGET_HOUR` | Scheduler hour (UTC) | `04` |
| `TARGET_MINUTE` | Scheduler minute (UTC) | `00` |

### Files

| File | Description |
|---|---|
| `futu.pem` | RSA private key (optional, for encrypted API protocol) |
| `FutuOpenD.xml` | OpenD configuration template |
| `start.sh` | OpenD startup script (injects env vars into XML) |
| `OptionPython/20241205stocklist.csv` | Stock list to process |
| `OptionPython/service_account.json` | Google Sheets OAuth service account |

### Secrets (Google Sheets)

The `service_account.json` is a Google Cloud service account key. Keep it safe:

```bash
# Encrypt with gpg (recommended before committing)
gpg -c service_account.json
# Decrypt when deploying
gpg -d service_account.json.gpg > OptionPython/service_account.json
```

## Deployment

### Prerequisites

- Docker & Docker Compose
- Futu account (ID + password)
- Google Sheets service account with access to the target sheet

### Setup

1. Clone the repository
2. Create `futu.pem` (RSA private key for protocol encryption, optional)
3. Configure `.env`:

```bash
FUTU_ACCOUNT_ID=28507949
FUTU_ACCOUNT_PWD=your_password
FUTU_OPEND_IP=0.0.0.0
FUTU_OPEND_PORT=11111
FUTU_RSA_FILE_PATH=/.futu/futu.pem
SERVICE_FILE=/app/service_account.json
SHEET_NAME=LID Risk Management
WORKSHEET_TITLE=Today
TARGET_HOUR=04
TARGET_MINUTE=00
```

4. Place Google service account key at `OptionPython/service_account.json`
5. Place stock list at `OptionPython/20241205stocklist.csv`

### Start

```bash
docker compose up -d
```

### Logs

```bash
docker logs futu-opend
docker logs option-python
```

### Stop

```bash
docker compose down
```

## Architecture

```
                    +-------------------+
                    |  futu-opend       |
                    |  (manhinhang/     |
                    |   futu-opend-     |
                    |   docker:ubuntu-  |
                    |   stable)         |
                    |                   |
                    |  API :11111      |
                    |  Telnet:22222    |
                    +--------+----------+
                             |
              (FutuOpenD XML + start.sh
               injects credentials)
                             |
                    +--------+----------+
                    |  option-python    |
                    |  (python:3.9-slim)|
                    |  app.py           |
                    +--------+----------+
                             |
              [Scheduled at TARGET_HOUR:TARGET_MINUTE]
                             |
                    +--------v----------+
                    |  Google Sheets    |
                    +-------------------+
```

## CI/CD

GitHub Actions workflows:

- **CI** (`ci.yml`): Validates docker-compose config and checks required files on push/PR
- **Deploy** (`deploy.yml`): Deploys to production droplet on main branch push (requires SSH secrets)

## Troubleshooting

### OpenD won't start
- Check `FUTU_ACCOUNT_ID` and `FUTU_ACCOUNT_PWD` in `.env`
- Verify `start.sh` has correct permissions: `chmod +x start.sh`

### option-python fails to connect
- Ensure `FUTU_OPEND_IP=futu-opend` (Docker network hostname)
- Check FutuOpenD API port is accessible: `docker exec option-python telnet futu-opend 11111`
- Verify account has market data permissions

### Google Sheets update fails
- Verify `service_account.json` is valid and not expired
- Ensure the service account has Editor access to the Google Sheet

### Protocol encryption
- If using `futu.pem`, ensure `FUTU_RSA_FILE_PATH=/.futu/futu.pem` is set and the file is mounted at that path in the container

## License

MIT