flowchart TB
    subgraph DataSources["📡 Data Sources"]
        STOCKAPI["Stock Market API<br/>stockapi.loadingtechnology.app"]
        NOTION["Notion API<br/>api.notion.com"]
        YAHOO["Yahoo Finance<br/>(earnings dates)"]
    end

    subgraph Droplet["🖥️ DO002 Droplet (Docker)"]
        OPEND["futu-opend<br/>(Futu Gateway)"]
        PYTHON["option-python<br/>app.py — 24/7 scheduler"]

        subgraph Modes["3 Sync Modes"]
            SUNDAY["📅 Sunday Scan<br/>Every Sunday"]
            DAILY["🕓 Daily Full Sync<br/>04:00 UTC daily"]
            QUICK["⏱ Quick B-S Sync<br/>Every 5 minutes"]
            INTRA["🚨 Intraday Monitor<br/>Every 15 min (market hrs)"]
        end

        MODULES["14 Python Modules<br/>bs_calculator / trade_planner<br/>risk_manager / volume_analyzer<br/>earnings_calendar / trend_analyzer<br/>flow_scanner / intraday_monitor<br/>performance_tracker<br/>notion_client / notion_sync<br/>stock_api_client"]
    end

    subgraph NotionDB["📊 Notion Workspace"]
        SNAPSHOT["📊 Daily Options Analysis<br/>200 stocks × 40+ fields"]
        HISTORICAL["📈 Historical Archive<br/>daily append-only"]
        TRADEJ["📝 Trade Journal<br/>manual + auto-populated"]
        ALERTS["🚨 Alerts<br/>intraday anomalies"]
        FLOW["🔍 Flow Alerts<br/>strike-level big trades"]
    end

    subgraph UserWorkflow["👤 User Daily Workflow"]
        DIR1["Open Notion Page<br/>Step 1-5 workflow guide"]
        DIR2["Filter 🔴異常<br/>Sort by Total Turnover"]
        DIR3["Set My Target Price<br/>My Stop Price / My Days"]
        DIR4["View Call/Put Plan<br/>Buy/Target/Stop/Contracts/R:R"]
        DIR5["Record trade in<br/>Trade Journal"]
    end

    %% Data Flows
    OPEND --> STOCKAPI
    STOCKAPI -->|"/quote/batch (50/chunk)"| PYTHON
    STOCKAPI -->|"/option/chain/{symbol}"| PYTHON
    STOCKAPI -->|"/capital-flow/{symbol}"| PYTHON
    STOCKAPI -->|"/indicator/macd/{symbol}"| PYTHON
    YAHOO -->|"earnings dates"| PYTHON

    PYTHON --> MODULES
    MODULES --> SUNDAY
    MODULES --> DAILY
    MODULES --> QUICK
    MODULES --> INTRA

    SUNDAY -->|"scan top 500<br/>rebuild stock list"| SNAPSHOT
    DAILY -->|"full data sync<br/>option chains + B-S"| SNAPSHOT
    DAILY -->|"append daily snapshot"| HISTORICAL
    QUICK -->|"price update + B-S<br/>with user inputs"| SNAPSHOT
    INTRA -->|"anomaly alerts<br/>during market hrs"| ALERTS
    INTRA -->|"flow scanner<br/>big trades"| FLOW

    SNAPSHOT --> DIR1
    DIR1 --> DIR2
    DIR2 --> DIR3
    DIR3 --> DIR4
    DIR4 --> DIR5
    DIR5 --> TRADEJ

    %% Sunday Scan Detail
    subgraph SundayDetail["Sunday Scan Detail"]
        S1["1. Batch quote 2085 US stocks"]
        S2["2. Sort by turnover → top 500"]
        S3["3. Check option chain (retry 2x)"]
        S4["4. compute_iv_smart()<br/>near-ATM ±30% stock price<br/>median IV + delta filter 0.15-0.85"]
        S5["5. Replace Notion DB<br/>remove old, add new"]
        S1 --> S2 --> S3 --> S4 --> S5
    end

    %% Daily Full Sync Detail
    subgraph DailyDetail["Daily Full Sync Detail"]
        D1["1. Query Notion stock list"]
        D2["2. Batch quote (20/chunk)"]
        D3["3. Fresh option chain CALL+PUT"]
        D4["4. near-ATM filter → IV/turnover/P-C/Anomaly"]
        D5["5. Black-Scholes → Call/Put Plan"]
        D6["6. Peak trade times (top 30)"]
        D7["7. Patch Notion entries"]
        D1 --> D2 --> D3 --> D4 --> D5 --> D6 --> D7
    end

    %% Quick Sync Detail
    subgraph QuickDetail["Quick B-S Sync Detail"]
        Q1["1. Query Notion stock list"]
        Q2["2. Batch quote (20/chunk)"]
        Q3["3. Read user: My Target Price<br/>My Stop Price, My Days"]
        Q4["4. Black-Scholes recompute<br/>Call/Put Buy/Target/Stop"]
        Q5["5. Patch Notion entries"]
        Q1 --> Q2 --> Q3 --> Q4 --> Q5
    end

    %% B-S Computation
    subgraph BSCompute["Black-Scholes Trade Plan"]
        BS1["Input: stock price, strike, IV, days"]
        BS2["3 scenarios: cost / profit / loss"]
        BS3["Buy price = (3×stop + target)/4"]
        BS4["Position size from risk amount"]
        BS5["Output: strike, buy, target, stop<br/>contracts, R:R ratio"]
        BS1 --> BS2 --> BS3 --> BS4 --> BS5
    end

    S4 --> BSCompute
    D4 --> BSCompute

    SNAPSHOT -.-> S4
    QUICK -.-> BSCompute
