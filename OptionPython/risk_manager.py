"""Risk Manager - replicates Excel 投資計算 risk formulas.

Excel formulas replicated:
  C12: =D6*0.02        (risk = 2% of high-risk portfolio)
  E12: =D12*3          (expected profit = 3x risk)
  D12: =C12/USDHKD     (convert to USD)

Config-driven: reads portfolio values from environment or config file.
"""

import os
import math

# Default portfolio allocation (from Excel 投資計算 / Source Data)
DEFAULT_PORTFOLIO = {
    'high_risk': 165038.02,   # D6: 高風險投資資產 (Futu + IBKR options)
    'medium_risk': 563407.78, # C5: 中風險投資資產
    'low_risk': 498782.97,    # C4: 極低風險投資資產
    'total_assets': 1227228.77  # C2: 總共資產
}

# Risk parameters from Excel
RISK_PCT_HIGH = 0.02     # C12: 2% of high-risk for options trading
RISK_PCT_MEDIUM = 0.02   # C13: 2% of medium-risk
RISK_PCT_LOW = 0.02      # C14: 2% of low-risk
RISK_REWARD_RATIO = 3    # 風險回報比 1:3

# USD/HKD exchange rate
USD_HKD_RATE = float(os.getenv('USD_HKD_RATE', '7.81'))


def get_risk_amount_hkd(
    portfolio: dict = None,
    risk_pct: float = None,
) -> float:
    """Calculate risk amount in HKD (replicates 投資計算 C12).
    
    C12: =D6*0.02 → Risk = high_risk_portfolio * 2%
    
    Returns risk amount in HKD.
    """
    if portfolio is None:
        portfolio = DEFAULT_PORTFOLIO
    if risk_pct is None:
        risk_pct = RISK_PCT_HIGH
    
    high_risk = portfolio.get('high_risk', DEFAULT_PORTFOLIO['high_risk'])
    return round(high_risk * risk_pct, 2)


def get_risk_amount_usd(
    risk_hkd: float = None,
    usd_hkd_rate: float = None,
) -> float:
    """Convert risk amount from HKD to USD (replicates 投資計算 D12).
    
    D12: =C12/USDHKD_rate
    """
    if risk_hkd is None:
        risk_hkd = get_risk_amount_hkd()
    if usd_hkd_rate is None:
        usd_hkd_rate = USD_HKD_RATE
    
    return round(risk_hkd / usd_hkd_rate, 2)


def get_expected_profit(
    risk_usd: float = None,
    reward_ratio: float = None,
) -> float:
    """Calculate expected profit (replicates 投資計算 E12).
    
    E12: =D12*3 → Expected profit = 3x risk
    """
    if risk_usd is None:
        risk_usd = get_risk_amount_usd()
    if reward_ratio is None:
        reward_ratio = RISK_REWARD_RATIO
    
    return round(risk_usd * reward_ratio, 2)


def get_risk_profile(portfolio: dict = None) -> dict:
    """Get complete risk profile for options trading.
    
    Returns dict with all risk parameters ready for trade_planner.
    """
    risk_hkd = get_risk_amount_hkd(portfolio)
    risk_usd = get_risk_amount_usd(risk_hkd)
    profit = get_expected_profit(risk_usd)
    
    return {
        'risk_hkd': risk_hkd,
        'risk_usd': risk_usd,
        'expected_profit_usd': profit,
        'risk_reward_ratio': RISK_REWARD_RATIO,
        'risk_pct': RISK_PCT_HIGH,
        'portfolio_high_risk': portfolio.get('high_risk', DEFAULT_PORTFOLIO['high_risk']) if portfolio else DEFAULT_PORTFOLIO['high_risk'],
    }


def update_portfolio_from_env() -> dict:
    """Read portfolio values from environment variables.
    
    Env vars:
        PORTFOLIO_HIGH_RISK, PORTFOLIO_MED_RISK, PORTFOLIO_LOW_RISK, PORTFOLIO_TOTAL
    """
    portfolio = DEFAULT_PORTFOLIO.copy()
    for key, env_key in [
        ('high_risk', 'PORTFOLIO_HIGH_RISK'),
        ('medium_risk', 'PORTFOLIO_MED_RISK'),
        ('low_risk', 'PORTFOLIO_LOW_RISK'),
        ('total_assets', 'PORTFOLIO_TOTAL'),
    ]:
        val = os.getenv(env_key)
        if val:
            try:
                portfolio[key] = float(val)
            except ValueError:
                pass
    return portfolio
