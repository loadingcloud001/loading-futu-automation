"""Black-Scholes Options Pricing Calculator.
Replicates the Excel 美股期權計算機 logic.

Supports 3 scenarios like the Excel:
  - cost_price (成本價): current market price → current option value
  - profit_price (盈利價): expected upside target → projected option value
  - loss_price (虧損價): stop loss stock price → projected option value at stop
"""

import math
from typing import Tuple

def norm_pdf(x: float) -> float:
    """Standard normal probability density function."""
    return (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-0.5 * x * x)

def norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function (NORMSDIST in Excel)."""
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


def black_scholes(
    S: float,      # underlying price
    K: float,      # strike price
    T: float,      # time to expiration (years)
    r: float,      # risk-free rate (decimal, e.g. 0.05 for 5%)
    sigma: float,  # volatility (decimal, e.g. 0.30 for 30%)
    q: float = 0.0 # dividend yield (decimal)
) -> dict:
    """Calculate Black-Scholes option prices and all Greeks.
    
    Returns dict with:
        call_price, put_price, delta_call, delta_put,
        gamma, theta_call, theta_put, vega, rho_call, rho_put,
        d1, d2
    """
    if T <= 0 or sigma <= 0 or S <= 0:
        return {
            'call_price': max(0, S - K),
            'put_price': max(0, K - S),
            'delta_call': 1.0 if S > K else 0.0,
            'delta_put': -1.0 if S < K else 0.0,
            'gamma': 0.0, 'theta_call': 0.0, 'theta_put': 0.0,
            'vega': 0.0, 'rho_call': 0.0, 'rho_put': 0.0,
            'd1': 0.0, 'd2': 0.0,
        }
    
    # d1 and d2 (matching Excel formula)
    d1 = (math.log(S / K) + (r - q + (sigma ** 2) / 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    
    # Option prices (matching Excel: NORMSDIST)
    call_price = S * math.exp(-q * T) * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    put_price = K * math.exp(-r * T) * norm_cdf(-d2) - S * math.exp(-q * T) * norm_cdf(-d1)
    
    # Greeks (matching Excel formulas)
    delta_call = math.exp(-q * T) * norm_cdf(d1)
    delta_put = math.exp(-q * T) * (norm_cdf(d1) - 1)
    gamma = (norm_pdf(d1) * math.exp(-q * T)) / (S * sigma * math.sqrt(T))
    
    # Theta (per year, then /365 for daily)
    theta_call = (-(S * norm_pdf(d1) * sigma * math.exp(-q * T)) / (2 * math.sqrt(T))
                  - r * K * math.exp(-r * T) * norm_cdf(d2)
                  + q * S * math.exp(-q * T) * norm_cdf(d1))
    theta_put = (-(S * norm_pdf(d1) * sigma * math.exp(-q * T)) / (2 * math.sqrt(T))
                 + r * K * math.exp(-r * T) * norm_cdf(-d2)
                 - q * S * math.exp(-q * T) * norm_cdf(-d1))
    
    vega = (S * math.exp(-q * T) * norm_pdf(d1) * math.sqrt(T)) / 100  # /100 for 1% vol change
    
    rho_call = (K * T * math.exp(-r * T) * norm_cdf(d2)) / 100
    rho_put = (-K * T * math.exp(-r * T) * norm_cdf(-d2)) / 100
    
    return {
        'call_price': round(call_price, 4),
        'put_price': round(put_price, 4),
        'delta_call': round(delta_call, 4),
        'delta_put': round(delta_put, 4),
        'gamma': round(gamma, 4),
        'theta_call': round(theta_call / 365, 6),  # daily theta
        'theta_put': round(theta_put / 365, 6),
        'vega': round(vega, 4),
        'rho_call': round(rho_call, 4),
        'rho_put': round(rho_put, 4),
        'd1': round(d1, 4),
        'd2': round(d2, 4),
    }


def option_analysis(
    stock_price: float,
    strike: float,
    profit_target: float,
    stop_loss: float,
    days_to_expiry: int,
    iv: float,           # implied volatility (decimal, e.g. 0.45 for 45%)
    risk_free_rate: float = 0.05,
    is_call: bool = True,
) -> dict:
    """Full option analysis matching Excel 投資計算 logic.
    
    3 scenarios (matching Excel 美股期權計算機 columns B/C/D):
      - cost (成本價): option value at current stock price
      - profit (盈利價): option value if stock hits profit target
      - loss (虧損價): option value if stock hits stop loss
    
    Args:
        stock_price: current underlying price
        strike: option strike price
        profit_target: expected stock price on profit (目標股價)
        stop_loss: stop loss stock price (止損股價)
        days_to_expiry: days until expiration
        iv: implied volatility as decimal (e.g. 0.45)
        risk_free_rate: annual risk-free rate (default 5%)
        is_call: True for call, False for put
    
    Returns dict with buy/sell/stop prices and position sizing.
    """
    T = days_to_expiry / 365.0
    
    # 3 scenarios
    cost_bs = black_scholes(stock_price, strike, T, risk_free_rate, iv)
    profit_bs = black_scholes(profit_target, strike, T, risk_free_rate, iv)
    loss_bs = black_scholes(stop_loss, strike, T, risk_free_rate, iv)
    
    price_key = 'call_price' if is_call else 'put_price'
    
    cost_option = cost_bs[price_key]
    profit_option = profit_bs[price_key]
    loss_option = loss_bs[price_key]
    
    # Buy price formula from Excel 投資計算 D29:
    # = (3 * stop_loss_option_price + take_profit_option_price) / 4
    buy_price = round((3 * loss_option + profit_option) / 4, 4)
    
    # Position sizing: risk_amount / (buy_price - stop_loss_price) * buy_price
    # This calculates how many contracts to buy based on risk tolerance
    # (implemented in trade_planner.py)
    
    return {
        'cost_scenario': {
            'stock_price': stock_price,
            'option_price': cost_option,
            'delta': cost_bs['delta_call'] if is_call else cost_bs['delta_put'],
            'gamma': cost_bs['gamma'],
            'theta': cost_bs['theta_call'] if is_call else cost_bs['theta_put'],
            'vega': cost_bs['vega'],
        },
        'profit_scenario': {
            'stock_price': profit_target,
            'option_price': profit_option,
        },
        'loss_scenario': {
            'stock_price': stop_loss,
            'option_price': loss_option,
        },
        'buy_price': buy_price,
        'take_profit_price': profit_option,
        'stop_loss_price': loss_option,
    }
