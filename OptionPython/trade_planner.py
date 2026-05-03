"""Trade Planner - replicates Excel 投資計算 logic for US stock options.

Computes:
  1. Buy/sell/stop option prices using Black-Scholes
  2. Position sizing based on risk tolerance
  3. Best trading time windows from historical volume data
  4. Risk/reward analysis
"""

from bs_calculator import option_analysis
from typing import Optional
import os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Dynamic risk from risk_manager if available
_DEFAULT_RISK_USD = None

def _get_default_risk():
    global _DEFAULT_RISK_USD
    if _DEFAULT_RISK_USD is None:
        try:
            from risk_manager import get_risk_amount_usd
            _DEFAULT_RISK_USD = get_risk_amount_usd()
        except ImportError:
            _DEFAULT_RISK_USD = 421.29  # fallback (182.41 HKD / 7.81 rate ≈ 23.35... hmm)
    return _DEFAULT_RISK_USD


def plan_trade(
    stock_code: str,
    stock_price: float,
    strike: float,
    profit_target_pct: float = 0.05,  # default 5% profit target
    stop_loss_pct: float = -0.03,      # default -3% stop loss
    days_to_expiry: int = 7,
    iv: Optional[float] = None,        # implied volatility from data
    iv_call: Optional[float] = None,   # Call IV from Daily Snapshot
    iv_put: Optional[float] = None,    # Put IV from Daily Snapshot
    is_call: bool = True,
    risk_amount: float = 422.70,  # Default from 投資計算: 2% of high-risk portfolio in USD
    risk_free_rate: float = 0.05,
    peak_trade_time: str = "",
    second_peak_time: str = "",
    low_trade_time: str = "",
) -> dict:
    """Plan a single option trade with full risk/reward analysis.
    
    Args:
        stock_code: e.g. 'US.NVDA'
        stock_price: current stock price
        strike: option strike price
        profit_target_pct: expected profit as % of stock price (e.g. 0.05 = 5%)
        stop_loss_pct: stop loss as % of stock price (e.g. -0.03 = -3%)
        days_to_expiry: days until expiration
        iv: override IV (if provided, used for both call and put)
        iv_call: call IV from data (decimal)
        iv_put: put IV from data (decimal)
        is_call: True = Long Call, False = Long Put
        risk_amount: risk amount in account currency
        risk_free_rate: annual risk-free rate
        peak_trade_time: best time to trade (HKT)
        second_peak_time: 2nd best time
        low_trade_time: low volume time (avoid)
    
    Returns dict with complete trade plan.
    """
    # Determine IV
    if iv is not None:
        use_iv = iv
    elif is_call and iv_call is not None and iv_call > 0:
        use_iv = iv_call
    elif not is_call and iv_put is not None and iv_put > 0:
        use_iv = iv_put
    else:
        use_iv = 0.40  # default 40% if no data
    
    # Calculate target prices
    profit_target = stock_price * (1 + profit_target_pct)
    stop_loss = stock_price * (1 + stop_loss_pct)
    
    # Run Black-Scholes 3-scenario analysis
    bs_result = option_analysis(
        stock_price=stock_price,
        strike=strike,
        profit_target=profit_target,
        stop_loss=stop_loss,
        days_to_expiry=days_to_expiry,
        iv=use_iv,
        risk_free_rate=risk_free_rate,
        is_call=is_call,
    )
    
    buy_price = bs_result['buy_price']
    stop_price = bs_result['stop_loss_price']
    profit_price = bs_result['take_profit_price']
    
    # Position sizing formula from Excel 投資計算 E29:
    # position_value = risk_amount / (buy_price - stop_price) * buy_price
    price_spread = buy_price - stop_price
    if price_spread <= 0:
        position_value = 0
        contracts = 0
    else:
        position_value = (risk_amount / price_spread) * buy_price
        contracts = max(1, int(position_value / (buy_price * 100)))  # 100 shares per contract
    
    # Risk/reward
    potential_profit = (profit_price - buy_price) * contracts * 100
    potential_loss = (stop_price - buy_price) * contracts * 100
    risk_reward_ratio = abs(potential_profit / potential_loss) if potential_loss != 0 else 0
    
    direction = '📈 Long Call' if is_call else '📉 Long Put'
    
    return {
        'stock': stock_code,
        'direction': direction,
        'stock_price': round(stock_price, 2),
        'strike': strike,
        'iv_used': round(use_iv * 100, 1),
        'days_to_expiry': days_to_expiry,
        
        # Price plan
        'buy_option_price': buy_price,
        'stop_option_price': stop_price,
        'profit_option_price': profit_price,
        'profit_target_stock': round(profit_target, 2),
        'stop_loss_stock': round(stop_loss, 2),
        
        # Position sizing
        'risk_amount': risk_amount,
        'contracts': contracts,
        'position_value': round(position_value, 2),
        'potential_profit': round(potential_profit, 2),
        'potential_loss': round(potential_loss, 2),
        'risk_reward_ratio': round(risk_reward_ratio, 2),
        
        # Greeks
        'delta': bs_result['cost_scenario']['delta'],
        'gamma': bs_result['cost_scenario']['gamma'],
        'theta': bs_result['cost_scenario']['theta'],
        'vega': bs_result['cost_scenario']['vega'],
        
        # Timing
        'best_trade_time': peak_trade_time,
        'second_trade_time': second_peak_time,
        'avoid_time': low_trade_time,
        
        # Summary text
        'summary': (
            f'{direction} {stock_code} {strike} | '
            f'Buy @ {buy_price:.2f} | Stop @ {stop_price:.2f} | Target @ {profit_price:.2f} | '
            f'{contracts} contracts | Risk {risk_amount:.2f} | '
            f'R:R 1:{risk_reward_ratio:.1f} | '
            f'Best time: {peak_trade_time}'
        ),
    }


def batch_plan_trades(
    stocks_data: list,
    risk_amount: Optional[float] = None,
    default_days: int = 7,
    default_profit_pct: float = 0.05,
    default_stop_pct: float = -0.03,
) -> list:
    """Generate trade plans for multiple stocks from Daily Snapshot data.
    
    Args:
        stocks_data: list of dicts with keys:
            stock, stock_price, ivc, ivp, peak_trade_time, second_peak_time, low_trade_time, ...
        risk_amount: risk per trade
        default_days: default days to expiry
        default_profit_pct: profit target as % of stock price
        default_stop_pct: stop loss as % of stock price
    
    Returns list of trade plan dicts, sorted by risk/reward ratio descending.
    """
    plans = []
    if risk_amount is None:
        risk_amount = _get_default_risk()
    
    for s in stocks_data:
        stock_code = s.get('stock', '')
        stock_price = s.get('stock_price', 0)
        ivc = s.get('ivc', None)
        ivp = s.get('ivp', None)
        
        if stock_price <= 0:
            continue
        
        # Try both Call and Put plans
        for is_call, iv in [(True, ivc), (False, ivp)]:
            if iv is None or iv <= 0:
                continue
            
            plan = plan_trade(
                stock_code=stock_code,
                stock_price=stock_price,
                strike=round(stock_price, -1) if stock_price > 50 else round(stock_price, 0),
                days_to_expiry=default_days,
                iv_call=ivc,
                iv_put=ivp,
                is_call=is_call,
                risk_amount=risk_amount,
                profit_target_pct=default_profit_pct,
                stop_loss_pct=default_stop_pct,
                peak_trade_time=s.get('peak_trade_time', ''),
                second_peak_time=s.get('second_peak_time', ''),
                low_trade_time=s.get('low_trade_time', ''),
            )
            plans.append(plan)
    
    plans.sort(key=lambda x: abs(x['risk_reward_ratio']), reverse=True)
    return plans
