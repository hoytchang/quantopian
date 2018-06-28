# Invest in 10 stocks ranked by QVM (Quality, Value, Momentum) with weekly rebalance.

import quantopian.algorithm as algo
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters import QTradableStocksUS

from quantopian.pipeline.data import Fundamentals
from quantopian.pipeline.factors import AverageDollarVolume
from quantopian.pipeline.data import morningstar as mstar
from quantopian.pipeline.factors import Returns

def min_liq_univ():
    # replicate P123 universe "Minimum Liquidity NO OTC - Copy"
    # for 2005-01-03, this screen returns 3782 stocks.  P123 returns 4260 stocks.
    # for 2010-01-04, this screen returns 3543 stocks.  P123 returns 3686 stocks.
    # for 2016-01-01, this screen returns 3999 stocks.  P123 returns 3901 stocks.
    # for 2018-06-25, this screen returns 4428 stocks.  P123 returns 3894 stocks.
    
    # markt cap screen
    mktcap = Fundamentals.market_cap.latest
    mktcap_screen = mktcap > 50000000
    
    # price screen
    price_close = USEquityPricing.close.latest
    price_screen = price_close > 1
    
    # volume screen
    dollar_volume = AverageDollarVolume(window_length=20)
    volume_screen = dollar_volume > 200000
    
    # not OTC
    not_otc = ~mstar.share_class_reference.exchange_id.latest.startswith('OTC')
    
    return (mktcap_screen & price_screen & volume_screen & not_otc)


def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    # Rebalance every day, 1 hour after market open.
    algo.schedule_function(
        rebalance,
        #algo.date_rules.every_day(),
        algo.date_rules.week_start(),
        algo.time_rules.market_open(hours=1),
    )

    # Record tracking variables at the end of each day.
    algo.schedule_function(
        record_vars,
        algo.date_rules.every_day(),
        algo.time_rules.market_close(),
    )

    # Create our dynamic stock selector.
    algo.attach_pipeline(make_pipeline(), 'pipeline')


def make_pipeline():
    # get factors
    mktcap = Fundamentals.market_cap.latest
    price_close = USEquityPricing.close.latest
    
    # get factors, higher is better
    roe = Fundamentals.roe.latest
    fcf_yield = Fundamentals.fcf_yield.latest
    ret_20 = Returns(window_length = 20)
    
    # compare built-in valuation ratio with manually calculated
    price_close = USEquityPricing.close.latest
    fcf_per_share = Fundamentals.fcf_per_share.latest
    fcf_yield_manual = fcf_per_share / price_close
    
    # get ranks, masked by the universe, higher is better
    quality = roe.rank(method = 'average', mask = min_liq_univ())
    value = fcf_yield.rank(method = 'average', mask = min_liq_univ())
    momentum = ret_20.rank(method = 'average', mask = min_liq_univ())
    
    # combine ranks
    qvm = (quality + value + momentum).rank(method = 'average', mask = min_liq_univ())
    
    # Sort into 10 deciles
    num_quantiles = 10
    qvm_quantiles = qvm.quantiles(num_quantiles, mask = min_liq_univ())
    long_decile = qvm_quantiles.eq(num_quantiles-1)
    short_decile = qvm_quantiles.eq(0)
    
    # Get top 10 and bottom 10 stocks by rank
    long_10 = qvm.top(10, mask = min_liq_univ())
    short_10 = qvm.bottom(10, mask = min_liq_univ())
    
    return Pipeline(
        columns={
            'mktcap': mktcap,
            'fcf_yield': fcf_yield,
            'fcf_yield_manual': fcf_yield_manual,
            'roe': roe,
            'ret_20': ret_20,
            'quality': quality,
            'value': value,
            'momentum': momentum,
            'qvm': qvm,
            'long_10': long_10
        },
        screen = min_liq_univ()
    )


def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    context.output = algo.pipeline_output('pipeline')

    # These are the securities that we are interested in trading each day.
    context.security_list = context.output.index
        
    # Ten highest ranked stocks
    context.longs = context.output[context.output['long_10']].index


def rebalance(context, data):
    """
    Execute orders according to our schedule_function() timing.
    """
    # Get current positions
    my_positions = context.portfolio.positions
    
    # Equal weighting for all 10 positions
    long_weight = 0.1
    
    # Get our target names for our long and short baskets. We can display these
    target_long_symbols = [s.symbol for s in context.longs]
    log.info("Opening long positions each worth %.2f of our portfolio in: %s" \
                 % (long_weight, ','.join(target_long_symbols)))
    
    # Open long positions in our high p/e stocks.
    for security in context.longs:
        if data.can_trade(security):
            if security not in my_positions:
                order_target_percent(security, long_weight)
            else:
                log.info("Didn't open long position in %s" % security)
    
    closed_positions = []
    
    # Close our previous positions that are no longer in our pipeline.
    for security in my_positions:
        if security not in context.longs and data.can_trade(security):
            order_target_percent(security, 0)
            closed_positions.append(security)
    
    log.info("Closing our positions in %s." % ','.join([s.symbol for s in closed_positions]))


def record_vars(context, data):
    """
    Plot variables at the end of each day.
    """
    # Plot the number of positions over time.
    algo.record(num_positions=len(context.portfolio.positions))