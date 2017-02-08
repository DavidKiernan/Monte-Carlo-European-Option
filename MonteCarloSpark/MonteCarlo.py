# This runs Monte Carlo on Local Machine using Apache Spark
# Taken from the following https://www.youtube.com/watch?v=j5kdmOV_xO8 & http://www.codeandfinance.com/pricing-options-monte-carlo.html
# range can also be seen in earlier versions but from 3.x onwards only range is used

from pyspark import SparkContext
sc = SparkContext()

import datetime
import math
import random
import time
from operator import add

VOLATILITY = 0.3672
RISK_FREE_RATE = 0.0024
STRIKE_PRICE = 7
CURRENT_VALUE = 7.37
simulations = 9000

Time = (datetime.date(2016, 9, 21) - datetime.date(2016, 9, 3)).days / 365.0

discount_factor = math.exp(-RISK_FREE_RATE * Time)

# CALL OPTION
def sim_call_option_price(call_seed):
    random.seed(call_seed)
    asset_price = CURRENT_VALUE *math.exp((RISK_FREE_RATE - 0.5 * VOLATILITY**2) * Time + VOLATILITY * math.sqrt(Time) *
    random.gauss(0,1.0))
    return call_payoff(asset_price,STRIKE_PRICE)

def call_payoff(asset_price,STRIKE_PRICE): return max(0.0,STRIKE_PRICE - asset_price)

callSeeds = sc.parallelize([time.time() + i for i in range(simulations)])
results = callSeeds.map(sim_call_option_price)
sum = results.reduce(add)
callPrice = discount_factor * (sum / float(simulations))

print('Call Price: %.4f' % callPrice)

# PUT OPTION

def sim_put_option_price(put_seed):
    random.seed(put_seed)
    asset_price = CURRENT_VALUE *math.exp((RISK_FREE_RATE - 0.5 * VOLATILITY**2) * Time + VOLATILITY * math.sqrt(Time) *
    random.gauss(0,1.0))
    return put_payoff(asset_price,STRIKE_PRICE)

def put_payoff(asset_price,STRIKE_PRICE): return max(0.0,asset_price - STRIKE_PRICE)

putSeeds = sc.parallelize([time.time() + i for i in range(simulations)])
results = putSeeds.map(sim_put_option_price)
sum = results.reduce(add)
putPrice = discount_factor * (sum / float(simulations))

print('Put Price: %.4f' % putPrice)

