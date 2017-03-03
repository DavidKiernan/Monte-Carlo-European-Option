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
import matplotlib.pyplot as plt

VOLATILITY = 0.3672
RISK_FREE_RATE = 0.0024
STRIKE_PRICE = 7
CURRENT_VALUE = 7.37
simulations = 10000

# Decleared date for start and expiry
startDate = datetime.date(2016, 9, 3)
expiryDate = datetime.date(2016, 9, 21)

# Expiry as Int
expiry = (expiryDate - startDate).days

# CALL OPTION

def call_payoff(asset_price,STRIKE_PRICE):
    return max(0.0, STRIKE_PRICE - asset_price)


def sim_call_option_price(call_seed):
    random.seed(call_seed)
    asset_price = CURRENT_VALUE *math.exp((RISK_FREE_RATE - 0.5 * VOLATILITY**2) * T + VOLATILITY * math.sqrt(T)
    * random.gauss(0,1.0))
    return call_payoff(asset_price,STRIKE_PRICE)

call_option_price = [] # Stores option_Price
call_times = []
result_to_file = open("Call Result.txt", "a")  # open file before the for loop all results added the end put in loop to do as running
for i in range(1, expiry + 1):             # 1 .. EXPIRES inclusive
    T = i/365               # days in the future
    call_times.append(i)
    callSeeds = sc.parallelize([call_times[0] + i for i in range(simulations)])
    results = callSeeds.map(sim_call_option_price)
    sum = results.reduce(add)
    discount_factor = math.exp(-RISK_FREE_RATE * T)
    callPrice = discount_factor * (sum / float(simulations))
    call_option_price.append(callPrice)

    # Writes to file after opening it
    result_to_file.write('Call Price: %.4f' % callPrice)
    result_to_file.write(" at ")
    result_to_file.write(str(startDate + datetime.timedelta(days=i))+"\n")
    # Prints output to console
    print('Call Price: %.4f' % callPrice, " at ", startDate + datetime.timedelta(days=i))

result_to_file.write("\n")  # adds a new line in the file
result_to_file.close()      # closes the file once for loop is finished

# Graphs
plt.plot(call_times, call_option_price)
plt.xlabel('T')
plt.ylabel('Call Option Prices')
plt.show()


# PUT OPTION

def put_payoff(asset_price,STRIKE_PRICE):
   return max(0.0,asset_price - STRIKE_PRICE)

def sim_put_option_price(put_seed):
    random.seed(put_seed)
    asset_price = CURRENT_VALUE *math.exp((RISK_FREE_RATE - 0.5 * VOLATILITY**2) * T + VOLATILITY * math.sqrt(T) *
    random.gauss(0,1.0))
    return put_payoff(asset_price,STRIKE_PRICE)

put_option_price = [] # Stores option_Price
put_times = []
put_to_file = open("Put Result.txt", "a")  # open file before the for loop all results added the end put in loop to do as running
for i in range(1, expiry + 1):             # 1 .. EXPIRES inclusive
    T = i/365               # days in the future
    put_times.append(i)
    put_seed = sc.parallelize([put_times[0] + i for i in range(simulations)])
    results = put_seed.map(sim_put_option_price)
    sum = results.reduce(add)
    discount_factor = math.exp(-RISK_FREE_RATE * T)
    putPrice = discount_factor * (sum / float(simulations))
    put_option_price.append(putPrice)

    # Writes to file after opening it
    put_to_file.write('Call Price: %.4f' % putPrice)
    put_to_file.write(" at ")
    put_to_file.write(str(startDate + datetime.timedelta(days=i))+"\n")
    # Prints output to console
    print('Put Price: %.4f' % putPrice, " at ", startDate + datetime.timedelta(days=i))

put_to_file.write("\n")  # adds a new line in the file
put_to_file.close()      # closes the file once for loop is finished

# Graphs
plt.plot(put_times, put_option_price )
plt.xlabel('Time')
plt.ylabel('Put Option Prices')
plt.show()

