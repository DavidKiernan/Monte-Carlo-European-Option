# proof of concept Monte Carlo Simulation In Apache Spark On Azure For Call Option
# All Values had to be hard coded as appears not allowed user input

from pyspark.sql.types import *
from functools import reduce
import datetime
import math
import random
import time
from operator import add
VOLATILITY = 51.71 / 100      # Take Volatility from site. Not Needed if reading from yahoo as to automaticall divides by 100
RISK_FREE_RATE = 0.72 / 100  # Rate from the US daily Treasury Yield Curve Rate dividing by 100 to convert % to decimal
STRIKE_PRICE = 140
CURRENT_VALUE = 160.38
simulations = 1000000
count = 0  # used to control the loop for the variations

call_times = []  # Used to store call times to used in the parallelize 

# Decleared date for start and expiry of the contract
startDate = datetime.date.today()  # Todays date
expiryDate = datetime.date(2017, 5, 5) # mm dd

# Expiry as differece in days
expiry = (expiryDate - startDate).days


def call_payoff(asset_price,STRIKE_PRICE):
    return max(0.0, asset_price - STRIKE_PRICE)

def sim_call_option_price(call_seed):
    random.seed(call_seed)
    asset_price = CURRENT_VALUE *math.exp((RISK_FREE_RATE - 0.5 * VOLATILITY**2) * T + VOLATILITY * math.sqrt(T)
    * random.gauss(0,1.0))
    return call_payoff(asset_price,STRIKE_PRICE)

print("Date","Variation","Price", sep=",") # just prints out the heading for the file ( copy output window & paste into file on local machine )

# while loop just for different variations of the option price
while count < 3:
    for i in range(1, expiry + 1):             # 1 .. EXPIRES inclusive
        T = i/365               # days in the future
        call_times.append(i)
        callSeeds = sc.parallelize([call_times[0] + i for i in range(simulations)])
        results = callSeeds.map(sim_call_option_price)
        sum = results.reduce(add)
        discount_factor = math.exp(-RISK_FREE_RATE * T)
        callPrice = discount_factor * (sum / float(simulations))
                
        if (count == 0):
            print(startDate + datetime.timedelta(days=i),"Original", '%.2f'% callPrice, sep = "," )
        else:
        # Prints output to console
            print(startDate + datetime.timedelta(days=i),count, '%.2f'% callPrice, sep = ",")  # sep place a comma betwen each variable
        
    # Varify the parameters even will add odd will subtract
    if (count % 2 == 0):
        STRIKE_PRICE += 1.5
        VOLATILITY += 8.52 / 100 
        CURRENT_VALUE += 3.16
        RISK_FREE_RATE += 0.035 / 100
    else:
        STRIKE_PRICE -= 1
        VOLATILITY -= 6.26 / 100
        CURRENT_VALUE -= 2.44
        RISK_FREE_RATE -= 0.0175 / 100
    
    count +=1 # add 1 to count so loop will end
    