# Proof of concept that it is poosible to use Spark of Azure
# All Values had to be hard coded as appears not allowed user input

from pyspark.sql.types import *
import datetime
from functools import reduce
import math
import random
import time
from operator import add
VOLATILITY =  22.40 / 100 
RISK_FREE_RATE = 0.76 / 100  # Rate from the US daily Treasury Yield Curve Rate dividing by 100 to convert % to decimal
STRIKE_PRICE = 835.50
CURRENT_VALUE = 838.21
simulations = 10000000
count = 0  # used to control the loop for the variations

put_times = []  # Used to store days / 365 to used in the parallelize 

# Decleared date for start and expiry of the contract
startDate = datetime.date.today()  # Todays date
expiryDate = datetime.date(2017, 5, 5) # mm dd

# Expiry as differece in days
expiry = (expiryDate - startDate).days


def put_payoff(asset_price,STRIKE_PRICE):
    return max(0.0, STRIKE_PRICE - asset_price)

def sim_put_option_price(put_seed):
    random.seed(put_seed)
    asset_price = CURRENT_VALUE *math.exp((RISK_FREE_RATE - 0.5 * VOLATILITY**2) * T + VOLATILITY * math.sqrt(T)
    * random.gauss(0,1.0))
    return put_payoff(asset_price,STRIKE_PRICE)

print("Date","Variation","Price", sep=",") # just prints out the heading for the file ( copy output window & paste into file on local machine )

# while loop just for different variations of the option price
while count < 3:
    for i in range(1, expiry + 1):             # 1 .. EXPIRES inclusive
        T = i/365               # days in the future
        put_times.append(i)
        putSeeds = sc.parallelize([put_times[0] + i for i in range(simulations)])
        results = putSeeds.map(sim_put_option_price)
        sum = results.reduce(add)
        discount_factor = math.exp(-RISK_FREE_RATE * T)
        putPrice = discount_factor * (sum / float(simulations))
                
        if (count == 0):
            print(startDate + datetime.timedelta(days=i),"Original", '%.2f'% putPrice, sep = "," )
        else:
        # Prints output to console
            print(startDate + datetime.timedelta(days=i),count, '%.2f'% putPrice, sep = ",")  # sep place a comma betwen each variable
        
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