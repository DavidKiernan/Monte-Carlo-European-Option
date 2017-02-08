_author_ = 'David'

# Created 24 November 2016. MonteCarlo method
# This runs Monte Carlo on Local Machine
# Taken from the following https://www.youtube.com/watch?v=j5kdmOV_xO8 & http://www.codeandfinance.com/pricing-options-monte-carlo.html
# range can also be seen in earlier versions but from 3.x onwards only range is used

#THIS IS CODE FOR CALL OPTION

import datetime
import math
import random

VOLATILITY = 0.3672
RISK_FREE_RATE = 0.0024  # US DAILY TREASURY CURVE RATE 0.24%
STRIKE_PRICE = 7
CURRENT_VALUE = 7.37
simulations = 9000
put_payoffs = []
call_payoffs = []

TIME = (datetime.date(2013, 9, 21) - datetime.date(2013, 9, 3)).days / 365.0


def generate_asset_price(CURRENT_VALUE, VOLATILITY, RISK_FREE_RATE, TIME):
    return CURRENT_VALUE * math.exp((RISK_FREE_RATE - 0.5 * VOLATILITY ** 2.0)
                                    * TIME + VOLATILITY * math.sqrt(TIME) * random.gauss(0, 1.0))


# PUT PAYOFF
def put_payoff(asset_price, STRIKE_PRICE):
    return max(0.0, asset_price - STRIKE_PRICE)

discount_factor = math.exp(-RISK_FREE_RATE * TIME)

for i in range(simulations):
    asset_price = generate_asset_price(CURRENT_VALUE,VOLATILITY,RISK_FREE_RATE,TIME)
    put_payoffs.append(put_payoff(asset_price, STRIKE_PRICE))

put_price = discount_factor * (sum(put_payoffs) / float(simulations))

print('Put Price: %.4f' % put_price)

#CALL PAYOFF

def call_payoff(asset_price, STRIKE_PRICE):
    return max(0.0,  STRIKE_PRICE - asset_price )

for i in range(simulations):
    asset_price = generate_asset_price(CURRENT_VALUE,VOLATILITY,RISK_FREE_RATE,TIME)
    call_payoffs.append(call_payoff(asset_price, STRIKE_PRICE))

call_price = discount_factor * (sum(call_payoffs) / float(simulations))

print('Call Price: %.4f' % call_price)



