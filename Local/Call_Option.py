import datetime as dt  # start & end dates
import pandas as pd
import pandas_datareader as web  # pulling data from web in pandas data frame
import csv
import yahoo_finance as yf
import math
import random
simulations = 100  # number of simulation ran for the price
user_continue = "Y"  # sets to Y to enter loop
while user_continue == 'Y':
    try:
        ticker = str(input("Enter Stock Symbol: "))  # cast to string
        df1 = web.Options(ticker, "yahoo")  # gets ticker and where its from
        curr = yf.Share(ticker)  # gets the ticker for company need this one for the current price
        current = float(curr.get_open())  # gets current price, cast to float if varying

        risk_bool = True  # Sets bool to true to ensure a number is entered, although unlikely this can be a neg value
        while risk_bool is True:
            try:
                risk_free_rate = float(input("Enter Your Risk Free Rate: ")) / 100  # normally govt bond
                risk_bool = False

            except ValueError as ve:
                print("Must be Numeric value. Re-enter")

        start_date = dt.datetime.today().date()     # the current date
        date_control = start_date - dt.timedelta(1)     # gets yesterday date loop control

        date_bool = True
        while date_bool is True and date_control < start_date:   # true and controller is less than current date
            try:
                date_entry = input('Enter Expiry Date in DD-MM-YYYY format: ')
                day, month, year = map(int, date_entry.split('-'))     # maps the int value to appropriate value
                expiryDate = dt.datetime(year, month, day).date()
                date_control = expiryDate
                date_bool = False

                # as date bool is set to false, set it to True if its less than today's date
                if date_control < start_date:
                    date_bool = True
                    print("Enter today's date or future date:\n ")

            except ValueError as ve:
                print("Not valid date. Must be Numeric \nReenter Date")

        print("Writing To File")
        expiry = (expiryDate - start_date).days  # gets the difference of the dates as days

        file_all_option_data = (ticker.upper() + "_Call_Option_Data.csv")  # Auto write to file with ticket value
        df12 = df1.get_call_data(expiry=expiryDate)  # gets all option data on the expriry date or the next closes date
        df12.to_csv(file_all_option_data, sep=",", float_format='%.4f')  # writes file to 4dp, vol is auto converted to dec

        # reading certain columns from file currently a temp file
        temp_file = pd.read_csv(file_all_option_data, usecols=("Strike", "Expiry", "Type", "IV"), index_col=0)
        temp_file.to_csv("Temp_Call_File.csv")

        call_information = pd.read_csv('Temp_Call_File.csv', sep=',') # sep value between infomation
        find_rows = open("Temp_Call_File.csv", "r")  # reads the file
        reader = csv.reader(find_rows, delimiter=",")  # what separates the columns
        row_count = sum(1 for row in reader)

        result_file = (ticker.upper() + "_MC_Call_Result.csv")  # file that will be written to
        file = open(result_file, "w")
        file.write("DATE," "VARIATION,"  "PRICE," "STRIKE PRICE\n")

        # takes in the current price, volatility, risk_free_rate, time
        def generate_asset_price(s, v, r, t):
            return s * math.exp((r - 0.5 * v ** 2) * time + v * math.sqrt(t) * random.gauss(0, 1.0))

        def call_payoff(asset_price, current_price):
            return max(0.0, asset_price - current_price)

        ind = 0
        call_times = []   # list time append day / 365 as
        while ind < row_count - 1:  # -1 to remove header columns
            for row in call_information:
                strike = call_information.Strike[ind]  # gets the strike price at this row
                strike_price = float(strike)  # uses this stores in the variable
                vol = call_information.IV[ind]
                volatility = float(vol)

            for i in range(1, expiry, + 1):   # Inclusive all days
                time = i / 365
                call_times.append(i)

                payoffs = []
                for x in range(simulations):
                    asset_price = generate_asset_price(float(current), float(volatility), risk_free_rate, time)
                    payoffs.append(call_payoff(asset_price, float(strike_price)))
                    discount_factor = math.exp(-risk_free_rate * time)
                    price = discount_factor * (sum(payoffs) / float(simulations))

            # Write to file
                file.write(str(start_date + dt.timedelta(days=i)))
                file.write(",Original,")
                file.write('%.2f' % price + ",")
                file.write(str(strike_price) + "\n")

            ind += 1  # add 1 to the control of inner loop
        file.close()
        print("File Written\n")
    except Exception as e:
        print("Not valid Ticker\n")

    # convert to string and upper
    user_continue = str.upper(input("Press 'Y' to enter another symbol. \nAny other to exit: "))
