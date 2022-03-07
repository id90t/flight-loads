from collections import defaultdict
from email.policy import default
import sys
import mysql.connector
from dotenv import load_dotenv
import os
import plotly.express as px
import pandas as pd
from datetime import date

load_dotenv()

print("Running on " + os.getenv("MYSQL_DATABASE"))

def dbConnect():
    cnx = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        port=3306,
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"))
    return cnx

def fetchData(cnx, airlineCode, flightNumber, origin, destination, initRange, endRange):
    cur = cnx.cursor()
    
    dateRange = pd.date_range(initRange, endRange, freq='1D')
    dirPath = airlineCode + '-' + flightNumber
    try: 
        os.mkdir('./' + dirPath)
    except OSError as error: 
        print('It seems the directory already exists, ignoring...')
    for departureDateO in dateRange:
        # testDate = pd.to_datetime(departureDate)
        departureDateS = str(departureDateO) 
        departureDate = departureDateS[:10]
        # print(departureDate)
        # exit()
        # print(str(testDate.year +'-'+ testDate.month +'-'+ testDate.day))
        # print(str(departureDate.year))
        # print(departureDate, str(departureDate))
        # exit()
        cur.execute("select created_at as Queried_At, (available_seats / total_seats) * 100 as Availability\
            from flight_loads \
            where airline_code = '" + airlineCode + "'\
            and flight_number = '" + flightNumber + "' \
            and departure_airport = '" + origin + "' \
            and arrival_airport = '" + destination + "' \
            and total_seats <> 0\
            and departure_date = '" + departureDate + "'\
            and service_class = 'Y'\
            order by 1 \
            limit 10000;")

        dic = {'date': [], 'availability': []}
        counter = 0


        fileBaseName = airlineCode + '-' + flightNumber + '-' + departureDate

        htmlpath = dirPath + '/' + fileBaseName + '.html' 
        csvpath = dirPath + '/' + fileBaseName + '.csv'
        jpegpath = dirPath + '/' + fileBaseName + '.jpg'
        
        

        fileObject = open(csvpath, 'w')
        for (Queried_At, Availability) in cur:
            dic['date'].append(Queried_At)
            dic['availability'].append(Availability)
            counter = counter + 1
            fileObject.write(str(f'{Queried_At}, {Availability}\n'))
        print('Records Retrieved for departure date: ', departureDate, counter)
        df = pd.DataFrame.from_dict(dic)
        graphTitle = airlineCode + '-' + flightNumber + ' ' + origin + '-' + destination + ' ' + 'departing on: ' + initRange
        fig = px.line(df, x='date', y="availability", title=graphTitle)
        fig.write_html(htmlpath, auto_open=True)
        # px.write_image(fig, jpegpath, 'jpg')
        
        fileObject.close()

    cnx.close()

def help():
    print('\
 run python test.py help to get or with no arguments in order to get this menu\n \
run python test.py checkFlights <departureDate> <limit> to see the top queried flights for a given departure date, i.e: "python test.py checkFlights 2022-03-04 10"\n \
run python test.py buildAging <airlineCode> <flightNumber> <departureAirportCode> <arrivalAirportCode> <departureDate> to gather aging data for a given flight, i.e: "python test.py HA 17 LAS HNL 2022-03-04"\n'
    )

def checkFlights(departureDate, limit):
    cnx = dbConnect()
    cur = cnx.cursor()
    # departureDate = '2022-03-04'
    # limit = '5'
    print(departureDate, limit)
    cur.execute("select airline_code, flight_number, departure_airport, arrival_airport, count(*) as qty \
    from flight_loads \
    where departure_date = '" + departureDate + "' \
    group by airline_code, flight_number \
    order by 5 desc limit " + limit + ";")

    for (airline_code, flight_number, departure_airport, arrival_airport, qty) in cur:
        print(f'{airline_code} => {flight_number} => {departure_airport} => {arrival_airport} => {qty}')

    cnx.close()

def buildAging(params):
    cnx = dbConnect()
    airlineCode, flightNumber, origin, destination, initRange = params[2:7]
    try:
        endRange = params[7]
    except IndexError:
        endRange = initRange

    fetchData(cnx, airlineCode, flightNumber, origin, destination, initRange, endRange)

def test(parameters):
    airlineCode, flightNumber, origin, destination, initRange = parameters[2:7]
    # airlineCode, flightNumber, origin, destination, initRange = chopped
    try:
        endRange = parameters[7]
    except IndexError:
        endRange = initRange
     
    print (airlineCode, flightNumber, origin, destination, initRange, endRange)

    # print(str(chopped))
    # script, method, initRange, endRange = parameters
    # dateRange = pd.date_range(initRange, endRange, freq='1D')
    # for idate in dateRange:
    #     print(idate)

def init():
    try:
        method = sys.argv[1]
    except IndexError as error:
        method = ''
    print(method)
    match method:
        case 'checkFlights':
            departureDate = sys.argv[2]
            limit = sys.argv[3]
            checkFlights(departureDate, limit)
        case 'buildAging':
            buildAging(sys.argv)
        case 'help' | '':
            help()
        case 'test':
            test(sys.argv)



if __name__ == "__main__":
    init()