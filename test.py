from collections import defaultdict
from email.policy import default
import json
import sys
from turtle import tracer
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

def buildDictionary(cnx, airlineCode, flightNumber, origin, destination, initRange, endRange):
    cur = cnx.cursor()

    dateRange = pd.date_range(initRange, endRange, freq='1D')
    ddateRange = ''
    for departureDateO in dateRange:
        departureDateS = str(departureDateO) 
        departureDate = departureDateS[:10]
        ddateRange += "'" + departureDate + "',"
    sdateRange = ddateRange[:-1]

    dirPath = airlineCode + '-' + flightNumber
    try: 
        os.mkdir('./' + dirPath)
    except OSError as error: 
        print('It seems the directory already exists, ignoring...')

    dic = dict(departure_date = [], queried_at = [], availability = [], available_seats = [], total_seats = [], t_minus = [], t_minus_seconds = [])

    sqlQuery = "select departure_date as dep_date, created_at as Queried_At, (available_seats / total_seats) * 100 as Availability, available_seats, total_seats, (-1 * datediff(departure_date, created_at)) as t_minus, timestampdiff(second, departure_date, created_at) as t_minus_seconds\
            from flight_loads \
            where airline_code = '" + airlineCode + "'\
            and flight_number = '" + flightNumber + "' \
            and departure_airport = '" + origin + "' \
            and arrival_airport = '" + destination + "' \
            and total_seats <> 0\
            and departure_date in(" + sdateRange + ")\
            and service_class = 'Y'\
            and created_at <= departure_date\
            order by created_at asc \
            limit 10000;"
    # print(sqlQuery)
    cur.execute(sqlQuery)
    # if cur.rowcount <= 0:
    #     print("No rows were retrieved for the selected period")
    #     cnx.close()
    #     exit()
    fileBaseName = airlineCode + '-' + flightNumber

    htmlpath = dirPath + '/' + fileBaseName + '.html' 
    jsonpath = dirPath + '/' + fileBaseName + '.json'

    for (dep_date, Queried_At, Availability, available_seats, total_seats, t_minus, t_minus_seconds) in cur:        
        sdep_date = str(f'{dep_date}')
        departureDate = sdep_date[:10]
        qa = str(f'{Queried_At}')
        dic['departure_date'].append(departureDate)
        dic['queried_at'].append(qa)
        dic['availability'].append(Availability)
        dic['available_seats'].append(available_seats)
        dic['total_seats'].append(total_seats)
        dic['t_minus'].append(t_minus)
        dic['t_minus_seconds'].append(t_minus_seconds)

    print('Records Retrieved for departure date: ', departureDate, cur.rowcount)
    df = pd.DataFrame.from_dict(dic)
    graphTitle = airlineCode + '-' + flightNumber + ' ' + origin + '-' + destination
    df.sort_values(by=['queried_at'], inplace=True)
    fig = px.line(df, x='queried_at', y="availability", title=graphTitle, color='departure_date', markers=True)
    fig.write_html(htmlpath, auto_open=False)

    jfile = open(jsonpath, 'w')
    jfile.write(df.to_json())
    jfile.close()
    cnx.close()
    normalize(df, dateRange)

def writeFile(path, content, modifier):
    f= open(path, modifier)
    f.write(content)
    f.close()

def normalize(df, dateRange):
    print('kk')
    dic = dict(departure_date = [], queried_at = [], availability = [], available_seats = [], total_seats = [], t_minus = [], t_minus_seconds = [])
    zeroDf = pd.DataFrame.from_dict(dic)
    # for departureDate in dateRange:
    #     # depMinus10 = pd.to_datetime(departureDate) - pd.to_timedelta(10, unit='d')
    #     # ddf = df.query("queried_at > '" + str(depMinus10) + "'")
    #     zeroDf = pd.concat([zeroDf, ddf], ignore_index=True)
    #     writeFile('./t/filterd'+str(departureDate)+'.json', ddf.to_json(), 'w')
    # writeFile('./t/filterd_full_df.json', zeroDf.to_json(), 'w')
    ddf = df.query("t_minus > -11")
    
    ddf1 = ddf.sort_values(by=['t_minus_seconds'], ascending=False)
    fig = px.line(ddf1, x='t_minus_seconds', y="availability", title='t minus in secs', color='departure_date', markers=True)
    fig.write_html('./t/r.html', auto_open=False)
    writeFile('./t/r.json', ddf1.to_json(), 'w')

    ddf2 = ddf.sort_values(by=['queried_at'], ascending=False)
    fig1 = px.line(ddf2, x='queried_at', y="availability", title='created at', color='departure_date', markers=True)
    fig1.write_html('./t/s.html', auto_open=False)
    writeFile('./t/s.json', ddf2.to_json(), 'w')

    ddf3 = ddf.sort_values(by=['t_minus'], ascending=False)
    fig2 = px.line(ddf3, x='t_minus', y="availability", title='t minus in days', color='departure_date', markers=True)
    fig2.write_html('./t/t.html', auto_open=False)
    writeFile('./t/t.json', ddf3.to_json(), 'w')

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
        # and departure_date = '" + departureDate + "'\
        cur.execute("select departure_date as dep_date, created_at as Queried_At, (available_seats / total_seats) * 100 as Availability, available_seats, total_seats\
            from flight_loads \
            where airline_code = '" + airlineCode + "'\
            and flight_number = '" + flightNumber + "' \
            and departure_airport = '" + origin + "' \
            and arrival_airport = '" + destination + "' \
            and total_seats <> 0\
            and departure_date = '" + departureDate + "'\
            and service_class = 'Y'\
            order by created_at asc \
            limit 10000;")

        # if cur.rowcount <= 0:
        #     continue
        
        counter = 0


        fileBaseName = airlineCode + '-' + flightNumber + '-' + departureDate

        htmlpath = dirPath + '/' + fileBaseName + '.html' 
        csvpath = dirPath + '/' + fileBaseName + '.csv'
        jpegpath = dirPath + '/' + fileBaseName + '.json'
        
        

        fileObject = open(csvpath, 'w')
        dic = dict(departure_date = [], queried_at = [], availability = [], available_seats = [], total_seats = [])
        for (dep_date, Queried_At, Availability, available_seats, total_seats) in cur:
            id = len(dic['departure_date'])
            sdep_date = str(f'{dep_date}')
            departureDate = sdep_date[:10]
            qa = str(f'{Queried_At}')
            av = str(f'{round(Availability)}')
            dic['departure_date'].append(departureDate)
            dic['queried_at'].append(qa)
            dic['availability'].append(Availability)
            dic['available_seats'].append(available_seats)
            dic['total_seats'].append(total_seats)
            counter = counter + 1
            id = id + 1
            fileObject.write(str(f'{Queried_At}, {Availability}\n'))
        print('Records Retrieved for departure date: ', departureDate, counter)
        if counter == 0:
            fileObject.close()
            continue
        
        df = pd.DataFrame.from_dict(dic)
        graphTitle = airlineCode + '-' + flightNumber + ' ' + origin + '-' + destination + ' ' + 'departing on: ' + departureDate
        fig = px.line(df, x='queried_at', y="availability", title=graphTitle)
        fig.write_html(htmlpath, auto_open=False)
        # px.write_image(fig, jpegpath, 'jpg')
        jfile = open(jpegpath, 'w')
        jfile.write(df.to_json())
        jfile.close()
        
        fileObject.close()

    cnx.close()
    # print(df.to_json())

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

    print(airlineCode, flightNumber, origin, destination, initRange, endRange)
    # fetchData(cnx, airlineCode, flightNumber, origin, destination, initRange, endRange)
    buildDictionary(cnx, airlineCode, flightNumber, origin, destination, initRange, endRange)

def test(parameters):
    # dic1 = px.data.gapminder().query("continent == 'Oceania'")
    # dic1.to_json()
    dic1 = {
        "departure_date": {
            "1": "2022-03-10",
            "2": "2022-03-10",
            "3": "2022-03-10",
            "4": "2022-03-11",
            "5": "2022-03-11",
            "6": "2022-03-11"

        },
		"queried_at": {
			"1": "2022-03-08",
			"2": "2022-03-09",
			"3": "2022-03-10",
			"4": "2022-03-09",
			"5": "2022-03-10",
			"6": "2022-03-11",
		},
		"availability": {
			"1": "60",
			"2": "50",
			"3": "40",
			"4": "30",
			"5": "20",
			"6": "10",
		}
    }
    # dic1 = ''
    # print(dic1.to_json())
    # exit()
    # dic1 = {
    #     '2022-03-10': ['2022-03-08', '2022-03-09', '2022-03-10'], 'availability': ['60', '50', '40'], 
    #     '2022-03-11': ['2022-03-09', '2022-03-10', '2022-03-11'], 'availability': ['30', '20', '10']
    # }
    # dic2 = {'date': ['2022-03-10', '2022-03-11', '2022-03-12'], 'availability': ['30', '20', '10']}
    # dic = {'a1': dic1, 'a2': dic2}

    df = pd.DataFrame.from_dict(dic1)
    td = pd.to_timedelta(1,'d')
    # depMinus10 = pd.to_datetime(departureDate) - pd.to_timedelta(10, unit='d')
    iniDate = pd.to_datetime('2022-03-09') - td
    print(iniDate)
    df.query("queried_at > '" + str(iniDate) + "'", inplace=True)
    print(df)

    # df.sort_values(by=['queried_at'], inplace=True)
    # print(df.head(n=3))
    # exit()
    # q = df.query("departure_date == '2022-03-12'")
    # q.sort_values(by=['queried_at'])
    # print(q.head(n=20))
    # df.sort_index()
    
    # fig = px.line(df, x='queried_at', y="availability", color='departure_date', title="Mati Test", markers=True)
    # fig.write_html("./test.html", auto_open=False)

def init():
    try:
        method = sys.argv[1]
    except IndexError as error:
        method = ''
    # print(method)
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