"""This is a max values report for AL."""

import darbyAuth    # Custom module for Darby authentication
import requests
import datetime
import pytz
import json
import re
import csv
import time
import threading
import concurrent.futures

# Threading implementation
thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

# Define paths of URLs
baseUrl = 'https://al-apigateway.drivewithdarby.com/v1'
messagesPath = '/messages/vehicle/'
attributes = '&attributes=vehicleName,groupName,totalVehicleDistance,odometer,gpsOdometer,engTotalHoursOfOperation,totalFuelUsedinLtrs&limit=3000&messageType=BS6_PERIODIC'
page = '&page='

groupsPath = '/vehicles/groupId/'
groupUrlAttributes = '?page=0&pagelimit=200&attributes=vehicleId,vehicleName'

# Authentication
darbyToken = darbyAuth.getDarbyToken('alApiUser','1234567890-alApiUser')
print(f'Authenticated.\nYour token is: {darbyToken}\n\n')

headers = {}
headers['Authorization'] = darbyToken
headers['Content-Type'] = 'application/json'

session = requests.Session()

# Making vehicleId and vehicleName mapping from groupId
def getVehicleMetadataFromGroup(groupId, session):
    print(f'Calling Darby Groups API...\n{baseUrl + groupsPath + groupId}\n')
    with session.get(baseUrl + groupsPath + groupId, headers = headers) as response:
        responseText = json.loads(response.text)
        vehicleList = []
        for i in range(0, len(responseText)):
            vehiclesDict= {
                "vehicleId":responseText[i]['vehicleId'],
                "vehicleName":responseText[i]['vehicleName'],
                "groupName":responseText[i]['groupName']
                }
            vehicleList.append(vehiclesDict)
    return vehicleList

vehicles = getVehicleMetadataFromGroup('e3f062a8-c685-4376-9550-2252e4aa3879',session)
print(f"Group {vehicles[0]['groupName']} has {len(vehicles)} vehicles.")

for i in range(0,len(vehicles)):
    print(vehicles[i]['vehicleId'])

# Making URLs by forming the dates (5Am - 5AM) and rest of URL pieces
def getDateList():
    startTimeOfDay = datetime.datetime(2019,10,28,23,30,0,0,tzinfo=pytz.UTC)
    endTimeOfDAy = datetime.datetime(2019,10,29,23,29,0,0,tzinfo=pytz.UTC)
    dates = []
    for i in range(0,100):
        x = startTimeOfDay - datetime.timedelta(days=i)
        y = endTimeOfDAy - datetime.timedelta(days=i)
        startTs = int((x.timestamp()) * 1000)
        endTs = int((y.timestamp()) * 1000)
        dates.append('?startTime=' + str(startTs) + '&' + 'endTime=' + str(endTs))
    return dates

dl = getDateList()

# making complete list of URLs
def getUrlList():
    completeList = []
    for i in range(0, len(vehicles)):
        url = baseUrl + messagesPath + vehicles[i]['vehicleId']
        for j in range(0, len(dl)):
            completeList.append(url + dl[j] + attributes + page)
    return completeList

# Reversing the list to make latest first
completeUrls = getUrlList()
print(f'\n\n{len(completeUrls)} URLs generated..\n\n')
reversedCompleteUrls = completeUrls[::-1]

# Make a list of all messages for a vehicle for a day
def downloadPage(urlToCall, session):
    session = get_session()
    print('Making API Call')
    with session.get(urlToCall, headers = headers) as response:
        messages = json.loads(response.text)
        count = len(messages)
        return messages

def totalMessagesForADay(oneUrl):
    pageCounter = 0
    totalMessages = []
    while True:
        with concurrent.futures.ThreadPoolExecutor(max_workers=9) as executor:
            executor.map(downloadPage,(oneUrl + str(pageCounter)))
            messagesPerPage = downloadPage(oneUrl + str(pageCounter), session)
            messageCount = len(messagesPerPage)
            totalMessages.extend(messagesPerPage)
            print(f'Current : {messageCount}  |  Total: {len(totalMessages)}')
            if messageCount < 3000:
                print('Done.')
                print('-' * 10)
                print('\n')
                break
            elif messageCount == 3000:
                print('Continuing.')
                pageCounter = pageCounter + 1
                continue
    return totalMessages

#  Get max values for a day

def getMaxValuesForADay(messagesForADay):
    try:
        maxTotalVehicleDistance = max(item['totalVehicleDistance'] for item in messagesForADay)
        print(f'Maximum vehicle distance is: {maxTotalVehicleDistance} kms')
    except ValueError:
        print('There was a ValueError..Inserting NA instead.')
        maxTotalVehicleDistance = 'NA'

    # Max(Vehicle_Odometer)
    try:
        maxOdometer = max(item['odometer'] for item in messagesForADay)
        print(f'Maximum Odometer is: {maxOdometer} kms')
    except ValueError:
        print('There was a ValueError..Inserting NA instead.')
        maxOdometer = 'NA'
        
    # Max(GpsOdometer)
    try:
        maxGpsOdometer = max(item['gpsOdometer'] for item in messagesForADay)
        print(f'Maximum GPS Odometer is: {maxGpsOdometer} kms')
    except ValueError:
        print('There was a ValueError..Inserting NA instead.')
        maxGpsOdometer = 'NA'

    # Max(Total_Eng_Hours_of_Operation)
    try:
        maxEngTotalHoursOfOperation = max(item['engTotalHoursOfOperation'] for item in messagesForADay)
        print(f'Maximum Engine Total Hours of Operation is: {maxEngTotalHoursOfOperation} hrs')
    except ValueError:
        print('There was a ValueError..Inserting NA instead.')
        maxEngTotalHoursOfOperation = 'NA'

    # Max(Eng_Total_Fuel_Used)
    try:
        maxTotalFuelUsedinLtrs = max(item['totalFuelUsedinLtrs'] for item in messagesForADay)
        print(f'Maximum Total Fuel Used is: {maxTotalFuelUsedinLtrs} litres')
    except ValueError:
        print('There was a ValueError..Inserting NA instead.')
        maxTotalFuelUsedinLtrs = 'NA'
    return [ maxTotalVehicleDistance, maxOdometer, maxGpsOdometer, maxEngTotalHoursOfOperation, maxTotalFuelUsedinLtrs ]
        
    
# Parse vehicleID and timestamps from the URL

def parseUrl(oneUrl):
    vehicleIdMatches = re.findall(r"(\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b)",oneUrl)
    timestampMatches = re.findall(r"\d{13}",oneUrl)
    # Converting Timestamps into human readable IST
    startDateTs = int(int(timestampMatches[0]) / 1000)
    endDateTs = int(int(timestampMatches[1]) / 1000)
    startDatetime = datetime.datetime.utcfromtimestamp(startDateTs)
    endDatetime = datetime.datetime.utcfromtimestamp(endDateTs)
    localizedStart = pytz.UTC.localize(startDatetime)
    localizedEnd = pytz.UTC.localize(endDatetime)
    ist = pytz.timezone('Asia/Kolkata')
    startDatetimeInIst = localizedStart.astimezone(ist)
    endDatetimeInIst = localizedEnd.astimezone(ist)
    startDateString = startDatetimeInIst.strftime("%d-%b-%Y %H:%M:%S %p")
    endDateString = endDatetimeInIst.strftime("%d-%b-%Y %H:%M:%S %p")
    print(f'\nDates are: {startDateString} and {endDateString}')
    return [ vehicleIdMatches[0], startDateString, endDateString ]

# function create report and write to file


def reportCreator(inputlist):    
    for i in range(0, len(inputlist)):
        messagesPerDay = getMessagesforVehiclePerDay(inputlist[i])
        valuesPerday = getMaxValuesForADay(messagesPerDay)
        vehicleData = parseUrl(inputlist[i])
        recordInfo = {
            "VehicleId":vehicleData[0],
            "From Date":vehicleData[1],
            "To Date": vehicleData[2],
            "maxTotalVehicleDistance":valuesPerday[0],
            "maxOdometer":valuesPerday[1],
            "maxGpsOdometer":valuesPerday[2],
            "maxEngTotalHoursOfOperation":valuesPerday[3],
            "maxTotalFuelUsedinLtrs":valuesPerday[4]
            }
        recordRow.append(recordInfo)
        print('Writing to file now...')
        with open('/Users/sagardubeydarby/Desktop/cto_report/Concurrent_report/report2.csv', 'r+', newline='') as csvfile:
            wr = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
            fieldnames = ['VehicleId','From Date','To Date','maxTotalVehicleDistance',
            'maxOdometer','maxGpsOdometer','maxEngTotalHoursOfOperation','maxTotalFuelUsedinLtrs']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for i in range(0,len(recordRow)):
                writer.writerow(recordRow[i])
                print(f'Printing {i} rows...')
        print('done')

recordRow = []


# FINAL - Call all URLs one by one

startTiming = time.time()
for i in range(0, 20):
    totalMessagesForADay(reversedCompleteUrls[i])
endTiming = time.time()
msg ='Operation took {:.3f} seconds to complete.'
print(msg.format(endTiming-startTiming))