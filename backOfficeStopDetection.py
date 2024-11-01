import math
import json
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import csv
import tzlocal
import threading
import requests
from typing import List, Dict
from requests.exceptions import Timeout,RequestException
from fastapi.middleware.cors import CORSMiddleware
from collections import defaultdict
import time
stop_radius = 12
threshold_station_distance = 20 
import asyncio
import queue 
from logConfiguration import *
from collections import defaultdict, deque, OrderedDict

stoplistData = []
arrivedDistance = 0
arrivedTime = False
arrivedLatLongDict = {}
stop_management_policy_dict = {}
arrivedTimeDict = {}
stationLatLongDict = {}
number_of_sequences = 5
tripList_dict_SD = {}
tripID_Dict_SD = {}
vehRouteList = {}
tripList_dict = {}
performed_trips_confirmations = {}
tripIDStatusDriverTabletDict = {}
lastStoredConfirmation = 1

trip_update_queue = queue.Queue()
rawStopDetectedResultList = []
nstopDetectedResultList = []
shouldVLUDetectStop = True
TotalVehicleNos = 1
last_two_grouped_sequences = defaultdict(lambda: deque(maxlen=number_of_sequences))       
qualiflyingDepartingTripID = -1
qualiflyingApproachingTripID = -1

control_vectors = []
announceMessageStatusList = ["stopArrivalMessage","nextStopMessage"]
displayMessageStatusList = ["DisplayArrival","DisplayNextStop"]
previousDetectedStopID = 123456
logs_threading_lock = threading.Lock()
vlu_new_message = ""

app = FastAPI()
clients: Dict[str,WebSocket] = {}

retry_delay = 5  
origins = ["*"]
trip_update_queue = queue.Queue()
driverTabletTripDict = {}
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Message(BaseModel):
    MsgType: str
    iServiceID : str
    ConfirmationNum :str
    TripStatus:str
    StopID : str
    DeviceNumber : str
    Latitude :str
    Longitude : str
    DeviceTime : str
    VehNo : str

@app.websocket("/driverTablet")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    SocketStatus_Logs.info("Driver Tablet WebSocket Connected")
    client_ip_address = websocket.client.host
    clients[client_ip_address] = websocket
    try:
        while True:
            data = await websocket.receive_json()
            print(data)
    except WebSocketDisconnect:
        SocketStatus_Logs.info("Driver Tablet WebSocket Disconnected")
        del clients[client_ip_address]
    except Exception as e:
        SocketStatus_Logs.error(f"An error occurred: {e}")
        await websocket.close()
        del clients[client_ip_address]

def sendTripUpdated(tripDetails):
    try:
        
        trip_update_queue.put(tripDetails)

    except Exception as e:
        print(e)
        exception = (f"[Exception in sendTripUpdated: ] [{str(e)}]".strip())
        AllException_Logs.error(exception)

def sendMessageToDriverTablet(message):
     for key, client in list(clients.items()):
        try:
            asyncio.run(client.send_json(message))  
            DriverTabletTripUpdate_Logs.info("DriverTabletWS %s", message)
        except Exception as e:
            AllException_Logs.error(f"Error sending message to client: {e}")
def updateControlledVectorList(new_control_vectors):
    global control_vectors
    control_vectors = new_control_vectors

def tripListDictForStopDetection(new_tripList,tripIDs):
    global tripList_dict_SD,tripID_Dict_SD

    if 'tripList_dict_SD' in globals():
        del tripList_dict_SD
        
    if 'tripID_Dict_SD' in globals():
        del tripID_Dict_SD

    tripList_dict_SD = new_tripList
    tripID_Dict_SD = tripIDs

stop_status = OrderedDict()
raw_stop_status = OrderedDict()
stop_status_time = OrderedDict()


processed_statuses = {}

def findPreviousStopID(detectedStopID,uliveGPSTime, RouteID):
    global tripList_dict_SD,tripID_Dict_SD
    filtered_tripID_Dict_SD = {key: value for key, value in tripID_Dict_SD.items() if value['iRouteID'] == RouteID}
    filtered_tripList_dict_SD = {
        key: [trip for trip in trips if trip['iRouteID'] == RouteID]
        for key, trips in tripList_dict_SD.items() if any(trip['iRouteID'] == RouteID for trip in trips)
    }
    if detectedStopID in filtered_tripList_dict_SD:
        min_diff = float('inf')
        gpstime = datetime.strptime(uliveGPSTime, "%Y-%m-%d %H:%M:%S").time()

        scheduled_trip_list = filtered_tripList_dict_SD[detectedStopID]

        for trip in scheduled_trip_list:

            arrival_time_str = trip['ArrivalTime']

            dStopID = int(trip['iStopID'])
            dtripID = trip['tripID']
            arrival_time = datetime.strptime(arrival_time_str, "%H:%M:%S").time()
            time_diff = abs((datetime.combine(datetime.min, gpstime) - datetime.combine(datetime.min, arrival_time)).total_seconds())
            
            if time_diff <= min_diff:
                min_diff = time_diff
                gStopID = dStopID
             
                gtripID = dtripID

        if gtripID-1 in filtered_tripID_Dict_SD:
            prevTrip = filtered_tripID_Dict_SD[gtripID-1]
            gStopID = int(prevTrip['iStopID'])

        return gStopID
    
    else:
        return 0

def statusPreProcessing(status,bstatus,stopID,sequenceList, uliveGPSTime, uliveLatLong, uSpeed,distFromStop,announceStopMessage,displayStopMessage,busAngle,sequenceAngle,system_time_str, nextStopOnApproach, RouteID, VehNo):
    global stop_status, stop_status_time, TotalVehicleNos
    stop_id_status = str(stopID) + '-'+ str(RouteID) + status

    if stop_id_status not in stop_status or nextStopOnApproach :
        if len(stop_status) >= (27 * TotalVehicleNos):
            stop_status.popitem(last=False)
            stop_status_time.popitem(last=False)
            
        if bstatus == "Arrived,NextStop":
            statusList = bstatus.split(",")
            print("---------------------------------Dual Status-----------------------------------------")
            for i in range(len(statusList)):
                status = statusList[i]
                stop_id_status = str(stopID) + '-' + str(RouteID) + status
                if stop_id_status not in stop_status:
                    stop_status[stop_id_status] = status
                    stop_status_time[stop_id_status] = system_time_str
                    announceStopMessage = announceMessageStatusList[i]
                    displayStopMessage = displayMessageStatusList[i]
                    # if status == "NextStop":
                    statusFinalProcessing(stopID,uliveGPSTime,sequenceList,announceStopMessage,displayStopMessage,status,uSpeed,uliveLatLong,distFromStop,stop_status, nextStopOnApproach, RouteID, VehNo)
                    # else:
                    #     print('StatusPreProcessing')
                        #VLUStopDetectedNotProcessed_Logs.info("VLUStopDetectedNotProcessed: %s",[stopID, sequenceList[0],float(round(sequenceList[1],2)), uliveGPSTime,uSpeed, uliveLatLong, status, "Arrived Message was not sent for announcements as we marked Arrived and NextStop at the same time."])
                if len(stop_status) >= (27 * TotalVehicleNos):
                    stop_status.popitem(last=False)
                    stop_status_time.popitem(last=False)  

        else:
            if nextStopOnApproach:
                stop_id_status = str(stopID) + '-' + str(RouteID) + "Approaching"
                stop_status[stop_id_status] = "Approaching"
                stop_status_time[stop_id_status] = system_time_str
            else:
                stop_id_status = str(stopID) +'-'+  str(RouteID) + status
                stop_status[stop_id_status] = status
                stop_status_time[stop_id_status] = system_time_str

            statusFinalProcessing(stopID,uliveGPSTime,sequenceList,announceStopMessage,displayStopMessage,status,uSpeed,uliveLatLong,distFromStop,stop_status, nextStopOnApproach, RouteID, VehNo)

def updateStatus(stopID,sequenceList, uliveGPSTime, uliveLatLong, uSpeed,distFromStop,number_of_detected_stops, system_time_str):
    global previousDetectedStopID,stop_status, stop_status_time
    bstatus = ''
    nextStopOnApproach = False
    seqList = list(sequenceList)
    vectorID, angleD1, stopID, distFromStop, busAngle, sequenceAngle, RouteID, VehNo = sequenceList
    seqNum = int(vectorID)

    try:
        previousStopDepartedStopID = findPreviousStopID(stopID,uliveGPSTime, RouteID)
        lastStopID_NextStop = str(previousStopDepartedStopID)+ '-' + str(RouteID) +"NextStop"
        currentStopApproching = str(stopID)+ '-' + str(RouteID) +"Approaching"
        currentStopArrived = str(stopID)+ '-' + str(RouteID) +"Arrived"
        announcementTimeDifference = 0

        if seqNum >= -15 and seqNum <= -8:
            status = "Approaching"
            announceStopMessage = "approachingStopMessage"
            displayStopMessage = "DisplayApproaching"
            # print(stopID,status,seqNum,"----------------------------------------------")

            if lastStopID_NextStop in stop_status_time:
                lastAnnouncedNextStopTime = stop_status_time[lastStopID_NextStop]
                announcementTimeDifference = abs(system_time_str-lastAnnouncedNextStopTime).total_seconds()
                # print("------------------------------Next Stop Time Difference ---------------------------------",announcementTimeDifference)
                
            if lastStopID_NextStop not in stop_status or announcementTimeDifference > repeatNexStopOnApproachingTime:
                status = "NextStop"
                announceStopMessage = "nextStopMessage"
                displayStopMessage = "DisplayNextStop"
                nextStopOnApproach = True
                stop_status[lastStopID_NextStop] = status
                stop_status_time[lastStopID_NextStop] = system_time_str

                # if announcementTimeDifference > repeatNexStopOnApproachingTime:
                #     stop_status_time[lastStopID_status] = system_time_str

        elif seqNum >= -7 and seqNum <= -1:
            busSpeed = float(uSpeed)
            if busSpeed >= busSpeedLimitForStatus:
                if seqNum >= -7 and seqNum <= -4:
                    status = "Arrived"
                    announceStopMessage = "stopArrivalMessage"
                    displayStopMessage = "DisplayArrival"

                else:
                    bstatus = "Arrived,NextStop"
                    status = "NextStop"
                    announceStopMessage = "nextStopMessage"
                    displayStopMessage = "DisplayNextStop"
            else:

                if seqNum >= -7 and seqNum <= -4:
                    status = "Approaching"
                    announceStopMessage = "approachingStopMessage"
                    displayStopMessage = "DisplayApproaching"
                else:
                    status = "Arrived"
                    announceStopMessage = "stopArrivalMessage"
                    displayStopMessage = "DisplayArrival"

        ## if current stop has approaching or arrived status detected then only it goes for departing
        elif seqNum >= 1 and (currentStopArrived in stop_status or currentStopApproching in stop_status):
            status = "NextStop"
            announceStopMessage = "nextStopMessage"
            displayStopMessage = "DisplayNextStop"
        else:
            status = ""

        if status != "" :
            if status == "Approaching" or status == "Arrived":
                if lastStopID_NextStop in stop_status:
                    statusPreProcessing(status,bstatus,stopID,seqList, uliveGPSTime, uliveLatLong, uSpeed,distFromStop,announceStopMessage,displayStopMessage,busAngle,sequenceAngle,system_time_str,nextStopOnApproach, RouteID, VehNo)
                else:
                    if previousDetectedStopID == stopID:
                        statusPreProcessing(status,bstatus,stopID,seqList, uliveGPSTime, uliveLatLong, uSpeed,distFromStop,announceStopMessage,displayStopMessage,busAngle,sequenceAngle,system_time_str, nextStopOnApproach, RouteID, VehNo)

                previousDetectedStopID = stopID 
            
            else:
                if status == "NextStop"  or nextStopOnApproach:
                    statusPreProcessing(status,bstatus,stopID,seqList, uliveGPSTime, uliveLatLong, uSpeed,distFromStop,announceStopMessage,displayStopMessage,busAngle,sequenceAngle,system_time_str, nextStopOnApproach, RouteID, VehNo)
                    if nextStopOnApproach:
                        previousDetectedStopID = stopID 

    except Exception as e:
        print(e)
        exception = f"[Error in updateStatus function: ] [{str(e)}]".strip()
        AllException_Logs.error(exception)
        
def save_results():

    while True:
        try:
        
            if len(nstopDetectedResultList)>0:
                rows_to_write = '\n'.join(
                    ','.join(map(str, row)) for row in nstopDetectedResultList
                ) + '\n'
                
                            # Write all rows in a single I/O operation
                with open(output_file, 'a') as log_file:
                    log_file.write(rows_to_write)
                
                # Clear stopDetectedResult to avoid duplications
                nstopDetectedResultList.clear()

            time.sleep(60)
        except Exception as e:
            logging.error(f"Error in logging : {e}")
            
def smartSearchAlgo(liveLatLong, radius, busAngle, system_time_str, rSpeed, rliveGPSTime, control_vectors, RouteID, VehNo): 
    global stop_status, TotalVehicleNos
    results = []
    # print("Controlled Vector Count: ",len(control_vectors))
    for stop in control_vectors:
        try:
            istopID = stop['StopID']
            sequenceAngle = stop['DirectionDegrees']
            distanceFromStop = stop['MapDistanceFromStop']
            seqNum = stop['SeqNo']
            lat = stop['Lat']
            long = stop['Lng']
            routeID = stop["routeID"]
            if sequenceAngle == "NA":
                continue

            coord2 = (lat, long)
            sequenceAngle = float(sequenceAngle)
            currDistFromStop = calculateHaverSineDistance(liveLatLong, coord2)

            if currDistFromStop <= radius:
                deltaAngle = abs(busAngle - sequenceAngle)
                if deltaAngle <= 180:
                    pass
                else:
                    deltaAngle = 360-deltaAngle

                deltaAngle = round(deltaAngle,2)
                busAngle = round(busAngle,2)
                seqNum = int(seqNum)
                approchNextStopStatus = False

                if seqNum >= -15 and seqNum <= -8:
                    status = "Approaching"

                elif seqNum >= -7 and seqNum <= -1:
                
                    busSpeed = float(rSpeed)
                    if busSpeed >= busSpeedLimitForStatus:
                        if seqNum >= -7 and seqNum <= -4:
                            status = "Arrived"
                        else:
                            status = "Arrived,NextStop"
                    else:
                        if seqNum >= -7 and seqNum <= -4:
                            status = "Approaching"
                        else:
                            status = "Arrived"

                elif seqNum >= 1:
                    status = "NextStop"
                else:
                    status = ""
                ulat,ulong = liveLatLong
                if deltaAngle > busAngleThreshold:
                    # VLUAllStopDetected_Logs.info("VLUAllStopDetected: %s", [istopID,str(seqNum),str(deltaAngle),busAngle,sequenceAngle, "Invalid",system_time_str,rliveGPSTime,rSpeed,str(liveLatLong),"",""])
                    rawStopDetectedResultList.append([istopID,seqNum,deltaAngle,busAngle,sequenceAngle, "Invalid",system_time_str,rliveGPSTime,rSpeed,ulat,ulong,"", VehNo])
                else:  
                    results.append((istopID, seqNum, distanceFromStop, lat, long, currDistFromStop,deltaAngle,busAngle,sequenceAngle))
                    statusList = status.split(",")
                    for i in range(len(statusList)):
                        status = statusList[i]
                        stop_id_status = str(istopID)+ '-' + str(RouteID) + status

                        if stop_id_status not in raw_stop_status:
                            raw_stop_status[stop_id_status] = status
                            
                            if len(raw_stop_status) >= (27 * TotalVehicleNos):
                                raw_stop_status.popitem(last=False)
                            # VLUAllStopDetected_Logs.info("VLUAllStopDetected: %s",[istopID,str(seqNum),str(deltaAngle),busAngle,sequenceAngle, status,system_time_str, rliveGPSTime,rSpeed,str(liveLatLong),status,""])
                            rawStopDetectedResultList.append([istopID,seqNum,deltaAngle,busAngle,sequenceAngle, status,system_time_str, rliveGPSTime,rSpeed,ulat,ulong,status, VehNo])
                        else:
                            # VLUAllStopDetected_Logs.info("VLUAllStopDetected: %s",[istopID,str(seqNum),str(deltaAngle),busAngle,sequenceAngle, status,system_time_str, rliveGPSTime,rSpeed,str(liveLatLong),"",""])
                            rawStopDetectedResultList.append([istopID,seqNum,deltaAngle,busAngle,sequenceAngle, status,system_time_str, rliveGPSTime,rSpeed,ulat,ulong,"", VehNo])
                        
        except Exception as e:
            print(e)
            exception = f"[Error in smartSearchAlgo function: ] [{str(e)}]".strip()
            AllException_Logs.error(exception)

    return results

def stopDetectionAlgo(groupedSeq, rliveGPSTime, rliveLatLong, rSpeed, system_time_str):
    global stop_status, tripList_dict_SD,qualiflyingDepartingTripID, qualiflyingApproachingTripID
    # print(len(groupedSeq))
    # print(groupedSeq)
    sys_time = datetime.now() - LocaltimeDelta
    current_SystemTime = sys_time.time() 
    number_of_detected_stops = len(groupedSeq)
    max_seq1_for_stopID = defaultdict(lambda: (float(-999), None))

    for stopID2, sequences in groupedSeq.items():
        vector_data = list(sequences)
        for seq in vector_data:
            vectorID, angleD1, stopID, distFromStop, busAngle, sequenceAngle, RouteID, VehNO = seq
            vectorID = vectorID
            if vectorID > max_seq1_for_stopID[stopID2][0]:
                max_seq1_for_stopID[stopID2] = seq

    if number_of_detected_stops > 1:
        try:
            qualifyingDepartVectorMinValue = 999
            qualiflyingConfirmationNum = math.inf
            qualifyingDepartVectorMinSeq = ''
            qualifyingApproachVectorMaxValue = -999
            qualifyingApproachVectorMaxSeq = ''
            smallestTripID = 0

            stopID_depart_status = ""
            # print(tripIDStatusDriverTabletDict)
            # print("-------------------------------------------------------------------------------------------------------------------------")
            for stopID2, vector_data in list(max_seq1_for_stopID.items()):
                min_diff = float('inf')
                filtered_tripList_dict_SD = {
                    key: [trip for trip in trips if trip['iRouteID'] == RouteID]
                    for key, trips in tripList_dict_SD.items() if any(trip['iRouteID'] == RouteID for trip in trips)
                }
                if stopID in filtered_tripList_dict_SD:
                    trips_for_current_stopID = filtered_tripList_dict_SD[stopID]

                    for trip in trips_for_current_stopID:
                        arrival_time_str = trip['ArrivalTime']
                        arrival_time = datetime.strptime(arrival_time_str, "%H:%M:%S").time()
                        time_diff = abs((datetime.combine(datetime.min, current_SystemTime) - datetime.combine(datetime.min, arrival_time)).total_seconds())
                        tripID_pickedup = str(trip['confirmationNumber']) + "PICKEDUP"
                        if time_diff <= min_diff and tripID_pickedup not in tripIDStatusDriverTabletDict:
                            min_diff = time_diff
                            smallestTripID  = trip['tripID']

                    vectorID, angleD1,stopID,distFromStop,busAngle,sequenceAngle, RouteID, VehNO = vector_data
                    if vectorID > 0 :
                        if vectorID < qualifyingDepartVectorMinValue :
                            qualifyingDepartVectorMinValue = vectorID
                            qualifyingDepartVectorMinSeq = vector_data
                            stopID_depart_status = str(stopID) + '-'+ str(RouteID) + "NextStop"
                            qualiflyingDepartingTripID = smallestTripID

                        elif vectorID == qualifyingDepartVectorMinValue and smallestTripID < qualiflyingDepartingTripID:
                            qualifyingDepartVectorMinValue = vectorID
                            qualifyingDepartVectorMinSeq = vector_data
                            stopID_depart_status = str(stopID) + '-'+ str(RouteID) + "NextStop"
                            qualiflyingDepartingTripID = smallestTripID
                        else:
                            pass

                    if vectorID <= 0 :
                        if vectorID > qualifyingApproachVectorMaxValue :
                            qualifyingApproachVectorMaxValue = vectorID
                            qualifyingApproachVectorMaxSeq = vector_data
                            qualiflyingApproachingTripID = smallestTripID
                            # 
                        elif vectorID == qualifyingApproachVectorMaxValue and smallestTripID < qualiflyingApproachingTripID:
                            qualifyingApproachVectorMaxValue = vectorID
                            qualifyingApproachVectorMaxSeq = vector_data
                            qualiflyingApproachingTripID = smallestTripID
                        else:
                            pass
                        
                distFromStop = float(distFromStop)

            if abs(qualifyingDepartVectorMinValue) < abs(qualifyingApproachVectorMaxValue) and  stopID_depart_status not in stop_status:
                new_vector_data = qualifyingDepartVectorMinSeq
                if new_vector_data != "":
                    vectorID, angleD1, stopID, distFromStop, busAngle, sequenceAngle, RouteID, VehNO = new_vector_data
                    # print("Departing: ",stopID)
            else:
                new_vector_data = qualifyingApproachVectorMaxSeq
                if new_vector_data != "":
                    vectorID, angleD1, stopID, distFromStop, busAngle, sequenceAngle, RouteID, VehNO = new_vector_data
                    # print("Approaching: ",stopID)

            if new_vector_data != "":
                updateStatus(stopID, new_vector_data, rliveGPSTime, rliveLatLong, rSpeed,distFromStop,number_of_detected_stops,system_time_str)
                    # print(stopID, sequences, rliveGPSTime, rliveLatLong, rSpeed,distFromStop,number_of_detected_stops,busAngle,sequenceAngle)
            
        except Exception as e:
            print(e)
            exception = f"[Error in stopDetectionAlgo function  number_of_detected_stops>1: ] [{str(e)}]".strip()
            AllException_Logs.error(exception)
                    
    else:
        for stopID, vector_data in list(max_seq1_for_stopID.items()):
            try:
                vectorID, angleD1, stopID, distFromStop, busAngle,sequenceAngle, RouteID, VehNo = vector_data
                distFromStop = float(distFromStop)
                # print("Single stop detected: ",stopID)
                updateStatus(stopID, vector_data, rliveGPSTime, rliveLatLong, rSpeed,distFromStop,number_of_detected_stops,system_time_str)
                
            except Exception as e:
                print(e)
                exception = f"[Error in stopDetectionAlgo function  number_of_detected_stops = 1: ] [{str(e)}]".strip()
                AllException_Logs.error(exception)
def calculateHaverSineDistance(coord1, coord2):
    try:
        lat1, lon1 = coord1
        lat2, lon2 = coord2
        R = 6371000  # Radius of Earth in meters

        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)

        a = math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(
            math.radians(lat1)
        ) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) * math.sin(dlon / 2)

        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = round((R * c),2)
        return distance
    except Exception as e:
        logging.error("calculateHaverSineDistance: ", e)
        exception = f"[HaverSineDistance] [{str(e)}]".strip()
        AllException_Logs.error(exception)


def readLiveData(file_path):
    liveGPSData = []
    with open(file_path, mode='r') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            liveGPSData.append(((float(row['vLat']), float(row['vLong'])), row['dSpeed'],row['dtDeviceTime']))
    return liveGPSData

# live_gps_data = readLiveData('/home/itcurves/Downloads/GoldlineLive_19Aug.csv')

busDirectionAngle = 0.0
system_timezone = tzlocal.get_localzone()
        
def check_time_difference(arrival_time_str):
    arrival_time = datetime.strptime(arrival_time_str, '%H:%M:%S').time()
    current_time = (datetime.now()- LocaltimeDelta).time() 
    # Convert both times to datetime objects with today's date
    today_date = datetime.today().date()
    arrival_datetime = datetime.combine(today_date, arrival_time)
    current_datetime = datetime.combine(today_date, current_time)
    # Calculate the difference
    time_difference = abs((current_datetime - arrival_datetime).total_seconds())
    if time_difference <= maxBusLateAllowedTime:
        return True
    else:
        return False

def updateTripListDict(new_tripList,tripIDs,tripIDStopIDDict, totalVEh):
    global tripList_dict,tripID_Dict, gtripIDStopIDDict, total_stops, TotalVehicleNos
    tripList_dict = new_tripList
    tripID_Dict = tripIDs
    TotalVehicleNos = totalVEh
    gtripIDStopIDDict = tripIDStopIDDict
    total_stops = len(tripID_Dict)
    
def createMessageFormat(statusType, confirmationNumber,dataForDisplay, dataForSpeech, stationID, announcementFlag,announceRouteFlag,displayFlag,dCode, routeID,uSpeed,uliveLatLong,stop_name,sequence,delta,distFromStop,statusDetectedTime,scheduledTime,index_for_detected_stop,gpstime,tripIDToDelete,nextStopOnApproach, VehNo):
    global lastStoredConfirmation,performed_trips_confirmations
    try:

        formatted_message = {
            "MsgType" : statusType,
            "ConfirmationNumber" : confirmationNumber,
            "DataforAVA" : "",
            "DataForDisplay":dataForDisplay,
            "MessageForTextToSpeech" : dataForSpeech,
            "StationID": stationID,
            "bAnnounce" : announcementFlag,
            "EmailList" : "",
            "bAnnounceRoute":announceRouteFlag,
            "bDisplay":displayFlag,
            "DestSignageCode":dCode,
            "StatusDetectedTime":statusDetectedTime,
            "RouteID":routeID,
            "VectorID":sequence,
            "BusSpeed":uSpeed,
            "VehicleNo":VehNo
        
        }
        final_message = json.dumps(formatted_message)
   
        confirmationNo_with_status = str(confirmationNumber) + statusType
        if confirmationNo_with_status not in performed_trips_confirmations or nextStopOnApproach: 
            VLUPushMessages_Logs.info("MESSAGE RECEIVED: %s", final_message)
            
            if lastStoredConfirmation != confirmationNumber:
                lastStoredConfirmation = confirmationNumber
                
            performed_trips_confirmations[confirmationNo_with_status] = True
            if shouldVLUDetectStop:
                print(final_message)
            #flat,flong = uliveLatLong
            #nstopDetectedResultList.append([stop_name, stationID, sequence,delta, gpstime,statusDetectedTime, scheduledTime,uSpeed, flat,flong, statusType, dataForSpeech,displayFlag,announcementFlag,VehNo,confirmationNumber])

    except Exception as e:
        print(e)
        exception = f"[createMessageFormat: ] [{str(e)}]".strip()
        AllException_Logs.error(exception)


def process_diverTablet_trips_queue():
    while True:
        if not trip_update_queue.empty():
            message = trip_update_queue.get()
            if clients:
                sendMessageToDriverTablet(message)
                tripStatusUpdatingRes = requests.post(f"{api_base_url}{'VLUTripStatusUpdate'}",headers=headers, data=trip_payload)

                if tripStatusUpdatingRes:
                    print("BackOffice API %s", message)
                    
                break  
            else:
                trip_payload = json.dumps(message)
                
                while True:
                    try:
                        tripStatusUpdatingRes = requests.post(f"{api_base_url}{'VLUTripStatusUpdate'}",headers=headers, data=trip_payload)

                        if tripStatusUpdatingRes:
                            #DriverTabletTripUpdate_Logs.info("BackOffice API %s", message)
                            print("BackOffice API %s", message)
                            break  
                    except (Timeout, RequestException) as e:
                        if clients:
                            sendMessageToDriverTablet(message)
                            break
                        time.sleep(retry_delay)  
                       
                    except Exception as e:
                        if clients:
                            sendMessageToDriverTablet(message)
                            break
                        time.sleep(retry_delay)  
                    
                    

            trip_update_queue.task_done()

        time.sleep(0.2)

# Start the thread to process the queue
thread = threading.Thread(target=process_diverTablet_trips_queue)
thread.start()

def driverTabletTripsUpdater(msgType,iServiceID,confirmationNum,tripStatus,stopID,stopName,skippedPickedUp,tripID,uliveLatLong, VehNo, RouteID):
    global tripIDStatusDriverTabletDict
    latitude,longitude = uliveLatLong 
    trip_status_data = {  "MsgType":msgType,
                            "iServiceID": iServiceID,
                            "ConfirmationNum": str(confirmationNum),
                            "DeviceNumber" : deviceNumber,
                            "VehNo": str(VehNo),
                            "RouteID": RouteID,
                            "TripStatus" : tripStatus,
                            "StopID" : stopID,
                            # "StopName" : stopName,
                            "Latitude" : str(latitude),
                            "Longitude" : str(longitude),
                            "DeviceTime" : str(datetime.now()- LocaltimeDelta)
                        }

    try:
        tripID_tripStatus = str(confirmationNum)+ tripStatus
        if tripID_tripStatus not in tripIDStatusDriverTabletDict:
            sendTripUpdated(trip_status_data)
            
            if len(tripIDStatusDriverTabletDict) > 500:
                del tripIDStatusDriverTabletDict[next(iter(tripIDStatusDriverTabletDict))]
            tripIDStatusDriverTabletDict[tripID_tripStatus] = tripStatus

    except Exception as e:
        print(e)
        exception = f"[driverTabletTripsUpdater: ] [{str(e)}]".strip()
        Network_Logs.warning(exception)
def updateStopManagementPolicyDict(new_stop_policy):
    global stop_management_policy_dict
    stop_management_policy_dict = new_stop_policy
    
def statusFinalProcessing(stopID,uliveGPSTime,sequenceList,announceStopMessage,displayStopMessage,status,uSpeed,uliveLatLong,distFromStop,stop_status, nextStopOnApproach, RouteID, VehNo):
    global gArrivalTime,gStopID, gRouteID, gStopName,gdestinationCode, gconfirmationNumber, gtripID,tripList_dict,tripID_Dict,tripAllData,gServiceID,performed_trips_confirmations, total_stops, tripIDStatusDriverTabletDict
    try:
        index_for_detected_stop = -1
        filtered_trips = {key: value for key, value in tripID_Dict.items() if value['iRouteID'] == RouteID}
        tripList_dict_filtered = {
            key: [trip for trip in trips if trip['iRouteID'] == RouteID]
            for key, trips in tripList_dict.items() if any(trip['iRouteID'] == RouteID for trip in trips)
        }
        if stopID in tripList_dict_filtered:
            min_diff = float('inf')
            try:
                system_current_time_for_gps = (datetime.now() - LocaltimeDelta).time()
                
            except Exception as e:
                system_current_time_for_gps = datetime.strptime(uliveGPSTime, "%Y-%m-%d %H:%M:%S").time()
                
                print(e)
            scheduled_trip_list = tripList_dict_filtered[stopID]

            for trip in scheduled_trip_list:
                confirmationNumber = trip['confirmationNumber']
                stop_number = trip['StopNumber']
                arrival_time_str = trip['ArrivalTime']
                depart_time = trip['DepartTime']
                dRouteID = trip['iRouteID']
                dStopID = trip['iStopID']
                ddestinationCode = trip['DestSignageCode']
                dtripID = trip['tripID']
                dStopName = trip['PUPerson']
                arrival_time = datetime.strptime(arrival_time_str, "%H:%M:%S").time()
                time_diff = abs((datetime.combine(datetime.min, system_current_time_for_gps) - datetime.combine(datetime.min, arrival_time)).total_seconds())
                index_for_detected_stop += 1
                if time_diff <= min_diff:
                    min_diff = time_diff
                    stop_number= stop_number
                    gArrivalTime= arrival_time_str
                    gStopID = int(dStopID)
                    gRouteID=int(dRouteID)
                    gStopName = dStopName
                    gdestinationCode = ddestinationCode
                    gconfirmationNumber = confirmationNumber
                    gtripID = dtripID
                    gServiceID=trip['iServiceID']
                    detectedServiceID = trip['iServiceID']
                    detectedConfirmationNum = confirmationNumber
                    detectedStopID = int(dStopID)
                    detectedStopName = dStopName
                    detectedTripID = dtripID
            isBusOnTime = check_time_difference(gArrivalTime)

            if isBusOnTime:
                if status == "NextStop" and not nextStopOnApproach:
                    gtripID = gtripID+1
                    if gtripID in filtered_trips:
                        nextTrip = filtered_trips[gtripID]
                        gStopID = int(nextTrip['iStopID'])
                        gRouteID=int(nextTrip['iRouteID'])
                        gconfirmationNumber = nextTrip['confirmationNumber']
                        gArrivalTime  = nextTrip['ArrivalTime']
                        gServiceID=nextTrip['iServiceID']
                        gStopName = nextTrip['StopName']
                try:
                    stopListDD = []
                    prev_iRSTID = filtered_trips[gtripID]['iRSTId']  # Assuming the first stop as starting point
                    last_valid_stop = None  # To store the last stop with the same iRSTID

                    for offset in range(0, len(filtered_trips)):  # For the first 7 stops (0 to 6)
                        stopID = (gtripID + offset - 1) % total_stops + 1
                        current_iRSTID = filtered_trips[stopID]['iRSTId']

                        if current_iRSTID != prev_iRSTID:
                            # Append the last valid stop with the same iRSTID before breaking
                            if last_valid_stop and last_valid_stop not in stopListDD:
                                stopListDD.append(last_valid_stop)
                            break
                        else:
                            # Keep track of the last valid stop with the same iRSTID
                            last_valid_stop = {
                                "id": int(filtered_trips[stopID]['iStopID']),
                                "name": filtered_trips[stopID]['StopName'],
                                "remainingStops": offset,
                                "arrivalTime" : filtered_trips[stopID]['ArrivalTime']
                            }
                        # Update prev_iRSTID for the next iteration
                        prev_iRSTID = current_iRSTID
                        # After the loop ends, ensure the last stop with the same iRSTID is appended
                        if last_valid_stop and last_valid_stop not in stopListDD and len(stopListDD)<6:
                            stopListDD.append(last_valid_stop)

                except Exception as e:
                    print(e)

                bAnnounceType = "bAnnounce"+status
                bDisplayType = "bDisplay"+status

                if (gStopID,RouteID) in stop_management_policy_dict:
                    filtered_row = stop_management_policy_dict[gStopID,RouteID]
                    dataForSpeech = filtered_row[announceStopMessage]
                    dataForDisplay = filtered_row[displayStopMessage]
                    announcementFlag = filtered_row[bAnnounceType]
                    announceRouteFlag = filtered_row['bAnnounceRoute']
                    displayFlag = filtered_row[bDisplayType]
                    detected__time = (datetime.now() - LocaltimeDelta).time()
                    detected__time_str = detected__time.strftime("%H:%M:%S")
                    stop_name = filtered_row['stationName']
                    waitTimeAnnounce = filtered_row["waitTimeAnnounce"+status]
                    repeatDisplay = filtered_row["repeatDisplay"+status]
                    repeatAnnounce = filtered_row["repeatAnnounce"+status]

                    # createMessageFormat(gconfirmationNumber,status,dataForDisplay,dataForSpeech,gStopID,flagDict[str(announcementFlag)],flagDict[str(announceRouteFlag)],flagDict[str(displayFlag)],gdestinationCode,uliveGPSTime,gRouteID)
                if status == "NextStop" and str(stopID)+ '-'+ str(RouteID) +"Arrived" in stop_status and str(gStopID)+ '-'+ str(RouteID) + "Arrived" in stop_status:
                    dataForSpeech = "this is the case when arrived occured of next stop before departure of previous stop"
                    VLUStopDetectedNotProcessed_Logs.info("VLUStopDetectedNotProcessed: %s",[stop_name, gStopID, sequenceList[0],float(round(sequenceList[1],2)), system_current_time_for_gps,detected__time_str, gArrivalTime,uSpeed, uliveLatLong, status, dataForSpeech,distFromStop])
        
                else:
                    createMessageFormat(status, gconfirmationNumber, dataForDisplay, dataForSpeech, gStopID, announcementFlag, announceRouteFlag, displayFlag, gdestinationCode, gRouteID,uSpeed,uliveLatLong,stop_name,sequenceList[0],float(round(sequenceList[1],2)),distFromStop,detected__time_str,gArrivalTime,index_for_detected_stop,system_current_time_for_gps,gtripID,nextStopOnApproach, VehNo)
                    
                    if status == "Arrived" or status == "NextStop" :
                        # for key,item in filtered_trips.items():
                        #     pick_confNo = item["confirmationNumber"]
                        #     if  key < gtripID and pick_confNo not in tripIDStatusDriverTabletDict and item["TripStatus"] != "DROPPED": 
                        #         driverTabletTripsUpdater("TripUpdate",str(item["iServiceID"]),item["confirmationNumber"],"PICKEDUP",str(item['iStopID']),item['StopName'],"Skipped PickedUp",key,uliveLatLong, VehNo, RouteID)
                        #         # pickedUpConfirmationDict[pick_confNo] = "PICKEDUP"
                        #         # print("-------------------Sent PICKEDUP to Driver tablet as it missed above pickepkup--------------------")

                        if status == "Arrived":
                            driverTabletTripsUpdater("TripUpdate",str(gServiceID),gconfirmationNumber,"ATLOCATION",str(gStopID),gStopName,"",gtripID,uliveLatLong, VehNo, RouteID)

                        elif status == "NextStop" :

                            if nextStopOnApproach:         
                                btripID = gtripID-1
                                if btripID in filtered_trips:
                                    prevTrip = filtered_trips[btripID]
                                    currentConfirmationNumber = prevTrip['confirmationNumber']
                                    currentServiceID=prevTrip['iServiceID']
                                    # if currentConfirmationNumber not in pickedUpIRTPUConfirmationDict:
                                    driverTabletTripsUpdater("TripUpdate",str(currentServiceID),currentConfirmationNumber,"PICKEDUP",str(prevTrip['iStopID']),prevTrip['StopName'],"",btripID,uliveLatLong, VehNo, RouteID)
                                    # pickedUpConfirmationDict[currentConfirmationNumber] = "PICKEDUP"
                                    driverTabletTripsUpdater("TripUpdate",str(detectedServiceID),detectedConfirmationNum,"IRTPU",str(detectedStopID),detectedStopName,"",detectedTripID,uliveLatLong, VehNo, RouteID)

                            else:
                                driverTabletTripsUpdater("TripUpdate",str(detectedServiceID),detectedConfirmationNum,"PICKEDUP",str(detectedStopID),detectedStopName,"",detectedTripID,uliveLatLong, VehNo, RouteID)
                                driverTabletTripsUpdater("TripUpdate",str(gServiceID),gconfirmationNumber,"IRTPU",str(gStopID),gStopName,"",gtripID,uliveLatLong, VehNo, RouteID)                     
                    else:
                        pass
            else:
                dataForSpeech = f"Bus was late/early :  {maxBusLateAllowedTime}"
                VLUStopDetectedNotProcessed_Logs.info("VLUStopDetectedNotProcessed: %s",[stopID, sequenceList[0],float(round(sequenceList[1],2)), system_current_time_for_gps,uSpeed, uliveLatLong, status, dataForSpeech,distFromStop])
        else:
            print('Error in else block')
            VLUStopDetectedNotProcessed_Logs.info("VLUStopDetectedNotProcessed: %s",[f"StopID {stopID} not found in the tripList. Stop status is {status}"])

    except Exception as e:
        print(e)
        exception = f"[statusFinalProcessing: ] [{str(e)}]".strip()
        AllException_Logs.error(exception)