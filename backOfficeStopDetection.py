import math
import json
import copy
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import csv
import threading
import requests
from typing import List, Dict
from requests.exceptions import Timeout,RequestException
from fastapi.middleware.cors import CORSMiddleware
from collections import defaultdict
import time
import asyncio
import queue 
from logConfiguration import *
from collections import defaultdict, deque, OrderedDict
last_arrived_tripID = defaultdict(lambda: -1) 
stop_management_policy_dict = {}
number_of_sequences = 5
tripList_dict_SD = defaultdict(dict) 
tripID_Dict_SD = defaultdict(dict) 
tripList_dict = defaultdict(dict) 
tripID_Dict = defaultdict(dict)
performed_trips_confirmations = {}
tripIDStatusDriverTabletDict = {}
lastStoredConfirmation = 1
stop_detected_data_queue = queue.Queue()
trip_update_queue = queue.Queue()
rawStopDetectedResultDict = {}
nstopDetectedResultDict = {}
shouldVLUDetectStop = True
TotalVehicleNos = 1
last_two_grouped_sequences = defaultdict(lambda: deque(maxlen=number_of_sequences))       
qualiflyingDepartingTripID = -1
qualiflyingApproachingTripID = -1

latestgTripID = {}
control_vectors = []
announceMessageStatusList = ["stopArrivalMessage","nextStopMessage"]
displayMessageStatusList = ["DisplayArrival","DisplayNextStop"]
previousDetectedStopID = {}
logs_threading_lock = threading.Lock()
vlu_new_message = ""

app = FastAPI()
clients: Dict[str,WebSocket] = {}

retry_delay = 5  
origins = ["*"]
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
raw_stop_status = defaultdict(OrderedDict)
stop_status_time = OrderedDict()


processed_statuses = {}

def findPreviousStopID(detectedStopID,uliveGPSTime, RouteID):
    global tripList_dict_SD,tripID_Dict_SD
    if detectedStopID in tripList_dict_SD[RouteID]:
        min_diff = float('inf')
        gpstime = datetime.strptime(uliveGPSTime, "%Y-%m-%d %H:%M:%S").time()

        scheduled_trip_list = tripList_dict_SD[RouteID][detectedStopID]

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

        if gtripID-1 in tripID_Dict_SD[RouteID]:
            prevTrip = tripID_Dict_SD[RouteID][gtripID-1]
            gStopID = int(prevTrip['iStopID'])

        return gStopID
    
    else:
        return 0

PreProcessingLock = threading.RLock()
def statusPreProcessing(status,bstatus,stopID,sequenceList, uliveGPSTime, uliveLatLong, uSpeed,distFromStop,announceStopMessage,displayStopMessage,busAngle,sequenceAngle,system_time_str, nextStopOnApproach, RouteID, VehNo, gTripID):
    with PreProcessingLock:
        global stop_status, stop_status_time, TotalVehicleNos
 
        stop_id_status = f"{stopID}-{RouteID}{status}"

        if stop_id_status not in stop_status[RouteID] or nextStopOnApproach :
            print('Pre-Processing: ', stopID)
            if len(stop_status[RouteID]) >= 5:
                stop_status[RouteID].popitem(last=False)  # Remove the oldest entry
                stop_status_time[RouteID].popitem(last=False)
            
            
            if bstatus == "Arrived,NextStop":
                statusList = bstatus.split(",")
                print("---------------------------------Dual Status-----------------------------------------")
                for i in range(len(statusList)):
                    status = statusList[i]
                    stop_id_status = f"{stopID}-{RouteID}{status}"
                    if stop_id_status not in stop_status[RouteID]:
                        stop_status[RouteID][stop_id_status] = status
                        stop_status_time[RouteID][stop_id_status] = system_time_str
                        announceStopMessage = announceMessageStatusList[i]
                        displayStopMessage = displayMessageStatusList[i]
                        # if status == "NextStop":
                        statusFinalProcessing(stopID,uliveGPSTime,sequenceList,announceStopMessage,displayStopMessage,status,uSpeed,uliveLatLong,distFromStop,stop_status, nextStopOnApproach, RouteID, VehNo, system_time_str, gTripID)
                        # else:
                        #     print('StatusPreProcessing')
                            #VLUStopDetectedNotProcessed_Logs.info("VLUStopDetectedNotProcessed: %s",[stopID, sequenceList[0],float(round(sequenceList[1],2)), uliveGPSTime,uSpeed, uliveLatLong, status, "Arrived Message was not sent for announcements as we marked Arrived and NextStop at the same time."])
                    if len(stop_status[RouteID]) >=5:
                        stop_status[RouteID].popitem(last=False)
                        stop_status_time[RouteID].popitem(last=False)  

            else:
                if nextStopOnApproach:
                    stop_id_status = f"{stopID}-{RouteID}Approaching"
                    stop_status[RouteID][stop_id_status] = "Approaching"
                    stop_status_time[RouteID][stop_id_status] = system_time_str
                else:
                    stop_id_status = str(stopID) +'-'+  str(RouteID) + status
                    stop_status[RouteID][stop_id_status] = status
                    stop_status_time[RouteID][stop_id_status] = system_time_str

                statusFinalProcessing(stopID,uliveGPSTime,sequenceList,announceStopMessage,displayStopMessage,status,uSpeed,uliveLatLong,distFromStop,stop_status, nextStopOnApproach, RouteID, VehNo, system_time_str, gTripID)

processLock = threading.RLock()

def process_stop_detected_queue():
    global stop_detected_data_queue
    while True:
        with processLock:
        
            if not stop_detected_data_queue.empty():
                try:
                    
                    item = stop_detected_data_queue.get()
                    stopIDgrouped_sequences, liveGPSTime, liveLatLong, dSpeed, system_time_str, RouteID, VehNO= copy.deepcopy(item)
                    stopDetectionAlgo(stopIDgrouped_sequences, liveGPSTime, liveLatLong, dSpeed, system_time_str)
                    stop_detected_data_queue.task_done()
                except Exception as e:
                    print(e)
                    exception = f"[Error in process_stop_detected_queue function: ] [{str(e)}]".strip()

            time.sleep(0.1)  


updateStatusLock = threading.Lock()
def updateStatus(stopID,sequenceList, uliveGPSTime, uliveLatLong, uSpeed,distFromStop,number_of_detected_stops, system_time_str):
    with updateStatusLock:
        global previousDetectedStopID,stop_status, stop_status_time
        bstatus = ''
    
        nextStopOnApproach = False
        seqList = list(sequenceList)
        vectorID, angleD1, stopID, distFromStop, busAngle, sequenceAngle, RouteID, VehNo, gTripID = sequenceList
        seqNum = int(vectorID)

        try:
            previousStopDepartedStopID = findPreviousStopID(stopID, uliveGPSTime, RouteID)
            lastStopID_NextStop = f"{previousStopDepartedStopID}-{RouteID}NextStop"
            currentStopApproching = f"{stopID}-{RouteID}Approaching"
            currentStopArrived = f"{stopID}-{RouteID}Arrived"
            announcementTimeDifference = 0
            if RouteID not in stop_status:
                stop_status[RouteID] = OrderedDict()
                stop_status_time[RouteID] = OrderedDict()
            if seqNum >= StartAppSeq and seqNum <= EndAppSeq:
                status = "Approaching"
                announceStopMessage = "approachingStopMessage"
                displayStopMessage = "DisplayApproaching"
                # print(stopID,status,seqNum,"----------------------------------------------")

                if lastStopID_NextStop in stop_status_time[RouteID]:
                    lastAnnouncedNextStopTime = stop_status_time[RouteID][lastStopID_NextStop]
                    announcementTimeDifference = abs(system_time_str-lastAnnouncedNextStopTime).total_seconds()
                    # print("------------------------------Next Stop Time Difference ---------------------------------",announcementTimeDifference)
                    
                if lastStopID_NextStop not in stop_status[RouteID] or announcementTimeDifference > repeatNexStopOnApproachingTime:
                    status = "NextStop"
                    announceStopMessage = "nextStopMessage"
                    displayStopMessage = "DisplayNextStop"
                    nextStopOnApproach = True
                    stop_status[RouteID][lastStopID_NextStop] = status
                    stop_status_time[RouteID][lastStopID_NextStop] = system_time_str

            elif seqNum >= StartArrSeq and seqNum <= EndArrSeq:
                busSpeed = float(uSpeed)
                if busSpeed >= busSpeedLimitForStatus:
                    if seqNum >= StartArrSeq and seqNum <= MidArrSeq:
                        status = "Arrived"
                        announceStopMessage = "stopArrivalMessage"
                        displayStopMessage = "DisplayArrival"

                    else:
                        bstatus = "Arrived,NextStop"
                        status = "NextStop"
                        announceStopMessage = "nextStopMessage"
                        displayStopMessage = "DisplayNextStop"
                else:

                    if seqNum >= StartArrSeq and seqNum <= MidArrSeq:
                        status = "Approaching"
                        announceStopMessage = "approachingStopMessage"
                        displayStopMessage = "DisplayApproaching"
                    else:
                        status = "Arrived"
                        announceStopMessage = "stopArrivalMessage"
                        displayStopMessage = "DisplayArrival"

            ## if current stop has approaching or arrived status detected then only it goes for departing
            elif seqNum >= StartNextSeq and (currentStopArrived in stop_status[RouteID] or currentStopApproching in stop_status[RouteID]):
                status = "NextStop"
                announceStopMessage = "nextStopMessage"
                displayStopMessage = "DisplayNextStop"
            else:
                status = ""

            if status != "" :
                if status == "Approaching" or status == "Arrived":
                    if lastStopID_NextStop in stop_status[RouteID]:
                        statusPreProcessing(status,bstatus,stopID,seqList, uliveGPSTime, uliveLatLong, uSpeed,distFromStop,announceStopMessage,displayStopMessage,busAngle,sequenceAngle,system_time_str,nextStopOnApproach, RouteID, VehNo, gTripID)
                    else:
                        if previousDetectedStopID[VehNo] == stopID:
                            statusPreProcessing(status,bstatus,stopID,seqList, uliveGPSTime, uliveLatLong, uSpeed,distFromStop,announceStopMessage,displayStopMessage,busAngle,sequenceAngle,system_time_str, nextStopOnApproach, RouteID, VehNo, gTripID)

                    #if VehNo not in previousDetectedStopID:
                    previousDetectedStopID[VehNo] = stopID
                
                else:
                    if status == "NextStop"  or nextStopOnApproach:
                        statusPreProcessing(status,bstatus,stopID,seqList, uliveGPSTime, uliveLatLong, uSpeed,distFromStop,announceStopMessage,displayStopMessage,busAngle,sequenceAngle,system_time_str, nextStopOnApproach, RouteID, VehNo, gTripID)
                        if nextStopOnApproach:
                            previousDetectedStopID[VehNo] = stopID 

        except Exception as e:
            print(e)
            exception = f"[Error in updateStatus function: ] [{str(e)}]".strip()
            AllException_Logs.error(exception)
        
def save_results_to_txt():
    while True:
        try:
            for route_id, records in rawStopDetectedResultDict.items():
                if records:
                    records_by_vehno = {}
                    for row in records:
                        veh_no = row[-1] 
                        if veh_no not in records_by_vehno:
                            records_by_vehno[veh_no] = []
                        records_by_vehno[veh_no].append(row)

                    for veh_no, veh_records in records_by_vehno.items():
                        output_file = f"{vluStopDetectionResult_file_path}/stopDetectionAllResults-{veh_no}.txt"
                        
                        rows_to_write = '\n'.join(
                            ','.join(map(str, record)) for record in veh_records
                        ) + '\n'
                        
                        os.makedirs(vluStopDetectionResult_file_path, exist_ok=True)
                        with open(output_file, 'a') as log_file:
                            log_file.write(rows_to_write)

                    rawStopDetectedResultDict[route_id].clear()

            time.sleep(60)
        except Exception as e:
            logging.error(f"Error saving results to text file: {e}")

def save_results():
    while True:
        try:
            for route_id, records in nstopDetectedResultDict.items():
                if records:
                    records_by_vehno = {}

                    for row in records:
                        veh_no = row[-2]  
                        if veh_no not in records_by_vehno:
                            records_by_vehno[veh_no] = []
                        records_by_vehno[veh_no].append(row)

                    for veh_no, veh_records in records_by_vehno.items():
                        output_file = f"{vluStopDetectionResult_file_path}/processedStopDetectionResult-{veh_no}.txt"
                        
                        rows_to_write = '\n'.join(
                            ','.join(map(str, record)) for record in veh_records
                        ) + '\n'
                        
                        os.makedirs(vluStopDetectionResult_file_path, exist_ok=True)
                        with open(output_file, 'a') as log_file:
                            log_file.write(rows_to_write)

                    # Clear records for the route after writing
                    nstopDetectedResultDict[route_id].clear()

            time.sleep(60)
        except Exception as e:
            logging.error(f"Error in logging: {e}")
            
smartSearchLock = threading.RLock()
def smartSearchAlgo(liveLatLong, radius, busAngle, system_time_str, rSpeed, rliveGPSTime, control_vectors, RouteID, VehNo): 
    with smartSearchLock:
        try:
            global TotalVehicleNos, tripID_Dict, stop_detected_data_queue, last_arrived_tripID
            filtered_stopIDs = {}  # Dictionary to store StopID as key and globalTripID as value
            current_time = system_time_str.time()
            time_window = timedelta(minutes=10)

            grouped_sequences = defaultdict(list)  # To store the grouped sequences
            last_two_grouped_sequences.clear()
            last_arrived_id = last_arrived_tripID.get(RouteID, None)

            if RouteID in tripID_Dict:
                # Loop through the trips for the specified route
                for globalTripID, trip_info in tripID_Dict[RouteID].items():
                    if last_arrived_id:
                        if globalTripID <= last_arrived_id:
                            continue
                    if trip_info.get('TripStatus', '').lower() == 'dropped':
                        continue
                    trip_arrival_time = datetime.strptime(trip_info['ArrivalTime'], "%H:%M:%S").time()
                    time_diff = datetime.combine(datetime.min, trip_arrival_time) - datetime.combine(datetime.min, current_time)

                    if time_diff.total_seconds() > time_window.total_seconds():
                        break
                    if abs(time_diff.total_seconds()) <= time_window.total_seconds():
                        filtered_stopIDs[trip_info['iStopID']] = globalTripID  # Use StopID as key and globalTripID as value  # Use StopID as key and globalTripID as value
        except Exception as e:
            print(e)
            exception = f"[Error in smartSearchAlgo function: ] [{str(e)}]".strip()
            AllException_Logs.error(exception)
        if filtered_stopIDs:
            print(filtered_stopIDs)
            for stop in control_vectors:
                try:
                    istopID = stop['StopID']
                    sequenceAngle = stop['DirectionDegrees']

                    if istopID not in filtered_stopIDs:
                        continue
                    
                    globalTripID = filtered_stopIDs[istopID]  # Retrieve the globalTripID from the dictionary

                    if sequenceAngle == "NA":
                        continue

                    # Calculate distance and other logic
                    distanceFromStop = stop['MapDistanceFromStop']
                    seqNum = stop['SeqNo']
                    lat = stop['Lat']
                    long = stop['Lng']

                    coord2 = (lat, long)
                    currDistFromStop = calculateHaverSineDistance(liveLatLong, coord2)

                    if currDistFromStop <= radius:
                        deltaAngle = abs(float(busAngle) - float(sequenceAngle))
                        if deltaAngle > 180:
                            deltaAngle = 360 - deltaAngle
                        deltaAngle = round(deltaAngle, 2)
                        busAngle = round(busAngle, 2)
                        seqNum = int(seqNum)

                        if seqNum >= StartAppSeq and seqNum <= EndAppSeq:
                            status = "Approaching"
                        elif seqNum >= StartArrSeq and seqNum <= EndArrSeq:
                            busSpeed = float(rSpeed)
                            if busSpeed >= busSpeedLimitForStatus:
                                status = "Arrived" if seqNum >= StartArrSeq and seqNum <= MidArrSeq else "Arrived,NextStop"
                            else:
                                status = "Approaching" if seqNum >= StartArrSeq and seqNum <= MidArrSeq else "Arrived"
                        elif seqNum >= StartNextSeq:
                            status = "NextStop"
                        else:
                            status = ""

                        ulat,ulong = liveLatLong
                        if RouteID not in rawStopDetectedResultDict:
                            rawStopDetectedResultDict[RouteID] = []
                        if deltaAngle > busAngleThreshold:
                            # VLUAllStopDetected_Logs.info("VLUAllStopDetected: %s", [istopID,str(seqNum),str(deltaAngle),busAngle,sequenceAngle, "Invalid",system_time_str,rliveGPSTime,rSpeed,str(liveLatLong),"",""])
                            rawStopDetectedResultDict[RouteID].append([istopID,seqNum,deltaAngle,busAngle,sequenceAngle, "Invalid",system_time_str,rliveGPSTime,rSpeed,ulat,ulong,"", VehNo])
                        else:  
                            key = f"{istopID}-{RouteID}"
                            grouped_sequences[key].append([seqNum, deltaAngle, istopID, distanceFromStop, busAngle, sequenceAngle, RouteID, VehNo, globalTripID])
                            
                            # Status processing and rawStopDetectedResultList
                            statusList = status.split(",")
                            for stat in statusList:
                                stop_id_status = f"{istopID}-{RouteID}{stat}"
                                if stop_id_status not in raw_stop_status[RouteID]:
                                    raw_stop_status[RouteID][stop_id_status] = stat
                                    
                                    if len(raw_stop_status[RouteID]) >= 5:
                                        raw_stop_status[RouteID].popitem(last=False) 
                                    # Append to the list for this RouteID
                                    rawStopDetectedResultDict[RouteID].append([istopID, seqNum, deltaAngle, busAngle, sequenceAngle, stat, system_time_str, rliveGPSTime, rSpeed, *liveLatLong, stat, VehNo])
                                else:
                                    rawStopDetectedResultDict[RouteID].append([istopID, seqNum, deltaAngle, busAngle, sequenceAngle, stat, system_time_str, rliveGPSTime, rSpeed, lat, long, "", VehNo])

                except Exception as e:
                    print(e)
                    exception = f"[Error in smartSearchAlgo function: ] [{str(e)}]".strip()
                    AllException_Logs.error(exception)
        if grouped_sequences:
            for stopID, seq_list in grouped_sequences.items():
                last_two_grouped_sequences[stopID].extend(seq_list)

            if last_two_grouped_sequences:
                data_to_queue = copy.deepcopy(last_two_grouped_sequences)
                stop_detected_data_queue.put((data_to_queue, rliveGPSTime, liveLatLong, rSpeed, system_time_str, RouteID, VehNo))
stopDetectionLock = threading.Lock()
def stopDetectionAlgo(groupedSeq, rliveGPSTime, rliveLatLong, rSpeed, system_time_str):
    with stopDetectionLock: 
        local_groupedSeq = copy.deepcopy(groupedSeq)
        global stop_status, tripList_dict ,qualiflyingDepartingTripID, qualiflyingApproachingTripID

        sys_time = system_time_str
    
        #sys_time = datetime.strptime(rliveGPSTime, '%Y-%m-%d %H:%M:%S') if rliveGPSTime else None
        current_SystemTime = sys_time.time() 
        number_of_detected_stops = len(local_groupedSeq)
        max_seq1_for_stopID = defaultdict(lambda: (float(-999), None))
        
        for stopID2, sequences in local_groupedSeq.items():
            vector_data = list(sequences)
            for seq in vector_data:
                vectorID, angleD1, stopID, distFromStop, busAngle, sequenceAngle, RouteID, VehNO, gTripID = seq
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
                    vectorID, angleD1,stopID3,distFromStop,busAngle,sequenceAngle, RouteID, VehNO, gTripID = vector_data
                    if stopID3 in tripList_dict[RouteID]:
                        # trips_for_current_stopID = [trip for trip in tripList_dict[RouteID][stopID3] if trip.get('TripStatus', '').lower() != 'dropped']

                        # for trip in trips_for_current_stopID:
                        #     arrival_time_str = trip['ArrivalTime']
                        #     arrival_time = datetime.strptime(arrival_time_str, "%H:%M:%S").time()
                        #     time_diff = abs((datetime.combine(datetime.min, current_SystemTime) - datetime.combine(datetime.min, arrival_time)).total_seconds())
                        #     tripID_pickedup = str(trip['confirmationNumber']) + "PICKEDUP"
                        #     if time_diff <= min_diff and tripID_pickedup not in tripIDStatusDriverTabletDict:
                        #         min_diff = time_diff
                        smallestTripID  = gTripID

                        
                        if vectorID > 0 :
                            if vectorID < qualifyingDepartVectorMinValue :
                                qualifyingDepartVectorMinValue = vectorID
                                qualifyingDepartVectorMinSeq = vector_data
                                stopID_depart_status = str(stopID3) + '-'+ str(RouteID) + "NextStop"
                                qualiflyingDepartingTripID = smallestTripID

                            elif vectorID == qualifyingDepartVectorMinValue and smallestTripID < qualiflyingDepartingTripID:
                                qualifyingDepartVectorMinValue = vectorID
                                qualifyingDepartVectorMinSeq = vector_data
                                stopID_depart_status = str(stopID3) + '-'+ str(RouteID) + "NextStop"
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

                if abs(qualifyingDepartVectorMinValue) < abs(qualifyingApproachVectorMaxValue) and  stopID_depart_status not in stop_status[RouteID]:
                    new_vector_data = qualifyingDepartVectorMinSeq
                    if new_vector_data != "":
                        vectorID, angleD1, stopID, distFromStop, busAngle, sequenceAngle, RouteID, VehNO, gTripID= new_vector_data
                        # print("Departing: ",stopID)
                else:
                    new_vector_data = qualifyingApproachVectorMaxSeq
                    if new_vector_data != "":
                        vectorID, angleD1, stopID, distFromStop, busAngle, sequenceAngle, RouteID, VehNO, gTripID = new_vector_data
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
                    vectorID, angleD1, stopID, distFromStop, busAngle,sequenceAngle, RouteID, VehNo, gTripID= vector_data
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

        
def check_time_difference(arrival_time_str, system_time_str):
    arrival_time = datetime.strptime(arrival_time_str, '%H:%M:%S').time()
    current_time = (system_time_str).time() 
    #current_time = arrival_time 
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
                pass
                #print(final_message)
            flat,flong = uliveLatLong
            if routeID not in nstopDetectedResultDict:
                nstopDetectedResultDict[routeID] = []

            nstopDetectedResultDict[routeID].append([stop_name, stationID, sequence, delta, gpstime, statusDetectedTime, scheduledTime, uSpeed, flat, flong, statusType, dataForSpeech, displayFlag, announcementFlag, VehNo, confirmationNumber])

    except Exception as e:
        print(e)
        exception = f"[createMessageFormat: ] [{str(e)}]".strip()
        AllException_Logs.error(exception)


def process_diverTablet_trips_queue():
    while True:
        if not trip_update_queue.empty():
            message = trip_update_queue.get()
            trip_payload = json.dumps(message)
            if clients:
                sendMessageToDriverTablet(message)
                try:
                    #tripStatusUpdatingRes = requests.post(f"{api_base_url}{'VLUTripStatusUpdate'}",headers=headers, data=trip_payload)

                    #if tripStatusUpdatingRes:
                    print("BackOffice API %s", message)
                            
                    break 
                except (Timeout, RequestException) as e:
                    print(f"Request failed with error: {e}") 
                except Exception as e:
                    if clients:
                        #sendMessageToDriverTablet(message)
                        break
            else:
                while True:
                    try:
                        #tripStatusUpdatingRes = requests.post(f"{api_base_url}{'VLUTripStatusUpdate'}",headers=headers, data=trip_payload)

                        #if tripStatusUpdatingRes:
                        print("BackOffice API %s", message)
                        break  
                    except (Timeout, RequestException) as e:
                        if clients:
                            #sendMessageToDriverTablet(message)
                            break
                        time.sleep(retry_delay)  
                    except Exception as e:
                        if clients:
                            #sendMessageToDriverTablet(message)
                            break
                        time.sleep(retry_delay)  
                    
                    

            trip_update_queue.task_done()

        time.sleep(0.2)

threading.Thread(target=save_results_to_txt).start()
threading.Thread(target=process_stop_detected_queue).start()
threading.Thread(target=save_results).start()
threading.Thread(target=process_diverTablet_trips_queue).start()

driverTabletLock = threading.RLock()
def driverTabletTripsUpdater(msgType,iServiceID,confirmationNum,tripStatus,stopID,stopName,skippedPickedUp,tripID,uliveLatLong, VehNo, RouteID, system_time_str):
    with driverTabletLock:
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
                                "DeviceTime" : str(system_time_str)
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

def cleanup_old_records():
    global tripList_dict, tripID_Dict, tripList_dict_SD, tripID_Dict_SD, latestgTripID
    while True:
        # Delay of 2 minutes (120 seconds)
        time.sleep(120)

        # Assuming `latestgTripID` is a global dictionary with RouteID as keys
        for route_id, current_gtripID in latestgTripID.items():
            threshold_gtripID = current_gtripID - 7

            # Efficient deletion in tripID_Dict
            if route_id in tripID_Dict:
                for trip_id in list(tripID_Dict[route_id].keys()):
                    if trip_id >= threshold_gtripID:
                        break  # Stop as soon as we reach a trip_id >= threshold
                    del tripID_Dict[route_id][trip_id]

            # Efficient deletion in tripID_Dict_SD
            if route_id in tripID_Dict_SD:
                for trip_id in list(tripID_Dict_SD[route_id].keys()):
                    if trip_id >= threshold_gtripID:
                        break
                    del tripID_Dict_SD[route_id][trip_id]

            # Remove trips with tripID < threshold_gtripID in tripList_dict
            if route_id in tripList_dict:
                for stop_id, trips in tripList_dict[route_id].items():
                    tripList_dict[route_id][stop_id] = [
                        trip for trip in trips if trip['tripID'] >= threshold_gtripID
                    ]

            if route_id in tripList_dict_SD:
                for stop_id, trips in tripList_dict_SD[route_id].items():
                    tripList_dict_SD[route_id][stop_id] = [
                        trip for trip in trips if trip['tripID'] >= threshold_gtripID
                    ]
                    
finalProcessingLock = threading.Lock()
def statusFinalProcessing(stopID,uliveGPSTime,sequenceList,announceStopMessage,displayStopMessage,status,uSpeed,uliveLatLong,distFromStop,stop_status, nextStopOnApproach, RouteID, VehNo, system_time_str, gTripID2):
    with finalProcessingLock:
        global gArrivalTime,gStopID, gRouteID, gStopName,gdestinationCode, gconfirmationNumber, last_arrived_tripID, gtripID,tripList_dict,tripID_Dict,gServiceID,performed_trips_confirmations, total_stops, tripIDStatusDriverTabletDict
        print('Final-: ', stopID)
        try:
            index_for_detected_stop = -1
            if stopID in tripList_dict[RouteID]:
                min_diff = float('inf')
                try:
                    system_current_time_for_gps = (system_time_str).time()
                    #system_current_time_for_gps = datetime.strptime(uliveGPSTime, "%Y-%m-%d %H:%M:%S").time()
                    
                except Exception as e:
                    system_current_time_for_gps = datetime.strptime(uliveGPSTime, "%Y-%m-%d %H:%M:%S").time()
                    
                    print(e)
                scheduled_trip_list = [trip for trip in tripList_dict[RouteID][stopID] if trip.get('TripStatus', '').lower() != 'dropped']

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
                isBusOnTime = check_time_difference(gArrivalTime, system_time_str)

                if isBusOnTime:
                    if status == "NextStop" and not nextStopOnApproach:
                        gtripID = gtripID+1
                        if gtripID in tripID_Dict[RouteID]:
                            nextTrip = tripID_Dict[RouteID][gtripID]
                            gStopID = int(nextTrip['iStopID'])
                            gRouteID=int(nextTrip['iRouteID'])
                            gconfirmationNumber = nextTrip['confirmationNumber']
                            gArrivalTime  = nextTrip['ArrivalTime']
                            gServiceID=nextTrip['iServiceID']
                            gStopName = nextTrip['StopName']
                    try:
                        stopListDD = []
                        prev_iRSTID = tripID_Dict[RouteID][gtripID]['iRSTId']  # Assuming the first stop as starting point
                        last_valid_stop = None  # To store the last stop with the same iRSTID

                        for offset in range(0, len(tripID_Dict[RouteID])):  # For the first 7 stops (0 to 6)
                            stopID = (gtripID + offset - 1) % len(tripID_Dict[RouteID]) + 1
                            current_iRSTID = tripID_Dict[RouteID][stopID]['iRSTId']

                            if current_iRSTID != prev_iRSTID:
                                # Append the last valid stop with the same iRSTID before breaking
                                if last_valid_stop and last_valid_stop not in stopListDD:
                                    stopListDD.append(last_valid_stop)
                                break
                            else:
                                # Keep track of the last valid stop with the same iRSTID
                                last_valid_stop = {
                                    "id": int(tripID_Dict[RouteID][stopID]['iStopID']),
                                    "name": tripID_Dict[RouteID][stopID]['StopName'],
                                    "remainingStops": offset,
                                    "arrivalTime" : tripID_Dict[RouteID][stopID]['ArrivalTime']
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
                        detected__time = (UseTime).time()
                        detected__time_str = detected__time.strftime("%H:%M:%S")
                        stop_name = filtered_row['stationName']
                        waitTimeAnnounce = filtered_row["waitTimeAnnounce"+status]
                        repeatDisplay = filtered_row["repeatDisplay"+status]
                        repeatAnnounce = filtered_row["repeatAnnounce"+status]

                        # createMessageFormat(gconfirmationNumber,status,dataForDisplay,dataForSpeech,gStopID,flagDict[str(announcementFlag)],flagDict[str(announceRouteFlag)],flagDict[str(displayFlag)],gdestinationCode,uliveGPSTime,gRouteID)
                    if status == "NextStop" and str(stopID)+ '-'+ str(RouteID) +"Arrived" in stop_status[RouteID] and str(gStopID)+ '-'+ str(RouteID) + "Arrived" in stop_status[RouteID]:
                        dataForSpeech = "this is the case when arrived occured of next stop before departure of previous stop"
                        VLUStopDetectedNotProcessed_Logs.info("VLUStopDetectedNotProcessed: %s",[stop_name, gStopID, sequenceList[0],float(round(sequenceList[1],2)), system_current_time_for_gps,detected__time_str, gArrivalTime,uSpeed, uliveLatLong, status, dataForSpeech,distFromStop])
            
                    else:
                        createMessageFormat(status, gconfirmationNumber, dataForDisplay, dataForSpeech, gStopID, announcementFlag, announceRouteFlag, displayFlag, gdestinationCode, gRouteID,uSpeed,uliveLatLong,stop_name,sequenceList[0],float(round(sequenceList[1],2)),distFromStop,detected__time_str,gArrivalTime,index_for_detected_stop,system_current_time_for_gps,gtripID,nextStopOnApproach, VehNo)
                        
                        if status == "Arrived" or status == "NextStop" :
                            # for key,item in tripID_Dict[RouteID].items():
                            #     pick_confNo = item["confirmationNumber"]
                            #     if  key < gtripID and pick_confNo not in tripIDStatusDriverTabletDict and item["TripStatus"] != "DROPPED": 
                            #         driverTabletTripsUpdater("TripUpdate",str(item["iServiceID"]),item["confirmationNumber"],"PICKEDUP",str(item['iStopID']),item['StopName'],"Skipped PickedUp",key,uliveLatLong, VehNo, RouteID)
                            #         # pickedUpConfirmationDict[pick_confNo] = "PICKEDUP"
                            #         # print("-------------------Sent PICKEDUP to Driver tablet as it missed above pickepkup--------------------")

                            if status == "Arrived":
                                # tripID_Dict[RouteID][gtripID]["TripStatus"] = "DROPPED"
                                # tripID_Dict_SD[RouteID][gtripID]["TripStatus"] = "DROPPED"
                                # trip = next((trip for trip in tripList_dict[RouteID][gStopID] if trip['tripID'] == gtripID), None)
                                # #trip2 = next((trip for trip in tripList_dict_SD[RouteID][gStopID] if trip['tripID'] == gtripID), None)
                                # # If the trip exists, update the TripStatus 
                                # if trip:
                                #     trip['TripStatus'] = 'Dropped'
                                # # if trip2:
                                # #     trip['TripStatus'] = 'Dropped'
                                
                                driverTabletTripsUpdater("TripUpdate",str(gServiceID),gconfirmationNumber,"ATLOCATION",str(gStopID),gStopName,"",gtripID,uliveLatLong, VehNo, RouteID, system_time_str)
                                

                            elif status == "NextStop" :

                                if nextStopOnApproach:         
                                    btripID = gtripID-1
                                    if btripID in tripID_Dict[RouteID]:
                                        prevTrip = tripID_Dict[RouteID][btripID]
                                        currentConfirmationNumber = prevTrip['confirmationNumber']
                                        currentServiceID=prevTrip['iServiceID']
                                        # if currentConfirmationNumber not in pickedUpIRTPUConfirmationDict:
                                        driverTabletTripsUpdater("TripUpdate",str(currentServiceID),currentConfirmationNumber,"PICKEDUP",str(prevTrip['iStopID']),prevTrip['StopName'],"",btripID,uliveLatLong, VehNo, RouteID, system_time_str)
                                        # pickedUpConfirmationDict[currentConfirmationNumber] = "PICKEDUP"
                                        driverTabletTripsUpdater("TripUpdate",str(detectedServiceID),detectedConfirmationNum,"IRTPU",str(detectedStopID),detectedStopName,"",detectedTripID,uliveLatLong, VehNo, RouteID, system_time_str)

                                else:
                                    driverTabletTripsUpdater("TripUpdate",str(detectedServiceID),detectedConfirmationNum,"PICKEDUP",str(detectedStopID),detectedStopName,"",detectedTripID,uliveLatLong, VehNo, RouteID, system_time_str)
                                    driverTabletTripsUpdater("TripUpdate",str(gServiceID),gconfirmationNumber,"IRTPU",str(gStopID),gStopName,"",gtripID,uliveLatLong, VehNo, RouteID, system_time_str)                     
                                latestgTripID[RouteID] = gtripID
                                last_arrived_tripID[RouteID] = gTripID2-1
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
            
            
cleanup_thread = threading.Thread(target=cleanup_old_records)
cleanup_thread.daemon = True 
cleanup_thread.start()