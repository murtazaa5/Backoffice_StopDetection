
import random
import time
import asyncio
from paho.mqtt import client as mqtt_client

from backOfficeStopDetection import *
gps_message_queue = queue.Queue()

file = open('config_env.json')
loadedJsonFile = json.load(file)

broker = loadedJsonFile['broker']
port = loadedJsonFile['port']
username = loadedJsonFile['username']
password = loadedJsonFile['password']
client_id = f'python-mqtt-{random.randint(0,1000)}'

arrival_time_check_interval = 20
lastStoredGPSTime = 111111
tripListUpdateDict = {"TripListUpdateTime":datetime.now()- timedelta(seconds=20) - LocaltimeDelta}
topicgnss= '/jefftran/+/+/teltonika/gnss'
stop_management_policy_dict = {}
locationDict = {}
route_trips = {}
is_connected = False

last_live_lat_long = (0.0, 0.0)
stop_detected_data_queue = queue.Queue()
def calculate_initial_compass_bearing(lastLatLong, currentLatLong):
    lat1, lon1 = lastLatLong
    lat2, lon2 = currentLatLong

    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    delta_lon = lon2 - lon1

    x = math.cos(lat2) * math.sin(delta_lon)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(delta_lon)

    initial_bearing = math.atan2(x, y)
    initial_bearing = math.degrees(initial_bearing)
    compass_bearing = (initial_bearing + 360) % 360

    return compass_bearing


def on_connect(client, userdata, flags, rc):
    global is_connected
    if rc == 0:
        print("Connected to MQTT Broker")
        is_connected = True
        client.subscribe(topicgnss, 0)  # Subscribe upon successful connection
    else:
        print(f"Failed to connect, return code {rc}\n")

def on_disconnect(client, userdata, rc):
    global is_connected
    if rc != 0:
        is_connected = False
        print("Unexpected disconnection.")
        TeltonikaGPS_Logs.info("Unexpected disconnection from MQTT Broker")

lastStoredGPSTime = {}
last_live_lat_long = {}
vehicle_data = {}

def on_message(client, userdata, msg):
    try:
        AffiliateID = msg.topic.split("/")[2]
        vehicle_num = msg.topic.split("/")[3]
        
        # Decode the message payload
        payloadClean = msg.payload.decode('utf-8', errors='ignore').replace('""', '"')
        payloadJson = json.loads(payloadClean)
        try:
            if len(payloadJson) > 3:
                newPayLoad = {
                    "GpsLat": payloadJson["GPS_Lat"],
                    "GpsLong": payloadJson["GPS_Long"],
                    "Time": payloadJson["GPS_TS"].replace('\\u0000', "").replace('\x00', ''),
                    "Speed": payloadJson["Speed"]
                }

                gpsTime = int(newPayLoad['Time'])

                # Initialize vehicle data if not already present
                if vehicle_num not in lastStoredGPSTime:
                    lastStoredGPSTime[vehicle_num] = 0
                    last_live_lat_long[vehicle_num] = (0.0, 0.0)

                # Check if the new GPS time is greater than the last stored time
                if lastStoredGPSTime[vehicle_num] < gpsTime:
                    liveLatLong = (float(newPayLoad['GpsLat']), float(newPayLoad['GpsLong']))
                    
                    if last_live_lat_long[vehicle_num] != (0.0, 0.0) and liveLatLong != (0.0, 0.0): #and vehicle_num in ('651','650','630'):
                        currDistTravelled = calculateHaverSineDistance(last_live_lat_long[vehicle_num], liveLatLong)
                        systemTime = int(time.time())
                        gpsTimeDifference = gpsTime - lastStoredGPSTime[vehicle_num]
                        gps_system_timeDifference = abs(systemTime - gpsTime)

                        if currDistTravelled > 3 and gps_system_timeDifference < 45 and vehicle_num in vehRouteList:
                            busDirectionAngle = calculate_initial_compass_bearing(last_live_lat_long[vehicle_num], liveLatLong)
                            last_live_lat_long[vehicle_num] = liveLatLong
                            locationDict["Latitude"] = newPayLoad['GpsLat']
                            locationDict["Longitude"] = newPayLoad['GpsLong']
                            locationDict["Time"] = newPayLoad['Time']
                            locationDict["Speed"] = newPayLoad['Speed']
                            locationDict["VehicleNum"] = vehicle_num
                            locationDict["BusDirectionAngle"] = busDirectionAngle
                            locationDict["RouteID"] = vehRouteList[vehicle_num]
                            gps_message_queue.put(json.dumps(locationDict))
                    else:
                        last_live_lat_long[vehicle_num] = liveLatLong

                    lastStoredGPSTime[vehicle_num] = gpsTime
        except KeyError as e:
            print(f"Missing key in payload: {e}")

    except Exception as e:
        print(f"Error while processing on_message: {e}")

# Function to create and return an MQTT client
def connect_mqtt():
    client = mqtt_client.Client(client_id, clean_session=False)

    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    while True:
        try:
            client.connect(broker, port, keepalive=30)
            break  # Successful connection, break out of the loop
        except Exception as e:
            print(f"Teltonika Connection failed: {e}, retrying in 10 seconds...")
            
            time.sleep(10)  # Wait for 10 seconds before retrying

    return client

def testingSSA(radius):
    global busDirectionAngle, stop_detected_data_queue, vehRouteList
    last_sent_time = 0.0000
    while True:
        try:
            if not gps_message_queue.empty():
                location_json = gps_message_queue.get() 
                locationDict = json.loads(location_json) 

                if locationDict and locationDict["Time"] != last_sent_time:
                    liveLatLong = (float(locationDict["Latitude"]), float(locationDict["Longitude"]))
                    dSpeed = locationDict["Speed"]
                    RouteID = locationDict["RouteID"]
                    VehNo = locationDict["VehicleNum"]
                    system_time = datetime.now() - LocaltimeDelta

                    liveGPSTime = datetime.fromtimestamp(int(locationDict["Time"]), tz=system_timezone).strftime('%Y-%m-%d %H:%M:%S')
                    busDirectionAngle = float(locationDict["BusDirectionAngle"])
                    results = smartSearchAlgo(liveLatLong, radius, busDirectionAngle, system_time, dSpeed, liveGPSTime, route_trips[RouteID], RouteID, VehNo)
                    grouped_sequences = defaultdict(list)
                    last_two_grouped_sequences.clear()
                    
                    for result in results:
                        stopID, seq_no, distFromStop, latitude, longitude, distance, delta, busAngle, sequenceAngle = result

                        key = f"{stopID}-{RouteID}"
                        grouped_sequences[key].append([seq_no, delta, stopID, distFromStop, busAngle, sequenceAngle, RouteID, VehNo])

                    last_sent_time = locationDict["Time"]

                    for stopID, seq_list in grouped_sequences.items():
                        last_two_grouped_sequences[stopID].extend(seq_list)

                    if len(last_two_grouped_sequences) > 0:
                        stop_detected_data_queue.put((last_two_grouped_sequences, liveGPSTime, liveLatLong, dSpeed, system_time, RouteID, VehNo))

        except Exception as e:
            logging.error(f"Error in testingSSA function: {e}")
            print(e)
            exception = f"[Error in testingSSA function: ] [{str(e)}]".strip()
            AllException_Logs.error(exception)

# testingSSA(stop_radius)
def process_stop_detected_queue():
    global stop_detected_data_queue
    while True:
        if not stop_detected_data_queue.empty():
            try:

                item = stop_detected_data_queue.get()
                stopIDgrouped_sequences, liveGPSTime, liveLatLong, dSpeed, system_time_str, RouteID, VehNO = item
                stopDetectionAlgo(stopIDgrouped_sequences, liveGPSTime, liveLatLong, dSpeed, system_time_str)

            except Exception as e:
                print(e)
                exception = f"[Error in process_stop_detected_queue function: ] [{str(e)}]".strip()

        time.sleep(0.1)  

controlledVector_data_lock = threading.Lock()

async def  fetchControlledPoints():
    with controlledVector_data_lock:
        print("------------------------- Updating Controlled Vector ------------------------------------------")
        #allcontrolData = await GetVLU_ControlVectors(AffiliateID, -1)
        allControlparams = {"CompanyID": 98, "routeID": -1}

        response = requests.get(f"{ControlledPointsAPI}",headers=headers, params=allControlparams)
        allcontrolData = response.json()
        if allcontrolData and allcontrolData is not None:
            for controlPoint in allcontrolData:
                try:
                    route_id = controlPoint['RouteID']
                    trip = {
                        'StopID': controlPoint['StopID'],
                        'SeqNo': controlPoint['SeqNo'],
                        'Lat': float(controlPoint['Lat']),
                        'Lng': float(controlPoint['Lng']),
                        'routeID': controlPoint['RouteID'],
                        'DirectionDegrees': controlPoint['DirectionDegrees'],
                        'MapDistanceFromStop': controlPoint['MapDistanceFromStop']
                    }
                    
                    if route_id in route_trips:
                        route_trips[route_id].append(trip)
                    else:
                        route_trips[route_id] = [trip]
                except Exception as e:
                    print(e)
                    exception = f"[fetchControlledPoints: ] [{str(e)}]".strip()

            print("---------------------------Updated Controlled Vector ---------------------------------")
tripList_data_lock = threading.Lock()
async def fetchAllRoutes(allrouteparams):
    global tripList_dict,tripListUpdateDict,trip_list_for_DB, vehRouteList
    with tripList_data_lock:
        response = requests.post(f"{api_base_url}{'FetchRoutePlan'}",headers=headers, data=allrouteparams)
        if response and response != None:
            routes = response.json()

            if routes["triplist"] != "":

                trip_list = json.loads(routes["triplist"])
                trip_list_for_DB = []
                max_arrival_times = {}
                vehRouteList = {}
                current_date = datetime.now().date()
                for tripInfo in trip_list:
                    try:
                        vVehicleNo = tripInfo['vVehicleNo']
                        ArrivalTime = tripInfo['ArrivalTime']
                        ArrivalDateTime = datetime.combine(current_date, datetime.strptime(ArrivalTime, "%H:%M:%S").time())
                        
                        if vVehicleNo not in max_arrival_times:
                            max_arrival_times[vVehicleNo] = ArrivalDateTime
                        else:
                            max_arrival_times[vVehicleNo] = max(max_arrival_times[vVehicleNo], ArrivalDateTime)

                        iServiceID = tripInfo['iServiceID']
                        confirmationNumber = tripInfo['confirmationNumber']
                        PUPerson = tripInfo['PUPerson']
                        vAddress = tripInfo['vAddress']
                        dLatitude = tripInfo['dLatitude']
                        dLongitude = tripInfo['dLongitude']
                        StopNumber = tripInfo['StopNumber']
                        
                        DepartTime = tripInfo['DepartTime']
                        TripStatus = tripInfo['TripStatus']
                        Route = tripInfo['Route']
                        ManifestNumber = tripInfo['ManifestNumber']
                        dtReqIRTPU = tripInfo['dtReqIRTPU']
                        dtDriverLoc = tripInfo['dtDriverLoc']
                        dtActualPickup = tripInfo['dtActualPickup']
                        iRouteID = tripInfo['iRouteID']
                        iStopID = tripInfo['iStopID']
                        vTStopType = tripInfo['vTStopType']
                        tripColor = tripInfo['tripColor']
                        iRSTId = tripInfo['iRSTId']
                        iVehicleID = tripInfo['iVehicleID']
                        #vVehicleNo = vVehicleNo
                        iAffiliateID = tripInfo['iAffiliateID']
                        DestSignageCode = tripInfo['DestSignageCode']

                        if vVehicleNo not in vehRouteList:
                            vehRouteList[vVehicleNo] = iRouteID
                        trip_list_for_DB.append((confirmationNumber,PUPerson,vAddress,dLatitude,dLongitude,StopNumber,ArrivalTime,DepartTime,TripStatus,Route,ManifestNumber,dtReqIRTPU,dtDriverLoc,dtActualPickup,iRouteID,iStopID,vTStopType,tripColor,iRSTId,iVehicleID,vVehicleNo,iAffiliateID,DestSignageCode,iServiceID))

                    except Exception as e:
                        exception = f"[Exception in fetchAllRoutes: ] [{str(e)}]".strip()
                if len(trip_list_for_DB)>0:
                    tripList_dict,tripID_dict, tripIDStopIDDict = load_trip_list(trip_list_for_DB)
                    updateTripListDict(tripList_dict,tripID_dict, tripIDStopIDDict, len(vehRouteList))
                    tripListDictForStopDetection(tripList_dict,tripID_dict)
                    tripListUpdateDict["TripListUpdateTime"] = datetime.now() - LocaltimeDelta
                else:
                    tripListUpdateDict["TripListUpdateTime"] = datetime.now() - LocaltimeDelta

                print("-------------------------Updated tripList--------------------------")
                if max_arrival_times:
                    min_arrival_time = min(max_arrival_times.values())
                    threading.Thread(target=check_arrival_time, args=(min_arrival_time,), daemon=True).start()


def check_arrival_time(min_arrival_time_dt):
    async def check():
        while True:
            system_time = datetime.now() - LocaltimeDelta
            #min_arrival_time_dt = datetime.strptime(min_arrival_time, "%Y-%m-%d %H:%M:%S")

            if system_time >= min_arrival_time_dt:
                print("Recalling API...")
                await fetchAllRoutes(allrouteparams)
                current_size = get_folder_size()
                if current_size > stopDetection_max_size:
                    print(f"Folder size exceeds {stopDetection_max_size} bytes, deleting oldest files...")
                    delete_oldest_files(stopDetection_max_size,current_size)
                break
            else:
                await asyncio.sleep(arrival_time_check_interval)  # Non-blocking sleep

    asyncio.run(check())  
     
def load_trip_list(trips_data):
    trips_dict = defaultdict(list)
    tripID_dict = {}
    tripIDStopIDDict = {}
    globalTripID = 0
    try:

        for row in trips_data:
            # try:
            confirmationNumber,PUPerson,vAddress,dLatitude,dLongitude,StopNumber,ArrivalTime,DepartTime,TripStatus,Route,ManifestNumber,dtReqIRTPU,dtDriverLoc,dtActualPickup,iRouteID,iStopID,vTStopType,tripColor,iRSTId,iVehicleID,vVehicleNo,iAffiliateID,DestSignageCode,iServiceID = row
            # except Exception as e:
            #     iServiceID,confirmationNumber,PUPerson,vAddress,dLatitude,dLongitude,StopNumber,ArrivalTime,DepartTime,TripStatus,Route,ManifestNumber,dtReqIRTPU,dtDriverLoc,dtActualPickup,iRouteID,iStopID,vTStopType,tripColor,iRSTId,iVehicleID,vVehicleNo,iAffiliateID,DestSignageCode = row
            #     tripID += 1
            globalTripID += 1
            trips_dict[int(iStopID)].append({
                'tripID' : globalTripID,
                'confirmationNumber':int(confirmationNumber),
                'PUPerson': PUPerson,
                'vAddress': vAddress,
                'dLatitude': dLatitude,
                'dLongitude': dLongitude,
                'StopNumber': StopNumber,
                'ArrivalTime': ArrivalTime,
                'DepartTime': DepartTime,
                'iRouteID': iRouteID,
                'iStopID': iStopID,
                'iRSTId': iRSTId,
                'iVehicleID': iVehicleID,
                'vVehicleNo': vVehicleNo,
                'iAffiliateID': iAffiliateID,
                'DestSignageCode': DestSignageCode,
                'iServiceID': iServiceID
            })
            tripID_dict[globalTripID] = {'confirmationNumber':int(confirmationNumber),
                                   'iRouteID': iRouteID,
                                    'iStopID': iStopID,'ArrivalTime': ArrivalTime, 'iServiceID':iServiceID,'StopName':PUPerson,'iRSTId': iRSTId,"TripStatus":TripStatus}
            tripIDStopIDDict[globalTripID]=iStopID

           # globalTripID += 1

        return trips_dict, tripID_dict, tripIDStopIDDict

    except Exception as e:
        print(e)
        return {}, {}, {}
def load_stop_management_policy(stop_management_policy_data):
    policy_dict = {}
    
    try:
        for item in stop_management_policy_data:
            routeID = item["RouteID"]
            UniqueID = int(item['UniqueID'])
            policy_dict[UniqueID,routeID] = {
                    'routeID': item["RouteID"],
                    'stationID': item["StationID"],
                    'stationName': item["StationName"],
                    'approachingStopMessage': item["approachingStopMessage"],
                    'stopArrivalMessage': item["stopArrivalMessage"],
                    'nextStopMessage': item["nextStopMessage"],
                    'bAnnounceRoute': item['bAnnounceRoute'],
                    'bAnnounceApproaching': item['bAnnounceApproaching'],
                    'bAnnounceArrived': item['bAnnounceArrived'],
                    'bAnnounceNextStop': item['bAnnounceNextStop'],
                    'bDisplayApproaching': item['bDisplayApproaching'],
                    'bDisplayArrived': item['bDisplayArrived'],
                    'bDisplayNextStop': item['bDisplayNextStop'],
                    "DisplayApproaching": item["DisplayApproaching"],
                    "DisplayArrival": item["DisplayArrival"],
                    "DisplayNextStop":item["DisplayNextStop"],
                    "waitTimeAnnounceApproaching": item["waitTimeAnnounceApproaching"],
                    "waitTimeAnnounceArrived": item["waitTimeAnnounceArrived"],
                    "waitTimeAnnounceNextStop": item["waitTimeAnnounceNextStop"],
                    "repeatDisplayApproaching": item["repeatDisplayApproaching"],
                    "repeatDisplayArrived": item["repeatDisplayArrived"],
                    "repeatDisplayNextStop": item["repeatDisplayNextStop"],
                    "repeatAnnounceApproaching": item["repeatAnnounceApproaching"],
                    "repeatAnnounceArrived": item["repeatAnnounceArrived"],
                    "repeatAnnounceNextStop": item["repeatAnnounceNextStop"]
                }
            
                
            

        return policy_dict
    
    except Exception as e:
        print(e)
    
stationList_data_lock = threading.Lock()
class FetchBusData:

    def fetchAllStations(self,allstationsparams,horizonTripListCallingTime):
        global count, stop_management_policy_dict, horizonTripListCallingTime_global #, avaDownloadPathDict
        if horizonTripListCallingTime != "":
            horizonTripListCallingTime_global = horizonTripListCallingTime

        with stationList_data_lock:
            response = requests.post(f"{api_base_url}{'FetchStations'}",headers=headers, data=allstationsparams)
            if response and response != None:
                stations = response.json()
                station_list = json.loads(stations["stationlist"])
                stationListDataForDB = []
                stop_management_policy_dict = load_stop_management_policy(station_list)
                updateStopManagementPolicyDict(stop_management_policy_dict)

                try:

                    for item in station_list:
                        count = 0
                        UniqueID = int(item['UniqueID'])
                        stationName = item["StationName"]
                        stationID = item["StationID"]
                        routeID = item["RouteID"]
                        bAnnounceRoute = item['bAnnounceRoute']
                        latitude = item["Latitude"]
                        longitude = item["Longitude"]
                        lastModifiedDate = item["LastModifiedDate"]
                        approachingStopMessage = item["approachingStopMessage"]
                        stopArrivalMessage = item["stopArrivalMessage"]
                        nextStopMessage = item["nextStopMessage"]
                        DataForAVAApproaching = item["AVAApproaching"]
                        DataForAVAArrival = item["AVAArrival"]
                        DataForAVANextStop = item["AVANextStop"]
                        bAnnounceApproaching = item['bAnnounceApproaching']
                        bAnnounceArrived = item['bAnnounceArrived']
                        bAnnounceNextStop = item['bAnnounceNextStop']
                        bDisplayApproaching = item['bDisplayApproaching']
                        bDisplayArrived = item['bDisplayArrived']
                        bDisplayNextStop = item['bDisplayNextStop']
                        waitTimeAnnounceApproaching = item["waitTimeAnnounceApproaching"]
                        waitTimeAnnounceArrived = item["waitTimeAnnounceArrived"]
                        waitTimeAnnounceNextStop = item["waitTimeAnnounceNextStop"]
                        repeatDisplayApproaching = item["repeatDisplayApproaching"] 
                        repeatDisplayArrived = item["repeatDisplayArrived"] 
                        repeatDisplayNextStop = item["repeatDisplayNextStop"] 
                        repeatAnnounceApproaching = item["repeatAnnounceApproaching"] 
                        repeatAnnounceArrived = item["repeatAnnounceArrived"] 
                        repeatAnnounceNextStop = item["repeatAnnounceNextStop"] 
                        DisplayApproaching = item["DisplayApproaching"] 
                        DisplayArrival = item["DisplayArrival"] 
                        DisplayNextStop = item["DisplayNextStop"]

                        stationListDataForDB.append((UniqueID, routeID, stationID, stationName, latitude, longitude,approachingStopMessage, stopArrivalMessage, nextStopMessage,bAnnounceRoute, lastModifiedDate,bAnnounceApproaching, bAnnounceArrived, bAnnounceNextStop, bDisplayApproaching, bDisplayArrived, bDisplayNextStop,DisplayApproaching, DisplayArrival, DisplayNextStop,waitTimeAnnounceApproaching, waitTimeAnnounceArrived, waitTimeAnnounceNextStop,repeatDisplayApproaching, repeatDisplayArrived, repeatDisplayNextStop,repeatAnnounceApproaching, repeatAnnounceArrived, repeatAnnounceNextStop))
                        #avaDownloadPathDict[UniqueID,routeID]= [DataForAVAApproaching,DataForAVAArrival,DataForAVANextStop]

                    # vluDBObject.insertIntoStationList(stationListDataForDB)
                    # threading.Thread(target=audioVoiceObj.checkAudioFiles,).start()
                    print("-----------Updated StationList------------")

                except Exception as e:
                    exception = f"[Exception in fetchAllStations: ] [{str(e)}]".strip()
                    AllException_Logs.error(exception)

fetchBusdataObj = FetchBusData()
async def startReceivingGPS():
    stationListUpdated = True
    await fetchAllRoutes(allrouteparams)
    await fetchControlledPoints()
    if stationListUpdated:
        allstationsparams = json.dumps({"companyId": AffiliateID, "bSendAllStations": "true"})
        threading.Thread(target=fetchBusdataObj.fetchAllStations,args=(allstationsparams, horizonWindowForDeviceTripList,)).start()
        stationListUpdated = False
    client = connect_mqtt()
    client.loop_start() 
    threading.Thread(target=testingSSA,args=(stopRadiusForDetection,)).start()
    threading.Thread(target=process_stop_detected_queue).start()
