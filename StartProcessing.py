
import random
import time
import asyncio
from paho.mqtt import client as mqtt_client
import tzlocal
import geolocalisation_pb2 as geoloc
from protobuf_to_dict import protobuf_to_dict
from backOfficeStopDetection import *
gps_message_queue = queue.Queue()

system_timezone = tzlocal.get_localzone()
file = open('config_env.json')
loadedJsonFile = json.load(file)

broker = loadedJsonFile['broker']
port = loadedJsonFile['port']
username = loadedJsonFile['username']
password = loadedJsonFile['password']
client_id = f'python-mqtt-{random.randint(0,1000)}'

arrival_time_check_interval = 20
tripListUpdateDict = {"TripListUpdateTime":UseTime- timedelta(seconds=20)}

topicgnss = '/allegany/+/+/itxpt/ota/+/protobuf/gnss'


#topicgnss= '/jefftran/+/+/teltonika/gnss'

locationDict = {}
route_trips = {}
lastStoredGPSTime = {}
last_live_lat_long = {}
liveLatLong = {}
vehicle_data = {}
is_connected = False

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
        
def on_message(client, userdata, msg):
    try:
        if "gnss" in msg.topic and "jefftran" not in msg.topic:
            AffiliateID = msg.topic.split("/")[2]
            vehicle_num = msg.topic.split("/")[3]
            ################ for Jefftran #################### 
            # Decode the message payload
            # payloadClean = msg.payload.decode('utf-8', errors='ignore').replace('""', '"')
            # payloadJson = json.loads(payloadClean)
            # try:
            #     required_fields = ["GPS_Lat", "GPS_Long", "GPS_TS", "Speed"]
            #     if not all(field in payloadJson for field in required_fields):
            #         print("Payload is missing one or more required fields.")
            #         return
                
            #     newPayLoad = {
            #         "GpsLat": payloadJson["GPS_Lat"],
            #         "GpsLong": payloadJson["GPS_Long"],
            #         "Time": payloadJson["GPS_TS"].replace('\\u0000', "").replace('\x00', ''),
            #         "Speed": payloadJson["Speed"]
            #     }
            try:
                ################ for allegany #################### 
                pargeo = geoloc.GeoLocalisation()
                pargeo.ParseFromString(msg.payload)
                newPayLoad = {
                    "GpsLat": pargeo.latitude,
                    "GpsLong": pargeo.longitude,
                    "Time": pargeo.recorded_at_time,
                    "Speed": pargeo.speed_over_ground
                }
                newPayLoad['Time'] = int(newPayLoad['Time']) - (10  * 3600)
                gpsTime = int(newPayLoad['Time'])

                # Initialize vehicle data if not already present
                if vehicle_num not in lastStoredGPSTime:
                    lastStoredGPSTime[vehicle_num] = 0
                    last_live_lat_long[vehicle_num] = (0.0, 0.0)

                # Check if the new GPS time is greater than the last stored time
                if lastStoredGPSTime[vehicle_num] < gpsTime and vehicle_num in vehRouteList:
                    liveLatLong[vehicle_num] = (float(newPayLoad['GpsLat']), float(newPayLoad['GpsLong']))
                    
                    if last_live_lat_long[vehicle_num] != (0.0, 0.0) and liveLatLong[vehicle_num] != (0.0, 0.0): #and vehicle_num in ('651','650','630'):
                        currDistTravelled = calculateHaverSineDistance(last_live_lat_long[vehicle_num], liveLatLong[vehicle_num])
                        systemTime = int(time.time()) - (10  * 3600)
                        gpsTimeDifference = gpsTime - lastStoredGPSTime[vehicle_num]
                        gps_system_timeDifference = abs(systemTime - gpsTime)

                        if currDistTravelled > 3 and gps_system_timeDifference < 45 and SelectedRoute == vehRouteList[vehicle_num]:# and vehicle_num == '635':
                            #print(datetime.fromtimestamp(gpsTime))
                            
                            busDirectionAngle = calculate_initial_compass_bearing(last_live_lat_long[vehicle_num], liveLatLong[vehicle_num])
                            last_live_lat_long[vehicle_num] = liveLatLong[vehicle_num]
                            lastStoredGPSTime[vehicle_num] = gpsTime
                            locationDict = {
                                "Latitude": newPayLoad['GpsLat'],
                                "Longitude": newPayLoad['GpsLong'],
                                "Time": newPayLoad['Time'],
                                "Speed": newPayLoad['Speed'],
                                "VehicleNum": vehicle_num,
                                "BusDirectionAngle": busDirectionAngle,
                                "RouteID": vehRouteList[vehicle_num]
                            }
                            gps_message_queue.put(json.dumps(locationDict))
                    elif liveLatLong[vehicle_num] != (0.0, 0.0):
                        last_live_lat_long[vehicle_num] = liveLatLong[vehicle_num]

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
def convert_to_unix_timestamp(date_str):
    date_format = '%Y-%m-%d %H:%M:%S'  # Change as needed
    dt = datetime.strptime(date_str, date_format)
    return dt.timestamp()
    
def readGPSfromFile(file_path):
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)  # Use DictReader for easy column access
            for row in reader:
                gpsTime = int(convert_to_unix_timestamp(row.get('dtDeviceTime'))) # Adjust format as necessary
                vehicle_num = row.get('VehicleNo')  # Adjust column name as needed
                
                if vehicle_num not in lastStoredGPSTime:
                    lastStoredGPSTime[vehicle_num] = 0
                    last_live_lat_long[vehicle_num] = (0.0, 0.0)

                # Check if the new GPS time is greater than the last stored time
                if lastStoredGPSTime[vehicle_num] < gpsTime:
                    liveLatLong = (float(row['vLat']), float(row['vLong']))  # Adjust column names as needed
                    
                    if last_live_lat_long[vehicle_num] != (0.0, 0.0) and liveLatLong != (0.0, 0.0):
                        currDistTravelled = calculateHaverSineDistance(last_live_lat_long[vehicle_num], liveLatLong)
                        systemTime = int(time.time())
                        gpsTimeDifference = gpsTime - lastStoredGPSTime[vehicle_num]
                        gps_system_time_difference = abs(systemTime - gpsTime)

                        if currDistTravelled > 3:# and vehicle_num in ('631') :
                            locationDict = {}
                            busDirectionAngle = calculate_initial_compass_bearing(last_live_lat_long[vehicle_num], liveLatLong)
                            last_live_lat_long[vehicle_num] = liveLatLong
                            # Fill the location dictionary
                            locationDict["Latitude"] = row.get('vLat')  # Adjust column name as needed
                            locationDict["Longitude"] = row.get('vLong')  # Adjust column name as needed
                            locationDict["Time"] = convert_to_unix_timestamp(row.get('dtDeviceTime'))  # Adjust column name as needed
                            locationDict["Speed"] = row.get('dSpeed')  # Adjust column name as needed
                            locationDict["VehicleNum"] = vehicle_num
                            locationDict["BusDirectionAngle"] = busDirectionAngle
                            locationDict["RouteID"] = vehRouteList.get(vehicle_num, None) 
                            locationDict["UseTime"] = row.get('dtDeviceTime') #datetime.strptime(row.get('dtDeviceTime'), '%Y-%m-%d %H:%M:%S') if row.get('dtDeviceTime') else None
                            # Put the dictionary into the queue as a JSON string
                            gps_message_queue.put(json.dumps(locationDict))
                        else:
                            last_live_lat_long[vehicle_num] = liveLatLong
                    else:
                        last_live_lat_long[vehicle_num] = liveLatLong
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    
    
testingSSALock = threading.RLock()
def testingSSA(radius):
    global vehRouteList, UseTime
    while True:
        try:
            with testingSSALock:
                if not gps_message_queue.empty():
                    location_json = gps_message_queue.get() 
                    locationDict = json.loads(location_json) 

                    if locationDict:
                        liveLatLong = (float(locationDict["Latitude"]), float(locationDict["Longitude"]))
                        dSpeed = locationDict["Speed"]
                        RouteID = locationDict["RouteID"]
                        VehNo = locationDict["VehicleNum"]
                        # UseTime = datetime.strptime(locationDict["UseTime"], '%Y-%m-%d %H:%M:%S')
                        # system_time = datetime.strptime(locationDict["UseTime"], '%Y-%m-%d %H:%M:%S') if locationDict["UseTime"] else None
                        UseTime = datetime.now() - LocaltimeDelta
                        system_time = UseTime
                        liveGPSTime = datetime.fromtimestamp(int(locationDict["Time"]), tz=system_timezone).strftime('%Y-%m-%d %H:%M:%S')
                        busDirectionAngle = float(locationDict["BusDirectionAngle"])
                        smartSearchAlgo(liveLatLong, radius, busDirectionAngle, system_time, dSpeed, liveGPSTime, route_trips[RouteID], RouteID, VehNo)

        except Exception as e:
            logging.error(f"Error in testingSSA function: {e}")
            print(e)
            exception = f"[Error in testingSSA function: ] [{str(e)}]".strip()
            AllException_Logs.error(exception)



controlledVector_data_lock = threading.Lock()

async def  fetchControlledPoints():
    with controlledVector_data_lock:
        print("------------------------- Updating Controlled Vector ------------------------------------------")
        #allcontrolData = await GetVLU_ControlVectors(AffiliateID, -1)
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
                current_date = UseTime.date()
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
                    tripListUpdateDict["TripListUpdateTime"] = UseTime
                else:
                    tripListUpdateDict["TripListUpdateTime"] = UseTime

                print("-------------------------Updated tripList--------------------------")
                # if max_arrival_times:
                #     min_arrival_time = min(max_arrival_times.values())
                #     threading.Thread(target=check_arrival_time, args=(min_arrival_time,), daemon=True).start()


def check_arrival_time(min_arrival_time_dt):
    async def check():
        while True:
            system_time = UseTime
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
    trips_dict = defaultdict(lambda: defaultdict(list)) 
    tripID_dict = defaultdict(dict) 
    tripIDStopIDDict = defaultdict(dict) 
    globalTripID_per_route = defaultdict(int)

    try:
        for row in trips_data:
            # Unpack the row data
            confirmationNumber, PUPerson, vAddress, dLatitude, dLongitude, StopNumber, ArrivalTime, DepartTime, TripStatus, Route, ManifestNumber, dtReqIRTPU, dtDriverLoc, dtActualPickup, iRouteID, iStopID, vTStopType, tripColor, iRSTId, iVehicleID, vVehicleNo, iAffiliateID, DestSignageCode, iServiceID = row

            globalTripID_per_route[int(iRouteID)] += 1
            globalTripID = globalTripID_per_route[int(iRouteID)]
            # Populate trips_dict, grouped by Route and then iStopID
            trips_dict[int(iRouteID)][int(iStopID)].append({
                'tripID': globalTripID,
                'confirmationNumber': int(confirmationNumber),
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

            # Populate tripID_dict, grouped by Route and then globalTripID
            tripID_dict[int(iRouteID)][globalTripID] = {
                'confirmationNumber': int(confirmationNumber),
                'iRouteID': iRouteID,
                'iStopID': iStopID,
                'ArrivalTime': ArrivalTime,
                'iServiceID': iServiceID,
                'StopName': PUPerson,
                'iRSTId': iRSTId,
                'TripStatus': TripStatus
            }

            # Populate tripIDStopIDDict, grouped by Route and then globalTripID
            tripIDStopIDDict[int(iRouteID)][globalTripID] = iStopID

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
        global count, stop_management_policy_dict, horizonTripListCallingTime_global
        if horizonTripListCallingTime != "":
            horizonTripListCallingTime_global = horizonTripListCallingTime

        with stationList_data_lock:
            try:
                response = requests.post(f"{api_base_url}{'FetchStations'}",headers=headers, data=allstationsparams)
                if response and response != None:
                    stations = response.json()
                    station_list = json.loads(stations["stationlist"])
                    stationListDataForDB = []
                    stop_management_policy_dict = load_stop_management_policy(station_list)
                    updateStopManagementPolicyDict(stop_management_policy_dict)

                    print("-----------Updated StationList------------")
            except Exception as e:
                    exception = f"[Exception in fetchAllStations: ] [{str(e)}]".strip()
                    AllException_Logs.error(exception)    

def get_vehicle_num_by_route_id(route_id):
    for vehicle_num, route in vehRouteList.items():
        if route == route_id:
            return vehicle_num
    return None

def UpdateHeartBeat():
    while True:
        try:
            data = {
                "RouteID": SelectedRoute,  
                "VehicleNo": get_vehicle_num_by_route_id(SelectedRoute), 
                "companyId": AffiliateID, 
            }
            response = requests.post(f"{api_base_url}{'UpdateHeartBeat'}", headers=headers, json=data)
            
            if response.status_code == 200:
                response_json = response.json()
                print(f"[UpdateHeartBeat Success]: {response_json}")
            else:
                print(f"[UpdateHeartBeat Error]: Status Code {response.status_code}, Response: {response.text}")
        
        except Exception as e:
            exception = f"[Exception in UpdateHeartBeat]: {str(e)}"
            print(exception)
        time.sleep(120)

def time_until_next_5_am():
    now = datetime.now()
    next_5_am = (now + timedelta(days=1)).replace(hour=5, minute=0, second=0, microsecond=0)
    if now.hour < 5:
        next_5_am = now.replace(hour=5, minute=0, second=0, microsecond=0)
    return (next_5_am - now).total_seconds()
            
def daily_task_scheduler():
    while True:
        sleep_time = time_until_next_5_am()
        print(f"Sleeping for {sleep_time} seconds until the next 5 AM...")
        time.sleep(sleep_time)  
        if not asyncio.get_event_loop().is_running():
            asyncio.run(startReceivingGPS())
        else:
            # If an event loop exists, schedule startReceivingGPS
            asyncio.create_task(startReceivingGPS())

fetchBusdataObj = FetchBusData()
async def startReceivingGPS():
    global stationListUpdated
    await fetchAllRoutes(allrouteparams)
    await fetchControlledPoints()
    if stationListUpdated:
        allstationsparams = json.dumps({"companyId": AffiliateID, "bSendAllStations": "true"})
        threading.Thread(target=fetchBusdataObj.fetchAllStations,args=(allstationsparams, horizonWindowForDeviceTripList,)).start()
        stationListUpdated = False
    client = connect_mqtt()
    client.loop_start() 
    
    # readGPSfromFile(gpsFile)
    threading.Thread(target=testingSSA,args=(stopRadiusForDetection,)).start()
    
    #threading.Thread(target=UpdateHeartBeat).start()
    scheduler_thread = threading.Thread(target=daily_task_scheduler)
    scheduler_thread.daemon = True  
    scheduler_thread.start()

