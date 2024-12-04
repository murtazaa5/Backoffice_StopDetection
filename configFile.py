import json
from pathlib import Path
from datetime import timedelta, datetime
current_path = Path.cwd()

#api_base_url = "https://demosdhsapi.itcurves.us/api/VLU/"
api_base_url = "http://localhost:49123/api/VLU/"
#ControlledPointsAPI = "https://central2rmdashboardapi.itcurves.us/PlayBack/GetVLU_ControlVectors"
ControlledPointsAPI = "https://alleganyrmdashboardapi.itcurves.us/PlayBack/GetVLU_ControlVectors"

AppVersion = '1.0'
isPulledSuccessfully = False
stopRadiusForDetection = 12
busSpeedLimitForStatus = 40.0

arrived = "arrived"
approaching = "approaching"
nextStop = "nextstop"
stopDetection_max_size = 20 * 1024 * 1024 
maxBusLateAllowedTime = 1200
repeatNexStopOnApproachingTime = 90
horizonWindowForDeviceTripList = 10
busAngleThreshold = 45.0

AffiliateID = 27

StartAppSeq = -1
EndAppSeq = -1
StartArrSeq = -1
MidArrSeq = -1
EndArrSeq = -1
StartNextSeq = -1
EndNextSeq =-1
if AffiliateID == 98:
    StartAppSeq = -15
    EndAppSeq = -8
    StartArrSeq = -7
    MidArrSeq = -4
    EndArrSeq = 0
    StartNextSeq = 1
elif AffiliateID == 27:
    StartAppSeq = -15
    EndAppSeq = -13
    StartArrSeq = -12
    MidArrSeq = -11
    EndArrSeq = 1
    StartNextSeq = 2
elif AffiliateID ==30:
    StartAppSeq = -15
    EndAppSeq = -13
    StartArrSeq = -12
    MidArrSeq = -11
    EndArrSeq = 1
    StartNextSeq = 2
    
current_time = datetime.now()
current_time_str = current_time.strftime("%m - %d %H:%M:%S")
retry_delay = 5  
deviceNumber = '20-7c-14-f2-b1-a2'
stationListUpdated = True

SelectedRoute = 1

LocaltimeDelta = timedelta(hours=10)

UseTime = datetime.now() - LocaltimeDelta

headers = {
            'Content-Type': 'application/json'
        }
allrouteparams = json.dumps({"companyId":AffiliateID, "vehicleNo": -1, "deviceNum":deviceNumber, "RouteId": 1, "getAll": True})
allControlparams = {"CompanyID": AffiliateID, "routeID": -1}
vlu_path = current_path / "VLU"
vlu_logs_path = vlu_path / "vlu_logs"
vlu_stop_detection_path = vlu_path / "vluStopDetection"

# Create the directories if they don't exist 
vlu_path.mkdir(exist_ok=True)
vlu_logs_path.mkdir(parents=True, exist_ok=True)
vlu_stop_detection_path.mkdir(parents=True, exist_ok=True)

# Define file paths
log_file_path = vlu_logs_path
vluStopDetectionResult_file_path = vlu_stop_detection_path
output_file = f"{vluStopDetectionResult_file_path}/processedStopDetectionResult.txt"
output_file_all_results = f"{vluStopDetectionResult_file_path}/stopDetectionAllResults.txt"

gpsFile = f"{vluStopDetectionResult_file_path}/Allegany_RedLine_Live_21Nov.csv"