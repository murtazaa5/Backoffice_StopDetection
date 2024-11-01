import os
import logging
from configFile import *

class SizedFileHandler(logging.FileHandler):
    def __init__(self, filename, mode="a", maxBytes=1.5 * 1024 * 1024, backupRatio=0.50, encoding=None, delay=False):
        self.maxBytes = maxBytes
        self.backupRatio = backupRatio
        super().__init__(filename, mode, encoding, delay)

    def emit(self, record):
        if self.should_rollover():
            self.do_rollover()
        logging.FileHandler.emit(self, record)

    def should_rollover(self):
        if self.stream is None:
            self.stream = self._open()
        self.stream.seek(0, os.SEEK_END)
        return self.stream.tell() + len(self.format(logging.LogRecord("", 0, "", 0, "", (), None))) >= self.maxBytes

    def do_rollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        keep_bytes = int(self.maxBytes * (1 - self.backupRatio))
        with open(self.baseFilename, "r") as f:
            data = f.read()
        trim_position = max(len(data) - keep_bytes, 0)
        with open(self.baseFilename, "w") as f:
            f.write(data[trim_position:])

def create_logger(name, file_name, level=logging.DEBUG):
    log_dir = os.path.dirname(file_name)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    file_handler = SizedFileHandler(file_name)
    formatter = logging.Formatter("\n%(asctime)s -  %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

def get_folder_size():
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(vluStopDetectionResult_file_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total_size += os.path.getsize(fp)
            except OSError as e:
                logging.error(f"Error getting size of {fp}: {e}")
    return total_size

def delete_oldest_files(target_size, total_size):
    files = []
    for dirpath, dirnames, filenames in os.walk(vluStopDetectionResult_file_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                files.append((fp, os.path.getmtime(fp)))
            except OSError as e:
                logging.error(f"Error getting creation time of {fp}: {e}")

    files.sort(key=lambda x: x[1])  # Sort files by creation time (oldest first)

    deleted_size = 0
    for file, ctime in files:
        if total_size - deleted_size <= target_size * 2 / 3:
            break
        try:
            file_size = os.path.getsize(file)
            os.remove(file)
            deleted_size += file_size
            logging.info(f"Deleted {file}, size: {file_size} bytes")
        except OSError as e:
            logging.error(f"Error deleting {file}: {e}")

# Create loggers with valid paths
log_directory = vluStopDetectionResult_file_path  
SocketStatus_Logs = create_logger("socket", os.path.join(log_directory, "SocketStatus_Logs.txt"))
AllException_Logs = create_logger("Exceptions", os.path.join(log_directory, "AllException_Logs.txt"))
Network_Logs = create_logger("internetIssue", os.path.join(log_directory, "Network_Logs.txt"))
TeltonikaGPS_Logs = create_logger("Teltonika", os.path.join(log_directory, "TeltonikaGPS_Logs.txt"))
VLUPushMessages_Logs = create_logger("VLUPushMessages_Logs", os.path.join(log_directory, "VLUPushMessages_Logs.txt"))
VLUStopDetectedNotProcessed_Logs = create_logger("VLUStopDetectedNotProcessed_Logs", os.path.join(log_directory, "VLUStopDetectedNotProcessed_Logs.txt"))
DriverTabletTripUpdate_Logs = create_logger("DriverTabletTripUpdate_Logs", os.path.join(log_directory, "DriverTabletTripUpdate_Logs.txt"))
