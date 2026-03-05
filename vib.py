# coding:UTF-8
import threading
import time
import socket
import ssl
import os
import json
import signal
import logging
import serial
from serial import SerialException
import paho.mqtt.client as mqtt
import datetime
import varvib


# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/home/pi/vmt/vib.log")
    ]
)
log = logging.getLogger("vib")


# =============================================================================
# Device Model (Modbus RTU over Serial)
# =============================================================================

class SerialConfig:
    portName = '/dev/ttyUSB0'
    baud = 9600


class DeviceModel:

    auchCRCHi = [
        0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81,
        0x40, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0,
        0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01,
        0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41,
        0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81,
        0x40, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0,
        0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01,
        0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
        0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81,
        0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0,
        0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01,
        0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
        0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81,
        0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0,
        0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01,
        0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
        0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81,
        0x40]

    auchCRCLo = [
        0x00, 0xC0, 0xC1, 0x01, 0xC3, 0x03, 0x02, 0xC2, 0xC6, 0x06, 0x07, 0xC7, 0x05, 0xC5, 0xC4,
        0x04, 0xCC, 0x0C, 0x0D, 0xCD, 0x0F, 0xCF, 0xCE, 0x0E, 0x0A, 0xCA, 0xCB, 0x0B, 0xC9, 0x09,
        0x08, 0xC8, 0xD8, 0x18, 0x19, 0xD9, 0x1B, 0xDB, 0xDA, 0x1A, 0x1E, 0xDE, 0xDF, 0x1F, 0xDD,
        0x1D, 0x1C, 0xDC, 0x14, 0xD4, 0xD5, 0x15, 0xD7, 0x17, 0x16, 0xD6, 0xD2, 0x12, 0x13, 0xD3,
        0x11, 0xD1, 0xD0, 0x10, 0xF0, 0x30, 0x31, 0xF1, 0x33, 0xF3, 0xF2, 0x32, 0x36, 0xF6, 0xF7,
        0x37, 0xF5, 0x35, 0x34, 0xF4, 0x3C, 0xFC, 0xFD, 0x3D, 0xFF, 0x3F, 0x3E, 0xFE, 0xFA, 0x3A,
        0x3B, 0xFB, 0x39, 0xF9, 0xF8, 0x38, 0x28, 0xE8, 0xE9, 0x29, 0xEB, 0x2B, 0x2A, 0xEA, 0xEE,
        0x2E, 0x2F, 0xEF, 0x2D, 0xED, 0xEC, 0x2C, 0xE4, 0x24, 0x25, 0xE5, 0x27, 0xE7, 0xE6, 0x26,
        0x22, 0xE2, 0xE3, 0x23, 0xE1, 0x21, 0x20, 0xE0, 0xA0, 0x60, 0x61, 0xA1, 0x63, 0xA3, 0xA2,
        0x62, 0x66, 0xA6, 0xA7, 0x67, 0xA5, 0x65, 0x64, 0xA4, 0x6C, 0xAC, 0xAD, 0x6D, 0xAF, 0x6F,
        0x6E, 0xAE, 0xAA, 0x6A, 0x6B, 0xAB, 0x69, 0xA9, 0xA8, 0x68, 0x78, 0xB8, 0xB9, 0x79, 0xBB,
        0x7B, 0x7A, 0xBA, 0xBE, 0x7E, 0x7F, 0xBF, 0x7D, 0xBD, 0xBC, 0x7C, 0xB4, 0x74, 0x75, 0xB5,
        0x77, 0xB7, 0xB6, 0x76, 0x72, 0xB2, 0xB3, 0x73, 0xB1, 0x71, 0x70, 0xB0, 0x50, 0x90, 0x91,
        0x51, 0x93, 0x53, 0x52, 0x92, 0x96, 0x56, 0x57, 0x97, 0x55, 0x95, 0x94, 0x54, 0x9C, 0x5C,
        0x5D, 0x9D, 0x5F, 0x9F, 0x9E, 0x5E, 0x5A, 0x9A, 0x9B, 0x5B, 0x99, 0x59, 0x58, 0x98, 0x88,
        0x48, 0x49, 0x89, 0x4B, 0x8B, 0x8A, 0x4A, 0x4E, 0x8E, 0x8F, 0x4F, 0x8D, 0x4D, 0x4C, 0x8C,
        0x44, 0x84, 0x85, 0x45, 0x87, 0x47, 0x46, 0x86, 0x82, 0x42, 0x43, 0x83, 0x41, 0x81, 0x80,
        0x40]

    def __init__(self, deviceName, portName, baud, ADDR):
        log.info("Initialize device: {}".format(deviceName))
        self.deviceName   = deviceName
        self.ADDR         = ADDR
        self.deviceData   = {}                  # [FIX 1] instance-level dict
        self._lock        = threading.Lock()    # [FIX 2] thread safety
        self.isOpen       = False
        self.loop         = False
        self.serialPort   = None
        self.serialConfig = SerialConfig()
        self.serialConfig.portName = portName
        self.serialConfig.baud     = baud
        self.TempBytes    = []                  # [FIX 1] instance-level list
        self.statReg      = None

    def get_crc(self, datas, dlen):
        tempH = 0xff
        tempL = 0xff
        for i in range(0, dlen):
            tempIndex = (tempH ^ datas[i]) & 0xff
            tempH = (tempL ^ self.auchCRCHi[tempIndex]) & 0xff
            tempL = self.auchCRCLo[tempIndex]
        return (tempH << 8) | tempL

    # [FIX 2] Lock around dict writes
    def set(self, key, value):
        with self._lock:
            self.deviceData[key] = value

    # [FIX 2] Lock around dict reads
    def get(self, key):
        with self._lock:
            return self.deviceData.get(key, None)

    def remove(self, key):
        with self._lock:
            del self.deviceData[key]

    def openDevice(self):
        self.closeDevice()
        try:
            self.serialPort = serial.Serial(self.serialConfig.portName, self.serialConfig.baud, timeout=0.5)
            self.isOpen = True
            log.info("{} opened".format(self.serialConfig.portName))
            t = threading.Thread(target=self.readDataTh, args=("Data-Received-Thread", 10,))
            t.daemon = True
            t.start()
            log.info("{} ready".format(self.deviceName))
        except SerialException:
            log.error("Failed to open {}".format(self.serialConfig.portName))

    def readDataTh(self, threadName, delay):
        while True:
            if self.isOpen and self.serialPort is not None:
                try:
                    tLen = self.serialPort.inWaiting()
                    if tLen > 0:
                        data = self.serialPort.read(tLen)
                        if data:
                            self.onDataReceived(data)
                    else:
                        time.sleep(0.01)  # avoid CPU spinning when idle
                except Exception as ex:
                    log.debug("Serial read on {}: {}".format(self.deviceName, ex))
            else:
                time.sleep(0.1)
                break

    def closeDevice(self):
        self.isOpen = False
        port = self.serialPort
        self.serialPort = None
        if port is not None:
            port.close()

    def onDataReceived(self, data):
        tempdata = bytes.fromhex(data.hex())
        for val in tempdata:
            self.TempBytes.append(val)
            if self.TempBytes[0] != self.ADDR:
                del self.TempBytes[0]
                continue
            if len(self.TempBytes) > 2:
                if not (self.TempBytes[1] == 0x03):
                    del self.TempBytes[0]
                    continue
                tLen = len(self.TempBytes)
                if tLen == self.TempBytes[2] + 5:
                    tempCrc = self.get_crc(self.TempBytes, tLen - 2)
                    if (tempCrc >> 8) == self.TempBytes[tLen - 2] and (tempCrc & 0xff) == self.TempBytes[tLen - 1]:
                        self.processData(self.TempBytes[2])
                    else:
                        del self.TempBytes[0]

    def processData(self, length):
        if self.statReg is not None:
            for i in range(int(length / 2)):
                raw = self.TempBytes[2 * i + 3] << 8 | self.TempBytes[2 * i + 4]
                # [FIX 1] signed 16-bit conversion for velocity registers
                if raw >= 32768:
                    raw -= 65536
                if 0x3D <= self.statReg <= 0x3F:    # angle (degrees)
                    value = raw / 32768 * 180
                elif self.statReg == 0x40:           # temperature (°C)
                    value = raw / 100
                else:
                    value = raw
                self.set(str(self.statReg), value)
                self.statReg += 1
            self.TempBytes.clear()

    def sendData(self, data):
        try:
            self.serialPort.write(data)
        except Exception as ex:
            log.error("Serial write error on {}: {}".format(self.deviceName, ex))

    def readReg(self, regAddr, regCount):
        self.statReg = regAddr
        self.sendData(self.get_readBytes(self.ADDR, regAddr, regCount))

    def writeReg(self, regAddr, sValue):
        self.unlock()
        time.sleep(0.1)
        self.sendData(self.get_writeBytes(self.ADDR, regAddr, sValue))
        time.sleep(0.1)
        self.save()

    def get_readBytes(self, devid, regAddr, regCount):
        tempBytes = [None] * 8
        tempBytes[0] = devid
        tempBytes[1] = 0x03
        tempBytes[2] = regAddr >> 8
        tempBytes[3] = regAddr & 0xff
        tempBytes[4] = regCount >> 8
        tempBytes[5] = regCount & 0xff
        tempCrc = self.get_crc(tempBytes, 6)
        tempBytes[6] = tempCrc >> 8
        tempBytes[7] = tempCrc & 0xff
        return tempBytes

    def get_writeBytes(self, devid, regAddr, sValue):
        tempBytes = [None] * 8
        tempBytes[0] = devid
        tempBytes[1] = 0x06
        tempBytes[2] = regAddr >> 8
        tempBytes[3] = regAddr & 0xff
        tempBytes[4] = sValue >> 8
        tempBytes[5] = sValue & 0xff
        tempCrc = self.get_crc(tempBytes, 6)
        tempBytes[6] = tempCrc >> 8
        tempBytes[7] = tempCrc & 0xff
        return tempBytes

    def startLoopRead(self):
        self.loop = True
        t = threading.Thread(target=self.loopRead)
        t.daemon = True
        t.start()

    def loopRead(self):
        while self.loop:
            self.readReg(0x3A, 13)
            time.sleep(0.2)

    def stopLoopRead(self):
        self.loop = False

    def unlock(self):
        self.sendData(self.get_writeBytes(self.ADDR, 0x69, 0xb588))

    def save(self):
        self.sendData(self.get_writeBytes(self.ADDR, 0x00, 0x0000))


# =============================================================================
# Main
# =============================================================================

# [FIX 3] Ensure export directory exists
os.makedirs(varvib.exp_path, exist_ok=True)

# Devices
device0 = DeviceModel("WTVB02-0", "/dev/ttyUSB0", 9600, 0x50)
device0.openDevice()
device0.startLoopRead()

device1 = DeviceModel("WTVB02-1", "/dev/ttyUSB1", 9600, 0x51)
device1.openDevice()
device1.startLoopRead()

# Config from varvib.py
hostname         = varvib.mqtt_host
port             = varvib.mqtt_port
tls              = varvib.mqtt_tls
username         = varvib.mqtt_user
password         = varvib.mqtt_pass
topic            = varvib.mqtt_topic
mqtt_min_interval = varvib.mqtt_min_interval
interval         = varvib.interval
path             = varvib.exp_path

# MQTT setup
mqtt_connected     = False
last_reconnect_at  = 0.0
reconnect_running  = False
RECONNECT_COOLDOWN = 30

def on_connect(client, userdata, flags, reason_code, properties):
    global mqtt_connected
    mqtt_connected = (reason_code == 0)
    if mqtt_connected:
        log.info("MQTT connected to {}:{}".format(hostname, port))
    else:
        log.warning("MQTT connect failed, reason code: {}".format(reason_code))

def on_disconnect(client, userdata, flags, reason_code, properties):
    global mqtt_connected
    mqtt_connected = False
    log.warning("MQTT disconnected")

def broker_reachable():
    try:
        with socket.create_connection((hostname, port), timeout=3):
            return True
    except OSError:
        return False

def mqtt_connect():
    global mqtt_connected
    try:
        client.connect(hostname, port, 60)
        client.loop_start()
    except Exception as ex:
        log.error("MQTT connect error: {}".format(ex))
        mqtt_connected = False

def reconnect_async():
    """Run broker reachability check and connect in background thread."""
    global reconnect_running
    try:
        if broker_reachable():
            log.info("Broker reachable, reconnecting...")
            mqtt_connect()
        else:
            log.warning("Broker not reachable, next retry in {}s".format(RECONNECT_COOLDOWN))
    finally:
        reconnect_running = False

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set(username, password)
if tls:
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
client.on_connect = on_connect
client.on_disconnect = on_disconnect

if broker_reachable():
    mqtt_connect()
else:
    log.warning("Broker not reachable at startup, will retry in loop")

# [FIX 5] Graceful shutdown handler
def shutdown(signum, frame):
    log.info("Shutting down...")
    device0.stopLoopRead()
    device1.stopLoopRead()
    device0.closeDevice()
    device1.closeDevice()
    client.loop_stop()
    client.disconnect()
    log.info("Shutdown complete")
    raise SystemExit(0)

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

# Main loop
time.sleep(1)
start_time = time.time()    # [FIX 8] drift-free tick base
tick_count = 0

while True:
    tick_count += 1
    loop_start = time.time()                        # measure actual loop start
    dt       = datetime.datetime.utcnow()
    date_utc = dt.strftime("%Y-%m-%d")
    hour_utc = dt.strftime("%Y-%m-%d_%H")
    ts_iso   = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    # [FIX 8] drift-free next target time
    next_start = start_time + tick_count * interval

    # [FIX 7] JSON payload — directly usable in Node-RED without parsing
    payload = {
        "ts": ts_iso,
        "d0": {
            "vx": device0.get("58"), "vy": device0.get("59"), "vz": device0.get("60"),
            "ax": device0.get("61"), "ay": device0.get("62"), "az": device0.get("63"),
            "t":  device0.get("64"),
            "sx": device0.get("65"), "sy": device0.get("66"), "sz": device0.get("67"),
            "fx": device0.get("68"), "fy": device0.get("69"), "fz": device0.get("70")
        },
        "d1": {
            "vx": device1.get("58"), "vy": device1.get("59"), "vz": device1.get("60"),
            "ax": device1.get("61"), "ay": device1.get("62"), "az": device1.get("63"),
            "t":  device1.get("64"),
            "sx": device1.get("65"), "sy": device1.get("66"), "sz": device1.get("67"),
            "fx": device1.get("68"), "fy": device1.get("69"), "fz": device1.get("70")
        },
        "interval":     interval,
        "process_time": 0.0     # placeholder, updated below after all work is done
    }

    # CSV — keep flat format for compatibility
    csv_line = "{},{},{},{}\n".format(
        ts_iso,
        ",".join("{}:{}".format(k, v) for k, v in {
            "vx0": payload["d0"]["vx"], "vy0": payload["d0"]["vy"], "vz0": payload["d0"]["vz"],
            "ax0": payload["d0"]["ax"], "ay0": payload["d0"]["ay"], "az0": payload["d0"]["az"],
            "t0":  payload["d0"]["t"],
            "sx0": payload["d0"]["sx"], "sy0": payload["d0"]["sy"], "sz0": payload["d0"]["sz"],
            "fx0": payload["d0"]["fx"], "fy0": payload["d0"]["fy"], "fz0": payload["d0"]["fz"],
        }.items()),
        ",".join("{}:{}".format(k, v) for k, v in {
            "vx1": payload["d1"]["vx"], "vy1": payload["d1"]["vy"], "vz1": payload["d1"]["vz"],
            "ax1": payload["d1"]["ax"], "ay1": payload["d1"]["ay"], "az1": payload["d1"]["az"],
            "t1":  payload["d1"]["t"],
            "sx1": payload["d1"]["sx"], "sy1": payload["d1"]["sy"], "sz1": payload["d1"]["sz"],
            "fx1": payload["d1"]["fx"], "fy1": payload["d1"]["fy"], "fz1": payload["d1"]["fz"],
        }.items()),
        interval
    )

    with open(path + hour_utc + ".csv", "a") as f:
        f.write(csv_line)

    # [FIX 4] Reconnect only after cooldown, non-blocking in background thread
    if not mqtt_connected:
        now = time.time()
        if not reconnect_running and now - last_reconnect_at >= RECONNECT_COOLDOWN:
            last_reconnect_at = now
            reconnect_running = True
            t = threading.Thread(target=reconnect_async)
            t.daemon = True
            t.start()

    # Measure actual processing time after all work, update payload, then publish
    process_time = round(time.time() - loop_start, 4)
    rest_zeit    = next_start - time.time()
    payload["process_time"] = round(rest_zeit, 4)
    payload_json = json.dumps(payload)

    if mqtt_connected and interval >= mqtt_min_interval:
        try:
            client.publish(topic, payload_json)
        except Exception as ex:
            log.error("MQTT publish error: {}".format(ex))

    rest_zeit = next_start - time.time()
    if rest_zeit > 0:
        time.sleep(rest_zeit)
    else:
        log.warning("Loop overrun by {:.3f}s on tick {}".format(-rest_zeit, tick_count))
