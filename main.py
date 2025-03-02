import machine
import neopixel
import dht
import time
import network
import urequests
import ujson
import os

# Constants (Customizable)
DEBUG = True
WIFI_CREDENTIALS = {
    "hoxcentical": "8142apostrophe", 
    "moto g(30)_9866": "6t37a2cqmiuci34",
}
SERVER_URL = "https://pulse-dash-app-h3dmb3akfveqh8au.canadaeast-01.azurewebsites.net/add-item"  # Cosmos write URL
LED_PIN = 48
SENSOR_PIN = 2
READ_DELAY = 30  # Seconds between sensor reads
BATCH_SIZE = 8   # Number of readings per batch
MAX_FILE_SIZE = 2 * 1024 * 1024  # ~2 MB in bytes
MEMORY_LIMIT = 100  # Max in-memory readings when file is full
WIFI_RETRY_LIMIT = 10
WIFI_RETRY_DELAY = 1
PUSH_RETRIES = 3
PUSH_RETRY_DELAY = 2
BLINK_DURATION = 0.25

# LED Colors
COLOR_OFF = (0, 0, 0)
COLOR_RED = (255, 0, 0)
COLOR_GREEN = (0, 255, 0)
COLOR_YELLOW = (255, 255, 0)
COLOR_BLUE = (0, 0, 255)

# Hardware
wlan = network.WLAN(network.STA_IF)
wlan.active(True)
led = neopixel.NeoPixel(machine.Pin(LED_PIN), 1)
sensor = dht.DHT22(machine.Pin(SENSOR_PIN))

# Logging
def log(message, level="INFO"):
    if DEBUG or level in ("ERROR", "WARNING"):
        print(f"[{level}] {time.ticks_ms() / 1000:.1f}s: {message}")

# LED Control
def blink(color, count=6, duration=BLINK_DURATION):
    for _ in range(count):
        led[0] = color
        led.write()
        time.sleep(duration)
        led[0] = COLOR_OFF
        led.write()
        time.sleep(duration)

def led_solid(color):
    led[0] = color
    led.write()

# Wi-Fi
def connect_wifi():
    try:
        if wlan.isconnected():
            log(f"Already connected: IP={wlan.ifconfig()[0]}")
            return True
        log("Scanning Wi-Fi networks...")
        for ssid, password in WIFI_CREDENTIALS.items():
            log(f"Connecting to {ssid}")
            wlan.connect(ssid, password)
            for retry in range(WIFI_RETRY_LIMIT):
                if wlan.isconnected():
                    log(f"Connected to {ssid}: IP={wlan.ifconfig()[0]}")
                    blink(COLOR_GREEN)
                    return True
                time.sleep(WIFI_RETRY_DELAY)
            log(f"Failed to connect to {ssid}", "WARNING")
        log("Wi-Fi connection failed", "ERROR")
        wlan.active(False)
        time.sleep(2)
        wlan.active(True)
        return False
    except OSError as e:
        log(f"Wi-Fi error: {e}", "ERROR")
        blink(COLOR_RED, 5)
        return False

# Sensor Reading
def read_sensor(retries=3):
    for attempt in range(retries):
        try:
            sensor.measure()
            temp = sensor.temperature()
            humid = sensor.humidity()
            log(f"Sensor read: Temp={temp}, Humidity={humid}")
            return temp, humid
        except OSError as e:
            log(f"Sensor error (attempt {attempt + 1}): {e}", "WARNING")
            blink(COLOR_YELLOW)
            time.sleep(1)
    log("Sensor read failed", "ERROR")
    return None, None

# Send Batch to Server
def push_to_server(batch):
    if not batch:
        return False
    for attempt in range(PUSH_RETRIES):
        try:
            headers = {"Content-Type": "application/json"}
            # Create a single payload with the entire batch
            payload = ujson.dumps([{
                "id": str(int(reading["timestamp"])),  # Unique ID based on timestamp
                "partitionKey": "sensor_" + str(machine.unique_id()),  # Unique partition key for this sensor
                "name": f"Temp: {reading['temperature']}°C, Humidity: {reading['humidity']}%"
            } for reading in batch])
            log(f"Sending batch of {len(batch)} readings (attempt {attempt + 1}): {payload}")
            response = urequests.post(SERVER_URL, data=payload, headers=headers, timeout=15)
            if response.status_code == 200:
                log("Batch uploaded successfully")
                blink(COLOR_GREEN, 2)
                response.close()
                return True
            log(f"Server rejected the batch: HTTP {response.status_code}, {response.text}", "WARNING")
            response.close()
        except OSError as e:
            log(f"Network error during batch push: {e}", "ERROR")
            blink(COLOR_RED)
        time.sleep(PUSH_RETRY_DELAY * (2 ** attempt))  # Exponential backoff for retries
    log("All batch upload attempts failed", "ERROR")
    return False

# Save Batch to File
def save_to_file(batch, filename="data.txt"):
    try:
        file_size = os.stat(filename)[6] if filename in os.listdir() else 0
        if file_size >= MAX_FILE_SIZE:
            log("File size limit reached", "ERROR")
            led_solid(COLOR_RED)
            return False
        with open(filename, "a") as f:
            for reading in batch:
                f.write(ujson.dumps(reading) + "\n")
        log(f"Saved batch of {len(batch)} to file (size: {file_size + len(batch) * 60} bytes)")
        return True
    except Exception as e:
        log(f"File save error: {e}", "ERROR")
        return False

# Flush File to Server
def flush_file(filename="data.txt"):
    if filename not in os.listdir():
        log("No file to flush")
        return True
    try:
        with open(filename, "r") as f:
            lines = f.readlines()
        batch = []
        for line in lines:
            reading = ujson.loads(line.strip())
            batch.append(reading)
            if len(batch) == BATCH_SIZE:
                if push_to_server(batch):  # Batch push
                    batch = []
                else:
                    log("Flush failed, keeping file", "ERROR")
                    return False
        if batch and push_to_server(batch):  # Send remaining items
            os.remove(filename)
            log("File flushed and deleted")
            led_solid(COLOR_OFF)  # Reset LED
            return True
        log("Flush incomplete", "WARNING")
        return False
    except Exception as e:
        log(f"Flush error: {e}", "ERROR")
        return False

# Main
def main():
    log("Starting AM2302 monitor...")
    batch = []
    while True:
        try:
            temp, humid = read_sensor()
            if temp is not None and humid is not None:
                reading = {"timestamp": time.time(), "temperature": temp, "humidity": humid}
                batch.append(reading)
                log(f"Added to batch (size: {len(batch)})")

                if len(batch) >= BATCH_SIZE:
                    if connect_wifi():
                        if flush_file():  # Flush any stored file first
                            if push_to_server(batch):
                                batch = []
                    if batch:  # If not sent or no connection
                        if not save_to_file(batch):
                            if len(batch) >= MEMORY_LIMIT:
                                log("Memory limit reached, discarding oldest", "WARNING")
                                batch.pop(0)
                        else:
                            batch = []
            time.sleep(READ_DELAY)
        except Exception as e:
            log(f"Unexpected error: {e}", "ERROR")
            blink(COLOR_RED, 10)
            time.sleep(60)

main()
