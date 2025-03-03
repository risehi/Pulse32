import machine
import neopixel
import dht
import time
import network
import urequests
import ujson
import os
import _thread
from datetime import datetime

# Constants (Customizable)
DEBUG = True
WIFI_CREDENTIALS = {
    "hoxcentical": "8142apostrophe",
    "moto g(30)_9866": "6t37a2cqmiuci34",
}
SERVER_URL = "https://pulse-dash-app-h3dmb3akfveqh8au.canadaeast-01.azurewebsites.net/add-item"
LED_PIN = 48
SENSOR_PIN = 2
READ_DELAY = 5
BATCH_SIZE = 8
MAX_FILE_SIZE = 2 * 1024 * 1024
MEMORY_LIMIT = 100
WIFI_RETRY_LIMIT = 20
WIFI_RETRY_DELAY = 2
PUSH_RETRIES = 3
PUSH_RETRY_DELAY = 2
BLINK_DURATION = 0.25

# LED Colors
COLOR_OFF = (0, 0, 0)
COLOR_RED = (255, 0, 0)
COLOR_GREEN = (0, 255, 0)
COLOR_YELLOW = (255, 255, 0)

# Global flags and shared data
sensor_data = []  # Shared list for sensor readings
data_lock = _thread.allocate_lock()  # Thread-safe access to data
flushing_file_lock = _thread.allocate_lock() # Thread-safe flushing

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
            try:
                urequests.get("https://www.google.com", timeout=5)
                return True
            except Exception as e:
                log(f"Connection test failed: {e}, reconnecting", "WARNING")
                wlan.disconnect()
        log("Scanning Wi-Fi networks...")
        for ssid, password in WIFI_CREDENTIALS.items():
            try:
                log(f"Connecting to {ssid}")
                wlan.connect(ssid, password)
                for retry in range(WIFI_RETRY_LIMIT):
                    if wlan.isconnected():
                        actual_ssid = wlan.config('essid')  # Get the actual connected SSID
                        log(f"Connected to {ssid}: IP={wlan.ifconfig()[0]}, Actual SSID={actual_ssid}")
                        blink(COLOR_GREEN)
                        return True
                    time.sleep(WIFI_RETRY_DELAY)
                log(f"Failed to connect to {ssid}", "WARNING")
            except OSError as e:
                log(f"Wi-Fi error for {ssid}: {e}", "ERROR")
                blink(COLOR_RED, 5)
        log("Wi-Fi connection failed", "ERROR")
        return False
    except Exception as e:
        log(f"Unexpected Wi-Fi error: {e}", "ERROR")
        blink(COLOR_RED, 5)
        return False

# Sensor Reading (Threaded)
def read_sensors(retries=3):
    for attempt in range(retries):
        try:
            # Group: upper_unit
            sensor.measure()
            upper_unit = {
                "temperature": sensor.temperature(),
                "humidity": sensor.humidity()
            }

            # Mock other groups (replace with real sensor measurements as needed)
            nursery = {
                "temperature": 24.0,  # Placeholder value
                "humidity": 50.1  # Placeholder value
            }
            grow_unit_0 = {
                "temperature": 22.0,  # Placeholder value
                "humidity": 40.2,
                "light": 300  # Placeholder value
            }

            # Combine groups into the sensorGroups structure
            sensor_groups = {
                "upper_unit": upper_unit,
                "nursery": nursery,
                "grow_unit_0": grow_unit_0
            }

            log(f"Sensor groups read: {sensor_groups}")
            return sensor_groups

        except OSError as e:
            log(f"Sensor error (attempt {attempt + 1}): {e}", "WARNING")
            blink(COLOR_YELLOW)
            time.sleep(1)

    log("Sensor read failed after retries", "ERROR")
    return None  # Return None if all retries fail


def sensor_thread():
    company_name = "company_ABC"  # Replace with actual company name
    log(f"Starting sensor thread for {company_name}")

    while True:
        try:
            sensor_groups = read_sensors()
            
            # Skip if no sensor data is collected
            if sensor_groups:
                reading = transform_reading(
                    company_name=company_name,
                    timestamp=time.time(),
                    sensor_groups=sensor_groups
                )

                # Add the reading to shared data
                with data_lock:
                    if len(sensor_data) < MEMORY_LIMIT:
                        sensor_data.append(reading)
                        log(f"Added reading with groups: {list(sensor_groups.keys())} for {company_name}")
                    else:
                        log("Sensor data buffer full, discarding reading", "WARNING")
        except Exception as e:
            log(f"Error in sensor thread: {e}", "ERROR")
        
        # Wait before the next read
        time.sleep(READ_DELAY)


# Transform Reading Helper Function
def transform_reading(company_name, timestamp, sensor_groups):
    return {
        "partitionKey": company_name,
        "timestamp": datetime.utcfromtimestamp(timestamp).isoformat() + "Z",
        "sensorGroups": sensor_groups
    }


# Send Batch to Server
def push_to_server(batch):
    try:
        for reading in batch:
            if not all(key in reading for key in ("id", "partitionKey", "name")):
                log(f"Invalid transformed reading detected: {reading}", "ERROR")
                return False
        payload = [{"id": r["id"], "partitionKey": r["partitionKey"], "name": r["name"]} for r in batch]
        log(f"Prepared payload for upload: {payload}")
        headers = {"Content-Type": "application/json"}
        response = urequests.post(SERVER_URL, data=ujson.dumps(payload), headers=headers)
        if response.status_code == 200:
            log("Batch uploaded successfully")
            blink(COLOR_GREEN)
            response.close()
            return True
        else:
            log(f"Server rejected batch: HTTP {response.status_code}: {response.text}", "ERROR")
            response.close()
            return False
    except Exception as e:
        log(f"Unexpected error during upload: {type(e).__name__}: {e}", "ERROR")
        blink(COLOR_RED)
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
        log(f"Saved batch of {len(batch)} readings to file")
        blink(COLOR_YELLOW)
        return True
    except Exception as e:
        log(f"Error saving to file: {e}", "ERROR")
        return False

# Flush File to Server
def flush_file(filename="data.txt"):
    global flushing_file_lock
    if not flushing_file_lock.acquire(False):  # Non-blocking attempt to acquire lock
        log("Flush already in progress, skipping")
        return False
    try:
        if filename not in os.listdir():
            log(f"No file to flush: {filename}")
            return True
        with open(filename, "r") as file:
            lines = file.readlines()
        batch = []
        processed_lines = 0
        for line_number, line in enumerate(lines, start=1):
            try:
                reading = ujson.loads(line.strip())
                log(f"Processing line {line_number}: {reading}")
                if not all(key in reading for key in ("timestamp", "partitionKey", "sensorGroups")):
                    log(f"Invalid entry found in file (line {line_number}): {reading}", "ERROR")
                    continue

                batch.append(transform_reading(reading))
                if len(batch) >= BATCH_SIZE:
                    log(f"Attempting to send batch of {len(batch)} readings")
                    if push_to_server(batch):
                        log(f"Batch successfully sent (up to line {line_number})")
                        processed_lines = line_number
                        batch = []
                    else:
                        log(f"Failed to flush batch (up to line {line_number})", "ERROR")
                        break
            except Exception as e:
                log(f"Error processing line {line_number}: {e}", "ERROR")
        if processed_lines < len(lines):
            with open(filename, "w") as file:
                for line in lines[processed_lines:]:
                    file.write(line)
            log(f"Partial flush completed. Remaining {len(lines) - processed_lines} lines saved back to {filename}")
        else:
            os.remove(filename)
            log(f"File flushed and deleted successfully: {filename}")
        return True
    except Exception as e:
        log(f"Error during flush: {e}", "ERROR")
        return False
    finally:
        flushing_file_lock.release()  # Ensure lock is released


# Main
def main():
    log("Starting monitor (Version: 2025-03-03 with debug logging and threading)")
    batch = []  # Local batch for processing

    # Start sensor thread
    _thread.start_new_thread(sensor_thread, ())

    while True:
        try:
            log(f"Main loop iteration at {time.ticks_ms() / 1000:.1f}s")
            
            # Flush file if it exists and Wi-Fi is available
            if "data.txt" in os.listdir() and connect_wifi():
                log("Connection restored, attempting to flush file...")
                flush_file()

            # Process sensor data from thread
            with data_lock:
                if sensor_data:
                    batch.extend(sensor_data)
                    sensor_data.clear()
                    log(f"Main loop collected {len(batch)} readings from sensor_data")

            # Handle batch when full
            if len(batch) >= BATCH_SIZE:
                log(f"Live batch ready: {batch}")
                transformed_batch = [transform_reading(reading) for reading in batch]
                if connect_wifi() and push_to_server(transformed_batch):
                    batch = []
                elif not save_to_file(batch):
                    if len(batch) >= MEMORY_LIMIT:
                        log("Memory limit reached, discarding oldest", "WARNING")
                        batch.pop(0)
                else:
                    batch = []

            time.sleep(1)  # Short delay to prevent tight looping

        except Exception as e:
            log(f"Unexpected error: {e}", "ERROR")
            blink(COLOR_RED, 10)
            time.sleep(60)

if __name__ == "__main__":
    main()
