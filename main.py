import machine
import neopixel
import dht
import time
import network
import urequests

# Constants
DEBUG = True
WIFI_CREDENTIALS = {
    "hoxcentical": "8142apostrophe", 
    "moto g(30)_9866": "6t37a2cqmiuci34",
}
API_KEY = "RSPS2CI7IHS0EEC6"
URL = "https://api.thingspeak.com/update"
LED_PIN = 48
SENSOR_PIN = 2
PUSH_INTERVAL = 30
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
def blink(color, count=3, duration=BLINK_DURATION):
    for _ in range(count):
        led[0] = color
        led.write()
        time.sleep(duration)
        led[0] = COLOR_OFF
        led.write()
        time.sleep(duration)

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
            for _ in range(WIFI_RETRY_LIMIT):
                if wlan.isconnected():
                    log(f"Connected to {ssid}: IP={wlan.ifconfig()[0]}")
                    blink(COLOR_GREEN)
                    return True
                time.sleep(WIFI_RETRY_DELAY)
            log(f"Failed to connect to {ssid}", "WARNING")
        log("Wi-Fi connection failed", "ERROR")
        blink(COLOR_RED)
        # Reset Wi-Fi module after failure
        log("Resetting Wi-Fi module...")
        wlan.active(False)
        time.sleep(1)
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
            log(f"Sensor read: Temp={temp}°C, Humidity={humid}%")
            return temp, humid
        except OSError as e:
            log(f"Sensor error (attempt {attempt + 1}): {e}", "WARNING")
            blink(COLOR_YELLOW)
            time.sleep(1)
    log("Sensor read failed after retries", "ERROR")
    blink(COLOR_RED)
    return None, None

# ThingSpeak
def push_to_thingspeak(temp, humid):
    if temp is None or humid is None:
        log(f"Invalid data: temp={temp}, humid={humid}", "ERROR")
        blink(COLOR_RED)
        return False
    payload = f"api_key={API_KEY}&field1={temp}&field2={humid}"
    for attempt in range(PUSH_RETRIES):
        response = None
        try:
            log(f"Sending to ThingSpeak (attempt {attempt + 1})")
            response = urequests.get(URL + "?" + payload, timeout=15)
            if response.status_code == 200:
                log("Data uploaded successfully")
                blink(COLOR_GREEN, 2)
                return True
            log(f"Upload failed: HTTP {response.status_code}", "WARNING")
        except OSError as e:
            log(f"Network error: {e}", "ERROR")
            blink(COLOR_RED)
        finally:
            if response:
                response.close()
        time.sleep(PUSH_RETRY_DELAY)
    log("All upload attempts failed", "ERROR")
    blink(COLOR_RED, 5)
    return False

# Main
def main():
    log("Starting AM2302 monitor...")
    WIFI_RETRY_INTERVAL = 300  # 5 minutes between retries when Wi-Fi is down
    while True:
        try:
            temp, humid = read_sensor()
            if temp is not None and humid is not None:
                if not wlan.isconnected():
                    log("Wi-Fi not connected, attempting to reconnect...")
                    if not connect_wifi():
                        log(f"Wi-Fi still unavailable, retrying in {WIFI_RETRY_INTERVAL}s", "WARNING")
                        time.sleep(WIFI_RETRY_INTERVAL)
                        continue
                # Wi-Fi is connected, proceed with upload
                push_to_thingspeak(temp, humid)
            time.sleep(PUSH_INTERVAL)
        except Exception as e:
            log(f"Unexpected error in main loop: {e}", "ERROR")
            blink(COLOR_RED, 10)
            time.sleep(60)

main()
