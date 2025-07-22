import time
import threading
import json
import sys
import os
import glob
import paho.mqtt.client as mqtt_client

sys.path.append("lib")

os.system('modprobe w1-gpio')


broker_address = "mqtt5"
#broker_address = "10.0.0.5"
topic_data_onewire = "data/onewire"
topic_status_lifesign = "status/lifesign"

base_dir = '/sys/bus/w1/devices/'
# Get all the filenames begin with 28 in the path base_dir.
device_folders = glob.glob(base_dir + '28*')


def load_config(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

config = load_config('config.json')

address_to_index = {
    config["WaterCold"]:    0, #TempSecondaryWaterBeforeExchange
    config["OilCold"]:      1, #TempPrimaryWaterAfterExchange
    config["WaterHot"]:     2, #TempSecondaryWaterAfterExchange
    config["OilHot"]:       3, #TempPrimaryWaterBeforeExchange
    config["Speicher"]:     4, #speicher
    config["Boiler"]:       5, #boiler
    '28-00e5d446f1a2': 6, #thermostatSens - humanTemperatureStart
    '28-2816d4436d0f': 7, #hydroT1
    '28-72afd4435e99': 8, #hydroT2
    '28-1883d4433809': 9, #hydroT3
    '28-46d6d443ef12': 10, #hydroT4 
}

# sens at setup
# address_to_index = {
#     '28-0a7fd443f68b': 0, #TempSecondaryWaterBeforeExchange
#     '28-401fd443f3d6': 1, #TempPrimaryWaterAfterExchange
#     '28-3bedd443470d': 2, #TempSecondaryWaterAfterExchange
#     '28-4637d4435430': 3, #TempPrimaryWaterBeforeExchange
#     '28-7969d4467d56': 4, #speicher
#     '28-115bd446aa94': 5, #boiler
#     '28-00e5d446f1a2': 6, #thermostatSens - humanTemperatureStart
#     '28-2816d4436d0f': 7, #hydroT1
#     '28-72afd4435e99': 8, #hydroT2
#     '28-1883d4433809': 9, #hydroT3
#     '28-46d6d443ef12': 10, #hydroT4 
#     '28-189bd443b9d7': 11, #old ry3t t1
#     '28-5511d4431aef': 12, #old ry3t t2
#     '28-662dd4439e50': 13, #old ry3t t3
#     '28-7d63d443377d': 14, #old ry3t t4
# }

# sens home
# address_to_index = {
#     '28-012063ecb1b7': 0, #label 1
#     '28-01206413697b': 1, #label 2
#     '28-435dd443fdd7': 2, #label 3
#     '28-78fcd443a68d': 3  #label 4
# }

def read_rom(device_folder):
    name_file = device_folder + '/name'
    with open(name_file, 'r') as file:
        return file.readline().strip()

def read_temp_raw(device_file):
    try:
        with open(device_file, 'r') as file:
            lines = file.readlines()
        if lines:  # Check if lines is not empty
            return lines
        else:
            raise IOError("No data read from device.")
    except IOError as e:
        print(f"An error occurred while reading {device_file}: {e}")
        return None

def read_temp(device_file):
    max_attempts = 10
    attempt = 0
    try:
        while attempt < max_attempts:
            lines = read_temp_raw(device_file)
            if lines is None:
                # Data couldn't be read from device file. Wait and retry.
                time.sleep(0.5)
                attempt += 1
                continue

            if lines[0].strip()[-3:] == 'YES':  # If YES signal is present
                equals_pos = lines[1].find('t=')  # Find the index of 't='
                if equals_pos != -1:
                    temp_string = lines[1][equals_pos + 2:]
                    temp_c = float(temp_string) / 1000.0  # Convert to Celsius
                    return temp_c
            else:
                # Wait for the YES signal before retrying.
                time.sleep(0.2)
            
            attempt += 1
    except Exception as e:
            print(f"An exception occurred: {e}")

    print(f"Failed to read temperature from {device_file} after {max_attempts} attempts.")
    return None
    
def readOneWire():
    while True:
        try:
            temperatures = [None] * len(address_to_index)

            # Read all temperatures and map them to the array based on their ROM
            for device_folder in device_folders:
                try:
                    rom = read_rom(device_folder)
                    device_file = device_folder + '/w1_slave'
                    temp_c = read_temp(device_file)
                    index = address_to_index.get(rom)
                    if index is not None:
                        temperatures[index] = temp_c
                except IOError as e:
                    print(f"Sensor {device_folder} not detected or cannot be read: {e}")

            print("Temperatures by index:", temperatures)
            onewire_data = {}
            onewire_data["TempSecondaryWaterBeforeExchange"] = temperatures[0]
            onewire_data["TempPrimaryWaterAfterExchange"] = temperatures[1]
            onewire_data["TempSecondaryWaterAfterExchange"] = temperatures[2] 
            onewire_data["TempPrimaryWaterBeforeExchange"] = temperatures[3] 
            onewire_data["TempSpeicher"] = temperatures[4]
            onewire_data["TempBoiler"] = temperatures[5]
            onewire_data["ThermostatSensor"] = temperatures[6]
            onewire_data["HydroT1"] = temperatures[7]
            onewire_data["HydroT2"] = temperatures[8]
            onewire_data["HydroT3"] = temperatures[9]
            onewire_data["HydroT4"] = temperatures[10]  
            # onewire_data["OldRy3tT1"] = temperatures[11]
            # onewire_data["OldRy3tT2"] = temperatures[12]
            # onewire_data["OldRy3tT3"] = temperatures[13]  
            # onewire_data["OldRy3tT4"] = temperatures[14]
            mqttclient.publish(topic_data_onewire, json.dumps(onewire_data), qos=0)
        except Exception as e:
            print(f"An exception occurred: {e}")
        time.sleep(1)

def on_connect(client, userdata, flags, reason_code):
    print(f"Connected with result code {reason_code}")

def on_publish(client, userdata, mid):
    print("Message published.")

mqttclient = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, "onewire")
mqttclient.on_connect = on_connect
mqttclient.on_publish = on_publish
mqttclient.enable_logger()

try:
    mqttclient.connect(broker_address, keepalive=60)
except Exception as e:
    print(f"An exception occurred: {e}")

mqttclient.loop_start()

def lifesign():
    lifesign = 0
    while True:
        lifesign += 1
        if(lifesign > 10): lifesign = 0
        mqttclient.publish(topic_status_lifesign + "/onewire", lifesign, qos=0)
        time.sleep(0.5)


def main():
    onewire_thread = threading.Thread(target=readOneWire, args=())
    onewire_thread.daemon = True  # Set the thread as a daemon so it will close when the main program exits
    onewire_thread.start()

    lifesign_thread = threading.Thread(target=lifesign, args=())
    lifesign_thread.daemon = True  # Set the thread as a daemon so it will close when the main program exits
    lifesign_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Program interrupted by user. Exiting.")

if __name__ == "__main__":
    main()