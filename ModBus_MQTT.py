#!/usr/bin/env python3
# V0.3 - MQTT with Modbus TCP Server Integration (pymodbus 3.9.2)

import time
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import json
import logging
import sys
import pytz
import os
from enum import Enum, auto
import threading
import struct
import asyncio

# Pymodbus 3.x imports
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext
from pymodbus.server import StartAsyncTcpServer
from pymodbus.device import ModbusDeviceIdentification

isRouter = False  

sys.path.append("lib")

# Configuration and initialization
def load_config(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

config_path = os.path.join('/root/RY3T/RY3T-Module-Homeassistant', 'config.json')
if(isRouter): 
    config = load_config(config_path)
else: 
    config = load_config('config.json')

# Modbus Configuration
MODBUS_SERVER_IP = config.get("modbus_server_ip", "0.0.0.0")
MODBUS_SERVER_PORT = config.get("modbus_server_port", 502)

#region: Modbus Register Mapping =====================================================================

# Modbus Register Addresses for LOXOL/Solarmanager
# Holding Registers (Read/Write) - Function Code 3/6/16
MODBUS_REGISTERS = {
    # Sensor Data Registers (Read-Only) - Starting at 40001
    "Heating": 40001,
    "Modes": 40002,  # Uses 2 registers (40003-40004)
    "Power_Modes": 40003,
    "Status": 40004,
    "Power_Level": 40005,  # Uses 2 registers (40008-40009)
    "Power_Limit": 40006,
    "Reister_07": 40007,    # Uses 2 registers (40011-40012)
    "Reister_08": 40008,      # Uses 2 registers (40013-40014)
    "Reister_09": 40009,     # Uses 2 registers (40015-40016)
    "Reister_10": 40010,       # Uses 2 registers (40017-40018)
    "Reister_12": 40011,  # Uses 2 registers (40019-40020)

    
    # Heater Control Registers (Read/Write) - Starting at 40100
    "Status": 40100,
    "ErrorCodes": 40101,
    "Automation": 40102,
    "PowerMode": 40103,
    "PowerLevel": 40104,
    "PowerLimit": 40105,
    "Miner1HSRT": 40106, # Uses 2 registers (40106-40107)
    "Miner1Power": 40108, # Uses 2 registers (40108-40109)
    "Miner1Uptime": 40110, # Uses 2 registers (40110-40111)
    "Miner2HSRT": 40112, # Uses 2 registers (40112-40113)
    "Miner2Power": 40114, # Uses 2 registers (40114-40115)
    "Miner2Uptime": 40116, # Uses 2 registers (40116-40117)
    "ColdWater": 40118, # Uses 2 registers (40118-40119)
    "ColdOil": 40120, # Uses 2 registers (40120-40121)
    "HotWater": 40122, # Uses 2 registers (40122-40123)
    "HotOil": 40124, # Uses 2 registers (40124-40125)
    "TempWaterTank1": 40126, # Uses 2 registers (40126-40127)
    "TempWaterTank2": 40128, # Uses 2 registers (40128-40129)
    "TempWaterTank3": 40130, # Uses 2 registers (40130-40131)
    "TempWaterTank4": 40132, # Uses 2 registers (40132-40133)
    "PumpPrimaryAnalogOut": 40134, # Uses 1 registers (40134)
    "PumpSecondaryAnalogOut": 40135, # Uses 1 registers (40135)
    
    # Control Commands (Write-Only) - Starting at 40200
    "HeaterCommand": 40200,  # 0=off, 1=on
    "HeaterMode": 40201,     # 1-8 for different modes
    "SystemReset": 40202,
}

# Reverse mapping for quick lookup
REGISTER_TO_NAME = {v: k for k, v in MODBUS_REGISTERS.items()}

#endregion

#region: MQTT Configuration =====================================================================

port = 1883

topic_data_temp_sensors = "data/onewire"
topic_data_miner = "data/miner"
topic_log_ha = "log/homeassistant"
topic_status_miner = "status/miner"
topic_status_automation = "status/automation"
topic_status_system = "status/system"
topic_status_error = "status/error"
topic_homeassistant_data_sensors = "homeassistant/data/sensors"
topic_homeassistant_data_heater = "homeassistant/data/heater"
topic_homeassistant_ctrl_heater = "homeassistant/ctrl/heater"
topic_ctrl_automation_homeassistant = "ctrl/automation/homeassistant"
topic_modbus_update = "modbus/update"  # New topic for Modbus updates

INIT_VAL = 0

sensor_attributes = {
    "Miner1HSRT": INIT_VAL,
    "Miner1Power": INIT_VAL,
    "Miner1Uptime": INIT_VAL,
    "Miner2HSRT": INIT_VAL,
    "Miner2Power": INIT_VAL,
    "Miner2Uptime": INIT_VAL,
    "ColdWater": INIT_VAL,
    "ColdOil": INIT_VAL,
    "HotWater": INIT_VAL,
    "HotOil": INIT_VAL,
    "TempWaterTank1": INIT_VAL,
    "TempWaterTank2": INIT_VAL,
    "TempWaterTank3": INIT_VAL,
    "TempWaterTank4": INIT_VAL,
    "PumpPrimaryAnalogOut": INIT_VAL,
    "PumpSecondaryAnalogOut": INIT_VAL,
}

heater_attributes = {
    "Status": INIT_VAL,
    "Errorcodes": INIT_VAL,
    "Automation": INIT_VAL,
    "PowerMode": INIT_VAL,
    "PowerLevel": INIT_VAL,
    "PowerLimit": INIT_VAL
}

last_handler_call_time = {}
modbus_context = None
modbus_server_task = None

#endregion

#region: Modbus Helper Functions =====================================================================

def float_to_modbus_registers(value):
    """Convert float to two 16-bit Modbus registers"""
    # Pack float as 32-bit IEEE 754
    packed = struct.pack('>f', float(value))
    # Unpack as two 16-bit integers
    high, low = struct.unpack('>HH', packed)
    return [high, low]

def modbus_registers_to_float(high, low):
    """Convert two 16-bit Modbus registers to float"""
    # Pack two 16-bit integers
    packed = struct.pack('>HH', high, low)
    # Unpack as float
    return struct.unpack('>f', packed)[0]

def update_modbus_register(name, value):
    """Update a Modbus register with the given value"""
    global modbus_context
    
    if modbus_context is None:
        return
    
    if name not in MODBUS_REGISTERS:
        print_mqtt(f"Unknown register name: {name}")
        return
    
    address = MODBUS_REGISTERS[name] - 40001  # Convert to 0-based addressing
    
    try:
        slave_context = modbus_context[0]  # Get the slave context (unit 0)
        
        # For float values, use two registers
        if isinstance(value, float):
            registers = float_to_modbus_registers(value)
            slave_context.setValues(3, address, registers)
        else:
            # For integer values, use single register
            slave_context.setValues(3, address, [int(value)])
        
        # Publish update to MQTT
        mqtt_client.publish(topic_modbus_update, 
                          json.dumps({"register": name, "value": value, "address": MODBUS_REGISTERS[name]}), 
                          qos=1)
    except Exception as e:
        print_mqtt(f"Error updating Modbus register {name}: {e}")

def read_modbus_register(name):
    """Read a value from a Modbus register"""
    global modbus_context
    
    if modbus_context is None:
        return None
    
    if name not in MODBUS_REGISTERS:
        print_mqtt(f"Unknown register name: {name}")
        return None
    
    address = MODBUS_REGISTERS[name] - 40001  # Convert to 0-based addressing
    
    try:
        slave_context = modbus_context[0]  # Get the slave context (unit 0)
        
        # Read two registers for float values
        if name in ["Miner1HSRT", "Miner1Power", "Miner2HSRT", "Miner2Power", 
                    "ColdWater", "ColdOil", "HotWater", "HotOil", 
                    "TempWaterTank1", "TempWaterTank2", "TempWaterTank3", "TempWaterTank4"]:
            values = slave_context.getValues(3, address, 2)
            return modbus_registers_to_float(values[0], values[1])
        else:
            # Read single register for integer values
            values = slave_context.getValues(3, address, 1)
            return values[0]
    except Exception as e:
        print_mqtt(f"Error reading Modbus register {name}: {e}")
        return None

#endregion

#region: Modbus Server Setup =====================================================================

async def run_async_modbus_server():
    """Setup and run the async Modbus TCP server"""
    global modbus_context
    
    # Create data blocks for different types of registers
    # We'll use holding registers (function code 3) for all data
    store = ModbusSlaveContext(
        di=ModbusSequentialDataBlock(0, [0]*100),     # Discrete Inputs
        co=ModbusSequentialDataBlock(0, [0]*100),     # Coils
        hr=ModbusSequentialDataBlock(0, [0]*500),     # Holding Registers (40001-40500)
        ir=ModbusSequentialDataBlock(0, [0]*100)      # Input Registers
    )
    
    modbus_context = ModbusServerContext(slaves=store, single=True)
    
    # Server identification
    identity = ModbusDeviceIdentification()
    identity.VendorName = 'RY3T'
    identity.ProductCode = 'HA-Gateway'
    identity.VendorUrl = 'https://ry3t.com'
    identity.ProductName = 'RY3T HomeAssistant Gateway'
    identity.ModelName = 'HA-Gateway-Modbus'
    identity.MajorMinorRevision = '3.9.2'
    
    print_mqtt(f"Starting Modbus TCP server on {MODBUS_SERVER_IP}:{MODBUS_SERVER_PORT}")
    
    try:
        # Start the async server
        await StartAsyncTcpServer(
            context=modbus_context,
            identity=identity,
            address=(MODBUS_SERVER_IP, MODBUS_SERVER_PORT)
        )
    except Exception as e:
        print_mqtt(f"Failed to start Modbus server: {e}")
        # Try with a different port if default fails
        if MODBUS_SERVER_PORT == 502:
            print_mqtt("Trying alternative port 5020...")
            await StartAsyncTcpServer(
                context=modbus_context,
                identity=identity,
                address=(MODBUS_SERVER_IP, 5020)
            )

def start_modbus_server():
    """Start the Modbus server in a new thread with its own event loop"""
    def run_server():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_async_modbus_server())
        except Exception as e:
            print_mqtt(f"Modbus server error: {e}")
    
    server_thread = threading.Thread(target=run_server)
    server_thread.daemon = True
    server_thread.start()
    
    # Give the server time to start
    time.sleep(2)
    print_mqtt("Modbus TCP server started successfully")

#endregion

#region: MQTT Handlers with Modbus Updates =====================================================================

def update_handler_timestamp(handler_name):
    global last_handler_call_time
    last_handler_call_time[handler_name] = datetime.now(pytz.timezone('Europe/Zurich'))

def reset_stale_data():
    now = datetime.now(pytz.timezone('Europe/Zurich'))
    two_minutes_ago = now - timedelta(minutes=2)

    for handler_name, last_call_time in last_handler_call_time.items():
        if last_call_time < two_minutes_ago:
            data_keys_to_reset = handler_data_mapping.get(handler_name, [])
            for data_key in data_keys_to_reset:
                print_mqtt(f"Resetting data key: {data_key}")
                sensor_attributes[data_key] = 0
                update_modbus_register(data_key, 0)

handler_data_mapping = {
    'handle_miner1_data': ['Miner1HSRT', 'Miner1Power', 'Miner1Uptime'],
    'handle_miner2_data': ['Miner2HSRT', 'Miner2Power', 'Miner2Uptime'],
    'handle_one_wire_data': [
        'ColdWater', 'ColdOil',
        'HotWater', 'HotOil',
        "TempWaterTank1", "TempWaterTank2", "TempWaterTank3", "TempWaterTank4"
    ]
}

def logging_func():
    logging.basicConfig(
        datefmt="%H:%M:%S",
        format="%(asctime)s.%(msecs)03d %(message)s",
        level=logging.INFO,
    )

def on_connect(client, userdata, flags, reason_code):
    global mqtt_connected
    print_mqtt(f"Connected with result code {reason_code}")
    mqtt_connected = True
    client.subscribe(f"{topic_data_miner}/#")
    client.subscribe(f"{topic_data_temp_sensors}/#")
    client.subscribe(f"{topic_status_miner}/#")
    client.subscribe(f"{topic_status_automation}/#")
    client.subscribe(f"{topic_homeassistant_ctrl_heater}/#")
    client.subscribe(f"{topic_status_system}/#")
    client.subscribe(f"{topic_status_error}/#")

def on_publish(client, userdata, mid):
    print("Message published.")

def on_message(client, userdata, message):
    for topic_pattern, handler in topic_handlers.items():
        if topic_pattern in message.topic:
            handler_name = handler.__name__
            handler(message.payload)
            update_handler_timestamp(handler_name)
            break
    else:
        print_mqtt(f"No handler for topic {message.topic}")

def handle_miner1_data(payload):
    payload_data = json.loads(payload)
    print_mqtt(f"minerdata {payload_data}")
    sensor_attributes["Miner1HSRT"] = float(payload_data["HS RT"])
    sensor_attributes["Miner1Power"] = float(payload_data["Power"])
    sensor_attributes["Miner1Uptime"] = float(payload_data["Uptime"])
    
    # Update Modbus registers
    update_modbus_register("Miner1HSRT", sensor_attributes["Miner1HSRT"])
    update_modbus_register("Miner1Power", sensor_attributes["Miner1Power"])
    update_modbus_register("Miner1Uptime", sensor_attributes["Miner1Uptime"])

def handle_miner2_data(payload):
    payload_data = json.loads(payload)
    sensor_attributes["Miner2HSRT"] = float(payload_data["HS RT"])
    sensor_attributes["Miner2Power"] = float(payload_data["Power"])
    sensor_attributes["Miner2Uptime"] = float(payload_data["Uptime"])
    
    # Update Modbus registers
    update_modbus_register("Miner2HSRT", sensor_attributes["Miner2HSRT"])
    update_modbus_register("Miner2Power", sensor_attributes["Miner2Power"])
    update_modbus_register("Miner2Uptime", sensor_attributes["Miner2Uptime"])

field_mapping = {
    "WaterTank1": "TempWaterTank1",
    "WaterTank2": "TempWaterTank2",
    "WaterTank3": "TempWaterTank3",
    "WaterTank4": "TempWaterTank4",
    "ColdWater": "ColdWater",
    "ColdOil": "ColdOil",
    "HotWater": "HotWater",
    "HotOil": "HotOil",
    # Old Ry3t sensor names for systems with old arduino code
    "TempWaterTank1": "TempWaterTank1",
    "TempWaterTank2": "TempWaterTank2",
    "TempWaterTank3": "TempWaterTank3",
    "TempWaterTank4": "TempWaterTank4",
    "TempSecondaryWaterBeforeExchange": "ColdWater",
    "TempPrimaryWaterAfterExchange": "ColdOil",
    "TempSecondaryWaterAfterExchange": "HotWater",
    "TempPrimaryWaterBeforeExchange": "HotOil"
}

def handle_temp_senosr_data(payload):
    try:
        print_mqtt(payload)
        payload_data = json.loads(payload)

        for incoming_field, storage_field in field_mapping.items():
            value = payload_data.get(incoming_field)
            if value is not None:
                sensor_attributes[storage_field] = float(value)
                # Update Modbus register
                update_modbus_register(storage_field, float(value))

    except Exception as e:
        print_mqtt(f"Error handling RTD data: {e}")
        mqtt_client.publish("log/debug", f"Handling RTD data: {payload}", qos=1)
        print_mqtt(f"failed to read RTD data: {payload}")

def handle_core_data(payload):
    payload_data = json.loads(payload)
    sensor_attributes["PumpPrimaryAnalogOut"] = int(payload_data["pump1"])
    sensor_attributes["PumpSecondaryAnalogOut"] = int(payload_data["pump2"])
    
    # Update Modbus registers
    update_modbus_register("PumpPrimaryAnalogOut", sensor_attributes["PumpPrimaryAnalogOut"])
    update_modbus_register("PumpSecondaryAnalogOut", sensor_attributes["PumpSecondaryAnalogOut"])

def handle_status_automation(payload):
    payload_data = json.loads(payload)
    try:
        print_mqtt(f"automation status: {payload_data}")
        heater_attributes["Automation"] = payload_data["mode"]
        heater_attributes["PowerMode"] = payload_data["power_mode"]
        heater_attributes["PowerLevel"] = payload_data["power_level"]
        heater_attributes["PowerLimit"] = payload_data.get("power_limit", 0)
        
        # Update Modbus registers
        update_modbus_register("HeaterAutomation", heater_attributes["Automation"])
        update_modbus_register("HeaterPowerMode", heater_attributes["PowerMode"])
        update_modbus_register("HeaterPowerLevel", heater_attributes["PowerLevel"])
        update_modbus_register("HeaterPowerLimit", heater_attributes["PowerLimit"])
    except Exception as e:
        print_mqtt(f"Error handling automation status: {e}")

def handle_status_system(payload):
    print_mqtt(f"SYSTEM STATUS: {payload}")
    try:
        payload = str(payload.decode('utf-8'))
        heater_attributes["Status"] = payload
        # For Modbus, convert string status to numeric code
        status_code = 1 if payload == "OK" else 0
        update_modbus_register("HeaterStatus", status_code)
    except Exception as e:
        print_mqtt(f"Error handling system status: {e}")

def handle_status_error(payload):
    print_mqtt(f"SYSTEM ERROR: {payload}")
    try:
        payload = str(payload.decode('utf-8'))
        heater_attributes["Errorcodes"] = payload
        # For Modbus, convert error string to numeric code
        error_code = int(payload) if payload.isdigit() else 0
        update_modbus_register("HeaterErrorCodes", error_code)
    except Exception as e:
        print_mqtt(f"Error handling system error: {e}")

def handle_status_miner1(payload):
    payload_data = json.loads(payload)

def handle_status_miner2(payload):
    payload_data = json.loads(payload)

def handle_heater_control(payload):
    command_map = {
        1: "heat_on",
        0: "heat_off"
    }

    mode_map = {
        1: "Levels",
        2: "PV",
        3: "Speicher",
        4: "House",
        5: "Loxone",
        6: "InletOutlet",
        7: "PrimaryHeating",
        8: "TwoSpeicher"
    }

    power_mode_map = {
        1: "Low",
        2: "Normal",
        3: "High"
    }

    try:
        payload_data = json.loads(payload)

        command = payload_data.get("cmd")
        if isinstance(command, int):
            command = command_map.get(command)

        if command == "heat_on":
            print_mqtt("Heater on")
            state = "on"
            mode = payload_data.get("mode")
            if isinstance(mode, int):
                mode = mode_map.get(mode)

            if mode not in mode_map.values():
                print_mqtt(f"Invalid mode: {mode}")
                return
            
            power_mode = payload_data.get("power_mode")
            if isinstance(power_mode, int):
                power_mode = power_mode_map.get(power_mode)
            
            if power_mode is not None and power_mode not in power_mode_map.values():
                print_mqtt(f"Invalid power mode: {power_mode}")
                return

            power_level = payload_data.get("power_level")
            power_limit = payload_data.get("power_limit")

        elif command == "heat_off":
            print_mqtt("Heater off")
            state = "off"
            mode = None
            power_mode = None
            power_level = None
            power_limit = None
        else:
            print_mqtt(f"Invalid command: {command}")
            return

        cmd = {
            "state": state,
            "mode": mode
        }

        if power_mode is not None:
            cmd["power_mode"] = power_mode
        if power_level is not None:
            cmd["power_level"] = power_level
        if power_limit is not None:
            cmd["power_limit"] = power_limit

        print_mqtt(f"Sending command to Home Assistant: {cmd}")
        mqtt_client.publish(topic_ctrl_automation_homeassistant, json.dumps(cmd), qos=1)

    except Exception as e:
        print_mqtt(f"Error handling heater control: {e}")

# Topic handlers mapping
topic_handlers = {
    "data/onewire": handle_temp_senosr_data,
    "data/miner/1": handle_miner1_data,
    "data/miner/2": handle_miner2_data,
    "data/core/output": handle_core_data,
    "status/automation": handle_status_automation,
    "status/miner/1": handle_status_miner1,
    "status/miner/2": handle_status_miner2,
    "homeassistant/ctrl/heater": handle_heater_control,
    "status/system": handle_status_system,
    "status/error": handle_status_error
}

#endregion

#region: Modbus Write Handler =====================================================================

def check_modbus_writes():
    """Periodically check for Modbus write commands and process them"""
    while True:
        time.sleep(1)  # Check every second
        
        if modbus_context is None:
            continue
        
        try:
            # Check HeaterCommand register for changes
            command = read_modbus_register("HeaterCommand")
            if command is not None and command != 0:
                mode = read_modbus_register("HeaterMode")
                
                # Create MQTT command
                mqtt_command = {
                    "cmd": int(command),
                    "mode": int(mode) if mode else 1
                }
                
                # Send command via MQTT
                mqtt_client.publish(topic_homeassistant_ctrl_heater, 
                                  json.dumps(mqtt_command), qos=1)
                
                # Reset command register
                update_modbus_register("HeaterCommand", 0)
                
                print_mqtt(f"Processed Modbus command: {mqtt_command}")
                
        except Exception as e:
            print_mqtt(f"Error checking Modbus writes: {e}")

#endregion

# ====================================================================================================

def print_mqtt(message):
    if not isinstance(message, str):
        message = str(message)

    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    print(f"{timestamp} - {message}")
    mqtt_client.publish(topic_log_ha, message, qos=0)

def send_sensor_attributes():
    while(True):
        time.sleep(10)
        print_mqtt(f"Sending sensor attributes {sensor_attributes}")
        mqtt_client.publish(topic_homeassistant_data_sensors, json.dumps(sensor_attributes), qos=0)

def send_heater_attributes():
    while(True):
        time.sleep(10)
        print_mqtt(f"Sending heater attributes {heater_attributes}")
        mqtt_client.publish(topic_homeassistant_data_heater, json.dumps(heater_attributes), qos=0)

# MQTT client setup for local broker
if(isRouter): 
    mqtt_client = mqtt.Client("HomeAssistantGateway")
else: 
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "HomeAssistantGateway")

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_publish = on_publish
mqtt_client.enable_logger()

try:
    mqtt_client.connect(config["broker_address"], port, keepalive=60)
except Exception as e:
    print(f"An exception occurred: {e}")

mqtt_client.loop_start()

# Main function
def main():
    global mqtt_connected
    logging_func()
    
    # Start Modbus TCP server
    start_modbus_server()
    
    # Start thread for checking Modbus writes
    modbus_write_thread = threading.Thread(target=check_modbus_writes)
    modbus_write_thread.daemon = True
    modbus_write_thread.start()

    sensor_attributes_thread = threading.Thread(target=send_sensor_attributes)
    sensor_attributes_thread.daemon = True
    sensor_attributes_thread.start()

    heater_attributes_thread = threading.Thread(target=send_heater_attributes)
    heater_attributes_thread.daemon = True
    heater_attributes_thread.start()

    try:
        while True:
            reset_stale_data()

            if not mqtt_client.is_connected():
                print_mqtt("Local MQTT Client disconnected; trying to reconnect.")
                mqtt_connected = False
                try:
                    mqtt_client.reconnect()
                except Exception as e:
                    print_mqtt(f"Reconnection failed: {e}")
            time.sleep(10)
    except KeyboardInterrupt:
        print_mqtt("Program interrupted by user. Exiting.")

if __name__ == "__main__":
    main()