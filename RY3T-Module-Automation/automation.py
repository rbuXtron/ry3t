# V0.12

import time
import threading
import json
import sys
import paho.mqtt.client as mqtt_client
import math
from enum import Enum, auto
from ry3tcontrol import RY3TControl
import os
from heatcurve import calculate_temperatures
from weatherapiclient import WeatherApiClient
from simple_pid import PID

isRouter = True   

sys.path.append("lib")

def load_config(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

config_path = os.path.join('/root/RY3T/RY3T-Module-Automation', 'config.json')
config_vals_path = os.path.join('/root/RY3T/RY3T-Module-Automation', 'config_values.json')
if(isRouter): config = load_config(config_path)
else: config = load_config('config.json')

if(isRouter): config_values = load_config(config_vals_path)
else: config_values = load_config('config_values.json')

ctrl = RY3TControl(config["broker_address"], isRouter)

broker_address = config["broker_address"]
topic_ctrl_miner = "ctrl/miner"
topic_ctrl_core = "ctrl/core"
topic_ctrl_automation = "ctrl/automation"
topic_status_lifesign = "status/lifesign"
topic_data_onewire = "data/onewire"
topic_data_miner = "data/miner"
topic_req_gateway = "req/gateway"
topic_ctrl_heatswitch = "ctrl/core/heatSwitch"
topic_status_heating = "status/heating"
topic_status_automation = "status/automation"
topic_log_automation = "log/automation"
topic_status_system = "status/system"
topic_log_debug = "log/debug"

# weather client ======================================

client_id = 'uH8XLtnrmlkwrN4SioaVLcpgSSZsGRfO'
client_secret = 'QEpsjLGUnbNWaigW'
# geolocation wil 9500
latitude = 47.4625
longitude = 9.0411

outside_temperature = 100

weather_client = WeatherApiClient(client_id, client_secret, latitude, longitude)

# command to get current temperature. fetch if none is fetched or ~hourly?
# temperature, timestamp = weather_client.get_temperature()

# current control values ==============================

ctrl_values = {
    "ManualMode": True,
    "Miner1On": 0,
    "Miner2On": 0,
    "PumpPrimaryAnalogOut": 40,
    "PumpSecondaryAnalogOut": 40,
    "PumpEnable": 0,
    "Valve1AnalogOut": 100,
    "Valve2AnalogOut": 100,
    "Miner1PowerPercentage": 100,
    "Miner2PowerPercentage": 100,
    "Miner1PowerMode": 1,
    "Miner2PowerMode": 1
}

thermostat_in = 0

manual_mode = True

automation_factor = 4

#region current data values

INIT_VAL = 0

core_output = {
    "Miner1On": INIT_VAL,
    "Miner2On": INIT_VAL,
    "PumpEnable": INIT_VAL,
    "PumpPrimaryAnalogOut": INIT_VAL,
    "PumpSecondaryAnalogOut": INIT_VAL,
    "Valve1AnalogOut": INIT_VAL,
    "Valve2AnalogOut": INIT_VAL,
}

onewire_data = {
    "TempSecondaryWaterBeforeExchange": INIT_VAL,
    "TempPrimaryWaterAfterExchange": INIT_VAL,
    "TempSecondaryWaterAfterExchange": INIT_VAL,
    "TempPrimaryWaterBeforeExchange": INIT_VAL,
    "TempSpeicher": INIT_VAL,
    "TempBoiler": INIT_VAL,
    "TempWaterTank1": INIT_VAL,
    "TempWaterTank2": INIT_VAL,
    "ThermostatSensor": INIT_VAL,
    "OldRy3tT1": INIT_VAL,
    "OldRy3tT2": INIT_VAL,
    "OldRy3tT3": INIT_VAL,
    "OldRy3tT4": INIT_VAL
}

miner1_data = {
    "Miner1HSRT": INIT_VAL,
    "Miner1Power": INIT_VAL,
    "Miner1Temperature": INIT_VAL,
    "Miner1Uptime": INIT_VAL,
    "Miner1ChipTempMax": INIT_VAL,
    "Miner1Summary": ''
}
miner2_data = {
    "Miner2HSRT": INIT_VAL,
    "Miner2Power": INIT_VAL,
    "Miner2Temperature": INIT_VAL,
    "Miner2Uptime": INIT_VAL,
    "Miner2ChipTempMax": INIT_VAL,
    "Miner2Summary": ''
}

miner1_last_online = 0
miner2_last_online = 0

miner1_running = False
miner2_running = False

max_miner_offline_duration = 10 * 60

# region load config values
# thresholds ==================================

th_low_speicher = config_values["thresholds"]["low_speicher"]
th_high_speicher = config_values["thresholds"]["high_speicher"]

th_miner_chip_temp = config_values["thresholds"]["miner_chip_temp"]
th_miner_board_temp = config_values["thresholds"]["miner_board_temp"]

th_temp_primary_before_exchange = config_values["thresholds"]["temp_primary_before_exchange"]

th_temp_outside_start = config_values["thresholds"]["temp_outside_start"]
th_temp_outside_stop = config_values["thresholds"]["temp_outside_stop"]

th_min_rising_trend = config_values["thresholds"]["min_rising_trend"]

th_max_overheat = config_values["thresholds"]["temp_secondary_max_overheat"] # house mode: maximum temperature over the target value before power mode gets turned down

th_recovery_temp_primary_before_exchange = config_values["thresholds"]["recovery_temp_primary_before_exchange"]

# target values ===============================

tar_temp_primary_before_exchange = config_values["target_values"]["temp_primary_before_exchange"] # 63 normally

min_diff_supply_return = config_values["target_values"]["min_diff_supply_return"]
max_diff_supply_return = config_values["target_values"]["max_diff_supply_return"]

# inlet_outlet values ===============================

inlet_outlet_start_temp = config_values["inlet_outlet"]["start_temp"]
inlet_outlet_stop_temp = config_values["inlet_outlet"]["stop_temp"]
inlet_outlet_min_pump_speed = config_values["inlet_outlet"]["min_pump_speed"]

# valve_control values ===============================

valve_control_on_value = config_values["valve_control"]["on_value"]
valve_control_off_value = config_values["valve_control"]["off_value"]

# pump control ================================

pump_max_percent = config_values["pump_control"]["max_percent"]
pump_min_percent = config_values["pump_control"]["min_percent"]

old_temp_primary_before_exchange = 0
old_temp_secondary_after_exchange = 0

primary_probe_time = config_values["primary_control_loop"]["probe_time"]         # probe all 30 seconds
secondary_probe_time = config_values["secondary_control_loop"]["probe_time"]   # probe every 5 minutes

pump_shutdown_delay = config_values["pump_control"]["shutdown_delay"]

trend_too_low_counter = 0

pumps_enabled = False

#region PID setup
# initialize primary pid to regulate the hot oil to 63 degrees
primary_pid_P = config_values["primary_control_loop"]["P"]
primary_pid_I = config_values["primary_control_loop"]["I"]
primary_pid_D = config_values["primary_control_loop"]["D"]

primary_pid = PID(primary_pid_P, primary_pid_I, primary_pid_D, setpoint=tar_temp_primary_before_exchange)
primary_pid.output_limits = (pump_min_percent, pump_max_percent)
primary_pid.sample_time = primary_probe_time

# fetch initial outside temp
temperature, timestamp = weather_client.get_temperature()
if temperature is not None:
    print(f"initial outside temp: {temperature}")
    outside_temperature = temperature

# calculate resulting secondary circuit water temperatures
supply_temperature, return_temperature = calculate_temperatures(outside_temperature)

# initialize secondary pid to regulate to the calculated hot water temperature
secondary_pid_P = config_values["secondary_control_loop"]["P"]
secondary_pid_I = config_values["secondary_control_loop"]["I"]
secondary_pid_D = config_values["secondary_control_loop"]["D"]

secondary_pid = PID(secondary_pid_P, secondary_pid_I, secondary_pid_D, setpoint=supply_temperature)
secondary_pid.output_limits = (pump_min_percent, pump_max_percent)
secondary_pid.sample_time = secondary_probe_time


#region states setup

class RY3TState(Enum):
    ON = auto()
    OFF = auto()
    ERROR_SHUTDOWN = auto()
    SECURITY_SHUTDOWN = auto()

current_state = RY3TState.OFF

def change_state(new_state):
    global current_state
    if new_state in RY3TState:
        current_state = new_state
    else:
        print_mqtt("Invalid state")

current_system_state = ""

#region modes setup
# system mode allow/require switching between submodes during automation
# PrimaryHeating: switches between Speicher and House mode
# TwoSpeicher: switches between two individual Speicher tanks; submodes are Speicher and TwoSpeicher

class RY3TMode(Enum):
    Speicher = auto()
    House = auto()
    Loxone = auto()
    InletOutlet = auto() 
    PrimaryHeating = auto() # system mode
    TwoSpeicher = auto()    # system mode
    Levels = auto()
    PV = auto()

current_mode = RY3TMode.Speicher

def change_current_mode(new_mode):
    global current_mode
    if new_mode in RY3TMode:
        mqttclient.publish(topic_log_debug, f"changing automation mode to {new_mode.name}", qos=1)
        set_valve_position(new_mode)
        current_mode = new_mode
    else:
        print_mqtt("Invalid mode")

system_mode = RY3TMode.Speicher

def change_system_mode(new_mode):
    global system_mode
    if new_mode in RY3TMode:
        mqttclient.publish(topic_log_debug, f"changing system mode to {new_mode.name}", qos=1)
        system_mode = new_mode
    else:
        print_mqtt("Invalid mode")

def set_valve_position(mode):
    if system_mode != RY3TMode.PrimaryHeating and system_mode != RY3TMode.TwoSpeicher: return
    if(mode == RY3TMode.House or mode == RY3TMode.TwoSpeicher):
        mqttclient.publish(topic_ctrl_heatswitch, valve_control_on_value, qos=1)
    elif(mode == RY3TMode.Speicher):
        mqttclient.publish(topic_ctrl_heatswitch, valve_control_off_value, qos=1)

#region homeassistant setup

class HAState(Enum):
    ON = auto()
    OFF = auto()
    DISABLED = auto()

def change_HA_state(new_state):
    global current_HA_state
    if new_state in HAState:
        current_HA_state = new_state
    else:
        print_mqtt("Invalid state")

current_HA_state = HAState.DISABLED
try:
    if(config["homeassistant"] == "True"):
        change_HA_state(HAState.OFF)
except:
    change_HA_state(HAState.DISABLED)

#region loxone setup

class RY3TPowerMode (Enum):
    NONE = auto()
    LOW = auto()
    NORMAL = auto()
    HIGH = auto()

change_powermode = False
current_loxone_mode = RY3TPowerMode.NONE
current_miner_mode = RY3TPowerMode.NONE

#region custom powermodes
# powermodes "Low", "Normal", "High"
class PowerModes:
    def __init__(self, name, hashpercent, powermode, default_pump1, default_pump2):
        self.name = name
        self.hashpercent = hashpercent
        self.powermode = powermode
        self.default_pump1 = default_pump1
        self.default_pump2 = default_pump2

# power mode setup ============================

def config_powermode(powermode):
    if powermode.lower() == "low":
        return RY3TPowerMode.LOW
    elif powermode.lower() == "normal":
        return RY3TPowerMode.NORMAL
    elif powermode.lower() == "high":
        return RY3TPowerMode.HIGH
    elif powermode.isdigit() and 1 <= int(powermode) <= 10:
        return RY3TPowerMode[int(powermode)]
    else:
        print_mqtt(f"Invalid powermode in config file: {powermode}")
        return None  # Ensure to handle None return if powermode is invalid

current_power_mode = RY3TPowerMode.NONE

pm_hashrate = config_values["power_modes"]["low"]["hashrate"]
pm_powermode = config_powermode(config_values["power_modes"]["low"]["powermode"])
pm_pump1 = config_values["power_modes"]["low"]["pump1"]
pm_pump2 = config_values["power_modes"]["low"]["pump2"]

Low_Mode = PowerModes("low", pm_hashrate, pm_powermode, pm_pump1, pm_pump2)

pm_hashrate = config_values["power_modes"]["normal"]["hashrate"]
pm_powermode = config_powermode(config_values["power_modes"]["normal"]["powermode"])
pm_pump1 = config_values["power_modes"]["normal"]["pump1"]
pm_pump2 = config_values["power_modes"]["normal"]["pump2"]

Normal_Mode = PowerModes("normal", pm_hashrate, pm_powermode, pm_pump1, pm_pump2)

pm_hashrate = config_values["power_modes"]["high"]["hashrate"]
pm_powermode = config_powermode(config_values["power_modes"]["high"]["powermode"])
pm_pump1 = config_values["power_modes"]["high"]["pump1"]
pm_pump2 = config_values["power_modes"]["high"]["pump2"]

High_Mode = PowerModes("high", pm_hashrate, pm_powermode, pm_pump1, pm_pump2)

#region power levels setup

power_levels = []

current_power_level = 0

for i in range(1, 11):
    power_level = config_values["power_levels"][str(i)]
    hashrate = power_level["hashrate"]
    powermode = config_powermode(power_level["powermode"])
    pump1 = power_level["pump1"]
    pump2 = power_level["pump2"]
    
    power_mode = PowerModes(i, hashrate, powermode, pump1, pump2)
    power_levels.append(power_mode)

#region PV setup

PV_levels = []

current_PV_level = 0
current_PV_limit = 0

for i in range(1, 8):
    power_level = config_values["PV_levels"][str(i)]
    hashrate = 100 #irrelevant because high mode ignores hashrate
    powermode = RY3TPowerMode.HIGH
    pump1 = power_level["pump1"]
    pump2 = power_level["pump2"]
    
    power_mode = PowerModes(i, hashrate, powermode, pump1, pump2)
    PV_levels.append(power_mode)

def get_power_limit_index(limit: float):
    step_size = config_values["PV_levels"]["step_size"]
    start_value = config_values["PV_levels"]["start_value"]

    index = int((limit - start_value) / step_size)

    if index < 0:
        index = 0
    elif index >= len(PV_levels):
        index = len(PV_levels) - 1

    return index


#region heating mode setup

def set_heating_mode(mode_name: str):
    global system_mode

    mode = get_heating_mode(mode_name)

    if(system_mode == mode and current_mode == mode):
        print_mqtt(f"already in mode {mode.name}")
        return

    # if PV mode was previously active or is now active reset limit back to maximum since its a permanent setting
    if(current_mode == RY3TMode.PV):
        ctrl.set_miner_power_limit(99999)

    if(mode == RY3TMode.PrimaryHeating or mode == RY3TMode.TwoSpeicher):
        change_current_mode(RY3TMode.Speicher)
    else:
        change_current_mode(mode)
    
    change_system_mode(mode)

def get_heating_mode(mode_name: str):
    if(mode_name == "Speicher"):
        return RY3TMode.Speicher
    elif(mode_name == "House"):
        return RY3TMode.House
    elif(mode_name == "Loxone"):
        return RY3TMode.Loxone
    elif(mode_name == "InletOutlet"):
        return RY3TMode.InletOutlet
    elif(mode_name == "PrimaryHeating"):
        return RY3TMode.PrimaryHeating
    elif(mode_name == "TwoSpeicher"):
        return RY3TMode.TwoSpeicher
    elif(mode_name == "Levels"):
        return RY3TMode.Levels
    elif(mode_name == "PV"):
        return RY3TMode.PV

#region: MQTT Receiving

def on_connect(client, userdata, flags, reason_code):
    print(f"Connected with result code {reason_code}")
    client.subscribe(f"{topic_ctrl_core}/#")
    client.subscribe(f"{topic_ctrl_miner}/#")
    client.subscribe(f"{topic_ctrl_automation}/#")
    client.subscribe(f"{topic_data_onewire}/#")
    client.subscribe(f"{topic_data_miner}/#")
    client.subscribe(f"{topic_status_system}/#")


def on_message(client, userdata, message):
    #print_mqtt(f"Received message '{str(message.payload.decode())}' on topic '{message.topic}'")

    # Iterate over the topic_handlers to find a matching topic pattern
    for topic_pattern, handler in topic_handlers.items():
        if topic_pattern in message.topic:
            # If a handler function exists for the topic pattern, call it
            handler(message.payload)
            break
    else:
        print_mqtt(f"No handler for topic {message.topic}")

def on_publish(client, userdata, mid):
    #print("Message published.")
    pass

if(isRouter): mqttclient = mqtt_client.Client("brain")
else: mqttclient = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, "brain")
mqttclient.on_connect = on_connect
mqttclient.on_publish = on_publish
mqttclient.on_message = on_message
mqttclient.enable_logger()


def handle_core_pump1(payload):
    print_mqtt(f"Handling pump1 data: {payload}")
    print_mqtt("ignore dashboard pump commands")
    return
    global ctrl_values
    pump1_percent = payload
    ctrl_values["PumpPrimaryAnalogOut"] = float(pump1_percent.decode('utf-8'))

def handle_core_pump2(payload):
    print_mqtt(f"Handling pump2 data: {payload}")
    print_mqtt("ignore dashboard pump commands")
    return
    global ctrl_values
    pump2_percent = payload
    ctrl_values["PumpSecondaryAnalogOut"] = float(pump2_percent.decode('utf-8'))

def handle_miner1(payload):
    print_mqtt(f"Handling miner1 data: {payload}")
    print_mqtt("ignore dashboard miner commands")
    return
    global ctrl_values
    miner_cmd = json.loads(payload)
    try:
        if(miner_cmd["cmd"] == "set_target_freq"):
            ctrl_values["Miner1PowerPercentage"] = 100 + float(miner_cmd["percent"])
        elif(miner_cmd["cmd"] == "set_low_power"):
            ctrl_values["Miner1PowerMode"] = 0
        elif(miner_cmd["cmd"] == "set_normal_power"):
            ctrl_values["Miner1PowerMode"] = 1
        elif(miner_cmd["cmd"] == "set_high_power"):
            ctrl_values["Miner1PowerMode"] = 2
        else: print_mqtt("unknown command for miner1")
    except:
        print_mqtt(f"no cmd field found in miner1 command string")

def handle_miner2(payload):
    print_mqtt(f"Handling miner2 data: {payload}")
    print_mqtt("ignore dashboard miner commands")
    return
    global ctrl_values
    miner_cmd = json.loads(payload)
    try:
        if(miner_cmd["cmd"] == "set_target_freq"):
            ctrl_values["Miner2PowerPercentage"] = 100 + float(miner_cmd["percent"])
        elif(miner_cmd["cmd"] == "set_low_power"):
            ctrl_values["Miner2PowerMode"] = 0
        elif(miner_cmd["cmd"] == "set_normal_power"):
            ctrl_values["Miner2PowerMode"] = 1
        elif(miner_cmd["cmd"] == "set_high_power"):
            ctrl_values["Miner2PowerMode"] = 2
        else: print_mqtt("unknown command for miner2")
    except:
        print_mqtt(f"no cmd field found in miner2 command string")

def handle_valve1(payload):
    print_mqtt(f"Handling valve1 data: {payload}")
    global ctrl_values
    valve1_percent = payload
    ctrl_values["Valve1AnalogOut"] = valve1_percent.decode('utf-8')

def handle_valve2(payload):
    print_mqtt(f"Handling valve2 data: {payload}")
    global ctrl_values
    valve2_percent = payload
    ctrl_values["Valve2AnalogOut"] = valve2_percent.decode('utf-8')

def handle_thermostat(payload):
    #print_mqtt(f"Handling thermostat data: {payload}")
    global thermostat_in
    thermostat_in = payload
    thermostat_in = thermostat_in.decode('utf-8')

def handle_loxone(payload):
    print_mqtt(f"Handling loxone data: {payload}")
    payload_data = json.loads(payload)
    global current_loxone_mode
    global change_powermode
    if(payload_data["LOW"] == 1 and current_loxone_mode != RY3TPowerMode.LOW): 
        current_loxone_mode = RY3TPowerMode.LOW
        change_powermode = True
        mqttclient.publish(topic_log_debug, "loxone input changed to low", qos=1)
        print_mqtt("loxone input changed to low")
    elif(payload_data["NORMAL"] == 1 and current_loxone_mode != RY3TPowerMode.NORMAL): 
        current_loxone_mode = RY3TPowerMode.NORMAL
        change_powermode = True
        mqttclient.publish(topic_log_debug, "loxone input changed to normal", qos=1)
        print_mqtt("loxone input changed to normal")
    elif(payload_data["HIGH"] == 1 and current_loxone_mode != RY3TPowerMode.HIGH): 
        current_loxone_mode = RY3TPowerMode.HIGH
        change_powermode = True
        print_mqtt("loxone input changed to high")
        mqttclient.publish(topic_log_debug, "loxone input changed to high", qos=1)
    elif(payload_data["LOW"] == 0 and payload_data["NORMAL"] == 0 and payload_data["HIGH"] == 0): 
        current_loxone_mode = RY3TPowerMode.NONE
    print_mqtt(f"current loxone mode: {current_loxone_mode}")

def handle_manual_mode(payload):
    print_mqtt(f"Handling manual mode data: {payload}")
    global manual_mode
    payload = payload.decode('utf-8')
    manual_mode = True if payload == "True" else False
    if(manual_mode): change_state(RY3TState.OFF)
    print_mqtt(f"manual mode is now: {manual_mode}")

def handle_onewire(payload):
    global all_sensors_connected
    try:
        print_mqtt(payload)
        payload_data = json.loads(payload)

        field_mapping = {
            "WaterTank1": "TempWaterTank1",
            "WaterTank2": "TempWaterTank2",
            "WaterTank3": "TempWaterTank3",
            "WaterTank4": "TempWaterTank4",
            "ColdWater": "TempSecondaryWaterBeforeExchange",
            "ColdOil": "TempPrimaryWaterAfterExchange",
            "HotWater": "TempSecondaryWaterAfterExchange",
            "HotOil": "TempPrimaryWaterBeforeExchange",
            "Speicher": "TempSpeicher",
            "Boiler": "TempBoiler",
            "Thermostat": "ThermostatSensor",
            # Old Ry3t sensor names for systems with old arduino code
            "TempWaterTank1": "TempWaterTank1",
            "TempWaterTank2": "TempWaterTank2",
            "TempWaterTank3": "TempWaterTank3",
            "TempWaterTank4": "TempWaterTank4",
            "TempSecondaryWaterBeforeExchange": "TempSecondaryWaterBeforeExchange",
            "TempPrimaryWaterAfterExchange": "TempPrimaryWaterAfterExchange",
            "TempSecondaryWaterAfterExchange": "TempSecondaryWaterAfterExchange",
            "TempPrimaryWaterBeforeExchange": "TempPrimaryWaterBeforeExchange",
            "TempSpeicher": "TempSpeicher",
            "TempBoiler": "TempBoiler"
        }

        try:
            if payload_data.get("WaterTank2") == 0 or payload_data.get("WaterTank2") is None:
                payload_data["WaterTank2"] = payload_data.get("WaterTank1")
        except KeyError:
            print_mqtt("WaterTank2 not found in payload, trying old ry3t sensor names")
            try:
                if payload_data.get("TempWaterTank2") == 0 or payload_data.get("TempWaterTank2") is None:
                    payload_data["TempWaterTank2"] = payload_data.get("TempWaterTank1")
            except KeyError:
                print_mqtt("Old ry3t sensor name also not found in payload")

        for incoming_field, storage_field in field_mapping.items():
            value = payload_data.get(incoming_field)
            if value is not None:
                onewire_data[storage_field] = float(value)

        print_mqtt("OneWire data updated:")
        print_mqtt(onewire_data)

    except Exception as e:
        print_mqtt(f"Error handling RTD data: {e}")
        mqttclient.publish("log/debug", f"Handling RTD data: {payload}", qos=1)
        print_mqtt(f"failed to read RTD data: {payload}")

def handle_miner1_data(payload):
    payload_data = json.loads(payload)
    global miner1_running
    #print_mqtt(f"Handling miner1 data: {payload}")
    global miner1_last_online
    miner1_data["Miner1HSRT"] = float(payload_data["HS RT"])
    miner1_data["Miner1Power"] = float(payload_data["Power"])
    miner1_data["Miner1Temperature"] = float(payload_data["Temperature"])
    miner1_data["Miner1Uptime"] = float(payload_data["Uptime"])
    miner1_data["Miner1ChipTempMax"] = float(payload_data["ChipTempMax"])
    miner1_data["Miner1Summary"] = payload_data["Summary"]
    if payload_data["mineroff"]  == "true":
        miner1_running = False
    else:
        miner1_running = True
        miner1_last_online = time.time()

def handle_miner2_data(payload):
    payload_data = json.loads(payload)
    global miner2_running
    global miner2_last_online
    #print_mqtt(f"Handling miner2 data: {payload}")
    miner2_data["Miner2HSRT"] = float(payload_data["HS RT"])
    miner2_data["Miner2Power"] = float(payload_data["Power"])
    miner2_data["Miner2Temperature"] = float(payload_data["Temperature"])
    miner2_data["Miner2Uptime"] = float(payload_data["Uptime"])
    miner2_data["Miner2ChipTempMax"] = float(payload_data["ChipTempMax"])
    miner2_data["Miner2Summary"] = payload_data["Summary"]
    if payload_data["mineroff"]  == "true":
        miner2_running = False
    else:
        miner2_running = True
        miner2_last_online = time.time()

def handle_values(payload):
    payload_data = json.loads(payload)
    print_mqtt(f"Handling dashboard values data: {payload}")
    global ctrl_values
    ctrl_values["ManualMode"] = payload_data["ManualMode"]
    ctrl_values["Miner1On"] = payload_data["Miner1On"]
    ctrl_values["Miner2On"] = payload_data["Miner2On"]
    #dashboard_ctrl_values["PumpPrimaryAnalogOut"] = float(payload_data["PumpPrimaryAnalogOut"])
    #dashboard_ctrl_values["PumpSecondaryAnalogOut"] = float(payload_data["PumpSecondaryAnalogOut"])
    ctrl_values["PumpEnable"] = payload_data["PumpEnable"]
    ctrl_values["Valve1AnalogOut"] = float(payload_data["Valve1AnalogOut"])
    ctrl_values["Valve2AnalogOut"] = float(payload_data["Valve2AnalogOut"])
    #dashboard_ctrl_values["Miner1PowerPercentage"] = float(payload_data["Miner1PowerPercentage"])
    #dashboard_ctrl_values["Miner2PowerPercentage"] = float(payload_data["Miner2PowerPercentage"])
    #dashboard_ctrl_values["Miner1PowerMode"] = int(payload_data["Miner1PowerMode"])
    #dashboard_ctrl_values["Miner2PowerMode"] = int(payload_data["Miner2PowerMode"])

def handle_status_system(payload):
    global current_system_state
    print_mqtt(f"SYSTEM STATUS: {payload}")
    try:
        payload = str(payload.decode('utf-8'))
        current_system_state = payload
        if(payload == "Error"):
            stop_system(True)
            change_state(RY3TState.ERROR_SHUTDOWN)
            print_mqtt("security shutdown because of system status")
    except Exception as e:
        print_mqtt(f"Error handling system status: {e}")

def handle_core_data(payload):
    payload_data = json.loads(payload)
    core_output["Miner1On"] = payload_data["miner1"]
    core_output["Miner2On"] = payload_data["miner2"]
    core_output["PumpEnable"] = payload_data["pumpEnable"]
    core_output["PumpPrimaryAnalogOut"] = int(payload_data["pump1"])
    core_output["PumpSecondaryAnalogOut"] = int(payload_data["pump2"])
    core_output["Valve1AnalogOut"] = payload_data["valve1"]
    core_output["Valve2AnalogOut"] = payload_data["valve2"]

def handle_pump_enable(payload):
    print_mqtt(f"Handling pump enable data: {payload}")
    global pumps_enabled
    payload = payload.decode('utf-8')

    if(payload == "False"):
        disable_pumps()
    else:
        ctrl.set_pumps_enable(True)
        pumps_enabled = True

    print_mqtt(f"pumps enabled: {pumps_enabled}")

def handle_homeassistant(payload):
    global current_power_level, current_mode, system_mode, current_PV_level, current_PV_limit
    try:
        if isinstance(payload, bytes):  # Check if payload is bytes and decode it
            payload = payload.decode('utf-8')
        
        payload_data = json.loads(payload)
        
        if "state" not in payload_data:
            print_mqtt("Missing 'state' in payload")
            return

        if payload_data["state"] == "off":
            print_mqtt("turning off homeassistant")
            stop_heating()
            change_state(RY3TState.OFF)
            change_HA_state(HAState.OFF)
            return
        elif payload_data["state"] != "on":
            print_mqtt("invalid homeassistant command")
            return
        
        if "mode" not in payload_data:
            print_mqtt("Missing 'mode' in payload")
            return

        old_mode = current_mode
        old_system_mode = system_mode
        
        set_heating_mode(payload_data["mode"])
        change_HA_state(HAState.ON)
        
        if current_mode == RY3TMode.Levels:
            if "power_level" not in payload_data:
                print_mqtt("Missing 'power_level' in payload")
                return
            
            try:
                index = int(payload_data["power_level"]) - 1
                if index < 0 or index >= len(power_levels):
                    print_mqtt("Invalid power level index")
                    return

                set_custom_powermode(1, power_levels[index])
                set_custom_powermode(2, power_levels[index])
                current_power_level = index
            except ValueError:
                print_mqtt("Invalid 'power_level' value")
                return
            
        elif current_mode == RY3TMode.PV:
            if "power_limit" not in payload_data:
                print_mqtt("Missing 'power_limit' in payload")
                return
            
            try:
                index = get_power_limit_index(float(payload_data["power_limit"]))
                if index < 0 or index >= len(PV_levels):
                    print_mqtt("Invalid PV level index")
                    return

                limit = float(payload_data["power_limit"])
                if limit < config_values["PV_levels"]["minimum_power"]:
                    limit = config_values["PV_levels"]["minimum_power"]
                set_power_limit(limit)

                set_custom_powermode(1, PV_levels[index])
                set_custom_powermode(2, PV_levels[index])
                current_PV_level = index
            except ValueError:
                print_mqtt("Invalid 'power_level' value")
                return
                
        elif 'power_mode' in payload_data:
            powermode = payload_data['power_mode']
            if powermode == "Low":
                set_custom_powermode(1, Low_Mode)
                set_custom_powermode(2, Low_Mode)
            elif powermode == "Normal":
                set_custom_powermode(1, Normal_Mode)
                set_custom_powermode(2, Normal_Mode)
            elif powermode == "High":
                set_custom_powermode(1, High_Mode)
                set_custom_powermode(2, High_Mode)
            else:
                print_mqtt("Invalid 'power_mode' value")
                return

        if old_mode != current_mode or old_system_mode != system_mode:
            print_mqtt(f"mode changed to {current_mode.name}")
            print_mqtt(f"system mode changed to {system_mode.name}")
            if not manual_mode:
                start_stop()
    except json.JSONDecodeError:
        print_mqtt(f"JSON decode error for homeassistant command: {payload}")
    except Exception as e:
        print_mqtt(f"Unexpected error: {str(e)}")

topic_handlers = {
    "ctrl/core/pump1": handle_core_pump1,
    "ctrl/core/pump2": handle_core_pump2,
    "ctrl/miner/1": handle_miner1,
    "ctrl/miner/2": handle_miner2,
    "data/core/output": handle_core_data,
    "data/miner/1": handle_miner1_data,
    "data/miner/2": handle_miner2_data,
    "ctrl/core/valve1": handle_valve1,
    "ctrl/core/valve2": handle_valve2,
    "data/onewire": handle_onewire,
    "ctrl/automation/thermostat" : handle_thermostat,
    "ctrl/automation/manualmode" : handle_manual_mode,
    "ctrl/automation/loxone" : handle_loxone,
    "ctrl/automation/values" : handle_values,
    "ctrl/automation/pumpEnable" : handle_pump_enable,
    "ctrl/automation/homeassistant" : handle_homeassistant,
    "status/system" : handle_status_system
    }

#endregion ==================================================================================================================


#region: System Loop

def control_loop():
    while True:
        if(current_state != RY3TState.SECURITY_SHUTDOWN and current_state != RY3TState.ERROR_SHUTDOWN):
            control()
        else:
            recovery()
        time.sleep(1)

def control():
    if(manual_mode): return
    # check system for limits - temp max etc
    print_mqtt("check for security thresholds")
    security_shutdown()
    # check if the system should change current mode
    mode_change()
    # check if it should turn off
    # check if it should turn on
    print_mqtt(f"check if the system should change state, current state: {current_state}")
    start_stop()

#endregion ==================================================================================================================

#region: Start / Stop

def security_shutdown():
    #if(True): return
    shut_down = False
    if(miner1_data["Miner1ChipTempMax"] >= th_miner_chip_temp): shut_down = True
    if(miner2_data["Miner2ChipTempMax"] >= th_miner_chip_temp): shut_down = True
    if(miner1_data["Miner1Temperature"] >= th_miner_board_temp): shut_down = True
    if(miner2_data["Miner2Temperature"] >= th_miner_board_temp): shut_down = True
    if(onewire_data["TempPrimaryWaterBeforeExchange"] >= th_temp_primary_before_exchange): shut_down = True
    current_time = time.time()
    if(current_time - miner1_last_online > max_miner_offline_duration and miner1_running):
        print_mqtt(f"miner was running but has been offline for too long, try sending a start signal again")
        shut_down = True
    if(shut_down):
        stop_system(True)
        print_mqtt("security shutdown")
        change_state(RY3TState.SECURITY_SHUTDOWN)

def stop_heating():
    print_mqtt("turning off system")
    change_state(RY3TState.OFF)
    stop_system()

def stop_system(error=False):
    # Disable miners
    ctrl.set_miner_power(1, False)
    ctrl.set_miner_power(2, False)

    disable_pumps(error)

def disable_pumps(error=False):
    global pumps_enabled
    # Disable pumps, don't disable if pump_shutdown_delay is -1
    if error or current_mode != RY3TMode.InletOutlet or manual_mode:
        ctrl.set_pump_percent(2, 0)
        pumps_enabled = False
        print_mqtt("turning off pump2")

    elif current_mode == RY3TMode.InletOutlet:
        ctrl.set_pump_percent(2, inlet_outlet_min_pump_speed)

    threading.Thread(target=gradually_decrease_pump, args=(error,)).start()

def gradually_decrease_pump(error = False):
    global pumps_enabled
    #if(core_output["PumpPrimaryAnalogOut"] < pump_min_percent):
    #    pump_speed = core_output["PumpPrimaryAnalogOut"]
    #else:
    #    pump_speed = pump_min_percent
    
    print_mqtt("setting pump1 to min speed")
    pump_speed = pump_min_percent
    ctrl.set_pump_percent(1, pump_speed)

    while pump_speed > 10 and (not pumps_enabled or (current_mode == RY3TMode.InletOutlet and current_state != RY3TState.ON)):
        time.sleep(1)
        pump_speed -= 1
        print_mqtt(f"reducing pump1 speed, new speed: {pump_speed}")
        ctrl.set_pump_percent(1, pump_speed)

    ctrl.set_pump_percent(1, 0)
    if error or (not pumps_enabled and (current_mode != RY3TMode.InletOutlet or manual_mode)):
        print_mqtt("turning off pump_enable")
        ctrl.set_pumps_enable(False)

def start_heating():
    global pumps_enabled
    print_mqtt("turning on system")
    mqttclient.publish(topic_log_debug, f"start heating was called", qos=1)
    change_state(RY3TState.ON)
    if(current_mode == RY3TMode.Loxone and current_loxone_mode != RY3TPowerMode.NONE):
        set_loxone_parameters(current_loxone_mode)
    elif(current_mode == RY3TMode.Levels):
        set_levels_parameters(power_levels[current_power_level])
    elif(current_mode == RY3TMode.PV):
        set_levels_parameters(PV_levels[current_PV_level])
    else:
        ctrl_values["PumpPrimaryAnalogOut"] = Normal_Mode.default_pump1
        ctrl_values["PumpSecondaryAnalogOut"] = Normal_Mode.default_pump2
        ctrl_values["Miner1PowerPercentage"] = Normal_Mode.hashpercent
        ctrl_values["Miner2PowerPercentage"] = Normal_Mode.hashpercent
        primary_pid.output_limits = (Normal_Mode.default_pump1, pump_max_percent)
        secondary_pid.output_limits = (Normal_Mode.default_pump2, pump_max_percent)
        
    change_current_mode(current_mode)
    if(current_mode != RY3TMode.PV): set_power_limit(99999)
    ctrl.start(ctrl_values["Miner1PowerPercentage"], ctrl_values["Miner2PowerPercentage"], ctrl_values["Miner1PowerMode"], ctrl_values["Miner2PowerMode"], ctrl_values["PumpPrimaryAnalogOut"], ctrl_values["PumpSecondaryAnalogOut"], ctrl_values["Valve1AnalogOut"], ctrl_values["Valve2AnalogOut"])
    pumps_enabled = True

def set_loxone_parameters(mode: PowerModes):
    if(mode == RY3TPowerMode.LOW):
        ctrl_values["PumpPrimaryAnalogOut"] = Low_Mode.default_pump1
        ctrl_values["PumpSecondaryAnalogOut"] = Low_Mode.default_pump2
        ctrl_values["Miner1PowerPercentage"] = Low_Mode.hashpercent
        ctrl_values["Miner2PowerPercentage"] = Low_Mode.hashpercent
    elif(mode == RY3TPowerMode.NORMAL):
        ctrl_values["PumpPrimaryAnalogOut"] = Normal_Mode.default_pump1
        ctrl_values["PumpSecondaryAnalogOut"] = Normal_Mode.default_pump2
        ctrl_values["Miner1PowerPercentage"] = Normal_Mode.hashpercent
        ctrl_values["Miner2PowerPercentage"] = Normal_Mode.hashpercent
    elif(mode == RY3TPowerMode.HIGH):
        ctrl_values["PumpPrimaryAnalogOut"] = High_Mode.default_pump1
        ctrl_values["PumpSecondaryAnalogOut"] = High_Mode.default_pump2
        ctrl_values["Miner1PowerPercentage"] = High_Mode.hashpercent
        ctrl_values["Miner2PowerPercentage"] = High_Mode.hashpercent

def set_levels_parameters(level: PowerModes):
    set_powermodes(level.powermode)
    ctrl_values["PumpPrimaryAnalogOut"] = level.default_pump1
    ctrl_values["PumpSecondaryAnalogOut"] = level.default_pump2
    ctrl_values["Miner1PowerPercentage"] = level.hashpercent
    ctrl_values["Miner2PowerPercentage"] = level.hashpercent

def set_powermodes(mode: PowerModes):
    global current_power_mode, current_miner_mode
    if mode == RY3TPowerMode.LOW:
        ctrl_values["Miner1PowerMode"] = 0
        ctrl_values["Miner2PowerMode"] = 0
    elif mode == RY3TPowerMode.NORMAL:
        ctrl_values["Miner1PowerMode"] = 1
        ctrl_values["Miner2PowerMode"] = 1
    elif mode == RY3TPowerMode.HIGH:
        ctrl_values["Miner1PowerMode"] = 2
        ctrl_values["Miner2PowerMode"] = 2
    current_miner_mode = mode

def mode_change():
    # only allow mode changes if system is primary heating system
    if(system_mode == RY3TMode.PrimaryHeating):

        print_mqtt(f"temp water tank: {onewire_data['TempWaterTank2']}, needs to be below: {th_low_speicher} to go into speicher mode")
        # if system is in house mode and speicher is too cold, change to speicher mode, even if house heating is currently in progress
        if(current_mode == RY3TMode.House and onewire_data["TempWaterTank2"] < th_low_speicher):
            change_current_mode(RY3TMode.Speicher)
        # if system is in speicher mode and turned off, but house needs heating, turn on heating
        elif(current_mode == RY3TMode.Speicher and outside_temperature < th_temp_outside_start and current_state == RY3TState.OFF): #TODO add house heating condition other than outside temp -> e.g. backflow to cold
            change_current_mode(RY3TMode.House)
        print_mqtt(f"outside temp currently: {outside_temperature}, needs to be below: {th_temp_outside_start} to start in house mode")

    if(system_mode == RY3TMode.TwoSpeicher):
        # if system is in twoSpeicher(lower prio speicher) mode and speicer is too cold, change to speicher mode, even if twoSpeicher heating is currently in progress
        if(current_mode == RY3TMode.TwoSpeicher and onewire_data["TempWaterTank2"] < th_low_speicher):
            change_current_mode(RY3TMode.Speicher)
        # if system is in speicher mode and turned off, but twoSpeicher needs heating, turn on heating
        elif(current_mode == RY3TMode.Speicher and onewire_data["TempWaterTank4"] < th_low_speicher and current_state == RY3TState.OFF):
            change_current_mode(RY3TMode.TwoSpeicher)
        

def start_stop():
    if(current_state == RY3TState.SECURITY_SHUTDOWN): return
    global old_temp_primary_before_exchange
    # stop check if any threshold values have been reached
    print_mqtt(f"temp outside: {outside_temperature}, should start if below: {th_temp_outside_start}")
    print_mqtt(f"current heating mode: {current_mode}")
    print_mqtt(f"current state: {current_state}")

    if(check_stop_conditions() and current_state != RY3TState.OFF):
        if(current_mode == RY3TMode.Speicher and system_mode == RY3TMode.PrimaryHeating):
            change_current_mode(RY3TMode.House)
        elif(current_mode == RY3TMode.Speicher and system_mode == RY3TMode.TwoSpeicher):
            change_current_mode(RY3TMode.TwoSpeicher)
        else:
            stop_heating()

    if(current_state == RY3TState.ON): 
        mqttclient.publish(topic_status_heating, "True", qos=1)
        return

    # start if outside temp is too low and its house heating
    elif(current_mode == RY3TMode.House and outside_temperature < th_temp_outside_start): #TODO add house heating condition other than outside temp -> e.g. backflow to cold
        start_heating()
    # check if its speicher heating and speicher is too cold
    elif(current_mode == RY3TMode.Speicher and onewire_data["TempWaterTank2"] < th_low_speicher):
        start_heating()
    # check if its TwoSpeicher heating and speicher is too cold
    elif(current_mode == RY3TMode.TwoSpeicher and onewire_data["TempWaterTank4"] < th_low_speicher):
        start_heating()
    # check if its loxone mode and there is a command to start
    elif(current_mode == RY3TMode.Loxone and current_loxone_mode != RY3TPowerMode.NONE):
        start_heating()
    # check if its inlet_outlet mode and the inlet temperature is too low
    elif(current_mode == RY3TMode.InletOutlet and onewire_data["TempSecondaryWaterBeforeExchange"] < inlet_outlet_start_temp):
        start_heating()
    elif(current_mode == RY3TMode.Levels and (current_HA_state == HAState.ON or current_HA_state == HAState.DISABLED)):
        start_heating()
    elif(current_mode == RY3TMode.PV and (current_HA_state == HAState.ON or current_HA_state == HAState.DISABLED)):
        start_heating()

    if(current_state == RY3TState.OFF or current_state == RY3TState.SECURITY_SHUTDOWN): mqttclient.publish(topic_status_heating, "False", qos=1)

def check_stop_conditions():
    stop = False
    if(outside_temperature is not None and outside_temperature > th_temp_outside_stop and current_mode == RY3TMode.House): stop = True
    if(current_loxone_mode == RY3TPowerMode.NONE and current_mode == RY3TMode.Loxone): stop = True
    if(current_mode == RY3TMode.InletOutlet and onewire_data["TempSecondaryWaterBeforeExchange"] > inlet_outlet_stop_temp): stop = True
    if(current_mode == RY3TMode.Speicher and onewire_data["TempWaterTank1"] > th_high_speicher): stop = True
    if(current_mode == RY3TMode.TwoSpeicher and onewire_data["TempWaterTank3"] > th_high_speicher): stop = True
    #if(onewire_data["OldRy3tT1"] - onewire_data["OldRy3tT4"] <= 10): stop = True
    return stop

def recovery():
    # print_mqtt("start recovery time 30 minutes")
    # time.sleep(30 * 60) # 30 minutes
    # print_mqtt("system recovered")
    # hot oil needs to be below the threshold to start the system again after overheating
    if(current_state == RY3TState.SECURITY_SHUTDOWN and onewire_data["TempPrimaryWaterBeforeExchange"] < th_recovery_temp_primary_before_exchange):
        change_state(RY3TState.OFF)
    if(current_state == RY3TState.ERROR_SHUTDOWN and current_system_state != "Error"):
        change_state(RY3TState.OFF)

#endregion

#region: Primary Loop 

def primary_control_loop():
    while True:
        control_temp()

def control_temp():
    if(current_state != RY3TState.ON): return
    global old_temp_primary_before_exchange
    global old_temp_secondary_after_exchange
    global trend_too_low_counter
    global change_powermode
    ctrl_val = 0
    start_heating()
    print_mqtt("PRIMARY CONTROL LOOP:")

    # control logic:
    # use primary pump to keep hot oil at target temperature at all times
    # use secondary pump tp get hot water to set supply_temperature and cold water at return_temperature
    # increasing secondary pump keeps the hot water temperature lower, but it will decrease the deltaT between hot water and cold water
    # if the difference is too small (test threshold was at 6 degrees) and the pump cant be changed without deviating from the set supply_temperature change miner power mode
    # increase the difference: set miner to a lower power mode -> supply less heat to the system
    # decrease the difference: set miner to a higher power mode -> supply more heat to the system

    # control pump 1 - goal is to have the TP1 at the target temperature at all times
    diff = tar_temp_primary_before_exchange - onewire_data["TempPrimaryWaterBeforeExchange"]

    print_mqtt(f"current hot oil temp is {onewire_data['TempPrimaryWaterBeforeExchange']}")
    trend_primary = onewire_data["TempPrimaryWaterBeforeExchange"] - old_temp_primary_before_exchange
    trend_primary = trend_primary / primary_probe_time # trend in degrees per second
    print_mqtt(f"trend of hot oil is {trend_primary} degree Celsius per second. {onewire_data['TempPrimaryWaterBeforeExchange']} - {old_temp_primary_before_exchange}")
    summary = miner1_data["Miner1Summary"]
    if(summary != ""):
        mode = summary["SUMMARY"][0]["Power Mode"]
        print_mqtt(f"miner running in mode: {mode}")

    trend_secondary = onewire_data["TempSecondaryWaterAfterExchange"] - old_temp_secondary_after_exchange
    trend_secondary = trend_secondary / primary_probe_time
    print_mqtt(f"trend of hot water is {trend_secondary} degree Celsius per second. {onewire_data['TempSecondaryWaterAfterExchange']} - {old_temp_secondary_after_exchange}")

    if(trend_secondary < th_min_rising_trend):
        trend_too_low_counter += 1
    else:
        trend_too_low_counter = 0

    if(trend_too_low_counter >= 20 and onewire_data['TempSecondaryWaterAfterExchange'] < supply_temperature and onewire_data['TempPrimaryWaterBeforeExchange'] < tar_temp_primary_before_exchange and ctrl_values["PumpPrimaryAnalogOut"] >= 20 and current_mode == RY3TMode.House):
        trend_too_low_counter = 0
        increase_powermode(1)
        increase_powermode(2)

    # calculate new pump speed
    ctrl_val = primary_pid(onewire_data["TempPrimaryWaterBeforeExchange"])

    # # check if pump is at maximum and the temperature is still rising
    # if(old_temp_primary_before_exchange > 0 and old_temp_primary_before_exchange < onewire_data["TempPrimaryWaterBeforeExchange"] and dashboard_ctrl_values["PumpPrimaryAnalogOut"] == 100 and trend_primary > 0):
    #     #shut down
    #     print_mqtt("pump 1 at full power and temp still rising, shutting down")
    #     stop_system()
    #     change_state(RY3TState.SECURITY_SHUTDOWN)
    # # set ctrl val to pump percent
    # else:
    #     print_mqtt(f"sending {ctrl_val} to pump1")
    #     ctrl.set_pump_percent(1, ctrl_val)
    
    print_mqtt(f"sending {ctrl_val} to pump1")
    ctrl.set_pump_percent(1, ctrl_val)

    print_mqtt(f"current miner mode: {current_miner_mode}, current loxone mode: {current_loxone_mode}")
    # if loxone mode is active and sends signal to change power mode
    if(current_mode == RY3TMode.Loxone and current_loxone_mode != RY3TPowerMode.NONE and change_powermode == True):
        if(current_loxone_mode == RY3TPowerMode.LOW): 
            set_custom_powermode(1, Low_Mode)
            set_custom_powermode(2, Low_Mode)
        elif(current_loxone_mode == RY3TPowerMode.NORMAL): 
            set_custom_powermode(1, Normal_Mode)
            set_custom_powermode(2, Normal_Mode)
        elif(current_loxone_mode == RY3TPowerMode.HIGH): 
            set_custom_powermode(1, High_Mode)
            set_custom_powermode(2, High_Mode)
        change_powermode = False

    # if secondary system is too hot and pump is at maximum, decrease power mode
    if(onewire_data["TempSecondaryWaterAfterExchange"] > (th_max_overheat + supply_temperature) and ctrl_values["PumpSecondaryAnalogOut"] >= 100 and current_mode == RY3TMode.House):
        decrease_powermode(1)
        decrease_powermode(2)

    # if deltaT is too small, decrease power mode, if its too big, increase power mode
    diff_supply_return = onewire_data["TempSecondaryWaterAfterExchange"] - onewire_data["TempSecondaryWaterBeforeExchange"]
    if(diff_supply_return > max_diff_supply_return and current_mode == RY3TMode.House):
        #expecting both miners to have the same power mode if 2 miner mode
        #set power mode higher
        increase_powermode(1)
        increase_powermode(2)

        # for two miners:
        #ctrl.set_miner_power_mode(2, 2)
    elif(diff_supply_return < min_diff_supply_return and current_mode == RY3TMode.House):
        #set power mode lower
        decrease_powermode(1)
        decrease_powermode(2)

        # for two miners:
        #ctrl.set_miner_power_mode(2, 2)

    old_temp_primary_before_exchange = onewire_data["TempPrimaryWaterBeforeExchange"]
    old_temp_secondary_after_exchange = onewire_data["TempSecondaryWaterAfterExchange"]

    time.sleep(primary_probe_time)

#endregion

#region: Secondary Loop

def secondary_control_loop():
    while True:
       control_secondary()

def control_secondary():
    global pumps_enabled

    if(manual_mode or current_HA_state == HAState.OFF): return
    if(current_mode == RY3TMode.InletOutlet and not pumps_enabled): 
        ctrl.set_pumps_enable(True)
        #ctrl.set_miner_power_percent(1, 0)
        ctrl.set_miner_power_percent(2, inlet_outlet_min_pump_speed)
        pumps_enabled = True
        return
    if(current_state != RY3TState.ON): return
    if(current_mode != RY3TMode.House): return

    global supply_temperature, return_temperature

    print_mqtt("SECONDARY CONTROL LOOP:")

    # update supply temperature in case of a change to the outside_temperature
    # calculate set temperature in secondary cicuit:
    # use outside_temperature fetched from weather_client
    supply_temperature, return_temperature = calculate_temperatures(outside_temperature)

    # update target temperature of the secondary PID
    secondary_pid.setpoint = supply_temperature

    ctrl_val = secondary_pid(onewire_data["TempSecondaryWaterAfterExchange"])

    print_mqtt(f"sending {ctrl_val} to pump2")
    ctrl.set_pump_percent(2, ctrl_val)

    time.sleep(secondary_probe_time)

#endregion

#region: Power Modes

def set_power_limit(limit: float):
    global current_PV_limit

    if(current_mode == RY3TMode.PV):
        current_PV_limit = limit
        print_mqtt(f"setting power limit to {limit}")
        ctrl.set_miner_power_limit(limit)

last_change_time = 0
lock_duration = 600  # 10 minutes in seconds

#set custom powermode - PowerModes: Low_Mode - High_Mode
def set_custom_powermode(miner: int, mode: PowerModes):
    global last_change_time
    global current_miner_mode
    global primary_pid
    global current_power_mode

    if(current_mode != RY3TMode.Loxone and current_mode != RY3TMode.Levels and current_mode != RY3TMode.PV):	
        current_time = time.time()
        if current_time - last_change_time < lock_duration:
            print_mqtt(f"Power Mode increase blocked. Miner changed powermode {current_time - last_change_time} seconds ago. (minimum time between power mode changes: {lock_duration})")
            return

    print_mqtt(f"miner running in mode: {mode.name}")
    mqttclient.publish(topic_log_debug, f"changing power mode to {mode.name}", qos=1)

    set_powermode(miner, mode.powermode)

    power_modes = ["low", "normal", "high"]
    if isinstance(mode.name, str) and mode.name.lower() in power_modes:
        current_power_mode = config_powermode(mode.name)

    ctrl.set_miner_power_percent(miner, mode.hashpercent)
    set_pump_default_values(mode)
    primary_pid.output_limits = (mode.default_pump1, pump_max_percent)

def set_pump_default_values(mode):
    if(ctrl_values["PumpPrimaryAnalogOut"] < mode.default_pump1):
        ctrl.set_pump_percent(1, mode.default_pump1)
    if(ctrl_values["PumpSecondaryAnalogOut"] < mode.default_pump2):
        ctrl.set_pump_percent(2, mode.default_pump2)

# set MINER POWER MODE - RY3TPowerMode.LOW-HIGH
def set_powermode(miner: int, mode: RY3TPowerMode):
    global last_change_time
    global current_miner_mode
    global current_mode

    if(current_mode != RY3TMode.Loxone and current_mode != RY3TMode.Levels and current_mode != RY3TMode.PV):
        current_time = time.time()
        if current_time - last_change_time < lock_duration:
            print_mqtt(f"Power Mode increase blocked. Miner changed powermode {current_time - last_change_time} seconds ago. (minimum time between power mode changes: {lock_duration})")
            return

    print_mqtt(f"changing power modes to {mode}")

    if mode == RY3TPowerMode.LOW:
        print_mqtt("Setting power mode to Low.")
        ctrl.set_miner_power_mode(miner, 0)
        current_miner_mode = mode
        last_change_time = time.time()
    elif mode == RY3TPowerMode.NORMAL:
        print_mqtt("Setting power mode to Normal.")
        ctrl.set_miner_power_mode(miner, 1)
        current_miner_mode = mode
        last_change_time = time.time()
    elif mode == RY3TPowerMode.HIGH:
        print_mqtt("Setting power mode to High.")
        ctrl.set_miner_power_mode(miner, 2)
        current_miner_mode = mode
        last_change_time = time.time()
    else:
        print_mqtt("Invalid mode.")

def increase_powermode(miner: int):
    global last_change_time
    global current_miner_mode

    current_time = time.time()
    if current_time - last_change_time < lock_duration:
        print_mqtt(f"Power Mode increase blocked. Miner changed powermode {current_time - last_change_time} seconds ago. (minimum time between power mode changes: {lock_duration})")
        return

    summary = miner1_data["Miner1Summary"]
    if summary == "": return

    mode = summary["SUMMARY"][0]["Power Mode"]
    print_mqtt(f"miner running in mode: {mode}")

    if mode == "Low":
        print_mqtt("Going from Low to Normal mode.")
        #ctrl.set_miner_power_mode(miner, 1)
        set_custom_powermode(miner, Low_Mode)
        last_change_time = time.time()
        current_miner_mode = RY3TPowerMode.NORMAL
    elif mode == "Normal":
        print_mqtt("Going from Normal to High mode.")
        #ctrl.set_miner_power_mode(miner, 2)
        set_custom_powermode(miner, High_Mode)
        last_change_time = time.time()
        current_miner_mode = RY3TPowerMode.HIGH
    elif mode == "High":
        print_mqtt("Already at High Mode.")
    else:
        print_mqtt("Invalid mode.")

def decrease_powermode(miner: int):
    global last_change_time
    global current_miner_mode

    current_time = time.time()
    if current_time - last_change_time < lock_duration:
        print_mqtt(f"Power Mode decrease blocked. Miner changed powermode {current_time - last_change_time} seconds ago. (minimum time between power mode changes: {lock_duration})")
        return

    summary = miner1_data["Miner1Summary"]
    if(summary == ""): return

    mode = summary["SUMMARY"][0]["Power Mode"]
    print_mqtt(f"miner running in mode: {mode}")

    if mode == "Low":
        print_mqtt("Already at Low Mode.")
    elif mode == "Normal":
        print_mqtt("Going from Normal to Low mode.")
        #ctrl.set_miner_power_mode(miner, 0)
        set_custom_powermode(miner, Low_Mode)
        last_change_time = time.time()
        current_miner_mode = RY3TPowerMode.LOW
    elif mode == "High":
        print_mqtt("Going from High to Normal mode.")
        #ctrl.set_miner_power_mode(miner, 1)
        set_custom_powermode(miner, Normal_Mode)
        last_change_time = time.time()
        current_miner_mode = RY3TPowerMode.NORMAL
    else:
        print_mqtt("Invalid mode.")

#endregion

#region: MQTT Setup

try:
    mqttclient.connect(broker_address, keepalive=60)
except Exception as e:
    print(f"An exception occurred: {e}")


mqttclient.loop_start()

mqttclient.publish(topic_req_gateway + "/values", "values", qos=1)

def weather():
    global outside_temperature
    time.sleep(60 * 60) # 60 minutes
    temperature, timestamp = weather_client.get_temperature()
    if temperature is not None:
        print_mqtt(f"outside temp: {temperature}")
        outside_temperature = temperature

def lifesign():
    lifesign = 0
    while True:
        status()
        lifesign += 1
        if(lifesign > 10): lifesign = 0
        mqttclient.publish(topic_status_lifesign + "/automation", lifesign, qos=0)
        time.sleep(2)

def status():
    status = {
        "status": current_state.name,
        "mode": current_mode.name,
        "power_mode": current_power_mode.name if (current_mode != RY3TMode.Levels and current_mode != RY3TMode.PV) else "null",
        "power_level": current_power_level + 1 if current_mode == RY3TMode.Levels else "null",
        "power_limit": current_PV_limit if current_mode == RY3TMode.PV else "null",
    }
    mqttclient.publish(topic_status_automation, json.dumps(status), qos=0)

def print_mqtt(message):
    if not isinstance(message, str):
        message = str(message)

    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    print(f"{timestamp} - {message}")
    mqttclient.publish(topic_log_automation, message, qos=0)

#region: Main Function

def main():

    set_heating_mode(config["mode"])

    global outside_temperature, timestamp
    outside_temperature, timestamp = weather_client.get_temperature()

    control_thread = threading.Thread(target=control_loop, args=())
    control_thread.daemon = True  # Set the thread as a daemon so it will close when the main program exits
    control_thread.start()

    primary_control_thread = threading.Thread(target=primary_control_loop, args=())
    primary_control_thread.daemon = True  # Set the thread as a daemon so it will close when the main program exits
    primary_control_thread.start()

    secondary_control_thread = threading.Thread(target=secondary_control_loop, args=())
    secondary_control_thread.daemon = True  # Set the thread as a daemon so it will close when the main program exits
    secondary_control_thread.start()

    weather_thread = threading.Thread(target=weather, args=())
    weather_thread.daemon = True  # Set the thread as a daemon so it will close when the main program exits
    weather_thread.start()

    lifesign_thread = threading.Thread(target=lifesign, args=())
    lifesign_thread.daemon = True  # Set the thread as a daemon so it will close when the main program exits
    lifesign_thread.start()
    
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print_mqtt("Program interrupted by user. Exiting.")

if __name__ == "__main__":
    main()