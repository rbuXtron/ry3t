import paho.mqtt.client as mqtt_client
import json

class RY3TControl:

    def on_connect(self, client, userdata, flags, reason_code):
        self.print_mqtt(f"Connected with result code {reason_code}")
        #client.subscribe(f"{self.topic_data_miner}/#")

    def on_publish(self, client, userdata, mid):
        print("Message published.")

    def on_message(self, client, userdata, message):
        print(f"Received message '{str(message.payload.decode())}' on topic '{message.topic}'")
        
        # Iterate over the topic_handlers to find a matching topic pattern
        for topic_pattern, handler in self.topic_handlers.items():
            if topic_pattern in message.topic:
                # If a handler function exists for the topic pattern, call it with the payload
                handler(message.payload)
                break
        else:
            print("No handler for topic", message.topic)

    def __init__(self, broker_address, isRouter):
        self.broker_address = broker_address
        self.topic_ctrl_miner = "ctrl/miner"
        self.topic_ctrl_core = "ctrl/core"
        self.topic_data_miner = "data/miner"
        self.topic_log_automationctrl = "log/automationctrl"
        if(isRouter): self.mqttclient = mqtt_client.Client("control2") 
        else: self.mqttclient = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, "control")
        
        self.mqttclient.on_connect = self.on_connect
        self.mqttclient.on_publish = self.on_publish
        self.mqttclient.on_message = self.on_message
        self.mqttclient.enable_logger()

        self.topic_handlers = {
            "data/miner/1": self.handle_miner1_data,
            "data/miner/2": self.handle_miner2_data
        }

        try:
            self.mqttclient.connect(self.broker_address, keepalive=60)
        except Exception as e:
            print(f"An exception occurred: {e}")

        self.mqttclient.loop_start()

        self.print_mqtt("initialized")

    power_percentage_miner1 = 0
    power_percentage_miner2 = 0

    power_mode_miner1 = 1
    power_mode_miner2 = 1


    def lifesign(self):
        return
    

    def start(self, miner1Percent = 50, miner2Percent = 50, miner1PowerMode = 1, miner2PowerMode = 1, pump1Percent = 100, pump2Percent = 100, valve1Percent = 100, valve2Percent = 100):
        """
        Start the RY3T Heating System

        Parameters:
        minerPercent (float): miner power percent. 0 to 100
        minerPowerMode (int): miner power mode. 0: low, 1: normal, 2: high
        pumpPercent (float): pump percent. 0 to 100
        valvePercent (float): valve percent. 0 to 100
        """
        self.print_mqtt("start")

        # enable pumps
        self.set_pumps_enable(True)
        # set pump speeds
        self.print_mqtt("setting pumps")
        self.print_mqtt(f"pump1: {pump1Percent}")
        self.print_mqtt(f"pump2: {pump2Percent}")
        self.set_pump_percent(1, pump1Percent)
        self.set_pump_percent(2, pump2Percent)
        # enable miners
        self.set_miner_power(1, True)
        self.set_miner_power(2, True)
        # set miner power percentage
        self.print_mqtt("setting miners")
        self.print_mqtt(f"miner1: {miner1PowerMode}")
        self.print_mqtt(f"miner2: {miner2PowerMode}")
        self.set_miner_power_mode(1, miner1PowerMode)
        self.set_miner_power_mode(2, miner2PowerMode)
        # set miner power mode
        self.print_mqtt("setting miner mode")
        self.print_mqtt(f"miner1: {miner1Percent}")
        self.print_mqtt(f"miner2: {miner2Percent}")
        self.set_miner_power_percent(1, miner1Percent)
        self.set_miner_power_percent(2, miner2Percent)
        # set valves??
        self.print_mqtt("setting valves")
        self.print_mqtt(f"valve1: {valve1Percent}")
        self.print_mqtt(f"valve2: {valve2Percent}")
        self.set_valve_percent(1, valve1Percent)
        self.set_valve_percent(2, valve2Percent)

    def set_miner_power_percent(self, miner, percent):
        """
        Set the miner power percentage / Target Frequency -100 to 100

        Parameters:
        miner (int): miner number. 1 or 2
        percent (float): power percent. 0 to 100, sets the miner to -100 to 0
        """
        percent = float(percent.decode('utf-8')) if isinstance(percent, bytes) else float(percent)

        global power_percentage_miner1
        global power_percentage_miner2
        self.print_mqtt(f"Setting Miner {miner} power percentage to: {percent}")

        percent = percent - 100
        if(miner == 1):
            power_percentage_miner1 = percent + 100
        elif(miner == 2):
            power_percentage_miner2 = percent + 100


        self.print_mqtt(f"sending comm: {percent}")
        self.mqttclient.publish(self.topic_ctrl_miner + "/" + str(miner), json.dumps({"cmd": "set_target_freq", "percent": str(percent) , "token": ""}), qos=1)
        self.print_mqtt(f"Published message to topic {self.topic_ctrl_miner}/{miner}")

        return self.topic_ctrl_miner + "/" + str(miner), json.dumps({"cmd": "set_target_freq", "percent": str(percent) , "token": ""})


    def set_miner_power_mode(self, miner, mode):
        """
        Set the miner power mode

        Parameters:
        miner (int): miner number. 1 or 2
        percent (int): power mode. 0: low, 1: normal, 2: high
        """
        mode = int(mode.decode('utf-8')) if isinstance(mode, bytes) else int(mode)

        global power_mode_miner1
        global power_mode_miner2
        cmd = ""
        if(mode == 0): cmd = "set_low_power"
        elif(mode == 1): cmd = "set_normal_power"
        elif(mode == 2): cmd = "set_high_power"
        self.print_mqtt(f"Setting Miner {miner} power percentage to: {cmd}")
        
        if(miner == 1):
            power_mode_miner1 = mode
        elif(miner == 2):
            power_mode_miner2 = mode

        self.print_mqtt(f"sending comm: {cmd}")
        self.mqttclient.publish(self.topic_ctrl_miner + "/" + str(miner), json.dumps({"token": "", "cmd": cmd}), qos=1)
        self.print_mqtt(f"Published message to topic {self.topic_ctrl_miner}/{miner}")

        return self.topic_ctrl_miner + "/" + str(miner), json.dumps({"token": "", "cmd": cmd})


    def set_miner_power(self, miner, state):
        """
        Set the miner power state - On or Off

        Parameters:
        miner (int): miner number. 1 or 2
        state (bool): miner power state. True for On, False for Off
        """

        self.print_mqtt(f"Setting Miner {miner} power state to: {state}")
        cmd = "power_on" if state else "power_off" 
        self.mqttclient.publish(self.topic_ctrl_miner + "/" + str(miner), json.dumps({"token": "", "cmd": cmd}), qos=1)
        self.print_mqtt(f"Published message to topic {self.topic_ctrl_miner}/{miner}")
        #if state:
        #    self.mqttclient.publish(self.topic_ctrl_core + "/miner" + str(miner), "True", qos=1) 
        self.print_mqtt(f"Published message to topic {self.topic_ctrl_core}/miner{miner}")

        return self.topic_ctrl_miner + "/" + str(miner), json.dumps({"token": "", "cmd": cmd})
    
    def set_miner_power_limit(self, limit):
        """
        Set the miner power limit - 0 to 14kW (set up to 99999 for unlimited)
        In Two-Miner setup this is the total power limit for both miners combined. 
        
        Parameters:
        limit (int): power limit in kW. 0 to 14kW (set up to 99999 for unlimited)        
        """

        self.print_mqtt(f"Setting Miner power limit to: {limit}")
        self.mqttclient.publish(self.topic_ctrl_miner + "/1", json.dumps({"cmd": "adjust_power_limit", "power_limit": limit, "token": ""}), qos=1)
        self.print_mqtt(f"Published message to topic {self.topic_ctrl_miner}/1")

        return self.topic_ctrl_miner + "/1", json.dumps({"cmd": "adjust_power_limit", "limit": limit, "token": ""})

    def set_pump_percent(self, pump, percent):
        """
        Set the pump percentage 0 to 100

        Parameters:
        pump (int): pump number. 1 or 2
        percent (float): power percent. 0 to 100
        """
        self.print_mqtt(f"Setting Pump {pump} percentage to: {percent}")
        self.print_mqtt(f"sending comm: {percent}")
        self.mqttclient.publish(self.topic_ctrl_core + "/pump" + str(pump), str(percent), qos=1)
        self.print_mqtt(f"Published message to topic {self.topic_ctrl_core}/pump{pump}")

        return self.topic_ctrl_core + "/pump" + str(pump), str(percent)


    def set_pumps_enable(self, state):
        """
        Set the pumps power state - On or Off

        Parameters:
        state (bool): pumps power state. True for On, False for Off
        """
        self.print_mqtt(f"Setting Pumps power state to: {state}")
        state_string = "True" if state else "False" 
        self.mqttclient.publish(self.topic_ctrl_core + "/pumpEnable", state_string, qos=1)
        self.print_mqtt(f"Published message {state_string} to topic {self.topic_ctrl_core}/pumpEnable")

        return self.topic_ctrl_core + "/pumpEnable", state_string


    def set_valve_percent(self, valve, percent):
        """
        Set the valve percentage 0 to 100

        Parameters:
        pump (int): valve number. 1 or 2
        percent (float): power percent. 0 to 100
        """
        self.print_mqtt(f"Setting Valve {valve} percentage to: {percent}")
        self.print_mqtt(f"sending comm: {percent}")
        self.mqttclient.publish(self.topic_ctrl_core + "/valve" + str(valve), str(percent), qos=1)
        self.print_mqtt(f"Published message to topic {self.topic_ctrl_core}/valve{valve}")

        return self.topic_ctrl_core + "/valve" + str(valve),str(percent)


    def handle_miner1_data(self, payload):
        print("miner1 data")


    def handle_miner2_data(self, payload):
        print("miner2 data")

    def print_mqtt(self, message):
        if not isinstance(message, str):
            message = str(message)
        print(message)
        self.mqttclient.publish(self.topic_log_automationctrl, message, qos=0)