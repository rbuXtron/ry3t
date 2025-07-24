import time
import json
import os
from collections import deque
from typing import Dict, Any
import paho.mqtt.client as mqtt_client
from datetime import datetime

# Math utilities (Pure Python)
class MathUtils:
    @staticmethod
    def clip(value, min_val, max_val):
        return max(min_val, min(max_val, value))
    
    @staticmethod
    def sign(x):
        return 1 if x > 0 else -1 if x < 0 else 0
    
    @staticmethod
    def mean(values):
        return sum(values) / len(values) if values else 0


class ConfigManager:
    """Verwaltet die Konfiguration aus JSON-Datei"""
    
    def __init__(self, config_file: str = "pid_config.json"):
        self.config_file = config_file
        self.config = self.load_config()
        self.last_modified = 0
        
    def load_config(self) -> Dict[str, Any]:
        """Lädt Konfiguration aus JSON-Datei"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                self.last_modified = os.path.getmtime(self.config_file)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Konfiguration geladen")
                return config
            except Exception as e:
                print(f"Fehler beim Laden der Konfiguration: {e}")
                return self.get_default_config()
        else:
            print(f"Erstelle Standard-Konfiguration...")
            config = self.get_default_config()
            self.save_config(config)
            return config
    
    def check_and_reload(self):
        """Prüft ob Config geändert wurde und lädt sie neu"""
        if os.path.exists(self.config_file):
            current_modified = os.path.getmtime(self.config_file)
            if current_modified > self.last_modified:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Config-Änderung erkannt, lade neu...")
                self.config = self.load_config()
                return True
        return False
    
    def save_config(self, config: Dict[str, Any] = None):
        """Speichert Konfiguration in JSON-Datei"""
        if config is None:
            config = self.config
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"Fehler beim Speichern: {e}")
    
    def get_default_config(self) -> Dict[str, Any]:
        return {
            "mqtt": {
                "broker": "localhost",
                "port": 1883,
                "topics": {
                    "temp_data": "data/onewire",
                    "pump_control": "ctrl/core",
                    "status": "status/pid",
                    "log": "log/pid"
                }
            },
            "control": {
                "sample_time": 10.0,
                "primary_setpoint": 63.0,
                "secondary_delta_t": 3.0,
                "hard_control_threshold": 2.0
            },
            "primary_pid": {
                "kp": 4.0,
                "ki": 0.03,
                "kd": 20.0,
                "trend_weight": 4.0,
                "min_output": 15.0,
                "max_output": 100.0
            },
            "secondary_pid": {
                "kp": 6.0,
                "ki": 0.15,
                "kd": 15.0,
                "trend_weight": 2.5,
                "min_output": 10.0,
                "max_output": 100.0
            },
            "sensor_mapping": {
                "primary_supply": "TempPrimaryWaterBeforeExchange",
                "primary_return": "TempPrimaryWaterAfterExchange",
                "secondary_supply": "TempSecondaryWaterAfterExchange",
                "secondary_return": "TempSecondaryWaterBeforeExchange"
            }
        }


class TrendPID:
    """Einfacher Trend-PID Controller"""
    
    def __init__(self, config: Dict[str, Any]):
        self.kp = config['kp']
        self.ki = config['ki']
        self.kd = config['kd'] * config['trend_weight']
        self.trend_weight = config['trend_weight']
        self.min_output = config['min_output']
        self.max_output = config['max_output']
        
        self.last_error = 0.0
        self.integral = 0.0
        self.last_time = time.time()
        self.error_history = deque(maxlen=10)
        
    def update(self, setpoint: float, measured: float) -> float:
        current_time = time.time()
        dt = current_time - self.last_time
        
        if dt < 1.0:  # Mindestens 1 Sekunde
            return self.last_output
            
        error = setpoint - measured
        self.error_history.append(error)
        
        # P-Anteil
        p_term = self.kp * error
        
        # I-Anteil
        self.integral += error * dt
        self.integral = MathUtils.clip(self.integral, -50, 50)
        i_term = self.ki * self.integral
        
        # D-Anteil mit Trend
        if len(self.error_history) >= 2:
            derivative = (error - self.last_error) / dt
        else:
            derivative = 0
        d_term = self.kd * derivative
        
        # Ausgabe
        output = p_term + i_term + d_term
        output = MathUtils.clip(output, self.min_output, self.max_output)
        
        self.last_error = error
        self.last_time = current_time
        self.last_output = output
        
        return output
    
    def update_config(self, config: Dict[str, Any]):
        """Aktualisiert PID-Parameter"""
        self.kp = config['kp']
        self.ki = config['ki']
        self.kd = config['kd'] * config['trend_weight']
        self.trend_weight = config['trend_weight']
        self.min_output = config['min_output']
        self.max_output = config['max_output']


class HeatExchangerController:
    """Hauptcontroller für Wärmetauscher"""
    
    def __init__(self, config_file: str = "pid_config.json"):
        self.config_manager = ConfigManager(config_file)
        self.config = self.config_manager.config
        
        # MQTT Setup
        self.mqtt_client = None
        self.setup_mqtt()
        
        # PID Controller
        self.primary_pid = TrendPID(self.config['primary_pid'])
        self.secondary_pid = TrendPID(self.config['secondary_pid'])
        
        # Aktuelle Werte
        self.temperatures = {
            'primary_supply': 20.0,
            'primary_return': 20.0,
            'secondary_supply': 20.0,
            'secondary_return': 20.0
        }
        
        self.last_update = time.time()
        self.running = True
        
    def setup_mqtt(self):
        """MQTT Verbindung aufbauen"""
        try:
            self.mqtt_client = mqtt_client.Client("pid_controller")
            self.mqtt_client.on_connect = self.on_connect
            self.mqtt_client.on_message = self.on_message
            
            broker = self.config['mqtt']['broker']
            port = self.config['mqtt']['port']
            
            self.mqtt_client.connect(broker, port, 60)
            self.mqtt_client.loop_start()
            print(f"MQTT verbunden mit {broker}:{port}")
            
        except Exception as e:
            print(f"MQTT Fehler: {e}")
    
    def on_connect(self, client, userdata, flags, rc):
        """MQTT Connect Callback"""
        if rc == 0:
            print("MQTT verbunden")
            # Temperatur-Topic abonnieren
            topic = self.config['mqtt']['topics']['temp_data']
            client.subscribe(topic)
            print(f"Abonniert: {topic}")
        else:
            print(f"MQTT Verbindungsfehler: {rc}")
    
    def on_message(self, client, userdata, msg):
        """MQTT Message Callback"""
        try:
            if msg.topic == self.config['mqtt']['topics']['temp_data']:
                data = json.loads(msg.payload.decode())
                self.update_temperatures(data)
        except Exception as e:
            print(f"Fehler bei Nachricht: {e}")
    
    def update_temperatures(self, data: Dict[str, Any]):
        """Aktualisiert Temperaturen aus MQTT"""
        mapping = self.config['sensor_mapping']
        
        for key, sensor_name in mapping.items():
            if sensor_name in data:
                self.temperatures[key] = float(data[sensor_name])
    
    def control_loop(self):
        """Haupt-Regelschleife"""
        while self.running:
            try:
                # Config auf Änderungen prüfen
                if self.config_manager.check_and_reload():
                    self.config = self.config_manager.config
                    self.primary_pid.update_config(self.config['primary_pid'])
                    self.secondary_pid.update_config(self.config['secondary_pid'])
                
                # Regelung nur alle sample_time Sekunden
                current_time = time.time()
                if current_time - self.last_update < self.config['control']['sample_time']:
                    time.sleep(0.5)
                    continue
                
                # Primärkreis regeln (auf Solltemperatur)
                primary_pump = self.primary_pid.update(
                    self.config['control']['primary_setpoint'],
                    self.temperatures['primary_supply']
                )
                
                # Sekundärkreis regeln (auf Delta-T)
                current_delta_t = (self.temperatures['secondary_return'] - 
                                 self.temperatures['primary_return'])
                
                secondary_pump = self.secondary_pid.update(
                    self.config['control']['secondary_delta_t'],
                    current_delta_t
                )
                
                # Harte Regelung bei Überschreitung
                temp_error = self.temperatures['primary_supply'] - self.config['control']['primary_setpoint']
                if temp_error > self.config['control']['hard_control_threshold']:
                    primary_pump = min(100, primary_pump + temp_error * 10)
                    print(f"⚠️  HARTE REGELUNG: +{temp_error:.1f}°C über Sollwert!")
                
                # Pumpen ansteuern
                self.set_pumps(primary_pump, secondary_pump)
                
                # Status ausgeben
                self.log_status(primary_pump, secondary_pump, current_delta_t)
                
                self.last_update = current_time
                
            except Exception as e:
                print(f"Fehler in Regelschleife: {e}")
            
            time.sleep(0.1)
    
    def set_pumps(self, pump1: float, pump2: float):
        """Sendet Pumpenwerte über MQTT"""
        if self.mqtt_client:
            base_topic = self.config['mqtt']['topics']['pump_control']
            
            self.mqtt_client.publish(f"{base_topic}/pump1", str(int(pump1)), qos=1)
            self.mqtt_client.publish(f"{base_topic}/pump2", str(int(pump2)), qos=1)
    
    def log_status(self, pump1: float, pump2: float, delta_t: float):
        """Status ausgeben"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] P:{self.temperatures['primary_supply']:.1f}°C "
              f"(Soll:{self.config['control']['primary_setpoint']:.1f}) "
              f"Pumpe1:{pump1:.0f}% | "
              f"ΔT:{delta_t:.1f}°C "
              f"(Soll:{self.config['control']['secondary_delta_t']:.1f}) "
              f"Pumpe2:{pump2:.0f}%")
        
        # Status über MQTT
        if self.mqtt_client:
            status = {
                "timestamp": timestamp,
                "primary_temp": self.temperatures['primary_supply'],
                "primary_pump": pump1,
                "secondary_delta_t": delta_t,
                "secondary_pump": pump2
            }
            self.mqtt_client.publish(
                self.config['mqtt']['topics']['status'],
                json.dumps(status),
                qos=0
            )
    
    def start(self):
        """Controller starten"""
        print("PID-Controller gestartet")
        print(f"Config-Datei: {self.config_manager.config_file}")
        print("Änderungen an der Config werden automatisch übernommen")
        
        # Pumpen aktivieren
        if self.mqtt_client:
            base_topic = self.config['mqtt']['topics']['pump_control']
            self.mqtt_client.publish(f"{base_topic}/pumpEnable", "True", qos=1)
        
        self.control_loop()
    
    def stop(self):
        """Controller stoppen"""
        self.running = False
        
        # Pumpen deaktivieren
        if self.mqtt_client:
            self.set_pumps(0, 0)
            base_topic = self.config['mqtt']['topics']['pump_control']
            self.mqtt_client.publish(f"{base_topic}/pumpEnable", "False", qos=1)
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        
        print("PID-Controller gestoppt")


if __name__ == "__main__":
    controller = HeatExchangerController("pid_config.json")
    
    try:
        controller.start()
    except KeyboardInterrupt:
        print("\nBeende Controller...")
        controller.stop()