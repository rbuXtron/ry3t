import time
import json
import os
import threading
from collections import deque
from typing import Dict, Any, Optional
import paho.mqtt.client as mqtt_client
from datetime import datetime
from enum import Enum, auto

debug_mode = False  # Set to True for detailed debug output

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
    """Verwaltet die Konfiguration aus JSON-Datei mit Kompatibilität zum bestehenden System"""
    
    def __init__(self, config_file: str = "pid_config.json", legacy_config_path: str = None, legacy_values_path: str = None):
        self.config_file = config_file
        self.legacy_config_path = legacy_config_path
        self.legacy_values_path = legacy_values_path
        self.config = self.load_config()
        self.last_modified = 0
        
    def load_config(self) -> Dict[str, Any]:
        """Lädt Konfiguration - zuerst aus neuer JSON, dann aus Legacy-Dateien"""
        # Versuche neue Config zu laden
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                self.last_modified = os.path.getmtime(self.config_file)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Konfiguration geladen")
                return config
            except Exception as e:
                print(f"Fehler beim Laden der neuen Konfiguration: {e}")
        
        # Fallback: Legacy-Config laden und konvertieren
        if self.legacy_config_path and self.legacy_values_path:
            try:
                config = self.load_legacy_config()
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Legacy-Konfiguration konvertiert")
                # Neue Config speichern für zukünftige Nutzung
                self.save_config(config)
                return config
            except Exception as e:
                print(f"Fehler beim Laden der Legacy-Konfiguration: {e}")
        
        # Default Config erstellen
        print(f"Erstelle Standard-Konfiguration...")
        config = self.get_default_config()
        self.save_config(config)
        return config
    
    def load_legacy_config(self) -> Dict[str, Any]:
        """Konvertiert Legacy-Konfiguration in neues Format"""
        # Legacy config.json laden
        with open(self.legacy_config_path, 'r') as f:
            legacy_config = json.load(f)
        
        # Legacy config_values.json laden
        with open(self.legacy_values_path, 'r') as f:
            legacy_values = json.load(f)
        
        # In neues Format konvertieren
        return {
            "mqtt": {
                "broker": legacy_config.get("broker_address", "127.0.0.1"),
                "port": 1883,
                "client_id": "trend_pid_controller",
                "topics": {
                    "temp_data": "data/onewire",
                    "pump_control": "ctrl/core",
                    "pump_enable": "ctrl/automation/pumpEnable", 
                    "status": "status/pid",
                    "log": "log/pid",
                    "automation_status": "status/automation",
                    "heating_status": "status/heating"
                }
            },
            "control": {
                "sample_time": legacy_values.get("primary_control_loop", {}).get("probe_time", 25.0),
                "primary_setpoint": legacy_values.get("target_values", {}).get("temp_primary_before_exchange", 63.0),
                "secondary_delta_t": 3.0,
                "hard_control_threshold": 2.0,
                "startup_pump_speed": 50.0,
                "enable_on_startup": True
            },
            "primary_pid": {
                "kp": legacy_values.get("primary_control_loop", {}).get("P", 4.0),
                "ki": legacy_values.get("primary_control_loop", {}).get("I", 0.03),
                "kd": legacy_values.get("primary_control_loop", {}).get("D", 20.0),
                "trend_weight": 4.0,
                "min_output": legacy_values.get("pump_control", {}).get("min_percent", 20.0),
                "max_output": legacy_values.get("pump_control", {}).get("max_percent", 100.0)
            },
            "secondary_pid": {
                "kp": legacy_values.get("secondary_control_loop", {}).get("P", 6.0),
                "ki": legacy_values.get("secondary_control_loop", {}).get("I", 0.15),
                "kd": legacy_values.get("secondary_control_loop", {}).get("D", 15.0),
                "trend_weight": 2.5,
                "min_output": legacy_values.get("pump_control", {}).get("min_percent", 20.0),
                "max_output": legacy_values.get("pump_control", {}).get("max_percent", 100.0)
            },
            "sensor_mapping": {
                "primary_supply": "HotOil",
                "primary_return": "ColdOil", 
                "secondary_supply": "HotWater",
                "secondary_return": "ColdWater"
            },
            "integration": {
                "automation_mode": legacy_config.get("mode", "Speicher"),
                "manual_mode_topic": "ctrl/automation/manualmode",
                "heating_state_topic": "status/heating"
            },
            "mode_overrides": {
                "enabled": False,
                "force_manual_mode": None,
                "force_heating_active": None,
                "force_system_enabled": None,
                "force_pid_active": None,
                "ignore_mqtt_commands": False
            }
        }
    
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
                "broker": "127.0.0.1",
                "port": 1883,
                "client_id": "trend_pid_controller",
                "topics": {
                    "temp_data": "data/onewire",
                    "pump_control": "ctrl/core",
                    "pump_enable": "ctrl/automation/pumpEnable",
                    "status": "status/pid", 
                    "log": "log/pid",
                    "automation_status": "status/automation",
                    "heating_status": "status/heating"
                }
            },
            "control": {
                "sample_time": 25.0,
                "primary_setpoint": 63.0,
                "secondary_delta_t": 3.0,
                "hard_control_threshold": 2.0,
                "startup_pump_speed": 50.0,
                "enable_on_startup": True
            },
            "primary_pid": {
                "kp": 4.0,
                "ki": 0.03,
                "kd": 20.0,
                "trend_weight": 4.0,
                "min_output": 20.0,
                "max_output": 100.0
            },
            "secondary_pid": {
                "kp": 6.0,
                "ki": 0.15,
                "kd": 15.0,
                "trend_weight": 2.5,
                "min_output": 20.0,
                "max_output": 100.0
            },
            "sensor_mapping": {
                "primary_supply": "HotOil",
                "primary_return": "ColdOil",
                "secondary_supply": "HotWater",
                "secondary_return": "ColdWater"
            },
            "integration": {
                "automation_mode": "Speicher",
                "manual_mode_topic": "ctrl/automation/manualmode",
                "heating_state_topic": "status/heating"
            },
            "mode_overrides": {
                "enabled": False,
                "force_manual_mode": False,
                "force_heating_active": False,
                "force_system_enabled": False,
                "force_pid_active": False,
                "ignore_mqtt_commands": False,
                "description": {
                    "enabled": "Aktiviert Mode-Overrides (true/false)",
                    "force_manual_mode": "Erzwingt manuellen Modus (true/false/null für MQTT)",
                    "force_heating_active": "Erzwingt Heizung aktiv (true/false/null für MQTT)",
                    "force_system_enabled": "Erzwingt System aktiviert (true/false/null für MQTT)",
                    "force_pid_active": "Erzwingt PID-Kontrolle aktiv (true/false/null für MQTT)",
                    "ignore_mqtt_commands": "Ignoriert alle MQTT-Befehle komplett (true/false)"
                }
            }
        }


class TrendPID:
    """Einfacher Trend-PID Controller mit Trend-Ausgabe"""
    
    def __init__(self, config: Dict[str, Any], start_output: float = 50.0, reverse_acting: bool = False):
        self.kp = config['kp']
        self.ki = config['ki']
        self.kd = config['kd'] * config['trend_weight']
        self.trend_weight = config['trend_weight']
        self.min_output = config['min_output']
        self.max_output = config['max_output']
        self.reverse_acting = reverse_acting
        
        self.last_error = 0.0
        self.integral = 0.0
        self.last_time = time.time()
        self.error_history = deque(maxlen=10)
        self.temp_history = deque(maxlen=10)
        self.last_output = start_output
        self.first_run = True
        self.last_derivative = 0.0
        self.trend = 0.0
        
    def update(self, setpoint: float, measured: float) -> float:
        current_time = time.time()
        dt = current_time - self.last_time
        
        self.temp_history.append((current_time, measured))
        
        if self.first_run:
            self.first_run = False
            self.last_time = current_time
            return self.last_output
            
        if dt < 1.0:
            return self.last_output
        
        self.trend = self._calculate_trend()
        
        if self.reverse_acting:
            error = measured - setpoint
        else:
            error = setpoint - measured
            
        self.error_history.append(error)
        
        # P-Anteil
        p_term = self.kp * error
        
        # I-Anteil
        self.integral += error * dt
        if self.reverse_acting:
            if measured > setpoint and self.integral < 0:
                self.integral *= 0.9
        self.integral = MathUtils.clip(self.integral, -30, 30)
        i_term = self.ki * self.integral
        
        # D-Anteil
        if len(self.error_history) >= 2:
            derivative = (error - self.last_error) / dt
            self.last_derivative = derivative
        else:
            derivative = 0
        d_term = self.kd * derivative
        
        output = self.last_output + p_term + i_term + d_term
        
        # Sanfte Änderung
        max_change = 5.0
        output = MathUtils.clip(output, 
                               self.last_output - max_change,
                               self.last_output + max_change)
        
        output = MathUtils.clip(output, self.min_output, self.max_output)
        
        self.last_error = error
        self.last_time = current_time
        self.last_output = output
        
        return output
    
    def _calculate_trend(self):
        """Berechnet Temperatur-Trend in °C/min"""
        if len(self.temp_history) < 3:
            return 0.0
            
        times = [t[0] for t in self.temp_history]
        temps = [t[1] for t in self.temp_history]
        
        n = len(times)
        if n < 2:
            return 0.0
            
        t0 = times[0]
        x = [(t - t0) / 60.0 for t in times]
        y = temps
        
        x_mean = sum(x) / n
        y_mean = sum(y) / n
        
        num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
        den = sum((xi - x_mean) ** 2 for xi in x)
        
        if abs(den) < 1e-10:
            return 0.0
            
        slope = num / den
        return slope
    
    def get_status(self):
        """Gibt aktuellen Status zurück"""
        return {
            'p_term': self.kp * self.last_error,
            'i_term': self.ki * self.integral,
            'd_term': self.kd * self.last_derivative,
            'integral': self.integral,
            'trend': self.trend,
            'output': self.last_output
        }
    
    def update_config(self, config: Dict[str, Any]):
        """Aktualisiert PID-Parameter"""
        self.kp = config['kp']
        self.ki = config['ki']
        self.kd = config['kd'] * config['trend_weight']
        self.trend_weight = config['trend_weight']
        self.min_output = config['min_output']
        self.max_output = config['max_output']


class IntegratedHeatExchangerController:
    """Hauptcontroller mit vollständiger Integration in das bestehende System"""
    
    def __init__(self, config_file: str = "pid_config.json", legacy_config_path: str = None, legacy_values_path: str = None):
        self.config_manager = ConfigManager(config_file, legacy_config_path, legacy_values_path)
        self.config = self.config_manager.config
        
        # System-Status
        self.manual_mode = True
        self.heating_active = False
        self.system_enabled = False
        self.pid_control_active = False
        
        # MQTT Setup mit bestehender Topic-Struktur
        self.mqtt_client = None
        self.setup_mqtt()
        
        # Thread-sicherer Zugriff
        self.data_lock = threading.RLock()
        
        # Startwert für Pumpen
        startup_speed = self.config['control'].get('startup_pump_speed', 50.0)
        
        # PID Controller mit Startwert initialisieren
        self.primary_pid = TrendPID(self.config['primary_pid'], startup_speed, reverse_acting=False)
        self.secondary_pid = TrendPID(self.config['secondary_pid'], startup_speed, reverse_acting=False)
        
        # Aktuelle Werte
        self.temperatures = {
            'primary_supply': 30.0,
            'primary_return': 22.0,
            'secondary_supply': 28.0,
            'secondary_return': 20.0
        }
        
        self.last_update = time.time()
        self.running = True
        self.startup_complete = False
        
        # Topic-Handler für bestehende MQTT-Struktur
        self.topic_handlers = self.setup_topic_handlers()
        
    def setup_mqtt(self):
        """MQTT Verbindung mit vollständiger Topic-Integration"""
        try:
            client_id = self.config['mqtt'].get('client_id', 'trend_pid_controller')
            self.mqtt_client = mqtt_client.Client(client_id)
            self.mqtt_client.on_connect = self.on_connect
            self.mqtt_client.on_message = self.on_message
            
            broker = self.config['mqtt']['broker']
            port = self.config['mqtt']['port']
            
            self.mqtt_client.connect(broker, port, 60)
            self.mqtt_client.loop_start()
            print(f"MQTT verbunden mit {broker}:{port}")
            
        except Exception as e:
            print(f"MQTT Fehler: {e}")
    
    def setup_topic_handlers(self):
        """Setup für alle MQTT-Topic-Handler"""
        return {
            "data/onewire": self.handle_onewire_data,
            "ctrl/automation/manualmode": self.handle_manual_mode,
            "status/heating": self.handle_heating_status,
            "ctrl/automation/pumpEnable": self.handle_pump_enable,
            "status/automation": self.handle_automation_status
        }
    
    def on_connect(self, client, userdata, flags, rc):
        """MQTT Connect Callback mit allen relevanten Topics"""
        if rc == 0:
            print("MQTT verbunden")
            # Alle relevanten Topics abonnieren
            topics = [
                "data/onewire",
                "ctrl/automation/#",
                "status/heating",
                "status/automation"
            ]
            
            for topic in topics:
                client.subscribe(topic)
                print(f"Abonniert: {topic}")
        else:
            print(f"MQTT Verbindungsfehler: {rc}")
    
    def on_message(self, client, userdata, msg):
        """MQTT Message Callback mit Topic-Routing"""
        try:
            topic = msg.topic
            payload = msg.payload
            
            # Handler für Topic finden
            for topic_pattern, handler in self.topic_handlers.items():
                if topic_pattern in topic:
                    handler(payload)
                    break
            else:
                # Fallback für unbekannte Topics
                if topic == self.config['mqtt']['topics']['temp_data']:
                    self.handle_onewire_data(payload)
                    
        except Exception as e:
            print(f"Fehler bei MQTT-Nachricht {msg.topic}: {e}")
    
    def handle_onewire_data(self, payload):
        """Verarbeitet Temperaturdaten vom OneWire-System"""
        try:
            data = json.loads(payload.decode())
            
            with self.data_lock:
                mapping = self.config['sensor_mapping']
                temp_updates = []
                
                for key, sensor_name in mapping.items():
                    if sensor_name in data and data[sensor_name] is not None:
                        old_temp = self.temperatures.get(key, 0)
                        new_temp = float(data[sensor_name])
                        self.temperatures[key] = new_temp
                        temp_updates.append(f"{key}={new_temp:.1f}°C")
                        
                        # Debug-Output für große Änderungen
                        if abs(new_temp - old_temp) > 2.0:
                            if debug_mode:
                                print(f"[TEMP] {key}: {old_temp:.1f}°C → {new_temp:.1f}°C (große Änderung)")
                            
                
                # Zeige alle Updates in einer Zeile (nicht bei jedem kleinen Update)
                if temp_updates and len(temp_updates) == len(mapping):
                    if debug_mode:
                        print(f"[TEMP] Alle Sensoren aktualisiert: {', '.join(temp_updates)}")
                        #print(f"[TEMP] {', '.join(temp_updates)}")
                            
        except Exception as e:
            print(f"Fehler bei OneWire-Daten: {e}")
    
    def handle_manual_mode(self, payload):
        """Verarbeitet Manual-Mode-Befehle"""
        # Prüfe Config-Override
        if self.config.get('mode_overrides', {}).get('ignore_mqtt_commands', False):
            return
            
        override_value = self.config.get('mode_overrides', {}).get('force_manual_mode')
        if self.config.get('mode_overrides', {}).get('enabled', False) and override_value is not None:
            return
            
        try:
            payload_str = payload.decode('utf-8')
            old_mode = self.manual_mode
            self.manual_mode = payload_str.lower() == "true"
            
            if old_mode != self.manual_mode:
                print(f"[MODE] Manual Mode: {self.manual_mode}")
                
                if self.manual_mode:
                    self.disable_pid_control()
            
        except Exception as e:
            print(f"Fehler bei Manual-Mode: {e}")
    
    def handle_heating_status(self, payload):
        """Verarbeitet Heating-Status vom Hauptsystem"""
        # Prüfe Config-Override
        if self.config.get('mode_overrides', {}).get('ignore_mqtt_commands', False):
           #print("[MODE] Ignoriere Heating-Status wegen Config-Override")
            return
            
        override_value = self.config.get('mode_overrides', {}).get('force_heating_active')
        if self.config.get('mode_overrides', {}).get('enabled', False) and override_value is not None:
            #print("[MODE] Ignoriere Heating-Status wegen Config-Override")
            return
            
        try:
            print(f"[MQTT] Heating-Status empfangen: {self.heating_active}")
            payload_str = payload.decode('utf-8')
            old_status = self.heating_active
            self.heating_active = payload_str.lower() == "true"
            
            if old_status != self.heating_active:
                print(f"[STATUS] Heating Active: {self.heating_active}")
                
                if self.heating_active and not self.manual_mode:
                    self.enable_pid_control()
                else:
                    self.disable_pid_control()
                    
        except Exception as e:
            print(f"Fehler bei Heating-Status: {e}")
    
    def handle_pump_enable(self, payload):
        """Verarbeitet Pump-Enable-Befehle"""
        # Prüfe Config-Override
        if self.config.get('mode_overrides', {}).get('ignore_mqtt_commands', False):
            #print("[MODE] Ignoriere Pump-Enable wegen Config-Override")
            return
            
        override_value = self.config.get('mode_overrides', {}).get('force_system_enabled')
        if self.config.get('mode_overrides', {}).get('enabled', False) and override_value is not None:
            print("[MODE] Ignoriere Pump-Enable wegen Config-Override")
            return
            
        try:
            payload_str = payload.decode('utf-8')
            self.system_enabled = payload_str.lower() == "true"
            print(f"[PUMPS] System Enabled: {self.system_enabled}")
            
        except Exception as e:
            print(f"Fehler bei Pump-Enable: {e}")
    
    def handle_automation_status(self, payload):
        """Verarbeitet Automation-Status"""
        try:
            data = json.loads(payload.decode())
            if 'mode' in data:
                current_mode = data['mode']
                #print(f"[AUTO] Mode: {current_mode}")
                
        except Exception as e:
            print(f"Fehler bei Automation-Status: {e}")
    
    def apply_config_overrides(self):
        """Wendet Config-Overrides auf System-Status an"""
        overrides = self.config.get('mode_overrides', {})
        
        if not overrides.get('enabled', False):
            return
            
        # Manual Mode Override
        if overrides.get('force_manual_mode') is not None:
            self.manual_mode = overrides.get('force_manual_mode')
        
        # Heating Active Override
        if overrides.get('force_heating_active') is not None:
            self.heating_active = overrides.get('force_heating_active')
        
        # System Enabled Override
        if overrides.get('force_system_enabled') is not None:
            self.system_enabled = overrides.get('force_system_enabled')
        
        # PID Active Override
        if overrides.get('force_pid_active') is not None:
            self.pid_control_active = overrides.get('force_pid_active')
    
    def should_run_pid_control(self):
        """Prüft ob PID-Kontrolle laufen soll - mit Override-Schutz"""
        
        # Bei aktivierten Overrides die Override-Werte verwenden
        overrides = self.config.get('mode_overrides', {})
        if overrides.get('enabled', False):
            
            # Override-Werte direkt verwenden
            manual_mode = overrides.get('force_manual_mode', self.manual_mode)
            heating_active = overrides.get('force_heating_active', self.heating_active)
            system_enabled = overrides.get('force_system_enabled', self.system_enabled)
            pid_active = overrides.get('force_pid_active', self.pid_control_active)
            
            # Werte zur Sicherheit nochmal setzen
            if manual_mode is not None:
                self.manual_mode = manual_mode
            if heating_active is not None:
                self.heating_active = heating_active
            if system_enabled is not None:
                self.system_enabled = system_enabled
            if pid_active is not None:
                self.pid_control_active = pid_active
                
            return (pid_active and heating_active and system_enabled and not manual_mode)
        
        # Normale Logik ohne Overrides
        return (self.pid_control_active and 
                self.heating_active and 
                self.system_enabled and 
                not self.manual_mode)
    
    def enable_pid_control(self):
        """Aktiviert PID-Kontrolle"""
        # Prüfe Config-Override für PID
        override_value = self.config.get('mode_overrides', {}).get('force_pid_active')
        if self.config.get('mode_overrides', {}).get('enabled', False) and override_value is not None:
            if override_value != True:
                return
        
        if not self.pid_control_active and self.system_enabled and not self.manual_mode:
            self.pid_control_active = True
            print(f"[PID] PID-Kontrolle aktiviert")
            
            # Status an MQTT senden
            self.publish_pid_status({"active": True, "message": "PID control enabled"})
    
    def disable_pid_control(self):
        """Deaktiviert PID-Kontrolle"""
        # Prüfe Config-Override für PID
        override_value = self.config.get('mode_overrides', {}).get('force_pid_active')
        if self.config.get('mode_overrides', {}).get('enabled', False) and override_value is not None:
            if override_value != False:
                return
        
        if self.pid_control_active:
            self.pid_control_active = False
            print(f"[PID] PID-Kontrolle deaktiviert")
            
            # Status an MQTT senden
            self.publish_pid_status({"active": False, "message": "PID control disabled"})
    
    def startup_sequence(self):
        """Führt Startsequenz aus wenn konfiguriert"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] PID-Controller bereit")
        
        # Pumpen nur aktivieren wenn konfiguriert
        if self.config['control'].get('enable_on_startup', False):
            if self.mqtt_client:
                base_topic = self.config['mqtt']['topics']['pump_enable']
                self.mqtt_client.publish(base_topic, "True", qos=1)
                print(f"[STARTUP] System aktiviert")
                
        self.startup_complete = True
    
    def control_loop(self):
        """Haupt-Regelschleife mit Integration"""
        self.startup_sequence()
        
        while self.running:
            try:
                # Config auf Änderungen prüfen
                if self.config_manager.check_and_reload():
                    self.config = self.config_manager.config
                    self.primary_pid.update_config(self.config['primary_pid'])
                    self.secondary_pid.update_config(self.config['secondary_pid'])
                    print("[CONFIG] Konfiguration neu geladen")
                    # Config-Overrides anwenden
                    self.apply_config_overrides()
                
                # Config-Overrides anwenden falls aktiviert
                if self.config.get('mode_overrides', {}).get('enabled', False):
                    self.apply_config_overrides()
                
                # PID-Kontrolle nur wenn alle Bedingungen erfüllt sind
                if self.should_run_pid_control():
                    self.run_pid_control()
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Fehler in Hauptschleife: {e}")
                time.sleep(1)
    
    def run_pid_control(self):
        """Führt PID-Kontrolle alle 20-30s aus mit detaillierter Ausgabe"""
        current_time = time.time()
        if current_time - self.last_update < self.config['control']['sample_time']:
            return
        
        print(f"\n [{datetime.now().strftime('%H:%M:%S')}] ====== PID-REGELZYKLUS GESTARTET ======")
        
        with self.data_lock:
            # Vor der Regelung: Aktueller Zustand
            print(f" EINGANGSWERTE:")
            print(f"   Primär Vorlauf:    {self.temperatures['primary_supply']:6.1f}°C (Soll: {self.config['control']['primary_setpoint']:.1f}°C)")
            print(f"   Primär Rücklauf:   {self.temperatures['primary_return']:6.1f}°C")
            print(f"   Sekundär Vorlauf:  {self.temperatures['secondary_supply']:6.1f}°C")
            print(f"   Sekundär Rücklauf: {self.temperatures['secondary_return']:6.1f}°C")
            
            # Delta-T berechnen
            current_delta_t = (self.temperatures['primary_return'] - self.temperatures['secondary_return'])
            print(f"   Delta-T aktuell:   {current_delta_t:6.1f}°C (Soll: {self.config['control']['secondary_delta_t']:.1f}°C)")
            
            # Fehler berechnen
            temp_error = self.temperatures['primary_supply'] - self.config['control']['primary_setpoint']
            delta_t_error = current_delta_t - self.config['control']['secondary_delta_t']
            
            print(f"\n REGELABWEICHUNGEN:")
            print(f"   Temperatur-Fehler: {temp_error:+6.1f}°C")
            print(f"   Delta-T-Fehler:    {delta_t_error:+6.1f}°C")
            
            # PID-Regelung ausführen
            print(f"\n PID-BERECHNUNG:")
            
            # Primärkreis regeln
            print(f"   PRIMÄRKREIS (Pumpe 1):")
            old_primary_output = self.primary_pid.last_output
            primary_pump = self.primary_pid.update(
                self.config['control']['primary_setpoint'],
                self.temperatures['primary_supply']
            )
            primary_status = self.primary_pid.get_status()
            
            print(f"     P-Anteil: {primary_status['p_term']:+8.2f}")
            print(f"     I-Anteil: {primary_status['i_term']:+8.2f} (Integral: {primary_status['integral']:+6.2f})")
            print(f"     D-Anteil: {primary_status['d_term']:+8.2f}")
            print(f"     Trend:    {primary_status['trend']:+8.2f}°C/min")
            print(f"     Ausgabe:  {old_primary_output:.1f}% → {primary_pump:.1f}% (Δ{primary_pump-old_primary_output:+.1f}%)")
            
            # Sekundärkreis regeln
            print(f"   SEKUNDÄRKREIS (Pumpe 2):")
            old_secondary_output = self.secondary_pid.last_output
            secondary_pump = self.secondary_pid.update(
                self.config['control']['secondary_delta_t'],
                current_delta_t
            )
            secondary_status = self.secondary_pid.get_status()
            
            print(f"     P-Anteil: {secondary_status['p_term']:+8.2f}")
            print(f"     I-Anteil: {secondary_status['i_term']:+8.2f} (Integral: {secondary_status['integral']:+6.2f})")
            print(f"     D-Anteil: {secondary_status['d_term']:+8.2f}")
            print(f"     Trend:    {secondary_status['trend']:+8.2f}°C/min")
            print(f"     Ausgabe:  {old_secondary_output:.1f}% → {secondary_pump:.1f}% (Δ{secondary_pump-old_secondary_output:+.1f}%)")
            
            # Sicherheitsprüfung: Harte Regelung bei Überschreitung
            if temp_error > self.config['control']['hard_control_threshold']:
                old_pump = primary_pump
                primary_pump = min(100, primary_pump + temp_error * 10)
                print(f"\n NOTFALL-REGELUNG AKTIV!")
                print(f"   Temperatur {temp_error:.1f}°C über Sollwert!")
                print(f"   Pumpe 1: {old_pump:.0f}% → {primary_pump:.0f}% (Notfall-Erhöhung)")
            
            # Pumpen ansteuern
            print(f"\n STELLGROSSEN-AUSGABE:")
            print(f"   Pumpe 1: {primary_pump:.0f}% (Primärkreis)")
            print(f"   Pumpe 2: {secondary_pump:.0f}% (Sekundärkreis)")
            
            self.set_pumps(primary_pump, secondary_pump)
            
            # Warnungen prüfen
            warnings = []
            if abs(temp_error) > self.config['control']['hard_control_threshold']:
                warnings.append(f"Temperatur-Abweichung: {temp_error:+.1f}°C")
            if primary_pump >= 98:
                warnings.append("Pumpe 1 am Anschlag")
            if secondary_pump >= 98:
                warnings.append("Pumpe 2 am Anschlag")
            if abs(primary_status['trend']) > 1.0:
                warnings.append(f"Hoher Trend: {primary_status['trend']:+.2f}°C/min")
                
            if warnings:
                print(f"\n  WARNUNGEN:")
                for warning in warnings:
                    print(f"   • {warning}")
            else:
                print(f"\n System läuft im Normalbetrieb")
            
            # Erwartete Auswirkungen
            print(f"\n ERWARTETE AUSWIRKUNGEN:")
            if temp_error > 0.5:
                print(f"   Primär: Temperatur zu hoch → Pumpe erhöht → mehr Kühlung")
            elif temp_error < -0.5:
                print(f"   Primär: Temperatur zu niedrig → Pumpe reduziert → weniger Kühlung")
            else:
                print(f"   Primär: Temperatur im Sollbereich → sanfte Anpassung")
                
            if delta_t_error > 0.2:
                print(f"   Sekundär: Delta-T zu groß → Pumpe erhöht → kleinere Delta-T")
            elif delta_t_error < -0.2:
                print(f"   Sekundär: Delta-T zu klein → Pumpe reduziert → größere Delta-T")
            else:
                print(f"   Sekundär: Delta-T im Sollbereich → sanfte Anpassung")
            
            print(f" ====== PID-REGELZYKLUS BEENDET ======\n")
            
            self.last_update = current_time
    
    def set_pumps(self, pump1: float, pump2: float):
        """Sendet Pumpenwerte über MQTT"""
        if self.mqtt_client:
            base_topic = self.config['mqtt']['topics']['pump_control']
            
            self.mqtt_client.publish(f"{base_topic}/pump1", str(int(pump1)), qos=1)
            self.mqtt_client.publish(f"{base_topic}/pump2", str(int(pump2)), qos=1)
    
    def publish_pid_status(self, status_data: Dict[str, Any]):
        """Sendet PID-Status an MQTT"""
        if self.mqtt_client:
            status_topic = self.config['mqtt']['topics']['status']
            self.mqtt_client.publish(status_topic, json.dumps(status_data), qos=0)
    
    def start(self):
        """Controller starten"""
        print("Integrierter PID-Controller gestartet")
        print(f"Config-Datei: {self.config_manager.config_file}")
        print("Integration mit bestehendem Automation-System aktiv")
        print("Änderungen an der Config werden automatisch übernommen")
        
        self.control_loop()
    
    def stop(self):
        """Controller stoppen"""
        self.running = False
        self.disable_pid_control()
        
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        
        print("Integrierter PID-Controller gestoppt")


if __name__ == "__main__":
    # Pfade zu bestehenden Config-Dateien
    legacy_config = "/root/RY3T/RY3T-Module-Automation/config.json"
    legacy_values = "/root/RY3T/RY3T-Module-Automation/config_values.json"
    
    controller = IntegratedHeatExchangerController(
        config_file="pid_config.json",
        legacy_config_path=legacy_config,
        legacy_values_path=legacy_values
    )
    
    try:
        controller.start()
    except KeyboardInterrupt:
        print("\nBeende Controller...")
        controller.stop()