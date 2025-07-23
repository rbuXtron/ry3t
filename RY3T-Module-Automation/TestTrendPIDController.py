import time
import numpy as np
from collections import deque
from dataclasses import dataclass
from typing import Tuple, Optional, Dict, Any
import matplotlib.pyplot as plt
import json
import os
from datetime import datetime
import paho.mqtt.client as mqtt_client


class ConfigManager:
   """Verwaltet die Konfiguration aus JSON-Datei"""
   
   def __init__(self, config_file: str = "test_config.json"):
       self.config_file = config_file
       self.config = self.load_config()
       
   def load_config(self) -> Dict[str, Any]:
       """Lädt Konfiguration aus JSON-Datei"""
       if os.path.exists(self.config_file):
           try:
               with open(self.config_file, 'r', encoding='utf-8') as f:
                   config = json.load(f)
               print(f"Konfiguration geladen aus {self.config_file}")
               return config
           except Exception as e:
               print(f"Fehler beim Laden der Konfiguration: {e}")
               return self.get_default_config()
       else:
           print(f"Keine Konfigurationsdatei gefunden. Erstelle Standard-Konfiguration...")
           config = self.get_default_config()
           self.save_config(config)
           return config
   
   def save_config(self, config: Dict[str, Any] = None):
       """Speichert Konfiguration in JSON-Datei"""
       if config is None:
           config = self.config
           
       try:
           with open(self.config_file, 'w', encoding='utf-8') as f:
               json.dump(config, f, indent=4, ensure_ascii=False)
           print(f"Konfiguration gespeichert in {self.config_file}")
       except Exception as e:
           print(f"Fehler beim Speichern der Konfiguration: {e}")
   
   def get_default_config(self) -> Dict[str, Any]:
       """Gibt Standard-Konfiguration zurück"""
       return {
           "system": {
               "name": "Wärmetauscher-Regelung",
               "version": "1.0",
               "description": "Max-Temperatur Primär & Delta-T Sekundär",
               "sample_time": 10.0,
               "logging_enabled": True
           },
           "mqtt": {
               "enabled": False,
               "broker": "localhost",
               "port": 1883,
               "topic_ctrl_core": "ctrl/core",
               "topic_log": "log/heat_exchanger",
               "topic_data": "data/heat_exchanger"
           },
           "primary_circuit": {
               "max_temperature": 70.0,
               "safety_margin": 2.0,
               "min_pump_speed": 15.0,
               "max_pump_speed": 100.0,
               "pid": {
                   "kp": 4.0,
                   "ki": 0.03,
                   "kd": 20.0,
                   "trend_weight": 4.0,
                   "prediction_horizon": 20,
                   "integral_limits": [-30, 30]
               },
               "emergency": {
                   "enabled": True,
                   "boost_value": 20.0,
                   "critical_margin": 1.0
               }
           },
           "secondary_circuit": {
               "delta_t_setpoint": 5.0,
               "min_pump_speed": 10.0,
               "max_pump_speed": 100.0,
               "pid": {
                   "kp": 6.0,
                   "ki": 0.15,
                   "kd": 15.0,
                   "trend_weight": 2.5,
                   "prediction_horizon": 10,
                   "integral_limits": [-30, 30]
               },
               "feedforward": {
                   "base_factor": 0.4,
                   "low_delta_t_boost": 20.0,
                   "low_delta_t_threshold": 0.5
               }
           },
           "coordination": {
               "balance_enabled": True,
               "max_speed_difference": 40.0,
               "min_flow_difference": 5.0,
               "optimal_ratio_min": 0.7,
               "optimal_ratio_max": 1.5
           },
           "safety": {
               "temp_violation_limit": 5,
               "auto_reset_time": 300,
               "alarm_enabled": True
           },
           "data_logging": {
               "history_size": 360,
               "log_interval": 60,
               "export_format": "csv"
           }
       }
   
   def update_parameter(self, path: str, value: Any):
       """Aktualisiert einen einzelnen Parameter"""
       keys = path.split('.')
       config = self.config
       
       # Navigate to the parameter
       for key in keys[:-1]:
           if key in config:
               config = config[key]
           else:
               print(f"Pfad {path} nicht gefunden")
               return
       
       # Update the value
       if keys[-1] in config:
           old_value = config[keys[-1]]
           config[keys[-1]] = value
           print(f"Parameter {path}: {old_value} -> {value}")
           self.save_config()
       else:
           print(f"Parameter {keys[-1]} nicht gefunden")
   
   def reload_config(self):
       """Lädt Konfiguration neu aus Datei"""
       self.config = self.load_config()
       return self.config


class TrendPIDController:
   """Basis PID-Controller mit starkem Trend-Fokus für träge Systeme"""
   
   def __init__(self, config: Dict[str, Any], reverse_acting: bool = False):
       # PID-Parameter aus Config
       self.kp = config['kp']
       self.ki = config['ki']
       self.kd = config['kd'] * config['trend_weight']
       self.trend_weight = config['trend_weight']
       self.prediction_horizon = config['prediction_horizon']
       self.integral_limits = tuple(config['integral_limits'])
       
       # Weitere Parameter
       self.sample_time = 10.0  # Wird vom Hauptsystem gesetzt
       self.output_limits = (0, 100)  # Wird später angepasst
       self.reverse_acting = reverse_acting
       
       # PID Zustandsvariablen
       self.last_time = time.time()
       self.last_error = 0.0
       self.integral = 0.0
       self.last_output = 50.0
       
       # Trend-Analyse
       self.error_history = deque(maxlen=30)
       self.derivative_history = deque(maxlen=10)
       self.output_history = deque(maxlen=20)
       
       # Anti-Windup
       self.integral_limits = (-30, 30)
       
   def update(self, setpoint: float, measured_value: float, 
              feed_forward: float = 0.0) -> float:
       """
       PID-Update mit Trend-Vorhersage und Feed-Forward
       """
       current_time = time.time()
       dt = current_time - self.last_time
       
       if dt < self.sample_time * 0.5:
           return self.last_output
           
       # Fehler berechnen (reverse für Max-Regelung)
       if self.reverse_acting:
           error = measured_value - setpoint  # Umgekehrt für Maximum
       else:
           error = setpoint - measured_value  # Normal für Minimum
           
       self.error_history.append(error)
       
       # P-Anteil
       p_term = self.kp * error
       
       # I-Anteil mit Anti-Windup
       self.integral += error * (dt / 60.0)
       
       # Spezielle Anti-Windup für Max-Regelung
       if self.reverse_acting and measured_value > setpoint:
           self.integral = min(self.integral, 0)  # Kein positives Integral
       elif not self.reverse_acting and np.sign(error) != np.sign(self.last_error):
           self.integral *= 0.8
           
       self.integral = np.clip(self.integral, *self.integral_limits)
       i_term = self.ki * self.integral
       
       # D-Anteil mit erweiterter Trend-Analyse
       d_term = self._calculate_advanced_derivative(error, measured_value, dt)
       
       # Trend-Vorhersage
       trend_correction = self._predict_future_trend()
       
       # Gesamtausgabe
       output = p_term + i_term + d_term + trend_correction + feed_forward
       
       # Ausgabe begrenzen
       output = self._apply_output_limits(output)
       
       # Speichern
       self.last_error = error
       self.last_time = current_time
       self.last_output = output
       self.output_history.append(output)
       
       return output
   
   def _calculate_advanced_derivative(self, error: float, 
                                    measured_value: float, dt: float) -> float:
       """Erweiterte Ableitungsberechnung mit Glättung"""
       if len(self.error_history) < 2:
           return 0.0
           
       # Mehrpunkt-Ableitung
       if len(self.error_history) >= 5:
           errors = list(self.error_history)[-5:]
           derivatives = []
           for i in range(1, len(errors)):
               d = (errors[i] - errors[i-1]) / (self.sample_time / 60.0)
               derivatives.append(d)
           derivative = np.mean(derivatives)
       else:
           derivative = (error - self.last_error) / (dt / 60.0)
           
       self.derivative_history.append(derivative)
       
       # Verstärkung bei kritischem Trend (für Max-Regelung)
       if self.reverse_acting and len(self.derivative_history) >= 3:
           # Wenn Temperatur schnell steigt, stark gegensteuern
           if derivative < -0.5:  # Negative Ableitung = Temp steigt
               derivative *= 1.5
               
       return self.kd * derivative
   
   def _predict_future_trend(self) -> float:
       """Vorhersage des zukünftigen Trends"""
       if len(self.error_history) < 10:
           return 0.0
           
       x = np.arange(len(self.error_history))
       y = np.array(self.error_history)
       
       # Gewichtung
       weights = np.exp(0.1 * (x - x[-1]))
       
       try:
           # Quadratische Anpassung
           coeffs = np.polyfit(x, y, 2, w=weights)
           
           # Vorhersage
           future_x = len(x) + self.prediction_horizon
           predicted_error = np.polyval(coeffs, future_x)
           current_error = y[-1]
           
           prediction_change = predicted_error - current_error
           
           # Verstärkte Korrektur für Max-Regelung bei kritischem Trend
           if self.reverse_acting and prediction_change < -1.0:
               return self.kd * prediction_change * 0.5  # Stärkere Reaktion
           
           return self.kd * prediction_change * 0.3
           
       except:
           return 0.0
   
   def _apply_output_limits(self, output: float) -> float:
       """Ausgangsbegrenzung mit Rampe"""
       if len(self.output_history) > 0:
           max_change = 10.0
           output = np.clip(output, 
                          self.last_output - max_change,
                          self.last_output + max_change)
       
       return np.clip(output, *self.output_limits)
   
   def update_from_config(self, config: Dict[str, Any]):
       """Aktualisiert PID-Parameter aus Config"""
       self.kp = config['kp']
       self.ki = config['ki']
       self.kd = config['kd'] * config['trend_weight']
       self.trend_weight = config['trend_weight']
       self.prediction_horizon = config['prediction_horizon']
       self.integral_limits = tuple(config['integral_limits'])

   def reset(self):
       """Controller zurücksetzen"""
       self.integral = 0.0
       self.last_error = 0.0
       self.error_history.clear()
       self.derivative_history.clear()
       self.output_history.clear()


class MaxTempDeltaTController:
   """
   Spezialcontroller für Wärmetauscher mit JSON-Konfiguration und MQTT-Steuerung:
   - Primärkreis: Regelung AUF Maximaltemperatur (Sollwert) mit hartem Eingriff bei Überschreitung
   - Sekundärkreis: Regelung auf Delta-T zwischen Sek. und Prim. Rücklauf
   - MQTT-Integration für Pumpen- und Ventilsteuerung
   """
   
   def __init__(self, config_file: str = "test_config.json", mqtt_broker: str = None):
       """
       Initialisiert Controller mit Konfiguration aus JSON-Datei
       
       Args:
           config_file: Pfad zur Konfigurationsdatei
           mqtt_broker: MQTT Broker Adresse (optional, kann auch in config sein)
       """
       
       # Konfiguration laden
       self.config_manager = ConfigManager(config_file)
       self.config = self.config_manager.config
       
       # MQTT Setup
       self.mqtt_enabled = False
       self.mqtt_client = None
       if mqtt_broker or self.config.get('mqtt', {}).get('enabled', False):
           self._setup_mqtt(mqtt_broker or self.config['mqtt']['broker'])
       
       # System-Parameter
       self.sample_time = self.config['system']['sample_time']
       
       # Primärkreis-Parameter
       prim_config = self.config['primary_circuit']
       self.primary_setpoint = prim_config['max_temperature']  # Ist jetzt der Sollwert!
       self.safety_margin = prim_config['safety_margin']
       self.hard_control_threshold = 2.0  # Bei 2°C Überschreitung hart regeln
       
       # Sekundärkreis-Parameter
       sec_config = self.config['secondary_circuit']
       self.secondary_delta_t_setpoint = sec_config['delta_t_setpoint']
       
       # Primärkreis-PID (Normale Temperatur-Regelung)
       self.primary_pid = TrendPIDController(
           config=prim_config['pid'],
           reverse_acting=False  # Normal: mehr Pumpe = niedrigere Temp
       )
       self.primary_pid.sample_time = self.sample_time
       self.primary_pid.output_limits = (
           prim_config['min_pump_speed'],
           prim_config['max_pump_speed']
       )
       
       # Sekundärkreis-PID (Delta-T Regelung)
       self.secondary_pid = TrendPIDController(
           config=sec_config['pid'],
           reverse_acting=False
       )
       self.secondary_pid.sample_time = self.sample_time
       self.secondary_pid.output_limits = (
           sec_config['min_pump_speed'],
           sec_config['max_pump_speed']
       )
       
       # Sicherheitsfunktionen
       self.hard_control_active = False
       self.temp_violation_counter = 0
       self.last_violation_time = None
       
       # Letzte Stellwerte für MQTT und frühe Rückkehr
       self.last_primary_pump = 50.0
       self.last_secondary_pump = 50.0
       self.last_primary_valve = 100.0
       self.last_secondary_valve = 100.0
       self.last_primary_flow_temp = 50.0
       self.last_output = (50.0, 50.0, {})  # Für frühe Rückkehr
       
       # System-Historie
       history_size = self.config['data_logging']['history_size']
       self.system_history = {
           'time': deque(maxlen=history_size),
           'primary_temp': deque(maxlen=history_size),
           'secondary_temp': deque(maxlen=history_size),
           'primary_return': deque(maxlen=history_size),
           'secondary_return': deque(maxlen=history_size),
           'primary_flow': deque(maxlen=history_size),
           'secondary_flow': deque(maxlen=history_size),
           'delta_t': deque(maxlen=history_size),
           'safety_active': deque(maxlen=history_size)
       }
       
       self.last_update_time = time.time()
       self.last_log_time = time.time()
       
       # Für frühe Rückkehr bei zu schnellem Update
       self.last_full_output = None
       
       print(f"Controller initialisiert mit Konfiguration aus {config_file}")
       if self.mqtt_enabled:
           print(f"MQTT aktiviert - Broker: {self.mqtt_broker}")
   
   def _setup_mqtt(self, broker_address: str):
       """Initialisiert MQTT-Verbindung"""
       try:
           self.mqtt_broker = broker_address
           self.mqtt_client = mqtt_client.Client(
               mqtt_client.CallbackAPIVersion.VERSION1, 
               "heat_exchanger_controller"
           )
           
           # MQTT Topics aus Config oder Standard
           mqtt_config = self.config.get('mqtt', {})
           self.topic_ctrl_core = mqtt_config.get('topic_ctrl_core', 'ctrl/core')
           self.topic_log = mqtt_config.get('topic_log', 'log/heat_exchanger')
           self.topic_data = mqtt_config.get('topic_data', 'data/heat_exchanger')
           
           # Callbacks
           self.mqtt_client.on_connect = self._on_mqtt_connect
           self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
           
           # Verbinden
           self.mqtt_client.connect(broker_address, keepalive=60)
           self.mqtt_client.loop_start()
           self.mqtt_enabled = True
           
       except Exception as e:
           print(f"MQTT-Verbindung fehlgeschlagen: {e}")
           self.mqtt_enabled = False
   
   def _on_mqtt_connect(self, client, userdata, flags, rc):
       """MQTT Connect Callback"""
       if rc == 0:
           print("MQTT verbunden")
           self._mqtt_log("Heat Exchanger Controller connected")
           # Pumpen aktivieren
           self._mqtt_publish(f"{self.topic_ctrl_core}/pumpEnable", "True")
       else:
           print(f"MQTT Verbindungsfehler: {rc}")
   
   def _on_mqtt_disconnect(self, client, userdata, rc):
       """MQTT Disconnect Callback"""
       print("MQTT getrennt")
   
   def _mqtt_publish(self, topic: str, payload: str):
       """Publiziert MQTT Nachricht"""
       if self.mqtt_enabled and self.mqtt_client:
           try:
               self.mqtt_client.publish(topic, payload, qos=1)
           except Exception as e:
               print(f"MQTT Publish Fehler: {e}")
   
   def _mqtt_log(self, message: str):
       """Sendet Log-Nachricht über MQTT"""
       if self.mqtt_enabled:
           self._mqtt_publish(self.topic_log, message)
   
   def reload_config(self):
       """Lädt Konfiguration neu und aktualisiert alle Parameter"""
       self.config = self.config_manager.reload_config()
       
       # System-Parameter aktualisieren
       self.sample_time = self.config['system']['sample_time']
       
       # Primärkreis-Parameter
       prim_config = self.config['primary_circuit']
       self.primary_setpoint = prim_config['max_temperature']  # Sollwert!
       self.safety_margin = prim_config['safety_margin']
       
       # PID-Parameter aktualisieren
       self.primary_pid.update_from_config(prim_config['pid'])
       self.primary_pid.output_limits = (
           prim_config['min_pump_speed'],
           prim_config['max_pump_speed']
       )
       
       # Sekundärkreis-Parameter
       sec_config = self.config['secondary_circuit']
       self.secondary_delta_t_setpoint = sec_config['delta_t_setpoint']
       
       self.secondary_pid.update_from_config(sec_config['pid'])
       self.secondary_pid.output_limits = (
           sec_config['min_pump_speed'],
           sec_config['max_pump_speed']
       )
       
       print("Konfiguration neu geladen und angewendet")
       
   def update(self, 
              primary_supply_temp: float,
              primary_return_temp: float,
              secondary_supply_temp: float,
              secondary_return_temp: float,
              heat_demand: float = 50.0) -> Tuple[float, float, dict]:
       """
       Haupt-Update-Funktion
       
       Returns:
           primary_pump_speed: Primärpumpe % (regelt auf Max-Temp)
           secondary_pump_speed: Sekundärpumpe % (regelt auf Delta-T)
           status: Status-Dictionary
       """
       
       # Bei zu schnellem Update: letzte Werte zurückgeben
       current_time = time.time()
       dt = current_time - self.last_update_time
       
       if dt < self.sample_time * 0.5 and self.last_full_output is not None:
           return self.last_full_output
       
       # Aktuelle Delta-T berechnen
       current_delta_t = secondary_return_temp - primary_return_temp
       
       # Überprüfung ob harte Regelung nötig (Temperatur über Sollwert)
       temp_deviation = primary_supply_temp - self.primary_setpoint
       hard_control_needed = temp_deviation > self.hard_control_threshold
       
       # Feed-Forward Berechnung
       primary_ff = self._calculate_primary_feedforward(
           primary_supply_temp, heat_demand, hard_control_needed
       )
       secondary_ff = self._calculate_secondary_feedforward(
           current_delta_t, heat_demand
       )
       
       # Primärkreis-Regelung
       if hard_control_needed:
           # Harte Regelung bei Überschreitung
           self.hard_control_active = True
           # Aggressiver P-Anteil für schnelle Reaktion
           emergency_boost = temp_deviation * 15.0  # Starker Boost proportional zur Abweichung
           primary_pump_speed = min(100, self.primary_pid.last_output + emergency_boost)
           
           # Warnung ausgeben
           if self.config['safety']['alarm_enabled']:
               print(f"⚠️ HARTE REGELUNG AKTIV: Temperatur {primary_supply_temp:.1f}°C "
                     f"überschreitet Sollwert {self.primary_setpoint:.1f}°C um {temp_deviation:.1f}°C!")
       else:
           # Normale Regelung auf Solltemperatur
           self.hard_control_active = False
           primary_pump_speed = self.primary_pid.update(
               self.primary_setpoint,  # Sollwert anfahren
               primary_supply_temp,
               primary_ff
           )
       
       # Sekundärkreis-Regelung (auf Delta-T)
       secondary_pump_speed = self.secondary_pid.update(
           self.secondary_delta_t_setpoint,
           current_delta_t,
           secondary_ff
       )
       
       # Koordination der Pumpen
       primary_pump_speed, secondary_pump_speed = self._coordinate_pumps(
           primary_pump_speed, secondary_pump_speed,
           primary_supply_temp, current_delta_t,
           hard_control_needed
       )
       
       # Historie aktualisieren
       self._update_history(
           primary_supply_temp, secondary_supply_temp,
           primary_return_temp, secondary_return_temp,
           primary_pump_speed, secondary_pump_speed,
           current_delta_t, hard_control_needed
       )
       
       # Status
       status = self._create_status(
           primary_supply_temp, primary_return_temp,
           secondary_supply_temp, secondary_return_temp,
           primary_pump_speed, secondary_pump_speed,
           current_delta_t, heat_demand, hard_control_needed
       )
       
       # Statusmeldung wenn Intervall erreicht
       if self.config['system']['logging_enabled']:
           current_time = time.time()
           if current_time - self.last_log_time >= self.config['data_logging']['log_interval']:
               self._log_status(status)
               self.last_log_time = current_time
       
       # Aktoren über MQTT ansteuern (wenn aktiviert)
       self._control_actuators(primary_pump_speed, secondary_pump_speed)
       
       # Ausgabe für nächsten frühen Return speichern
       self.last_full_output = (primary_pump_speed, secondary_pump_speed, status)
       self.last_update_time = current_time
       
       return primary_pump_speed, secondary_pump_speed, status
   
   def _calculate_primary_feedforward(self, current_temp: float, 
                                    heat_demand: float, hard_control_active: bool) -> float:
       """Feed-Forward für Primärpumpe"""
       if hard_control_active:
           return 0  # Keine FF bei harter Regelung
           
       # Normale Feed-Forward basierend auf Abstand zum Sollwert
       temp_error = self.primary_setpoint - current_temp
       
       # Basis-FF proportional zum Fehler und Wärmebedarf
       base_ff = heat_demand * 0.3
       
       # Zusätzliche FF wenn weit unter Sollwert
       if temp_error > 5:
           base_ff += temp_error * 2  # Boost für schnelleres Aufheizen
       elif temp_error < -1:
           base_ff -= abs(temp_error) * 3  # Reduzierung bei Annäherung
           
       return np.clip(base_ff, -20, 40)
   
   def _calculate_secondary_feedforward(self, current_delta_t: float,
                                      heat_demand: float) -> float:
       """Feed-Forward für Sekundärpumpe (Delta-T Regelung)"""
       ff_config = self.config['secondary_circuit']['feedforward']
       
       # Basis FF
       base_ff = heat_demand * ff_config['base_factor']
       
       # Korrektur wenn Delta-T zu klein
       threshold = self.secondary_delta_t_setpoint * ff_config['low_delta_t_threshold']
       if current_delta_t < threshold:
           base_ff += ff_config['low_delta_t_boost']
       elif current_delta_t > self.secondary_delta_t_setpoint * 1.5:
           base_ff -= 10  # Pumpe reduzieren
           
       return np.clip(base_ff, 0, 30)
   
   def _coordinate_pumps(self, primary_speed: float, secondary_speed: float,
                        primary_temp: float, delta_t: float,
                        hard_control_active: bool) -> Tuple[float, float]:
       """
       Koordiniert beide Pumpen unter Berücksichtigung der speziellen Regelziele
       """
       coord_config = self.config['coordination']
       
       # Bei harter Regelung: Primärpumpe hat absolute Priorität
       if hard_control_active:
           # Sekundärpumpe muss mitziehen für Wärmeabfuhr
           if secondary_speed < primary_speed * 0.9:
               secondary_speed = primary_speed * 0.9
               
       elif coord_config['balance_enabled']:
           # Normale Koordination
           
           # Optimierung für bessere Sollwert-Erreichung
           temp_error = self.primary_setpoint - primary_temp
           
           # Wenn Primärtemperatur zu niedrig und Delta-T zu groß
           if temp_error > 3 and delta_t > self.secondary_delta_t_setpoint + 2:
               # Primärpumpe etwas reduzieren für höhere Temperatur
               primary_speed *= 0.95
               
           # Wenn Delta-T zu klein trotz hoher Sekundärpumpe
           if delta_t < self.secondary_delta_t_setpoint - 1 and secondary_speed > 70:
               # Primärfluss könnte zu hoch sein
               if primary_speed > 60 and temp_error < 2:
                   primary_speed *= 0.98
                   
       # Mindestdurchflüsse
       prim_min = self.config['primary_circuit']['min_pump_speed']
       sec_min = self.config['secondary_circuit']['min_pump_speed']
       primary_speed = max(primary_speed, prim_min)
       secondary_speed = max(secondary_speed, sec_min)
       
       # Sanfte Begrenzung der Differenz (außer bei harter Regelung)
       if coord_config['balance_enabled'] and not hard_control_active:
           speed_diff = abs(primary_speed - secondary_speed)
           max_diff = coord_config['max_speed_difference']
           
           if speed_diff > max_diff:
               # Zu große Differenz vermeiden
               avg = (primary_speed + secondary_speed) / 2
               primary_speed = avg + np.sign(primary_speed - secondary_speed) * max_diff/2
               secondary_speed = avg - np.sign(primary_speed - secondary_speed) * max_diff/2
       
       # Finale Begrenzung
       primary_speed = np.clip(primary_speed, 
                              self.config['primary_circuit']['min_pump_speed'],
                              self.config['primary_circuit']['max_pump_speed'])
       secondary_speed = np.clip(secondary_speed,
                                self.config['secondary_circuit']['min_pump_speed'],
                                self.config['secondary_circuit']['max_pump_speed'])
           
       return primary_speed, secondary_speed
   
   def _update_history(self, p_supply: float, s_supply: float,
                      p_return: float, s_return: float,
                      p_flow: float, s_flow: float,
                      delta_t: float, hard_control: bool):
       """Historie aktualisieren"""
       current_time = time.time() - self.last_update_time
       
       self.system_history['time'].append(current_time)
       self.system_history['primary_temp'].append(p_supply)
       self.system_history['secondary_temp'].append(s_supply)
       self.system_history['primary_return'].append(p_return)
       self.system_history['secondary_return'].append(s_return)
       self.system_history['primary_flow'].append(p_flow)
       self.system_history['secondary_flow'].append(s_flow)
       self.system_history['delta_t'].append(delta_t)
       self.system_history['safety_active'].append(1 if hard_control else 0)
   
   def _create_status(self, p_supply: float, p_return: float,
                     s_supply: float, s_return: float,
                     p_pump: float, s_pump: float,
                     delta_t: float, heat_demand: float,
                     hard_control_active: bool) -> dict:
       """Status-Dictionary erstellen"""
       temp_error = self.primary_setpoint - p_supply
       
       return {
           'primary': {
               'supply_temp': p_supply,
               'return_temp': p_return,
               'pump_speed': p_pump,
               'setpoint': self.primary_setpoint,
               'temp_error': temp_error,
               'hard_control_active': hard_control_active,
               'control_mode': 'HART' if hard_control_active else 'NORMAL'
           },
           'secondary': {
               'supply_temp': s_supply,
               'return_temp': s_return,
               'pump_speed': s_pump,
               'delta_t_actual': delta_t,
               'delta_t_setpoint': self.secondary_delta_t_setpoint,
               'delta_t_error': self.secondary_delta_t_setpoint - delta_t
           },
           'system': {
               'heat_demand': heat_demand,
               'violations': self.temp_violation_counter
           }
       }
   
   def _control_actuators(self, primary_pump: float, secondary_pump: float):
       """Steuert Pumpen und Ventile über MQTT"""
       if not self.mqtt_enabled:
           return
       
       # Nur senden wenn Änderung signifikant (>1%)
       if abs(primary_pump - self.last_primary_pump) > 1.0:
           self._mqtt_publish(f"{self.topic_ctrl_core}/pump1", str(primary_pump))
           self.last_primary_pump = primary_pump
           self._mqtt_log(f"Primärpumpe -> {primary_pump:.1f}%")
       
       if abs(secondary_pump - self.last_secondary_pump) > 1.0:
           self._mqtt_publish(f"{self.topic_ctrl_core}/pump2", str(secondary_pump))
           self.last_secondary_pump = secondary_pump
           self._mqtt_log(f"Sekundärpumpe -> {secondary_pump:.1f}%")
       
       # Ventile basierend auf Betriebszustand
       # Bei harter Regelung: Ventile voll öffnen für max. Durchfluss
       if self.hard_control_active:
           primary_valve = 100.0
           secondary_valve = 100.0
       else:
           # Normale Ventilstellung (kann erweitert werden)
           primary_valve = 100.0
           secondary_valve = 100.0
       
       if abs(primary_valve - self.last_primary_valve) > 1.0:
           self._mqtt_publish(f"{self.topic_ctrl_core}/valve1", str(primary_valve))
           self.last_primary_valve = primary_valve
       
       if abs(secondary_valve - self.last_secondary_valve) > 1.0:
           self._mqtt_publish(f"{self.topic_ctrl_core}/valve2", str(secondary_valve))
           self.last_secondary_valve = secondary_valve
   
   def _log_status(self, status: dict):
       """Gibt Status in Konsole aus"""
       timestamp = datetime.now().strftime("%H:%M:%S")
       mode = status['primary']['control_mode']
       log_msg = (f"[{timestamp}] Primär: {status['primary']['supply_temp']:.1f}°C "
                  f"(Soll: {self.primary_setpoint}°C) [{mode}], "
                  f"Delta-T: {status['secondary']['delta_t_actual']:.1f}°C "
                  f"(Soll: {self.secondary_delta_t_setpoint}°C)")
       print(log_msg)
       
       # Auch über MQTT loggen
       if self.mqtt_enabled:
           self._mqtt_log(log_msg)
           # Status als JSON publizieren
           self._mqtt_publish(
               f"{self.topic_data}/status",
               json.dumps({
                   'timestamp': timestamp,
                   'primary_temp': status['primary']['supply_temp'],
                   'primary_pump': status['primary']['pump_speed'],
                   'secondary_temp': status['secondary']['supply_temp'],
                   'secondary_pump': status['secondary']['pump_speed'],
                   'delta_t': status['secondary']['delta_t_actual'],
                   'control_mode': mode
               })
           )
   
   def export_history(self, filename: str = None):
       """Exportiert Historie in CSV-Datei"""
       if len(self.system_history['time']) == 0:
           print("Keine Daten zum Exportieren")
           return
           
       if filename is None:
           timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
           filename = f"waermetauscher_log_{timestamp}.csv"
       
       try:
           import csv
           with open(filename, 'w', newline='', encoding='utf-8') as f:
               writer = csv.writer(f)
               
               # Header
               writer.writerow([
                   'Zeit_min', 'Primär_Vorlauf', 'Primär_Rücklauf',
                   'Sekundär_Vorlauf', 'Sekundär_Rücklauf',
                   'Primär_Pumpe_%', 'Sekundär_Pumpe_%',
                   'Delta_T', 'Sicherheit_Aktiv'
               ])
               
               # Daten
               for i in range(len(self.system_history['time'])):
                   writer.writerow([
                       f"{self.system_history['time'][i]/60:.2f}",
                       f"{self.system_history['primary_temp'][i]:.2f}",
                       f"{self.system_history['primary_return'][i]:.2f}",
                       f"{self.system_history['secondary_temp'][i]:.2f}",
                       f"{self.system_history['secondary_return'][i]:.2f}",
                       f"{self.system_history['primary_flow'][i]:.1f}",
                       f"{self.system_history['secondary_flow'][i]:.1f}",
                       f"{self.system_history['delta_t'][i]:.2f}",
                       self.system_history['safety_active'][i]
                   ])
           
           print(f"Historie exportiert nach {filename}")
       except Exception as e:
           print(f"Fehler beim Export: {e}")
   
   def save_tuning_to_config(self):
       """Speichert aktuelle PID-Einstellungen in Config"""
       # Primär PID
       self.config_manager.update_parameter(
           'primary_circuit.pid.kp', self.primary_pid.kp
       )
       self.config_manager.update_parameter(
           'primary_circuit.pid.ki', self.primary_pid.ki
       )
       self.config_manager.update_parameter(
           'primary_circuit.pid.kd', self.primary_pid.kd / self.primary_pid.trend_weight
       )
       
       # Sekundär PID
       self.config_manager.update_parameter(
           'secondary_circuit.pid.kp', self.secondary_pid.kp
       )
       self.config_manager.update_parameter(
           'secondary_circuit.pid.ki', self.secondary_pid.ki
       )
       self.config_manager.update_parameter(
           'secondary_circuit.pid.kd', self.secondary_pid.kd / self.secondary_pid.trend_weight
       )
       
       print("PID-Einstellungen in Konfiguration gespeichert")
   
   def start_control(self):
       """Startet die Regelung und aktiviert alle Komponenten"""
       print("Starte Wärmetauscher-Regelung...")
       
       if self.mqtt_enabled:
           # Pumpen aktivieren
           self._mqtt_publish(f"{self.topic_ctrl_core}/pumpEnable", "True")
           time.sleep(0.5)
           
           # Initiale Pumpenwerte setzen
           self._mqtt_publish(f"{self.topic_ctrl_core}/pump1", "50")
           self._mqtt_publish(f"{self.topic_ctrl_core}/pump2", "50")
           
           # Ventile öffnen
           self._mqtt_publish(f"{self.topic_ctrl_core}/valve1", "100")
           self._mqtt_publish(f"{self.topic_ctrl_core}/valve2", "100")
           
           self._mqtt_log("Regelung gestartet - Pumpen aktiviert, Ventile geöffnet")
       
       print("Regelung aktiv")
   
   def stop_control(self):
       """Stoppt die Regelung und fährt sicher herunter"""
       print("Stoppe Wärmetauscher-Regelung...")
       
       if self.mqtt_enabled:
           # Pumpen langsam herunterfahren
           for speed in [40, 30, 20, 10]:
               self._mqtt_publish(f"{self.topic_ctrl_core}/pump1", str(speed))
               self._mqtt_publish(f"{self.topic_ctrl_core}/pump2", str(speed))
               time.sleep(2)
           
           # Pumpen deaktivieren
           self._mqtt_publish(f"{self.topic_ctrl_core}/pumpEnable", "False")
           
           # Ventile schließen
           self._mqtt_publish(f"{self.topic_ctrl_core}/valve1", "0")
           self._mqtt_publish(f"{self.topic_ctrl_core}/valve2", "0")
           
           self._mqtt_log("Regelung gestoppt - System heruntergefahren")
           
           # MQTT trennen
           if self.mqtt_client:
               self.mqtt_client.loop_stop()
               self.mqtt_client.disconnect()
       
       print("Regelung gestoppt")
   
   def __del__(self):
       """Cleanup beim Beenden"""
       if hasattr(self, 'mqtt_client') and self.mqtt_client:
           try:
               self.mqtt_client.loop_stop()
               self.mqtt_client.disconnect()
           except:
               pass
   
   def plot_performance(self):
       """Performance-Visualisierung"""
       if len(self.system_history['time']) < 10:
           print("Nicht genug Daten für Plot")
           return
           
       fig, axes = plt.subplots(4, 1, figsize=(12, 12))
       
       time_minutes = np.array(self.system_history['time']) / 60
       
       # Temperaturen mit Sollwert-Linie
       ax1 = axes[0]
       ax1.plot(time_minutes, self.system_history['primary_temp'], 
               'r-', linewidth=2, label='Primär Vorlauf')
       ax1.plot(time_minutes, self.system_history['secondary_temp'],
               'b-', linewidth=2, label='Sekundär Vorlauf')
       ax1.axhline(y=self.primary_setpoint, color='r', linestyle='--', 
                  linewidth=2, alpha=0.7, label='Primär Sollwert')
       ax1.axhline(y=self.primary_setpoint + self.hard_control_threshold, 
                  color='orange', linestyle=':', linewidth=2, alpha=0.7, 
                  label='Harte Regelung Grenze')
       
       # Harte Regelung Bereiche markieren
       safety_active = np.array(self.system_history['safety_active'])
       for i in range(1, len(safety_active)):
           if safety_active[i] > 0:
               ax1.axvspan(time_minutes[i-1], time_minutes[i], 
                         alpha=0.2, color='orange')
       
       ax1.set_ylabel('Temperatur (°C)')
       ax1.set_title('Wärmetauscher: Sollwert-Regelung Primär & Delta-T Sekundär')
       ax1.legend()
       ax1.grid(True, alpha=0.3)
       
       # Delta-T mit Sollwert
       ax2 = axes[1]
       ax2.plot(time_minutes, self.system_history['delta_t'],
               'g-', linewidth=2, label='Ist Delta-T')
       ax2.axhline(y=self.secondary_delta_t_setpoint, color='g', 
                  linestyle='--', alpha=0.7, label='Soll Delta-T')
       ax2.set_ylabel('Delta-T (°C)')
       ax2.legend()
       ax2.grid(True, alpha=0.3)
       
       # Pumpengeschwindigkeiten
       ax3 = axes[2]
       ax3.plot(time_minutes, self.system_history['primary_flow'],
               'r-', linewidth=2, label='Primärpumpe')
       ax3.plot(time_minutes, self.system_history['secondary_flow'],
               'b-', linewidth=2, label='Sekundärpumpe')
       ax3.set_ylabel('Pumpendrehzahl (%)')
       ax3.set_ylim(0, 105)
       ax3.legend()
       ax3.grid(True, alpha=0.3)
       
       # Rücklauftemperaturen
       ax4 = axes[3]
       ax4.plot(time_minutes, self.system_history['primary_return'],
               'r:', linewidth=2, label='Primär Rücklauf')
       ax4.plot(time_minutes, self.system_history['secondary_return'],
               'b:', linewidth=2, label='Sekundär Rücklauf')
       ax4.set_xlabel('Zeit (Minuten)')
       ax4.set_ylabel('Rücklauftemperatur (°C)')
       ax4.legend()
       ax4.grid(True, alpha=0.3)
       
       plt.tight_layout()
       plt.show()


# Simulator
class HeatExchangerSimulator:
   """Simuliert ein Wärmetauscher-System"""
   
   def __init__(self):
       self.primary_supply = 65.0
       self.primary_return = 55.0
       self.secondary_supply = 45.0
       self.secondary_return = 40.0
       
       # Kessel/Wärmequelle
       self.source_temp = 80.0
       
   def update(self, primary_pump: float, secondary_pump: float,
             heat_load: float, dt: float = 10.0):
       """Simuliert einen Zeitschritt"""
       
       # Durchflüsse
       primary_flow = primary_pump / 100 * 2.0
       secondary_flow = secondary_pump / 100 * 3.0
       
       # Primärkreis: Mischung mit Wärmequelle
       mixing_ratio = (100 - primary_pump) / 100  # Weniger Pumpe = mehr heiß
       self.primary_supply = (self.source_temp * mixing_ratio + 
                             self.primary_return * (1 - mixing_ratio))
       
       # Wärmeübertragung
       delta_t_log = ((self.primary_supply - self.secondary_return) + 
                     (self.primary_return - self.secondary_supply)) / 2
       
       heat_transferred = 2000 * delta_t_log * min(primary_flow, secondary_flow)
       
       # Temperaturen anpassen
       primary_cooling = heat_transferred / (500 * 4186) * dt
       self.primary_return = self.primary_supply - primary_cooling * 10
       
       secondary_heating = heat_transferred / (100 * 4186) * dt
       self.secondary_supply = self.secondary_return + secondary_heating * 5
       
       # Last
       heat_dissipated = heat_load / 100 * 15000
       load_cooling = heat_dissipated / (100 * 4186) * dt
       self.secondary_return = self.secondary_supply - 5 - load_cooling
       
       # Grenzen
       self.primary_supply = np.clip(self.primary_supply, 20, 85)
       self.secondary_return = np.clip(self.secondary_return, 20, 60)
       
       return {
           'primary_supply': self.primary_supply,
           'primary_return': self.primary_return,
           'secondary_supply': self.secondary_supply,
           'secondary_return': self.secondary_return
       }


# Test mit MQTT-Integration
if __name__ == "__main__":
   # Beispiel-Konfigurationsdatei erstellen (wird nur erstellt wenn nicht vorhanden)
   config_file = "test_config.json"
   
   # Controller mit Config-Datei und MQTT initialisieren
   # MQTT-Broker kann hier oder in test_config.json definiert werden
   controller = MaxTempDeltaTController(
       config_file=config_file,
       mqtt_broker="localhost"  # Oder IP-Adresse des Brokers
   )
   
   # System starten
   controller.start_control()
   
   # Zeige aktuelle Konfiguration
   print("\nAktuelle Konfiguration:")
   print(f"- Primär Solltemperatur: {controller.primary_setpoint}°C")
   print(f"- Harte Regelung ab: +{controller.hard_control_threshold}°C Überschreitung")
   print(f"- Sekundär Delta-T: {controller.secondary_delta_t_setpoint}°C")
   print(f"- Abtastzeit: {controller.sample_time}s")
   print(f"- MQTT: {'Aktiv' if controller.mqtt_enabled else 'Inaktiv'}\n")
   
   # Simulator
   sim = HeatExchangerSimulator()
   sim.source_temp = 80.0  # Heiße Quelle für Test
   
   # Beispiel: Parameter zur Laufzeit ändern
   print("Beispiel: Ändere Primär Solltemperatur auf 75°C...")
   controller.config_manager.update_parameter('primary_circuit.max_temperature', 75.0)
   controller.reload_config()
   print(f"Neue Solltemperatur: {controller.primary_setpoint}°C\n")
   
   # Simulation mit variierendem Wärmebedarf
   duration_minutes = 20
   dt = 10
   steps = duration_minutes * 60 // dt
   
   # Wärmebedarf-Szenario
   heat_profile = np.concatenate([
       np.ones(steps//4) * 30,      # Start: niedrig
       np.ones(steps//4) * 70,      # Dann: hoch
       np.ones(steps//4) * 90,      # Sehr hoch (Test harte Regelung)
       np.ones(steps//4) * 50       # Normal
   ])
   
   for i in range(steps):
       heat_demand = heat_profile[i]
       
       # Controller Update (steuert Pumpen automatisch wenn MQTT aktiv)
       p_pump, s_pump, status = controller.update(
           sim.primary_supply,
           sim.primary_return,
           sim.secondary_supply,
           sim.secondary_return,
           heat_demand
       )
       
       # Simulator Update
       sim.update(p_pump, s_pump, heat_demand, dt)
       
       # Status alle 1 Minute
       if i % 6 == 0:
           print(f"Zeit: {i*dt/60:.1f} min | Bedarf: {heat_demand:.0f}%")
           print(f"  Primär: {sim.primary_supply:.1f}°C (Soll: {controller.primary_setpoint}°C, "
                 f"Fehler: {status['primary']['temp_error']:+.1f}°C), "
                 f"Pumpe: {p_pump:.0f}%")
           print(f"  Delta-T: {status['secondary']['delta_t_actual']:.1f}°C "
                 f"(Soll: {controller.secondary_delta_t_setpoint}°C), Pumpe: {s_pump:.0f}%")
           if status['primary']['hard_control_active']:
               print("  ⚠️  HARTE REGELUNG AKTIV!")
           print()
   
   # Performance Plot
   controller.plot_performance()
   
   # Nach Simulation: System stoppen und Historie exportieren
   controller.stop_control()
   controller.export_history()
   
   # Optional: Optimierte PID-Werte speichern
   # controller.save_tuning_to_config()