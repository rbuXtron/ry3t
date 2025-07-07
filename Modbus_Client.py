#!/usr/bin/env python3
# LOXONE Simulator - Modbus TCP Client
# Simulates a LOXONE system reading sensor data and controlling heating

import time
import struct
import random
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException
from datetime import datetime
import threading
import argparse

# ANSI color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

# Register mapping (same as server)
MODBUS_REGISTERS = {
    # Sensor Data Registers (Read-Only)
    "Miner1HSRT": 40001,
    "Miner1Power": 40003,
    "Miner1Uptime": 40005,
    "Miner2HSRT": 40006,
    "Miner2Power": 40008,
    "Miner2Uptime": 40010,
    "ColdWater": 40011,
    "ColdOil": 40013,
    "HotWater": 40015,
    "HotOil": 40017,
    "TempWaterTank1": 40019,
    "TempWaterTank2": 40021,
    "TempWaterTank3": 40023,
    "TempWaterTank4": 40025,
    "PumpPrimaryAnalogOut": 40027,
    "PumpSecondaryAnalogOut": 40028,
    
    # Heater Control Registers (Read/Write)
    "HeaterStatus": 40100,
    "HeaterErrorCodes": 40101,
    "HeaterAutomation": 40102,
    "HeaterPowerMode": 40103,
    "HeaterPowerLevel": 40104,
    "HeaterPowerLimit": 40105,
    
    # Control Commands (Write-Only)
    "HeaterCommand": 40200,
    "HeaterMode": 40201,
    "SystemReset": 40202,
}

class LoxoneSimulator:
    def __init__(self, host='localhost', port=502):
        self.host = host
        self.port = port
        self.client = ModbusTcpClient(host=self.host, port=self.port)
        self.connected = False
        self.running = False
        
        # Simulated room temperatures for heating control
        self.room_temps = {
            "living_room": 20.5,
            "bedroom": 19.0,
            "bathroom": 22.0,
            "kitchen": 21.0
        }
        
        # Heating setpoints
        self.setpoints = {
            "living_room": 21.0,
            "bedroom": 19.0,
            "bathroom": 23.0,
            "kitchen": 21.0
        }
        
        # Current heating state
        self.heating_on = False
        self.heating_mode = 5  # Loxone mode
        
    def connect(self):
        """Connect to Modbus server"""
        try:
            print(f"{Colors.CYAN}Attempting to connect to {self.host}:{self.port}...{Colors.ENDC}")
            self.connected = self.client.connect()
            if self.connected:
                print(f"{Colors.GREEN}✓ LOXONE connected to Modbus server at {self.host}:{self.port}{Colors.ENDC}")
                # Test read to verify connection
                try:
                    result = self.client.read_holding_registers(address=0, count=1, slave=0)
                    if not result.isError():
                        print(f"{Colors.GREEN}✓ Connection verified - Modbus communication working{Colors.ENDC}")
                    else:
                        print(f"{Colors.YELLOW}⚠ Connected but test read failed: {result}{Colors.ENDC}")
                except Exception as e:
                    print(f"{Colors.YELLOW}⚠ Connected but test failed: {e}{Colors.ENDC}")
            else:
                print(f"{Colors.RED}✗ LOXONE failed to connect to {self.host}:{self.port}{Colors.ENDC}")
                print(f"{Colors.YELLOW}Possible causes:")
                print(f"  - Modbus server not running")
                print(f"  - Wrong IP address or port")
                print(f"  - Firewall blocking connection")
                print(f"  - Port < 1024 needs sudo{Colors.ENDC}")
        except Exception as e:
            print(f"{Colors.RED}✗ Connection error: {e}{Colors.ENDC}")
            self.connected = False
        return self.connected
    
    def disconnect(self):
        """Disconnect from Modbus server"""
        self.running = False
        self.client.close()
        print(f"{Colors.YELLOW}LOXONE disconnected from Modbus server{Colors.ENDC}")
    
    def modbus_registers_to_float(self, registers):
        """Convert two 16-bit Modbus registers to float"""
        if len(registers) < 2:
            return None
        packed = struct.pack('>HH', registers[0], registers[1])
        return struct.unpack('>f', packed)[0]
    
    def read_float_register(self, name):
        """Read a float value from two consecutive registers"""
        if name not in MODBUS_REGISTERS:
            return None
            
        address = MODBUS_REGISTERS[name] - 40001
        
        try:
            result = self.client.read_holding_registers(address=address, count=2, slave=0)
            if not result.isError():
                return self.modbus_registers_to_float(result.registers)
        except ModbusException:
            pass
        return None
    
    def read_int_register(self, name):
        """Read an integer value from a single register"""
        if name not in MODBUS_REGISTERS:
            return None
            
        address = MODBUS_REGISTERS[name] - 40001
        
        try:
            result = self.client.read_holding_registers(address=address, count=1, slave=0)
            if not result.isError():
                return result.registers[0]
        except ModbusException:
            pass
        return None
    
    def write_register(self, name, value):
        """Write a value to a register"""
        if name not in MODBUS_REGISTERS:
            return False
            
        address = MODBUS_REGISTERS[name] - 40001
        
        try:
            result = self.client.write_register(address=address, value=int(value), slave=0)
            return not result.isError()
        except ModbusException:
            return False
    
    def read_all_temperatures(self):
        """Read all temperature sensors"""
        temps = {}
        temp_sensors = [
            "ColdWater", "ColdOil", "HotWater", "HotOil",
            "TempWaterTank1", "TempWaterTank2", "TempWaterTank3", "TempWaterTank4"
        ]
        
        for sensor in temp_sensors:
            value = self.read_float_register(sensor)
            if value is not None:
                temps[sensor] = value
        
        return temps
    
    def simulate_room_temperatures(self):
        """Simulate room temperature changes"""
        for room in self.room_temps:
            # Random temperature fluctuation
            change = random.uniform(-0.1, 0.1)
            
            # If heating is on and room is below setpoint, increase temp
            if self.heating_on and self.room_temps[room] < self.setpoints[room]:
                change += 0.15
            # If heating is off and room is above outside temp, decrease
            elif not self.heating_on and self.room_temps[room] > 15.0:
                change -= 0.05
                
            self.room_temps[room] += change
            self.room_temps[room] = round(self.room_temps[room], 1)
    
    def check_heating_requirement(self):
        """Check if heating is required based on room temperatures"""
        # Check if any room is below setpoint - 0.5°C (hysteresis)
        need_heating = False
        for room, temp in self.room_temps.items():
            if temp < (self.setpoints[room] - 0.5):
                need_heating = True
                break
        
        # Check if all rooms are above setpoint + 0.5°C
        can_stop_heating = True
        for room, temp in self.room_temps.items():
            if temp < (self.setpoints[room] + 0.5):
                can_stop_heating = False
                break
        
        return need_heating, can_stop_heating
    
    def control_heating(self, turn_on, mode=5):
        """Send heating control command"""
        if turn_on:
            print(f"{Colors.YELLOW}LOXONE: Turning heating ON (Mode: {mode}){Colors.ENDC}")
            self.write_register("HeaterCommand", 1)
            time.sleep(0.1)
            self.write_register("HeaterMode", mode)
            self.heating_on = True
        else:
            print(f"{Colors.BLUE}LOXONE: Turning heating OFF{Colors.ENDC}")
            self.write_register("HeaterCommand", 0)
            self.heating_on = False
    
    def display_status(self):
        """Display current status in LOXONE style"""
        print(f"\n{Colors.HEADER}{'='*60}")
        print(f"LOXONE HOME AUTOMATION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}{Colors.ENDC}")
        
        # Room temperatures
        print(f"\n{Colors.BOLD}Room Temperatures:{Colors.ENDC}")
        for room, temp in self.room_temps.items():
            setpoint = self.setpoints[room]
            status = f"{Colors.GREEN}✓{Colors.ENDC}" if temp >= setpoint else f"{Colors.RED}↓{Colors.ENDC}"
            print(f"  {room.replace('_', ' ').title():15} {temp:5.1f}°C (Set: {setpoint:.1f}°C) {status}")
        
        # System temperatures
        print(f"\n{Colors.BOLD}System Temperatures:{Colors.ENDC}")
        temps = self.read_all_temperatures()
        for name, value in temps.items():
            print(f"  {name:15} {value:5.1f}°C")
        
        # Heating status
        heater_status = self.read_int_register("HeaterStatus")
        heater_mode = self.read_int_register("HeaterAutomation")
        print(f"\n{Colors.BOLD}Heating System:{Colors.ENDC}")
        print(f"  Status:         {'ON' if heater_status else 'OFF'}")
        print(f"  Mode:           {heater_mode}")
        print(f"  Control:        {'LOXONE Active' if self.heating_on else 'LOXONE Standby'}")
        
        # Miner status
        print(f"\n{Colors.BOLD}Miner Status:{Colors.ENDC}")
        miner1_power = self.read_float_register("Miner1Power")
        miner2_power = self.read_float_register("Miner2Power")
        if miner1_power:
            print(f"  Miner 1:        {miner1_power:.0f}W")
        if miner2_power:
            print(f"  Miner 2:        {miner2_power:.0f}W")
        
    def run_automation(self, interval=10):
        """Run LOXONE automation logic"""
        self.running = True
        print(f"\n{Colors.GREEN}LOXONE Automation Started{Colors.ENDC}")
        print(f"Checking temperatures every {interval} seconds\n")
        
        cycle = 0
        while self.running:
            try:
                # Simulate room temperature changes
                self.simulate_room_temperatures()
                
                # Check heating requirements
                need_heating, can_stop = self.check_heating_requirement()
                
                # Control heating based on requirements
                if need_heating and not self.heating_on:
                    self.control_heating(turn_on=True, mode=5)  # Mode 5 = Loxone
                elif can_stop and self.heating_on:
                    self.control_heating(turn_on=False)
                
                # Display status every 3 cycles
                if cycle % 3 == 0:
                    self.display_status()
                
                cycle += 1
                time.sleep(interval)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"{Colors.RED}Error in automation: {e}{Colors.ENDC}")
                time.sleep(interval)
        
        print(f"\n{Colors.YELLOW}LOXONE Automation Stopped{Colors.ENDC}")
    
    def manual_control_mode(self):
        """Interactive manual control mode"""
        print(f"\n{Colors.HEADER}LOXONE MANUAL CONTROL MODE{Colors.ENDC}")
        print("Commands:")
        print("  1 - Turn heating ON")
        print("  0 - Turn heating OFF")
        print("  s - Show status")
        print("  r - Read all sensors")
        print("  t - Set room temperature")
        print("  q - Quit")
        
        while True:
            try:
                cmd = input(f"\n{Colors.CYAN}LOXONE> {Colors.ENDC}").strip().lower()
                
                if cmd == '1':
                    mode = input("Select mode (1-8, default 5 for Loxone): ").strip()
                    mode = int(mode) if mode else 5
                    self.control_heating(turn_on=True, mode=mode)
                    
                elif cmd == '0':
                    self.control_heating(turn_on=False)
                    
                elif cmd == 's':
                    self.display_status()
                    
                elif cmd == 'r':
                    temps = self.read_all_temperatures()
                    print(f"\n{Colors.BOLD}All Sensor Readings:{Colors.ENDC}")
                    for name, value in temps.items():
                        print(f"  {name:20} {value:6.1f}°C")
                    
                elif cmd == 't':
                    room = input("Room (living_room/bedroom/bathroom/kitchen): ").strip()
                    if room in self.setpoints:
                        temp = float(input(f"New setpoint for {room}: "))
                        self.setpoints[room] = temp
                        print(f"Setpoint updated: {room} = {temp}°C")
                    
                elif cmd == 'q':
                    break
                    
                else:
                    print("Unknown command")
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"{Colors.RED}Error: {e}{Colors.ENDC}")

def main():
    parser = argparse.ArgumentParser(description='LOXONE Simulator - Modbus Client')
    parser.add_argument('--host', default='localhost', help='Modbus server host')
    parser.add_argument('--port', type=int, default=502, help='Modbus server port')
    parser.add_argument('--mode', choices=['auto', 'manual'], default='auto',
                        help='Operation mode: auto (automation) or manual (interactive)')
    parser.add_argument('--interval', type=int, default=10, 
                        help='Automation check interval in seconds')
    
    args = parser.parse_args()
    
    # Create LOXONE simulator
    loxone = LoxoneSimulator(host=args.host, port=args.port)
    
    # Connect to server
    if not loxone.connect():
        return
    
    try:
        if args.mode == 'auto':
            # Run automation
            loxone.run_automation(interval=args.interval)
        else:
            # Manual control mode
            loxone.manual_control_mode()
            
    finally:
        loxone.disconnect()

if __name__ == "__main__":
    main()