#!/usr/bin/env python3
"""
Interactive Modbus Test Client für pymodbus 3.9.2
Ermöglicht direktes Lesen und Schreiben von Registern
"""

from pymodbus.client import ModbusTcpClient
import struct
import time
from datetime import datetime

# Server Configuration
SERVER_IP = "localhost"  # Ändere auf deine Docker-Host IP
SERVER_PORT = 5020

# Register Reference
REGISTER_INFO = {
    # Sensor Data (Read-Only)
    40001: ("Heating", "int", "is heating active (0=OFF, 1=ON)"),
    40002: ("Modes", "int", "Current Modes (bitmask)"),
    40003: ("Power_Modes", "int", "Power Modes (1-8)"),
    40004: ("Status", "int", "System Status (0=OK, 1=Error)"),
    40005: ("Power_Level", "int", "Power Level (0-100)"),
    40006: ("Power_Limit", "int", "Power Limit (0-100)"),
    40007: ("Register_07", "int", "Register_07"),
    40008: ("Register_08", "int", "Register_08"),
    40009: ("Register_09", "int", "Register_09§"),
    40010: ("Register_10", "int", "Register_10"),
    40011: ("Register_11", "int", "Register_11"),

    
    # Heater Status (Read/Write)
    40100: ("Status", "int", "Heater Status (0=OFF, 1=ON)"),
    40101: ("ErrorCodes", "int", "Heater Error Codes"),
    40102: ("Automation", "int", "Heater Automation Mode"),
    40103: ("PowerMode", "int", "Heater Power Mode"),
    40104: ("PowerLevel", "int", "Heater Power Level"),
    40105: ("PowerLimit", "int", "Heater Power Limit"),
    40106: ("Miner1HSRT", "float", "Miner 1 HSRT (High Speed Register Transfer)"),  # Uses 2 registers (40106-40107)
    40108: ("Miner1Power", "float", "Miner 1 Power"),  # Uses 2 registers (40108-40109)
    40110: ("Miner1Uptime", "float", "Miner 1 Uptime"),  # Uses 2 registers (40110-40111)
    40112: ("Miner2HSRT", "float", "Miner 2 HSRT (High Speed Register Transfer)"),  # Uses 2 registers (40112-40113)
    40114: ("Miner2Power", "float", "Miner 2 Power"),  # Uses 2 registers (40114-40115)
    40116: ("Miner2Uptime", "float", "Miner 2 Uptime"),  # Uses 2 registers (40116-40117)
    40118: ("ColdWater", "float", "Cold Water Temperature (°C)"),  # Uses 2 registers (40118-40119)
    40120: ("ColdOil", "float", "Cold Oil Temperature (°C)"),  # Uses 2 registers (40120-40121)
    40122: ("HotWater", "float", "Hot Water Temperature (°C)"),  # Uses 2 registers (40122-40123)
    40124: ("HotOil", "float", "Hot Oil Temperature (°C)"),  # Uses 2 registers (40124-40125)
    40126: ("TempWaterTank1", "float", "Water Tank 1 Temperature (°C)"),  # Uses 2 registers (40126-40127)
    40128: ("TempWaterTank2", "float", "Water Tank 2 Temperature (°C)"),  # Uses 2 registers (40128-40129)
    40130: ("TempWaterTank3", "float", "Water Tank 3 Temperature (°C)"),  # Uses 2 registers (40130-40131)
    40132: ("TempWaterTank4", "float", "Water Tank 4 Temperature (°C)"),  # Uses 2 registers (40132-40133)
    40134: ("PumpPrimaryAnalogOuut", "float", "Primary Pump Analog Output"),  # Uses 2 registers (40134-40135
    40136: ("PumpSecondaryAnalogOut", "float", "Secondary Pump Analog Output"),  # Uses 2 registers (40136-40137)
    
    
    # Control Commands (Write-Only)
    40200: ("HeaterCommand", "int", "Heater Command (0=OFF, 1=ON)"),
    40201: ("HeaterMode", "int", "Heater Mode (1-8)"),
    40202: ("SystemReset", "int", "System Reset Command"),
}

def float_from_registers(registers):
    """Convert two 16-bit registers to float"""
    if len(registers) < 2:
        return None
    packed = struct.pack('>HH', registers[0], registers[1])
    return struct.unpack('>f', packed)[0]

def float_to_registers(value):
    """Convert float to two 16-bit registers"""
    packed = struct.pack('>f', float(value))
    high, low = struct.unpack('>HH', packed)
    return [high, low]

class InteractiveModbusClient:
    def __init__(self, host=SERVER_IP, port=SERVER_PORT):
        self.client = None
        self.host = host
        self.port = port
        
    def connect(self):
        """Connect to Modbus server"""
        self.client = ModbusTcpClient(self.host, port=self.port)
        if not self.client.connect():
            print(f"❌ Kann nicht zu {self.host}:{self.port} verbinden!")
            self.port = 5020
            self.client = ModbusTcpClient(self.host, port=self.port)
            if not self.client.connect():
                return False
        print(f"✅ Verbunden mit {self.host}:{self.port}")
        return True
    
    def disconnect(self):
        """Disconnect from server"""
        if self.client:
            self.client.close()
            print("🔌 Verbindung geschlossen")
    
    def read_register(self, address, count=1):
        """Read register(s) at given address"""
        modbus_addr = address - 40001  # Convert to 0-based
        result = self.client.read_holding_registers(modbus_addr, count=count, slave=0)
        if result.isError():
            print(f"❌ Fehler beim Lesen: {result}")
            return None
        return result.registers
    
    def write_register(self, address, value):
        """Write value to register"""
        modbus_addr = address - 40001  # Convert to 0-based
        result = self.client.write_register(modbus_addr, value, slave=0)
        if result.isError():
            print(f"❌ Fehler beim Schreiben: {result}")
            return False
        return True
    
    def write_registers(self, address, values):
        """Write multiple values to consecutive registers"""
        modbus_addr = address - 40001  # Convert to 0-based
        result = self.client.write_registers(modbus_addr, values, slave=0)
        if result.isError():
            print(f"❌ Fehler beim Schreiben: {result}")
            return False
        return True
    
    def interactive_read(self):
        """Interactive read mode"""
        print("\n📖 REGISTER LESEN")
        print("-" * 40)
        
        # Show reference
        show_ref = input("Register-Referenz anzeigen? (j/n): ")
        if show_ref.lower() == 'j':
            self.show_register_reference()
        
        while True:
            try:
                address = input("\nRegister-Adresse (z.B. 40015) oder 'q' zum Beenden: ")
                if address.lower() == 'q':
                    break
                
                address = int(address)
                
                # Get info about register
                if address in REGISTER_INFO:
                    name, dtype, desc = REGISTER_INFO[address]
                    print(f"📋 {name}: {desc}")
                    
                    if dtype == "float":
                        # Read 2 registers for float
                        registers = self.read_register(address, 2)
                        if registers:
                            value = float_from_registers(registers)
                            print(f"✅ Wert: {value:.2f}")
                            print(f"   Raw: {registers}")
                    else:
                        # Read single register for int
                        registers = self.read_register(address, 1)
                        if registers:
                            print(f"✅ Wert: {registers[0]}")
                else:
                    # Unknown register - ask for type
                    dtype = input("Datentyp (int/float): ").lower()
                    if dtype == "float":
                        registers = self.read_register(address, 2)
                        if registers:
                            value = float_from_registers(registers)
                            print(f"✅ Wert: {value:.2f}")
                            print(f"   Raw: {registers}")
                    else:
                        registers = self.read_register(address, 1)
                        if registers:
                            print(f"✅ Wert: {registers[0]}")
                            
            except ValueError:
                print("❌ Ungültige Eingabe!")
            except Exception as e:
                print(f"❌ Fehler: {e}")
    
    def interactive_write(self):
        """Interactive write mode"""
        print("\n✍️  REGISTER SCHREIBEN")
        print("-" * 40)
        print("⚠️  Vorsicht: Schreiben kann das System beeinflussen!")
        
        # Show reference
        show_ref = input("Register-Referenz anzeigen? (j/n): ")
        if show_ref.lower() == 'j':
            self.show_register_reference(write_only=True)
        
        while True:
            try:
                address = input("\nRegister-Adresse (z.B. 40200) oder 'q' zum Beenden: ")
                if address.lower() == 'q':
                    break
                
                address = int(address)
                
                # Get info about register
                if address in REGISTER_INFO:
                    name, dtype, desc = REGISTER_INFO[address]
                    print(f"📋 {name}: {desc}")
                    
                    if dtype == "float":
                        value = float(input("Float-Wert eingeben: "))
                        registers = float_to_registers(value)
                        if self.write_registers(address, registers):
                            print(f"✅ Geschrieben: {value} -> Register {address}-{address+1}")
                    else:
                        value = int(input("Integer-Wert eingeben: "))
                        if self.write_register(address, value):
                            print(f"✅ Geschrieben: {value} -> Register {address}")
                else:
                    # Unknown register
                    dtype = input("Datentyp (int/float): ").lower()
                    if dtype == "float":
                        value = float(input("Float-Wert eingeben: "))
                        registers = float_to_registers(value)
                        if self.write_registers(address, registers):
                            print(f"✅ Geschrieben: {value} -> Register {address}-{address+1}")
                    else:
                        value = int(input("Integer-Wert eingeben: "))
                        if self.write_register(address, value):
                            print(f"✅ Geschrieben: {value} -> Register {address}")
                            
            except ValueError:
                print("❌ Ungültige Eingabe!")
            except Exception as e:
                print(f"❌ Fehler: {e}")
    
    def show_register_reference(self, write_only=False):
        """Show register reference"""
        print("\n📋 REGISTER REFERENZ")
        print("=" * 80)
        print(f"{'Adresse':<8} {'Name':<25} {'Typ':<6} {'Beschreibung':<40}")
        print("-" * 80)
        
        for addr, (name, dtype, desc) in sorted(REGISTER_INFO.items()):
            if write_only and addr < 40100:
                continue
            print(f"{addr:<8} {name:<25} {dtype:<6} {desc:<40}")
    
    def monitor_registers(self):
        """Monitor specific registers continuously"""
        print("\n📡 REGISTER MONITORING")
        print("-" * 40)
        
        registers = []
        print("Gib die zu überwachenden Register ein (getrennt mit Komma)")
        print("Beispiel: 40015,40100,40003")
        input_str = input("Register: ")
        
        try:
            for reg in input_str.split(','):
                addr = int(reg.strip())
                if addr in REGISTER_INFO:
                    registers.append((addr, REGISTER_INFO[addr]))
                else:
                    dtype = input(f"Datentyp für Register {addr} (int/float): ").lower()
                    registers.append((addr, (f"Reg_{addr}", dtype, f"Register {addr}")))
        except:
            print("❌ Ungültige Eingabe!")
            return
        
        interval = int(input("Update-Intervall in Sekunden (Standard: 2): ") or "2")
        
        print(f"\n📡 Überwache {len(registers)} Register (Ctrl+C zum Beenden)")
        print("-" * 60)
        
        try:
            while True:
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"\n[{timestamp}]", end="")
                
                for addr, (name, dtype, desc) in registers:
                    if dtype == "float":
                        regs = self.read_register(addr, 2)
                        if regs:
                            value = float_from_registers(regs)
                            print(f" | {name}: {value:.2f}", end="")
                    else:
                        regs = self.read_register(addr, 1)
                        if regs:
                            print(f" | {name}: {regs[0]}", end="")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n\n✋ Monitoring beendet")

def main():
    print("="*60)
    print("INTERACTIVE MODBUS CLIENT")
    print("="*60)
    
    # Get server address
    host = input(f"Server IP [{SERVER_IP}]: ") or SERVER_IP
    port = input(f"Server Port [{SERVER_PORT}]: ") or SERVER_PORT
    port = int(port)
    
    client = InteractiveModbusClient(host, port)
    
    if not client.connect():
        return
    
    try:
        while True:
            print("\n" + "="*60)
            print("HAUPTMENÜ")
            print("="*60)
            print("1. Register lesen")
            print("2. Register schreiben")
            print("3. Register-Referenz anzeigen")
            print("4. Register überwachen")
            print("5. Quick-Test (alle Sensoren)")
            print("0. Beenden")
            print("-"*60)
            
            choice = input("Wähle Option: ")
            
            if choice == "1":
                client.interactive_read()
            elif choice == "2":
                client.interactive_write()
            elif choice == "3":
                client.show_register_reference()
            elif choice == "4":
                client.monitor_registers()
            elif choice == "5":
                print("\n📊 QUICK TEST - Alle Sensoren")
                print("-" * 60)
                # Read all temperature sensors
                for addr in [40011, 40013, 40015, 40017, 40019, 40021, 40023, 40025]:
                    name, _, desc = REGISTER_INFO[addr]
                    regs = client.read_register(addr, 2)
                    if regs:
                        value = float_from_registers(regs)
                        print(f"{desc:<30}: {value:>6.2f}°C")
                # Read heater status
                regs = client.read_register(40100, 1)
                if regs:
                    print(f"{'Heater Status':<30}: {'ON' if regs[0] == 1 else 'OFF':>6}")
            elif choice == "0":
                break
            else:
                print("❌ Ungültige Option!")
            
            if choice != "0":
                input("\nDrücke Enter zum Fortfahren...")
                
    except Exception as e:
        print(f"\n❌ Fehler: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()