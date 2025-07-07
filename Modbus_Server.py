#!/usr/bin/env python3
"""
Finaler Modbus-MQTT Bridge Server
=================================
Verwendet bewährte Methoden für pymodbus 3.9.2
"""

from pymodbus.server import StartTcpServer
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext
from pymodbus.datastore.store import BaseModbusDataBlock
import threading
import time
import struct
import json
import logging
from datetime import datetime
from pathlib import Path
import sys

# Farben
try:
    from colorama import init, Fore, Style
    init()
except:
    class Fore:
        GREEN = YELLOW = CYAN = RED = MAGENTA = BLUE = ''
    class Style:
        RESET_ALL = ''

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


class MonitoredDataBlock(BaseModbusDataBlock):
    """Custom DataBlock der alle Änderungen tracked"""
    
    def __init__(self, values):
        """Initialize with values"""
        self.values = values if values else [0] * 510
        self.access_count = 0
        self.write_count = 0
        
        # JSON setup
        self.data_dir = Path("modbus_mqtt_data")
        self.data_dir.mkdir(exist_ok=True)
        self.json_file = self.data_dir / "modbus_to_mqtt.json"
        
        # Float mappings
        self.float_mappings = {
            0: {'name': 'solar_power', 'unit': 'W'},
            2: {'name': 'solar_voltage', 'unit': 'V'},
            4: {'name': 'solar_current', 'unit': 'A'},
            6: {'name': 'solar_energy_today', 'unit': 'kWh'},
            8: {'name': 'solar_energy_total', 'unit': 'kWh'},
            20: {'name': 'temp_outside', 'unit': '°C'},
            22: {'name': 'temp_inside', 'unit': '°C'},
            24: {'name': 'humidity', 'unit': '%'},
            26: {'name': 'pressure', 'unit': 'hPa'},
            40: {'name': 'battery_voltage', 'unit': 'V'},
            42: {'name': 'battery_current', 'unit': 'A'},
            44: {'name': 'battery_soc', 'unit': '%'},
            46: {'name': 'battery_temp', 'unit': '°C'}
        }
        
        # Start monitoring thread
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        print(f"{Fore.GREEN}MonitoredDataBlock initialisiert{Style.RESET_ALL}")
    
    def validate(self, address, count):
        """Validate address range"""
        if address < 0 or address + count > len(self.values):
            return False
        return True
    
    def getValues(self, address, count=1):
        """Get values from datablock"""
        self.access_count += 1
        if not self.validate(address, count):
            return []
        return self.values[address:address + count]
    
    def setValues(self, address, values):
        """Set values in datablock - WICHTIGSTE METHODE"""
        if not self.validate(address, len(values)):
            return
        
        self.write_count += 1
        timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        
        # Log EVERY write operation
        print(f"\n{Fore.GREEN}=== WRITE #{self.write_count} ==={Style.RESET_ALL}")
        print(f"Zeit: {timestamp}")
        print(f"Adresse: {address}, Anzahl: {len(values)}")
        print(f"Werte: {values}")
        
        # Update values and track changes
        for i, value in enumerate(values):
            addr = address + i
            old_value = self.values[addr]
            self.values[addr] = value
            
            if old_value != value:
                print(f"{Fore.CYAN}Register {addr:3d}: {old_value} → {value}{Style.RESET_ALL}")
                
                # Check if part of float
                self._check_float_change(addr)
        
        # Always save to JSON after changes
        self._save_json()
        print(f"{Fore.GREEN}=== WRITE COMPLETE ==={Style.RESET_ALL}\n")
    
    def _check_float_change(self, address):
        """Check if address is part of a float register pair"""
        # Check if it's the start of a float
        if address in self.float_mappings:
            self._print_float(address)
        
        # Check if it's the second register of a float
        if address > 0 and (address - 1) in self.float_mappings:
            self._print_float(address - 1)
    
    def _print_float(self, start_addr):
        """Print float value"""
        if start_addr + 1 < len(self.values):
            try:
                bytes_data = struct.pack('>HH', self.values[start_addr], self.values[start_addr + 1])
                float_val = struct.unpack('>f', bytes_data)[0]
                mapping = self.float_mappings[start_addr]
                print(f"  {Fore.MAGENTA}→ {mapping['name']}: {float_val:.2f} {mapping['unit']}{Style.RESET_ALL}")
            except:
                pass
    
    def _save_json(self):
        """Save current state to JSON"""
        try:
            # Collect all float values
            float_values = {}
            for addr, mapping in self.float_mappings.items():
                if addr + 1 < len(self.values):
                    try:
                        bytes_data = struct.pack('>HH', self.values[addr], self.values[addr + 1])
                        float_val = struct.unpack('>f', bytes_data)[0]
                        if float_val != 0.0:
                            float_values[mapping['name']] = {
                                'value': float_val,
                                'unit': mapping['unit'],
                                'address': addr,
                                'timestamp': datetime.now().isoformat()
                            }
                    except:
                        pass
            
            # Collect all non-zero registers
            non_zero = {}
            for i in range(min(255, len(self.values))):
                if self.values[i] != 0:
                    non_zero[str(i)] = self.values[i]
            
            # Create JSON structure
            data = {
                'timestamp': datetime.now().isoformat(),
                'statistics': {
                    'total_writes': self.write_count,
                    'total_reads': self.access_count
                },
                'float_values': float_values,
                'raw_registers': non_zero
            }
            
            # Write to file
            with open(self.json_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            print(f"{Fore.GREEN}✓ JSON gespeichert: {self.json_file}{Style.RESET_ALL}")
            
        except Exception as e:
            print(f"{Fore.RED}JSON Fehler: {e}{Style.RESET_ALL}")
    
    def _monitor_loop(self):
        """Background monitoring thread"""
        print(f"{Fore.YELLOW}Monitor-Thread gestartet{Style.RESET_ALL}")
        last_write_count = 0
        
        while self.monitoring:
            time.sleep(30)  # Check every 30 seconds
            
            if self.write_count > last_write_count:
                print(f"\n{Fore.YELLOW}[MONITOR] {self.write_count - last_write_count} neue Writes{Style.RESET_ALL}")
                last_write_count = self.write_count
                
                # Show some current values
                print("Aktuelle Werte (nicht null):")
                count = 0
                for i in range(50):
                    if self.values[i] != 0:
                        print(f"  Reg[{i:3d}] = {self.values[i]}")
                        count += 1
                        if count >= 10:
                            print("  ...")
                            break


def run_server(host='0.0.0.0', port=5020):
    """Run the Modbus server"""
    
    host = '127.0.0.1'
    
    print("\n" + "="*70)
    print(f"{Fore.GREEN}MODBUS-MQTT BRIDGE SERVER (FINAL VERSION){Style.RESET_ALL}")
    print("="*70)
    print(f"Host: {host}:{port}")
    print(f"Slave ID: 1")
    print(f"Daten: modbus_mqtt_data/modbus_to_mqtt.json")
    print("="*70)
    
    # Create the custom datablock
    datablock = MonitoredDataBlock([0] * 510)
    
    # Create slave context for slave ID 1
    slaves = {
        1: ModbusSlaveContext(
            di=None,
            co=None,
            hr=datablock,
            ir=None
        )
    }
    
    # Create server context
    context = ModbusServerContext(slaves=slaves, single=False)
    
    # Server identity
    identity = ModbusDeviceIdentification()
    identity.VendorName = 'Final Bridge'
    identity.ProductCode = 'FB'
    identity.ProductName = 'Modbus-MQTT Bridge'
    identity.ModelName = 'Final 1.0'
    
    # Status thread
    def status_thread():
        time.sleep(5)  # Initial delay
        while True:
            print(f"\n{Fore.YELLOW}=== STATUS ==={Style.RESET_ALL}")
            print(f"Total Writes: {datablock.write_count}")
            print(f"Total Reads: {datablock.access_count}")
            print(f"Letzte Aktivität: {datetime.now().strftime('%H:%M:%S')}")
            print("="*30 + "\n")
            time.sleep(60)
    
    threading.Thread(target=status_thread, daemon=True).start()
    
    print(f"\n{Fore.CYAN}Server bereit!{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Warte auf Modbus-Clients (Slave ID = 1)...{Style.RESET_ALL}")
    print(f"\n{Fore.GREEN}Client-Beispiel:{Style.RESET_ALL}")
    print("  client.write_register(address=10, value=12345, slave=1)")
    print("  client.write_registers(address=20, values=[111, 222], slave=1)\n")
    
    try:
        # Start the server
        StartTcpServer(
            context=context,
            identity=identity,
            address=(host, port)
        )
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Server wird beendet...{Style.RESET_ALL}")
        datablock.monitoring = False
        print(f"Finale Statistik: {datablock.write_count} Writes, {datablock.access_count} Reads")
        print(f"{Fore.GREEN}Server beendet.{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}Server Fehler: {e}{Style.RESET_ALL}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Parse arguments
    host = sys.argv[1] if len(sys.argv) > 1 else '127.0.0.1'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5020
    
    run_server(host, port)