#!/usr/bin/env python3
"""
Modbus TCP Server mit Debug für pymodbus 3.x
"""

from pymodbus.server import StartTcpServer
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock
from pymodbus.datastore import ModbusSlaveContext, ModbusServerContext
import threading
import time
import logging
from datetime import datetime

# Detailliertes Logging einrichten
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger('ModbusServer')

class MonitoringModbusSlaveContext(ModbusSlaveContext):
    """Erweiterte SlaveContext-Klasse mit Monitoring"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.access_count = {
            'read_coils': 0,
            'read_discrete_inputs': 0,
            'read_holding_registers': 0,
            'read_input_registers': 0,
            'write_coils': 0,
            'write_registers': 0
        }
        self.last_access = None
    
    def log_access(self, fx_name, address, count=1, values=None):
        """Loggt jeden Zugriff"""
        self.last_access = datetime.now()
        timestamp = self.last_access.strftime("%H:%M:%S.%f")[:-3]
        
        if 'read' in fx_name:
            self.access_count[fx_name] = self.access_count.get(fx_name, 0) + 1
            # Verwende super() um Rekursion zu vermeiden
            current_values = super().getValues(self._get_fx_code(fx_name), address, count)
            log.info(f"[{timestamp}] READ: {fx_name} - Adresse: {address}, Anzahl: {count}, Werte: {current_values}")
        else:
            self.access_count[fx_name] = self.access_count.get(fx_name, 0) + 1
            log.info(f"[{timestamp}] WRITE: {fx_name} - Adresse: {address}, Werte: {values}")
    
    def _get_fx_code(self, fx_name):
        """Mappt Funktionsnamen zu Funktionscodes"""
        mapping = {
            'read_coils': 1,
            'read_discrete_inputs': 2,
            'read_holding_registers': 3,
            'read_input_registers': 4,
            'write_coils': 1,
            'write_registers': 3
        }
        return mapping.get(fx_name, 3)
    
    def getValues(self, fx, address, count=1):
        """Überschreibt getValues mit Logging"""
        fx_names = {1: 'read_coils', 2: 'read_discrete_inputs', 
                   3: 'read_holding_registers', 4: 'read_input_registers'}
        if fx in fx_names:
            self.log_access(fx_names[fx], address, count)
        return super().getValues(fx, address, count)
    
    def setValues(self, fx, address, values):
        """Überschreibt setValues mit Logging"""
        fx_names = {1: 'write_coils', 3: 'write_registers'}
        if fx in fx_names:
            self.log_access(fx_names[fx], address, len(values), values)
        return super().setValues(fx, address, values)

def create_debug_server():
    """Erstellt den Debug Server"""
    # Monitoring SlaveContext verwenden
    store = MonitoringModbusSlaveContext(
        di=ModbusSequentialDataBlock(0, [0]*100),
        co=ModbusSequentialDataBlock(0, [0]*100),
        hr=ModbusSequentialDataBlock(0, [0]*100),
        ir=ModbusSequentialDataBlock(0, [0]*100)
    )
    context = ModbusServerContext(slaves=store, single=True)
    
    # Beispielwerte setzen
    log.info("Initialisiere Beispielwerte...")
    store.setValues(3, 0, [1234, 5678, 42, 100, 200])
    store.setValues(1, 0, [1, 0, 1, 0, 1])
    
    return context, store

def update_values(store, running):
    """Update-Thread für dynamische Werte"""
    counter = 0
    while running['active']:
        try:
            # Counter in Register 10 (ohne Logging)
            values = store.store['h'].getValues(10, 1)
            store.store['h'].setValues(10, [counter % 65536])
            
            # Sinus-Wert in Register 11
            import math
            sin_value = int((math.sin(counter * 0.1) + 1) * 50)
            store.store['h'].setValues(11, [sin_value])
            
            # Toggle Coil 10
            current = store.store['c'].getValues(10, 1)[0]
            store.store['c'].setValues(10, [1 - current])
            
            counter += 1
            time.sleep(1)
        except Exception as e:
            log.error(f"Update-Fehler: {e}")

def print_statistics(store, running):
    """Zeigt Statistiken an"""
    while running['active']:
        time.sleep(10)  # Alle 10 Sekunden
        print("\n" + "="*60)
        print(f"SERVER STATISTIKEN - {datetime.now().strftime('%H:%M:%S')}")
        print("="*60)
        print("Zugriffe:")
        for func, count in store.access_count.items():
            if count > 0:
                print(f"  {func}: {count}")
        if store.last_access:
            print(f"\nLetzter Zugriff: {store.last_access.strftime('%H:%M:%S')}")
        print("="*60 + "\n")

def main():
    """Hauptfunktion"""
    HOST = '0.0.0.0'
    PORT = 5020
    
    # Server erstellen
    context, store = create_debug_server()
    
    # Identity
    identity = ModbusDeviceIdentification()
    identity.VendorName = 'Debug Test Server'
    identity.ProductCode = 'DTS'
    identity.MajorMinorRevision = '1.0'
    
    # Running Flag (dict damit es in Threads geändert werden kann)
    running = {'active': True}
    
    # Threads starten
    update_thread = threading.Thread(target=update_values, args=(store, running))
    update_thread.daemon = True
    update_thread.start()
    
    stats_thread = threading.Thread(target=print_statistics, args=(store, running))
    stats_thread.daemon = True
    stats_thread.start()
    
    try:
        print("\n" + "="*60)
        print(f"MODBUS TCP SERVER MIT DEBUG")
        print("="*60)
        print(f"Adresse: {HOST}:{PORT}")
        print(f"Gestartet: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\nVerfügbare Register:")
        print("  - Holding Register 0-99 (FC 3/6/16)")
        print("  - Input Register 0-99 (FC 4)")
        print("  - Coils 0-99 (FC 1/5/15)")
        print("  - Discrete Inputs 0-99 (FC 2)")
        print("\nVoreingestellte Werte:")
        print("  - HR[0-4]: [1234, 5678, 42, 100, 200]")
        print("  - HR[10]: Counter (0-65535)")
        print("  - HR[11]: Sinus-Wert (0-100)")
        print("  - Coil[0-4]: [1, 0, 1, 0, 1]")
        print("  - Coil[10]: Toggle jede Sekunde")
        print("\nAlle Client-Zugriffe werden geloggt!")
        print("="*60 + "\n")
        
        log.info(f"Starte Server auf {HOST}:{PORT}")
        
        # Server starten (blockiert)
        StartTcpServer(
            context=context,
            identity=identity,
            address=(HOST, PORT)
        )
        
    except KeyboardInterrupt:
        print("\nServer wird beendet...")
        running['active'] = False
    except Exception as e:
        log.error(f"Server-Fehler: {e}")
        running['active'] = False

if __name__ == "__main__":
    main()