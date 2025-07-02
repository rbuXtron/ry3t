#!/usr/bin/env python3
"""
Korrigierter Modbus TCP Client
"""

from pymodbus.client import ModbusTcpClient
import time
import sys


" IP-Adresse zu einem Server angeben"
def test_connection(host='localhost', port=5020):
    """Testet die Verbindung zum Server"""
    print(f"\nTeste Verbindung zu {host}:{port}...")
    
    client = ModbusTcpClient(host, port=port)
    
    # Mehrere Verbindungsversuche
    for attempt in range(3):
        if client.connect():
            print(f"✓ Verbindung erfolgreich!")
            return client
        print(f"✗ Verbindungsversuch {attempt + 1} fehlgeschlagen")
        time.sleep(1)
    
    print(f"\nFEHLER: Konnte keine Verbindung herstellen!")
    print("Mögliche Ursachen:")
    print("1. Server läuft nicht")
    print("2. Falscher Port")
    print("3. Firewall blockiert")
    print(f"4. Server läuft nicht auf {host}:{port}")
    return None

def main():
    # Verbindung testen
    client = test_connection()
    if not client:
        sys.exit(1)
    
    print("\nStarte Datenabfrage...\n")
    
    try:
        while True:
            # Test ob Verbindung noch steht
            if not client.is_socket_open():
                print("Verbindung verloren! Versuche neu zu verbinden...")
                client = test_connection()
                if not client:
                    break
            
            try:
                # Holding Register 0-4 lesen
                result = client.read_holding_registers(address=0, count=5)
                if hasattr(result, 'registers'):
                    print(f"Holding Register 0-4: {result.registers}")
                else:
                    print(f"Fehler beim Lesen: {result}")
                
                # Counter und dynamische Werte
                result = client.read_holding_registers(address=10, count=2)
                if hasattr(result, 'registers'):
                    print(f"Register 10 (Counter): {result.registers[0]}")
                
                # Coils lesen
                result = client.read_coils(address=0, count=5)
                if hasattr(result, 'bits'):
                    print(f"Coils 0-4: {result.bits[:5]}")
                
                # Toggle Coil
                result = client.read_coils(address=10, count=1)
                if hasattr(result, 'bits'):
                    print(f"Coil 10 (Toggle): {result.bits[0]}")
                
                print("-" * 50)
                time.sleep(2)
                
            except Exception as e:
                print(f"Kommunikationsfehler: {e}")
                time.sleep(1)
                
    except KeyboardInterrupt:
        print("\n\nProgramm wird beendet...")
    finally:
        if client:
            client.close()
        print("Client beendet.")

if __name__ == "__main__":
    main()