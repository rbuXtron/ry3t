from pymodbus.client import ModbusTcpClient

client = ModbusTcpClient('localhost', port=5020)
client.connect()

# Zeige alle Methoden
print("Methoden:")
for method in dir(client):
    if not method.startswith('_'):
        print(f"  - {method}")

# Zeige die Signatur
import inspect
print("\nread_holding_registers Signatur:")
print(inspect.signature(client.read_holding_registers))

client.close()