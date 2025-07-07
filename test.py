from pymodbus.client import ModbusTcpClient

client = ModbusTcpClient('localhost', port=5020)
if client.connect():
    # WICHTIG: slave=1 verwenden!
    result = client.write_register(address=10, value=12345, slave=1)
    print(f"Result: {result}")
    client.close()