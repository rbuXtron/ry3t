def calculate_temperatures(outside_temperature):
    """
    Calculate the supply_temperature and return_temperature based on the given outside_temperature.

    Parameters:
    outside_temperature (float): The outside temperature in degrees Celsius.

    Returns:
    tuple: A tuple containing the supply_temperature and return_temperature.
    """
    # Calculate supply_temperature
    supply_temperature = (-3/4) * outside_temperature + 34
    
    # Calculate return_temperature
    return_temperature = (-3/8) * outside_temperature + 27
    
    # return supply_temperature, return_temperature

    # test in horn, just heat to 40 / 30
    return supply_temperature, return_temperature