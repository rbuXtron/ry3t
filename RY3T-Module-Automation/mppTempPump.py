################################################################################
# mpptracking für Temperatur und Pumpen
################################################################################

import requests
import json
import time
import pyads

################################################################################
# Kontakt zur FEB- Steuerung mit API- Variablen
status_url = "http://192.168.1.200/status.asp"
set_url = "http://192.168.1.200/"
url = "http://192.168.1.200/goform/ReadWrite"

shutdown = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]

second_pump_A = "M0600_LIC001_E001_PU03_XS01_API"
second_pump_B = "M0600_LIC001_E002_PU03_XS01_API"
prim_pump_1 = "M0600_LIC001_E001_PU01_XS01_API"
prim_pump_2 = "M0600_LIC001_E001_PU02_XS01_API"
prim_pump_3 = "M0600_LIC001_E002_PU01_XS01_API"
prim_pump_4 = "M0600_LIC001_E002_PU02_XS01_API"

switch_on_all_A = ["M0600_LIC001_E001_AG01_SB01_API_set_1.asp","M0600_LIC001_E001_AG01_SB02_API_set_1.asp",
                 "M0600_LIC001_E001_AG01_SB03_API_set_1.asp", "M0600_LIC001_E001_AG01_SB04_API_set_1.asp",
                 "M0600_LIC001_E001_AG01_SB05_API_set_1.asp", "M0600_LIC001_E001_AG01_SB06_API_set_1.asp",
                 "M0600_LIC001_E001_AG01_SB07_API_set_1.asp", "M0600_LIC001_E001_AG01_SB08_API_set_1.asp"],
switch_on_all_B =["M0600_LIC001_E002_AG01_SB01_API_set_1.asp","M0600_LIC001_E002_AG01_SB02_API_set_1.asp",
                 "M0600_LIC001_E002_AG01_SB03_API_set_1.asp", "M0600_LIC001_E002_AG01_SB04_API_set_1.asp",
                 "M0600_LIC001_E002_AG01_SB05_API_set_1.asp", "M0600_LIC001_E002_AG01_SB06_API_set_1.asp",
                 "M0600_LIC001_E002_AG01_SB07_API_set_1.asp", "M0600_LIC001_E002_AG01_SB08_API_set_1.asp"]
switch_off_all_A = ["M0600_LIC001_E001_AG01_SB01_API_set_0.asp","M0600_LIC001_E001_AG01_SB02_API_set_0.asp",
                 "M0600_LIC001_E001_AG01_SB03_API_set_0.asp", "M0600_LIC001_E001_AG01_SB04_API_set_0.asp",
                 "M0600_LIC001_E001_AG01_SB05_API_set_0.asp", "M0600_LIC001_E001_AG01_SB06_API_set_0.asp",
                 "M0600_LIC001_E001_AG01_SB07_API_set_0.asp", "M0600_LIC001_E001_AG01_SB08_API_set_0.asp"]
switch_off_all_B =["M0600_LIC001_E002_AG01_SB01_API_set_0.asp","M0600_LIC001_E002_AG01_SB02_API_set_0.asp",
                 "M0600_LIC001_E002_AG01_SB03_API_set_0.asp", "M0600_LIC001_E002_AG01_SB04_API_set_0.asp",
                 "M0600_LIC001_E002_AG01_SB05_API_set_0.asp", "M0600_LIC001_E002_AG01_SB06_API_set_0.asp",
                 "M0600_LIC001_E002_AG01_SB07_API_set_0.asp", "M0600_LIC001_E002_AG01_SB08_API_set_0.asp"]

################################################################################
# Globale- Variablen
#shutdown_all = False
shutdown_all_arr = [False,False,False,False]
shutdown_all_all = True
shutdown_all_start = False
counter = 0
DeltaT_inletA_1: float = 0
DeltaT_inletA_2: float = 0
DeltaT_inletB_1: float = 0
DeltaT_inletB_2: float = 0

OutletA_1: float = 0
OutletA_2: float = 0
OutletA_3: float = 0
OutletA_4: float = 0
OutletB_1: float = 0
OutletB_2: float = 0
OutletB_3: float = 0
OutletB_4: float = 0

TrendA_main: float = 0
TransB_main: float = 0
Trend_outlet_1: float = 0
Trend_outlet_2: float = 0
Trend_outlet_3: float = 0
Trend_outlet_4: float = 0

timer_A: bool = False
timer_B: bool = False



################################################################################
# Connect Beckhoff read and write via ADS- Variablen
# 4 - 20mA für Leistung 600kW
# 20mA ensprechen 100% wobei die max. Frequenz mit 45% gedeckelt (810Hz)
# die Frequenzsteuerung wird über das Programm s19_pro.py geregelt
# ----------------------------------------------------------------
# dieses Programm regelt vor allem die Temperatuen und die ensprechenden Pumpenleistungen
# falls die geforderte Leistung unter 6mA (40kW) fällt stellt die Anlage ab
# ---------------------------------------------------------------- 
def AdsConect(set_volt):
    ads_net_id="5.59.200.206.1.1"
    power_set = [0,0,0,0]
    
    try:
        plc=pyads.Connection(ads_net_id,pyads.PORT_TC3PLC1)
        plc.open()
        print("Connecting to TwinCAT PLC..")
        print("Current connection status:",plc.is_open)
        print("Current Status:",plc.read_state())
        print("Reading Devices List..")
        varList=["MAIN.PWD_SETTING.Power_percent_1","MAIN.PWD_SETTING.Power_percent_2","MAIN.PWD_SETTING.Power_percent_3","MAIN.PWD_SETTING.Power_percent_4"]
        vardata=plc.read_list_by_name(varList)
        for k , v in vardata.items():
            print(str(k) + " : " + str(v))
            if k == "MAIN.PWD_SETTING.Power_percent_1":
                power_set[0] = v
            elif k == "MAIN.PWD_SETTING.Power_percent_2":
                power_set[1] = v
            elif k == "MAIN.PWD_SETTING.Power_percent_3":
                power_set[2] = v
            elif k == "MAIN.PWD_SETTING.Power_percent_4":
                power_set[3] = v

        print("Power_Volt :" + str(set_volt))
        power_gesamt = plc.read_by_name('MAIN.ModbusTCP.power_gesamt')
        set_volt = 5*(power_gesamt/600000)
        plc.write_by_name('MAIN.SPANNUNG.tmp_volt', set_volt)
        volt = plc.read_by_name('MAIN.SPANNUNG.tmp_volt')
        print("volt : " + str(volt))
        print("Closing the Connections..")
        plc.close()
        print("Current Status:",plc.is_open)
        return power_set
    except:
        print("Unable connecting to TwinCAT PLC..")
        return [100,100,100,100]

################################################################################
# API set Pump_RPM
def MppSec(variable, value):
    print("Mpp traking auf Pumpe : " + str(variable))
    data = 'redirect=status.asp&variable=' + str(variable) + '&value=' + str(value) + '&write=Write'
    try:
        response = requests.post(url, data = data)
        print(str(response))
        print("mpp traking to : " + str(value))
    except:
        print("unable mpp traking to : " + str(variable))
        
################################################################################
# Connect to read and write Beckhoff ADS 
def StatusConnect():
    file_json = open('setting.json', "r")
    data_json = json.loads(file_json.read())
    deltaT_prim_sec_In_A = float(data_json['deltaT_primIn_A'])
    deltaT_prim_sec_In_B = float(data_json['deltaT_primIn_B'])
    tube_outlet_01 = float(data_json['tube_outlet_01'])
    tube_outlet_02 = float(data_json['tube_outlet_02'])
    tube_outlet_03 = float(data_json['tube_outlet_03'])
    tube_outlet_04 = float(data_json['tube_outlet_04'])
    file_json.close()
    
    return(deltaT_prim_sec_In_A, deltaT_prim_sec_In_B, tube_outlet_01, tube_outlet_02, tube_outlet_03, tube_outlet_04)    

################################################################################
# switch off or on system
def switch_on_off(pool):
    print("pool : " + str(pool))
    for command in pool:
        setcommand = set_url + command 
        response = requests.get(url = setcommand)
        print("SwitchCommand : " + str(setcommand) + " response : " + str(response))
    time.sleep(10)

################################################################################
# get status from FEB
def getStatus():
        response = requests.get(url = status_url, stream= True)
        out = response.text
        out = out.replace("</code></pre></body></html>","")
        out = out.replace("<html><head></head><body><pre><code>\n","")
        out = out.replace(",\n}","\n}")
        js_res = json.loads(out)
        return js_res
    
################################################################################
# main programm
while True:
    heat_power = 0
    try:
        js_res = getStatus()
        
        inletTemp_1 = js_res["M0600_LIC001_E010_MG07_TF01"]
        inletTemp_2 = js_res["M0600_LIC001_E010_MG04_TF01"]
        inletTemp_3 = js_res["M0600_LIC001_E030_MG04_TF01"]
        inletTemp_4 = js_res["M0600_LIC001_E030_MG07_TF01"]
        outletTemp_1 = js_res["M0600_LIC001_E010_MG03_TF01"]
        outletTemp_2 = js_res["M0600_LIC001_E010_MG06_TF01"]
        outletTemp_3 = js_res["M0600_LIC001_E030_MG03_TF01"]
        outletTemp_4 = js_res["M0600_LIC001_E030_MG06_TF01"]

        shutdown_all_arr[0] = bool(int(js_res["M0600_LIC001_E001_AG01_SB01"]))
        shutdown_all_arr[1] = bool(int(js_res["M0600_LIC001_E001_AG05_SB01"]))
        shutdown_all_arr[2] = bool(int(js_res["M0600_LIC001_E002_AG01_SB01"]))
        shutdown_all_arr[3] = bool(int(js_res["M0600_LIC001_E002_AG05_SB01"]))
        
        power_prim_pump_1 = float(js_res["M0600_LIC001_E001_PU01_SW01_API"])
        power_prim_pump_2 = float(js_res["M0600_LIC001_E001_PU02_SW01_API"])
        power_prim_pump_3 = float(js_res["M0600_LIC001_E002_PU01_SW01_API"])
        power_prim_pump_4 = float(js_res["M0600_LIC001_E002_PU02_SW01_API"])
        
        power_sec_pump_A = float(js_res["M0600_LIC001_E001_PU03_SW01_API"])
        power_sec_pump_B = float(js_res["M0600_LIC001_E002_PU03_SW01_API"])

        
        outletTemp_main  = float(js_res["M0600_LIC001_EZ01_T1_API"])  
        inletTemp_main = float(js_res["M0600_LIC001_EZ01_T2_API"])

        
        inletTemp_sec_A = float(js_res["M0600_LIC001_E010_MG01_TF01"])
        inletTemp_sec_B = float(js_res["M0600_LIC001_E030_MG01_TF01"])
        outletTemp_sec_A1 = float(js_res["M0600_LIC001_E010_MG02_TF01"])
        outletTemp_sec_A2 = float(js_res["M0600_LIC001_E010_MG05_TF01"])
        outletTemp_sec_B1 = float(js_res["M0600_LIC001_E030_MG02_TF01"])
        outletTemp_sec_B2 = float(js_res["M0600_LIC001_E030_MG05_TF01"])
        
        #################################################################
        # cube 1    
        shutdown[0] = bool(int(js_res["M0600_LIC001_E001_AG01_SB01"]))
        shutdown[1] = bool(int(js_res["M0600_LIC001_E001_AG02_SB01"]))
        shutdown[2] = bool(int(js_res["M0600_LIC001_E001_AG03_SB01"]))
        shutdown[3] = bool(int(js_res["M0600_LIC001_E001_AG04_SB01"]))
        #################################################################
        # cube 2         
        shutdown[4] = bool(int(js_res["M0600_LIC001_E001_AG05_SB01"]))
        shutdown[5] = bool(int(js_res["M0600_LIC001_E001_AG06_SB01"]))
        shutdown[6] = bool(int(js_res["M0600_LIC001_E001_AG07_SB01"]))
        shutdown[7] = bool(int(js_res["M0600_LIC001_E001_AG08_SB01"]))
        #################################################################
        # cube 3         
        shutdown[8] = bool(int(js_res["M0600_LIC001_E002_AG01_SB01"]))
        shutdown[9] = bool(int(js_res["M0600_LIC001_E002_AG02_SB01"]))
        shutdown[10] = bool(int(js_res["M0600_LIC001_E002_AG03_SB01"]))
        shutdown[11] = bool(int(js_res["M0600_LIC001_E002_AG04_SB01"]))
        #################################################################
        # cube 4         
        shutdown[12] = bool(int(js_res["M0600_LIC001_E002_AG05_SB01"]))
        shutdown[13] = bool(int(js_res["M0600_LIC001_E002_AG06_SB01"]))
        shutdown[14] = bool(int(js_res["M0600_LIC001_E002_AG07_SB01"]))
        shutdown[15] = bool(int(js_res["M0600_LIC001_E002_AG08_SB01"]))  
        
        heat_power = int(float(js_res["M0600_LIC001_EZ01_API_Power"]))
        
    except:
        print("unable gat data from FEB")
        inletTemp_main = 55.55        

    set_volt = 5*(float(heat_power/300000))
    power_setting_SPS = AdsConect(set_volt)
    percent = round(100/16*((power_setting_SPS[0])-4),0)
    percent_1 = power_setting_SPS[1]
    print(" Power prozent : " + str(percent_1))
    
    ################################################################################
    # shut down if Condition to hot or lower 8mA
    #0if outletTemp_1 > 62 or outletTemp_2 > 62:
    #    switch_on_off(switch_off_all_A)
            
    if inletTemp_main > 49.5 or percent < 8:
        print("Temperatur inletTemp_main to hight : " + str(inletTemp_main) + " or percent to low :" + str(percent))
        switch_on_off(switch_off_all_A)
        switch_on_off(switch_off_all_B)
        print("shutdown Array : " + str(shutdown_all_arr))
        if power_sec_pump_B > 23.0:
            MppSec(second_pump_B, str(23))
        if power_sec_pump_A > 23.0:
            MppSec(second_pump_A, str(23))
        if shutdown_all_arr[0] == True:
            MppSec(prim_pump_1, str(0.5))
        if shutdown_all_arr[1] == True:
            MppSec(prim_pump_2, str(0.5))
        if shutdown_all_arr[2] == True:
            MppSec(prim_pump_3, str(0.5))
        if shutdown_all_arr[3] == True:
            MppSec(prim_pump_4, str(0.5))
        time.sleep(45)
        shutdown_all = False
        continue
    ################################################################################
    # power on if Temperatur Condition low or higher 8mA    
    elif inletTemp_main < 44.0 and percent > 8:
        shutdown_all_all = True
        print("Inlet Temperatur Main : " + str(inletTemp_main)+ " percent : " + str(percent))
        
        ######################################
        # Regelung von Hand
        ######################################
        #shutdown_all = True
        
        if shutdown_all_start == True:
            #switch_on_off(switch_on_all_A)
            #switch_on_off(switch_off_all_A)
            switch_on_off(switch_on_all_B)
            
            ########################################################################
            # die commands werden solange ausgeführt bis die pumpen die geförterte
            # Leistung meldet
            while power_sec_pump_A < 25.0:
                print(" second_pump_A : " + str(power_sec_pump_A))
                MppSec(second_pump_A, str(75))
                time.sleep(10)
                js_res = getStatus()
                power_sec_pump_A = float(js_res["M0600_LIC001_E001_PU03_SW01_API"])
                if shutdown[0] == False:
                    continue
            while power_prim_pump_1 < 25.0:
                print(" prim_pump_1 : " + str(power_prim_pump_1))
                MppSec(prim_pump_1, str(75))
                time.sleep(10)
                js_res = getStatus()
                power_prim_pump_1 = float(js_res["M0600_LIC001_E001_PU01_SW01_API"])
                if shutdown[0] == False:
                    continue
            while power_prim_pump_2 < 25.0:
                print(" prim_pump_2 : " + str(power_prim_pump_2))
                MppSec(prim_pump_1, str(75))
                time.sleep(10)
                js_res = getStatus()
                power_prim_pump_2 = float(js_res["M0600_LIC001_E001_PU02_SW01_API"])
                if shutdown[4] == False:
                    continue
            while power_sec_pump_B < 25.0:
                print(" second_pump_B : " + str(power_sec_pump_B))
                MppSec(second_pump_B, str(75))
                time.sleep(10)
                js_res = getStatus()
                power_sec_pump_B = float(js_res["M0600_LIC001_E002_PU03_SW01_API"])
                if shutdown[12] == False:
                    continue    
            while power_prim_pump_3 < 25.0:
                print(" prim_pump_3 : " + str(power_prim_pump_3))
                MppSec(prim_pump_3, str(75))
                time.sleep(10)
                js_res = getStatus()
                power_prim_pump_3 = float(js_res["M0600_LIC001_E002_PU01_SW01_API"])
                if shutdown[8] == False:
                    continue
            while power_prim_pump_4 < 25.0:    
                print(" prim_pump_4 : " + str(power_prim_pump_4))
                MppSec(prim_pump_4, str(75))
                time.sleep(10)
                js_res = getStatus()
                power_prim_pump_4 = float(js_res["M0600_LIC001_E002_PU02_SW01_API"])
                if shutdown[12] == False:
                    continue    
            time.sleep(10)
        print("Temp Sec_Inlet cold : " + str(inletTemp_main))
    elif inletTemp_main < 49.0 and percent > 7:
        if not shutdown[8]:
            print("Spülen.....")
            switch_on_off(["M0600_LIC001_E002_AG01_SB01_API_set_1.asp"])
            MppSec(second_pump_A, str(55))
            MppSec(prim_pump_3, str(100))
            MppSec(prim_pump_4, str(100))
            MppSec(second_pump_B, str(100))
            shutdown_all_all = False           
    else:
        if power_sec_pump_B > 23.0:
            MppSec(second_pump_B, str(23))
        if power_sec_pump_A > 23.0:
            MppSec(second_pump_A, str(23))
        print("Temp Sec_Inlet OK : " + str(inletTemp_main))
        shutdown_all_all = False
        
        
    ################################################################################
    # trend calculation

        
    if (float(inletTemp_1)) >= (float(inletTemp_2)):     
        inlet_prim_A = (float(inletTemp_1))
    else:
        inlet_prim_A = (float(inletTemp_2))
            
    if (float(inletTemp_3)) >= (float(inletTemp_4)):     
        inlet_prim_B = (float(inletTemp_3))
    else:
        inlet_prim_B = (float(inletTemp_3))

    (deltaT_primIn_A, deltaT_primIn_B, tube_outlet_01,tube_outlet_02,tube_outlet_03,tube_outlet_04) = StatusConnect()
    counter = counter + 1 
    print("\n\n###########################################")
    if counter == 1:
        DeltaT_inletA_1 = inlet_prim_A - inletTemp_sec_A
        DeltaT_inletA = DeltaT_inletA_1
        DeltaT_inletB_1 = inlet_prim_B - inletTemp_sec_B
        DeltaT_inletB = DeltaT_inletB_1
        OutletA_1 = float(outletTemp_1)
        OutletA_2 = float(outletTemp_2)
        OutletA_3 = float(outletTemp_3)
        OutletA_4 = float(outletTemp_4)
        Trend_A_main = DeltaT_inletA_2 - DeltaT_inletA_1
        Trend_B_main = DeltaT_inletB_2 - DeltaT_inletB_1
        Trend_Out_1 = OutletB_1 - OutletA_1
        Trend_Out_2 = OutletB_2 - OutletA_2
        Trend_Out_3 = OutletB_3 - OutletA_3
        Trend_Out_4 = OutletB_4 - OutletA_4
        print("DeltaT_inletA_1 : " + str(DeltaT_inletA_1))
        print("DeltaT_inletB_1 : " + str(DeltaT_inletB_1))
        print("Trendout B : " + str(OutletB_3)+ " to A : " + str(OutletA_3))
        
    elif counter == 2:
        DeltaT_inletA_2 = inlet_prim_A - inletTemp_sec_A
        DeltaT_inletA = DeltaT_inletA_2
        DeltaT_inletB_2 = inlet_prim_B - inletTemp_sec_B
        DeltaT_inletB = DeltaT_inletB_2
        OutletB_1 = float(outletTemp_1)
        OutletB_2 = float(outletTemp_2)
        OutletB_3 = float(outletTemp_3)
        OutletB_4 = float(outletTemp_4)
        Trend_A_main = DeltaT_inletA_1 - DeltaT_inletA_2
        Trend_B_main = DeltaT_inletB_1 - DeltaT_inletB_2
        Trend_Out_1 = OutletA_1 - OutletB_1
        Trend_Out_2 = OutletA_2 - OutletB_2
        Trend_Out_3 = OutletA_3 - OutletB_3
        Trend_Out_4 = OutletA_4 - OutletB_4           
        print("DeltaT_inletA_2 : " + str(DeltaT_inletA_2))
        print("DeltaT_inletB_2 : " + str(DeltaT_inletB_2))
        print("Trendout A : " + str(OutletA_3)+ " to A : " + str(OutletB_3))
        counter = 0

    ################################################################################
    # mpp tracking if schutdown   
    if shutdown_all_all == True:
        if DeltaT_inletA_2 == 0:
            DeltaT_inletA_2 = DeltaT_inletA_1
        if (inlet_prim_A - inletTemp_sec_A) >= 0:
            stepA = round(((DeltaT_inletA_2 - deltaT_primIn_A)*1.1),1)
        else:
            stepA = round(((deltaT_primIn_A - DeltaT_inletA_2)*1.1),1)
        print("#########################################################")
        print("Soll Temperatur Delta System  A : " + str(deltaT_primIn_A))
        print("Ist Temperatur Delta System  A : " + str(DeltaT_inletA_2))
        print("Trend : " + str((Trend_A_main)))
        print("inlet A : " + str(inlet_prim_A))
        print("inletsec : " + str(inletTemp_sec_A))
        print("outlet Temp 1 : " + str(outletTemp_1))
        print("outlet Temp 2 : " + str(outletTemp_2))
        print("Regelung steps: " + str(stepA))
            
        if stepA != 0:
            value = str(power_sec_pump_A + stepA)
            if power_sec_pump_A < 50:
                value = str(50)
            if float(outletTemp_1) > 67 or float(outletTemp_2) > 67:
                value = str(100)
                MppSec(second_pump_A, value)  
            if Trend_A_main == 0 and stepA == 0:
                print("warte system A : " + str(power_sec_pump_A))
            else:
                if stepA < 0:
                    print("Regle A auf langsam : " + str(power_sec_pump_A) + " to " + str(power_sec_pump_A + stepA))
                else:
                    print("Regle A auf schnell : " + str(power_sec_pump_A) + " to " + str(power_sec_pump_A + stepA))

                MppSec(second_pump_A, value)
        else:
            print("Secundärsystem A nicht in Betrieb")
            if power_sec_pump_A > 7:
                    
                #MppSec(second_pump_A, power_sec_pump_A)    
                print("-------------------------------\n")
        
            
        if DeltaT_inletB_2 == 0:
            DeltaT_inletB_2 = DeltaT_inletB_1
                
        if (inlet_prim_B - inletTemp_sec_B) >= 0:
            stepB = round(((DeltaT_inletB_2 - deltaT_primIn_B)*1.1),1)
        else:
            stepB = round(((deltaT_primIn_B - DeltaT_inletB_2)*1.1),1)
            
        print("#########################################################")
        print("Soll Temperatur Delta System  B : " + str(deltaT_primIn_B))
        print("Ist Temperatur Delta System  B : " + str(DeltaT_inletB_2))
        print("inlet B : " + str(inlet_prim_B))
        print("inletsec : " + str(inletTemp_sec_B))
        print("outlet Temp 3 : " + str(outletTemp_3))
        print("outlet Temp 4 : " + str(outletTemp_4))
        print("Regelung steps: " + str(stepB))
        if stepB != 0:
            value_b = str(power_sec_pump_B + stepB)
            if power_sec_pump_B < 20:
                value_b = str(20)
            if float(outletTemp_3) > 67 or float(outletTemp_4) > 67:
                value_b = str(100)
                MppSec(second_pump_B, value_b)  
            if Trend_B_main == 0 and stepB == 0:
                print("warte system B : " + str(power_sec_pump_B))
            else:
                if stepB < 0:
                    print("Regle B auf langsam : " + str(power_sec_pump_B) + " to " + str(power_sec_pump_B + stepB))
                else:
                    print("Regle B auf schnell : " + str(power_sec_pump_B) + " to " + str(power_sec_pump_B + stepB))

                MppSec(second_pump_B, value_b)
        else:
            print("Secundärsystem B nicht in Betrieb")
            if power_sec_pump_B > 7:
                #MppSec(second_pump_B, power_sec_pump_B)    
                print("-------------------------------\n")
            
        ####################################################################
        # Regelung primär Pumpe 1
        ####################################################################
        if OutletB_1 == 0:
            OutletB_1 = OutletA_1
        step1 = round(((OutletB_1 - tube_outlet_01)*1.2),1)
            
        print("#########################################################")
        print("Vorgabe Temperatur für Tube 1 : " + str(tube_outlet_01))
        print("Ist Temperatur für Tube 1 : " + str(OutletB_1))
        print("Step Pumpe für Tube 1 : " + str(step1))
        print("Power on Pumpe für Tube 1 : " + str(shutdown[0]))
        print("Trend_Out : " + str(Trend_Out_1))
        
        if shutdown[0]:
            if step1 != 0:
                value_prim1 = str(power_prim_pump_1 + step1)
                if power_prim_pump_1 < 25:
                    value_prim1 = str(55)
                if float(outletTemp_1) > 68:
                    value_prim1 = str(100)
                if Trend_Out_1 == 0:
                    print("warte system Prim_1 : " + str(power_prim_pump_1))
                else:
                    if step1 < 0:
                        if Trend_Out_1 >= 0:
                            print("Regle Pumpe 1 auf langsam : " + str(power_prim_pump_1) + " to " + str(power_prim_pump_1 + step1))
                            if power_prim_pump_1 < 35:
                                value_prim1 = str(50)
                            MppSec(prim_pump_1, value_prim1)
                    else:
                        if Trend_Out_1 <= 0:
                            print("Regle Pumpe 1 schnell : " + str(power_prim_pump_1) + " to " + str(power_prim_pump_1 + step1))

                            MppSec(prim_pump_1, value_prim1)
                #print("Wanne 1 nicht aktiv")
            else:
                #MppSec(prim_pump_1, "23")
                print("warte system 1 : " + str(power_prim_pump_1))
        else:
            print("Wanne 1 nicht aktiv")
            if power_prim_pump_1 > 25:
                MppSec(prim_pump_1, "50")
        print("-------------------------------")
                
        ####################################################################
        # Regelung primär Pumpe 2
        ####################################################################
        if OutletB_2 == 0:
            OutletB_2 = OutletA_2
        step2 = round(((OutletB_2 - tube_outlet_02)*1.1),1)
            
        print("#########################################################")
        print("Vorgabe Temperatur für Tube 2 : " + str(tube_outlet_02))
        print("Ist Temperatur für Tube 2 : " + str(OutletB_2))
        print("Step Pumpe für Tube 2 : " + str(step2))
        print("Power on Pumpe für Tube 2 : " + str(shutdown[4]))
        print("Trend_Out : " + str(Trend_Out_2))
            
        if shutdown[4]:
            if step2 != 0:
                value_prim2 = str(power_prim_pump_2 + step2)
                if power_prim_pump_2 < 35:
                    value_prim2 = str(45)
                if float(outletTemp_2) > 68:
                    value_prim2 = str(100)
                if Trend_Out_2 == 0:
                    print("warte system Prim_2 : " + str(power_prim_pump_2))
                else:
                    if step2 < 0:
                        if Trend_Out_2 >= 0:
                            print("Regle Pumpe 2 auf langsam : " + str(power_prim_pump_2) + " to " + str(power_prim_pump_2 + step2))
                            if power_prim_pump_2 < 35:
                                value_prim2 = str(50)
                            MppSec(prim_pump_2, value_prim2)
                    else:
                        if Trend_Out_2 <= 0:
                            print("Regle Pumpe 2 schnell : " + str(power_prim_pump_2) + " to " + str(power_prim_pump_2 + step2))
                        
                            MppSec(prim_pump_2, value_prim2)
                        #print("Wanne 2 nicht aktiv")

            else:
                    #MppSec(prim_pump_2, "23")
                print("warte system 2 : " + str(power_prim_pump_2))
        else:
            print("Wanne 2 nicht aktiv")
            if power_prim_pump_2 < 25:
                MppSec(prim_pump_2, "50")
        print("-------------------------------")
            
        ####################################################################
        # Regelung primär Pumpe 3
        ####################################################################
        if OutletB_3 == 0:
            OutletB_3 = OutletA_3
        step3 = round(((OutletB_3 - tube_outlet_03)*1.1),1)
            
        print("#########################################################")
        print("Vorgabe Temperatur für Tube 3 : " + str(tube_outlet_03))
        print("Ist Temperatur für Tube 3 : " + str(OutletB_3))
        print("Step Pumpe für Tube 3 : " + str(step3))
        print("Power on Pumpe für Tube 3 : " + str(shutdown[8]))
        print("Trend_Out : " + str(Trend_Out_3))
        
        if shutdown[8] == True:
            if step3 != 0:
                value_prim3 = str(power_prim_pump_3 + step3)
                if power_prim_pump_3 < 30:
                    value_prim3 = str(50)
                    MppSec(prim_pump_3, value_prim3)
                if float(outletTemp_3) > 68:
                    value_prim3 = str(100)
                    MppSec(prim_pump_3, value_prim3)
                if Trend_Out_3 == 0 :
                    print("warte system Prim_3 : " + str(power_prim_pump_3))
                else:
                    if step3 < 0:
                        if Trend_Out_3 >= 0:
                            MppSec(prim_pump_3, value_prim3)
                            print("Regle Pumpe 3 auf langsam : " + str(power_prim_pump_3) + " to " + str(power_prim_pump_3 + step3))
                    else:
                        if Trend_Out_3 <= 0:
                            print("Regle Pumpe 3 schnell : " + str(power_prim_pump_3) + " to " + str(power_prim_pump_3 + step3))
    
                            MppSec(prim_pump_3, value_prim3)
                        #print("Wanne 3 nicht aktiv")

            else:
                    #MppSec(prim_pump_3, )
                print("warte system 3 : " + str(power_prim_pump_3))
        else:
            print("Wanne 3 nicht aktiv")
            if power_prim_pump_3 > 10:
                MppSec(prim_pump_3, "1")
        print("-------------------------------")
            
        ####################################################################
        # Regelung primär Pumpe 4
        ####################################################################
        if OutletB_4 == 0:
            OutletB_4 = OutletA_4
        step4 = round(((OutletB_4 - tube_outlet_04)*1.1),1)
            
        print("#########################################################")
        print("Vorgabe Temperatur für Tube 4 : " + str(tube_outlet_04))
        print("Ist Temperatur für Tube 4 : " + str(OutletB_4))
        print("Step Pumpe für Tube 4 : " + str(step4))
        print("Power on Pumpe für Tube 4 : " + str(shutdown[12]))
        print("Trend_Out : " + str(Trend_Out_4))
        
        if shutdown[12]:
            if step4 != 0:
                value_prim4 = str(power_prim_pump_4 + step4)
                if power_prim_pump_4 < 35:
                    value_prim4 = str(45)
                if float(outletTemp_4) > 68:
                    value_prim4 = str(100)            
                if Trend_Out_4 == 0:
                    print("warte system Prim_4 : " + str(power_prim_pump_4))
                else:
                    if step4 < 0:
                        if Trend_Out_4 >= 0:
                            print("Regle Pumpe 4 auf langsam : " + str(power_prim_pump_4) + " to " + str(power_prim_pump_4 + step4))
                            MppSec(prim_pump_4, value_prim4)
                    else:
                        if Trend_Out_4 <= 0:
                            print("Regle Pumpe 4 schnell : " + str(power_prim_pump_4) + " to " + str(power_prim_pump_4 + step4))

                            MppSec(prim_pump_4, value_prim4)
                        #print("Wanne 4 nicht aktiv")

            else:
                print("warte system 4 : " + str(power_prim_pump_4))
        else:
            print("Wanne 4 nicht aktiv")
            if power_prim_pump_4 > 25 or power_prim_pump_4 < 25:
                MppSec(prim_pump_4, "50")
        print("-------------------------------")
    time.sleep(30)

         