from Press_11_alert import *
from Press_21_alert import *
from Press_24_alert import *

def alert(press,start,end):
    if press == 11:
        return press_11_alert_generator(start,end)
    elif press == 21:
        return press_21_alert_generator(start,end)
    elif press == 24:
        return press_24_alert_generator(start,end)
    else:
        raise('Invalid Press Number')
    
print(alert(24,datetime(2023,6,13,10,0,0),datetime(2023,6,13,11,0,0)))