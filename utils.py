# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 14:38:56 2016

@author: Maxime
"""

import datetime

def minute(time):
    """
    convert time (timedelta format) to minutes
    """
    try:
        D_diff = time.days
    except:
        D_diff = 0
    try:
        S_diff = time.seconds
    except:
        S_diff = 0
        
    #convert seconds to minutes and hours to minutes too
    dif =(int(D_diff)*(60*24))+(int(S_diff)/60)
    return(dif)
#
#
def imputation(a,xa,ya,xb,yb):
    """
    impute the NA with a linear regression model
    """
    # y = ax+b
    # Y => variable X => Time
    slope = (yb - ya)/(xb - xa)
    intercept = ya
    var = (slope*(a - xa))+ intercept
    return (var)
#
#
def difference(Start,theoretical_time):
    """
    compute the difference into two times (datetime format)
    y - x -> theoretical_time - Start
    """
    x = Start
    y = theoretical_time
    diff = y-x
    return(diff)
#
#
def rules(dif):
    """
    rules of decision to assign a corrected time to the real time
    """
    decision=0
    if dif > 15:
        decision="Skip"
    elif dif < (-9):
        decision="NaN"
    return(decision)
#
#
def correction(Start,theoretical_time):
    """
    Function used to correct the real time, according to two different rules.
    See the 'rules' function for further indications
    """
    diff = difference(Start,theoretical_time)
    dif = minute(diff)
    y = rules(dif)
    return(y)
#
#
def julian_time(time):
    """
    compute the julian time (integer) of a day (aly in the datetime format)
    """
    time.isoformat()
    zx = time.toordinal()
    return(zx)
#
#
def hourly_time(time):
    """
    compute the hourly time (integer) of a time (already in the datetime format)
    """
    hourly = int(time.hour)+((int(time.minute))/60)
    return(hourly)
#
#
def date_format(time):
    """
    transform the date into this format YYYY-MM-DD
    """
    Y = str(time.year)
    M = str(time.month)
    D = str(time.day)
    date = Y+" "+M+" "+D
    return(date)
#
#
def datetime_format(string):
    """
    convert a date (string format) to a date (datetime format)
    """
    year = int(string[:4])
    month = int(string[5:7])
    day = int(string[8:10])
    try:
        hour = int(string[11:13])
    except:
        hour = 0
    try:
        minute = int(string[14:16])
    except:
        minute=0
    try:
        seconde = int(string[17:19])
    except:
        seconde = 0
    tx = datetime.datetime(year,month,day,hour,minute,seconde)
    return(tx)
#
#
def Time(x):
    """
    Function used to compute the julian time
    For further indications see 'julian_time' and 'hourly_time' functions
    """
    zx = julian_time(x)
    RD = datetime.datetime(2016,1,1)
    RD = julian_time(RD)
    hourly =(hourly_time(x)/24)
    jtime =(zx+hourly-RD)+1
    Jtime = "%.4f"% jtime #keep four digits
    return(Jtime)
#
# 
def Yes_or_No(var):
    """
    Put in lower case var, and check if var is alpha ? 
    """
    var = var.lower()
    var2 = var.isalpha()
    if var2 == True:
        if var[0] == 'y':
            result = True
        else:
            result = False
    else:
        result = False
    return(result)
#
#
def correct_running_average_result (dataframe,x,y,selected_window):
    """
    Determine (compute) the window to use to do a running average.
    As excel do, if you chosse an even number (x) as window. Pandas will select (x/2) values before the individual you want to
    compute the running average and ((x/2)-1) values after this individual. (=> x/2 (before) + 1 (the individual) + (x/2)-1) 
    
    In this case, it is important to be sure you give to pandas an uneven nummber.

    It is also indispensable to adapt the window regarding to the index of the considered individual.
    if index = 0 ------> window = 2 (3 but index = -1 not exist)
    if index = 1 ------> window = 3
    if index = 2 ------> window = 5
    if index = 3 ------> window = 7
    """
        
    condition = (selected_window % 2) == 0
    window = selected_window
    if condition == True:
        # even number
        window += 1
        # uneven number
    else:
        selected_window += 1
        # even number
        
    treshold = (selected_window/2)-1
    # we will work through a dataframe
    # i refers to the index
    for i in range (0,int(treshold)):
        # upper limit = i+1
        # lower limit = (i-0)
        # so we need to select (i-0)+i+1
        if i == 0:
            dataframe[x][str(dataframe.index[i])] = (dataframe.iloc[:2][y] ).mean()
        else:
            dataframe[x][str(dataframe.index[i])] = (dataframe.iloc[:i+i+1][y] ).mean()
        
    return(dataframe)
#
#
def delta_comp (day,wc_min_ind,wc_max_ind,wc_min,wc_midnight,wc_23_45_00,wc_max):
    x1 = 0
    x2 = 0
    if wc_min_ind < wc_max_ind : # if min is before max
        x1 = (wc_min - wc_midnight) + (wc_23_45_00 - wc_max)
        x2= (wc_max - wc_min)
        
    elif wc_min_ind > wc_max_ind : #if max is before min
        x1 = (wc_min - wc_max)
        x2 = (wc_23_45_00 - wc_min) + (wc_max - wc_midnight)
        
    return(x2,x1)
#