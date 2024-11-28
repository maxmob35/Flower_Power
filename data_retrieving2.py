# -*- coding: utf-8 -*-
"""
Created on  Mon Sep 12 11:15:14 2016

@author: Maxime Mobailly - Robert Van Loo (supervisor and reviewer) - Wageningen University

Data retrieving, use this script to retrieve the raw data from the cloud in the first hand.
And to correct/standardize the time but also to impute missing data.

This script will return a file (one by sensor) with the pre-processed data 
and one file with the raw data (all the sensors)
"""

import time
import datetime
import os
import pickle
import utils as hom
import requests
import pandas as pan
import numpy as np
from statsmodels.formula.api import ols


def data_retrieving(username,path0,auth_header,loc2name,parameter2,path2,folder_name,subset,name,experiments_parameters,digits):
    """
    Retrieve the data from the cloud and write them (air temperature, light and 
    water content) into different files (by sensors). To do this, two different
    parts are recquired. In the first hand, we will download the raw data, and in a second 
    part we will standardize the time to avoid keeping any time offset. 
    Each 15 minutes, we normally should acquire new data. 
    """   
    
    #==============================================================================
    #               Preparation steps
    #
    #  Open a new folder where store the data and get the name of the script used
    #  And define several parameters, path, counter (incrementation) ...
    #==============================================================================
    
        # if you have already retrieved data from the cloud using the API, you can continue retrieving
        # the new data and merge the results file into one single file.
        # For this, you need to read parameters from the last synchronisation you have alredy done (starting date, ...)
    
    path1 = path0+'\\'+'last_synchronisation_parameters_pbr_'+username+'_'+name
    
        # As said in the docstring we assume that we get a new data every 15 minutes.
    delta = datetime.timedelta(minutes = 15)
    
        # rename the variables using a sigle letter (a kind of abbreviation we could say).
    a='capture_ts'
    T='air_temperature_celsius'
    L='par_umole_m2s' # It is wrong, mole m-² d-1
    W='vwc_percent'
    
        # use to incremente the dictionary created to sumarize the missing data
    v = 0 
            
        # Create a new text file containing the raw data
    Raw_data = open('raw_data'+username+'.txt','a')
            # 'a' meaning append, that means we write something new at the end of the file,
            # we will never overwrite the existing data.
        
        # By defaut we will not append the data
    append_data = False
    
    delta_down_limit = experiments_parameters.loc['delta_down_limit']['Reply']
    if hom.Yes_or_No(delta_down_limit) == True:
        treshold_delta_down_15min = experiments_parameters.loc['treshold_delta_down_15min']['Reply']
        treshold_delta_down_1day = experiments_parameters.loc['treshold_delta_down_1day']['Reply']
        data_treshold = pan.DataFrame([[15,treshold_delta_down_15min],[1455,treshold_delta_down_1day]],columns = ['minutes','percentage'])
        mod = ols(formula = 'percentage ~ minutes' ,data = data_treshold)
        res = mod.fit()
        intercept_tp = res.params.Intercept
        slope_tp = res.params.minutes
        
    #=====================================================================================================
    #               Writing the data and a summary
    #
    #=====================================================================================================  
    
        # data appending
    appending_data = experiments_parameters.loc['appending_data']['Reply']
    
    if hom.Yes_or_No(appending_data) == True:
            #check if a file does exists or not yet
        if os.path.exists(path2) == True:
            append_data = True
        
        # open a new text file called summary, where the summary will be written.
    Summary = open('summary.txt','a')
    
        #create an empty dictionary to prapare a parameter file for the next retrieving.
    sensors = {}
    
        #create an empty dictionary to summarize for the missing data
    missing_data = {}
    
    #==============================================================================
    #               Define the starting date
    #
    #  Define the starting date for which you want to begin retrieving data
    #============================================================================== 
       
    if append_data == False: 
        # While loop 2 -
        counter_while_loop2 = 0
        
        test_start_date = False
        while test_start_date == False:
            start_date = experiments_parameters.loc['start_date']['Reply']
            try:
                Start = hom.datetime_format(start_date)
                test_start_date = True
            except:
                print('That was no valid date.  Try again...')
                counter_while_loop2 += 1
                if counter_while_loop2 == 10:
                    quit ()
                    print(" There is a problem to define the starting date. Something is going wrong. \n"
                          " Check the format of the reply you have to give or check the input file ")
        
        del (counter_while_loop2)
        # end of the While loop 2 -
        
        print('You want to retrieve data from', Start)
        
            #compute the julian time for the starting date
        z1 = hom.julian_time(Start)
    
        
        # Otherwise, we need to download the parameters from the last synchronisation event ( = path1).
        # In this file, the last time for which we have already retrieved the data is stored using pickler.
    
    else:
        with open(path1,'rb') as last_synchronisation_event:
            # "rb" meaning read binary file
            # the file at hte path1 will be call last_synchronisation_event from now.
            depickler = pickle.Unpickler(last_synchronisation_event)
            stored_data = depickler.load()
            
            # stored_data is a tuple (the last time already retrieved, the name/path of the older folder)
        last_synchronisation_parameters = stored_data[0]
        last_synchronisation_parameters = pan.DataFrame.from_dict(last_synchronisation_parameters,orient = 'index',dtype = None)
        last_synchronisation_parameters.rename(columns = {0:'Sensor',1:'first time (raw data)',2:'last time (raw_data)',3:'Time for the last True data',
                      4:'Imputed values before the first real value',5:'number of imputed data or NA',
                      6:'data_following_value_real_time',7:'data_following_value_temperature',8:'data_following_value_light',
                      9:'data_following_value_water_content',10:'data_following_value_notifications'}, inplace =True)
        last_synchronisation_parameters.set_index("Sensor", inplace =True)

    print('#####    #####')
    #==============================================================================
    #               Define the ending date
    #
    #  Define the ending date for which you want to stop to retrieve the data
    #==============================================================================        
        
    ending_date_1 = experiments_parameters.loc['ending_date_1']['Reply']
    
    if hom.Yes_or_No(ending_date_1) == True:
          # in this case, withdraw one day to the current date.
        end = time.strftime('%Y-%m-%d')
        end = hom.datetime_format(end)
        end = end - datetime.timedelta(minutes = 15)
            
    else:
        # in this case, we want to choose our own ending date
            # While loop 3 -
    
        counter_while_loop3 = 0
            
        test_ending_date = False
        while test_ending_date == False:
            ending_date = experiments_parameters.loc['end_date']['Reply']
            try:
                end = hom.datetime_format(ending_date)
                test_ending_date = True
            except:
                print('That was no valid date.  Try again...')
                counter_while_loop3 += 1
                if counter_while_loop3 == 10:
                    quit ()
                    print(" There is a problem to define the ending date. Something is going wrong. \n"
                          " Check the format of the reply you have to give or check the input file ")
        # end of the While loop 3 -
    
        del(counter_while_loop3)
        
        # compute the julian time for the ending date
    z2 = hom.julian_time(end)
    
    
    print('You want to retrieve data to', end)
    print('#####    #####')
    print(subset)
    #==============================================================================
    #               compute the last time for which we should have data.
    #==============================================================================
    
        # change the hour of the ending date, to compute the last time  we normally should have data.
    End = end.replace(hour = 23,minute = 45)
                            
    print('#####    #####')
    
    #==============================================================================
    #               Retrieve the data and store them by sensor in a list (d1)
    #==============================================================================
        
    
        #               #####
       # #              #####
      #   #             #####
     #  #  #    # From now we are going to work sensor by sensor. And retrieved them from the cloud.
    #   #   #           #####
   #    #    #          #####
  #     #     #         #####
 # # # # # # # #
    
    #####
    #####
    
    # We use s as a counter, to count the number of sensor analyzed.
    s = 0 
        
        # To work on the different sensors, we are going to use a for loop
        # For loop, -> retrieve the raw data sensor by sensor.
        # Keep in mind that the sensor are sorted.
    
    for Sensor in subset:
        s += 1
        
            # Remember, sensors' nicknames are used as keys.
            # We have {sensor's nickname : loc_id}
        
        loc_id = loc2name[Sensor]
        
        print(Sensor)
        
        # if you have chosen to append/combine the new data with data already existing/retrieving
        if append_data == True: 
           Start = last_synchronisation_parameters.loc[Sensor]['Time for the last True data'] + datetime.timedelta(days = 1)
           Start = Start.replace(hour = 00,minute = 00)
           
           #compute the julian time for the starting date
           z1 = hom.julian_time(Start) 
    
            # create a new text file for each sensor
        Sensor_file = open(Sensor+".txt",'a')

            # create 1 list to store the raw data
            # a new one for each sensor.
        d1 = []
        
            # imputed -> number of imputed data before the first True data.
            # we could count how many imputed values or mssing we have before the first True data.
        imputed = 0
        
        
        #               #####
       # #              #####
      #   #             #####
     #  #  #    # We could only retrieve the data 10 days by 10 days
    #   #   #           #####
   #    #    #          #####
  #     #     #         #####
 # # # # # # # #
        
    
        x1=z1 # while loop requirement. z1 => julian time of the starting date.
            # While loop 5 - Retrieve the data 10 days by 10 days.
    
        while x1 <= z2:
            x2 = x1+10
            a1=Start.fromordinal(x1) # -> return the julian time in a datetime format
            a2=Start.fromordinal(x2) # -> 
            SD=a1.isoformat()+'Z' # -> convert the time to a string
            ED=a2.isoformat()+'Z'
            x1 = x1+10
    
            Internet_connection = False
            # we assume that we are not connected to internet.
            attempt = 1
            # we could try 10 times to make a connection with the website.
    
                # While loop 6 - 
            while Internet_connection == False:
                if attempt <= 11: # try 10 to request the data times, and after just quit if it is still not working.
                    try:
                        req = requests.get('https://apiflowerpower.parrot.com/sensor_data/v2' +
                            '/sample/location/{loc_id}'.format(loc_id = loc_id),
                            headers = auth_header,
                            params={'from_datetime_utc': SD,
                            'to_datetime_utc': ED})
                            
                        response = req.json()
                        samp = response['samples']
                        # -> list of dict, one dict for one sensor.
                    
                        Internet_connection = True
                        
                    except:  
                        print(attempt,'attempt(s):','Failed to establish a connection with the website. waiting time of two minutes before the next attempt')
                        attempt += 1
                        # wait 1 minutes before to try once again.
                        time.sleep(60)
                
                else:
                    print('Failed to establish a connection with the website')
                    Raw_data.close()
                    Sensor_file.close()
                    Summary.close()
                    quit()
            # end of the While loop 6 -
            
                ## For memory, the data are stored in one list, but for each time point, the data are stored in one dictionnary 
                ## So, we have d1[{capture_ts : time1 , air_temperature_celsius : tp , par_umole_m2s : light , vwc_percent : WC} ....
                ##      ....      {capture_ts : timex , air_temperature_celsius : tp , par_umole_m2s : light , vwc_percent : WC}]
    
    
                # extand the list to have the dat for more than 10 days in the same list.
            d1.extend(samp)
            
        # end of the While loop 5 - Retrieve the data 10 days by 10 days.
            
        ## All the data for the selected sensor are now written/stored in the list d1.
            
        ## restore the while loop requirement, for the next 
        #x1 = z1
        
        
        #===================================================================================================================
        #       Write the raw data into Dataframe, and combine the results of all the sensors
        #===================================================================================================================
    
        if len(d1) == 0:
            # if len(d1) = 0, that means we don't have data at all for this particular period and for this sensor.
            # we want to keep a trace of this result as well.            
            d1_ = [{'capture_ts' : 'NaN','air_temperature_celsius' :'NaN', 'par_umole_m2s' : 'NaN','vwc_percent' : 'NaN'}]
            raw_data = pan.DataFrame.from_dict(d1_,orient = 'columns',dtype = None)
            
        else:
            raw_data = pan.DataFrame.from_dict(d1,orient = 'columns',dtype = None)
            digits_raw_data = pan.Series(digits, index = ['air_temperature_celsius','par_umole_m2s','vwc_percent'])
            raw_data = raw_data.round(digits_raw_data)
        
            # add a column with the name of the sensor selected before combining the data
        raw_data.insert(0,'Sensor',Sensor)
    
            # now, append the dataframe to have one single dataframe with the raw data for all the sensor
        if s == 1:
            # we first need to create the big dataframe
            raw_data_all_sensors = raw_data.copy()
        elif s > 1:
            # append the dataframe
            frames = [raw_data_all_sensors,raw_data]
            raw_data_all_sensors = pan.concat(frames)
            
        #####
        #####
        # The raw data were already retrieved from the cloud
        # they are stored in a dictionnary (d1)
        #####
        #####

        #               #####
       # #              #####
      #   #             #####
     #  #  #    # Pre-Process the data - Part 1, standardize the time and impute missing values
    #   #   #           #####
   #    #    #          #####
  #     #     #         #####
 # # # # # # # #       
            
            # create a dictionary (d2) to store new (pre-processed) data
        d2 = {}
    
            # we are going to redo a time scale.
            # we need to use a theoretical time.
    
        theoretical_time = Start
    
            # The raw data are stored in the list d1. We need a loop through this list.
            # the counter i, is used as indentation throug the list d1.
            # Each element of the list corresponds to a dictionnary (one for each time point)
    
        i = 0
            # begin with the first time point.
        
            #=====================================================
            #       Have we available data for this sensor ?
            #=====================================================
        try:
            
            # Do not forget, we have changed/replaced the variable's name at the beginning by only one letter.
            # a = for time, T for temperature, L for Light and W for Water.
            
            # we use this nomenclature (a0, T0, L0 and W0) only for i = 0 (for the first values)
            
            a0 = d1[i][a]
            T0 = d1[i][T]
            L0 = d1[i][L]
            W0 = d1[i][W]
            
                #======================================================================
                #       Does the first time point correspond to the theoretical time?
                #======================================================================
            #convert the first time to a datetime format
            time_a0 = datetime.datetime(int(a0[:4]),int(a0[5:7]),int(a0[8:10]),int(a0[11:13]),int(a0[14:16]))    
              
            # Check if the first time point could be considered as similar or not.
            # difference = current_time - putative startin time
            dif = (time_a0-theoretical_time)
                # -> We have a result similar to datetime.timedelta(day(s),second(s))by doing the subtraction
            
            # convert the result to minutes  
            Y1 = hom.minute(dif)
    
            ## We have 3 different possibilities
    
###----------------###----------------###----------------###----------------###----------------###----------------###----------------###
#            
#    # -> no difference, so time_a0 = theoretical_time                          (a)
#    
#    # -> we have a difference
#    
#    #           -> the difference is only due to one missing data               (b)
#                       #In this case we don't add any NA, we simply paste the values of the next time point. (= 'Pasting')
#    
#    #           -> the difference is due to more than one missing data          (c)
#                       #How many NA we need to add ? For the last missing value we also paste data from the next time point
#    
###----------------###----------------###----------------###----------------###----------------###----------------###----------------###
            
            nb_NA_required = 0
            
            if Y1 <= 7 :                #(a)
                # we consider the current time equal to 00:00 when the time belong to the interval [00:00:00, 00:07:00]
            
                d2[0] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),a0,T0,L0,W0,"True","True"]
                     # create a key [0] in the dictionary {d2} and put the data.
                
                # Z is used as a counter for the keys of the dictionnary {d2}
                z = 0
                
            elif Y1 <= (15+7) :         #(b)
                # we consider the current time equal to 00:15 when the time belong to the interval [00:08:00, 00:22:00]
                if append_data == True:
                    # As written before, if you have just a missing value, we will duplicate the next time point to replace
                    # this missing time point.
                    # Nevertheless, if you have already retrieved data perhaps you have already retrieved the missing point
                    # little explanation of why you could have already retrieved the missing data:
                    # the script will write the retrieved data into a file, only if you have the data for the entire day
                    # So from the 00:00:00 till 23:45:00. Nevertheless, if you have a time point at 23:58:00 for example, the data
                    # will not be written in the file. But keep in memory for the next time you will retrieve the data for this sensor.
                    # And only if you need to use the pasing procedure.
                    
                    if last_synchronisation_parameters.loc[Sensor]['data_following_value_notifications'] == 'True':
                        d2[0] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),last_synchronisation_parameters[Sensor]['data_following_value_real_time']
                        ,last_synchronisation_parameters.loc[Sensor]['data_following_value_temperature'],last_synchronisation_parameters.loc[Sensor]['data_following_value_light']+0.1,
                        last_synchronisation_parameters.loc[Sensor]['data_following_value_water_content'],"True","True"]
                            # there is a +0.1. see the end of the script.
                        theoretical_time += delta
                        d2[1] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),a0,T0,L0,W0,"True","True"]
                        z = 1
                    else:
                        d2[0] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),str(theoretical_time),T0,L0,W0,"Pasting","True"]
                        theoretical_time += delta
                        d2[1] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),a0,T0,L0,W0,"True","True"]
                        z = 1
                else:
                    d2[0] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),str(theoretical_time),T0,L0,W0,"Pasting","True"]
                    theoretical_time += delta
                    d2[1] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),a0,T0,L0,W0,"True","True"]
                    z = 1
            
                
            else:                       #(c)
            
                nb_NA_required = (Y1//15)
                # for number of missing data needed.
                
                modulo = (Y1%15)#give the rest of the division
                
                if modulo >7: #try to take into account that we don't have obligatory 00:00 or 00:15 or 00:30 or 00:45.
                    nb_NA_required += 1
    
                for z in range(0,int(nb_NA_required)-1):#-1 in order to paste the values from the next available time.
                    d2[z] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),str(theoretical_time),'NaN','NaN','NaN',"imputed","True"]
                    theoretical_time += delta
                    z += 1
                    
                d2[z] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),str(theoretical_time),T0,L0,W0,"Pasting","True"]
                theoretical_time += delta
                z += 1
                d2[z] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),a0,T0,L0,W0,"True","True"]
    
        #======================================================================
        # we just retrieved now the first time point.
        #======================================================================

        #               #####
       # #              #####
      #   #             #####
     #  #  #    #               water_content_correction
    #   #   #           #####
   #    #    #          #####
  #     #     #         #####
 # # # # # # # #  
                
                    # we add a part a bit bellow in this to correct the values for water content, indeed 
                    # we could have some little variation due to the measure itself (errors or disconnecting battery due to too warm conditions).
                    # As we need to compute delta up and down, it is preferable to remove all the extraordinary
                    # low values to avoid any overestimation
                    # So we consider we could not have for a 15 minutes scale a decrease up to 1 % 
                    # and for a day scale a decrease up to 10 %.
                #  Using these two tresholds we can set up a linear curve allowing to to compute a treshold anywhen.
                #  percent = f(minutes) , 15 minutes scale, slope = 0.00625 and intercept = 0.90625
                
            ref_water_content = W0
            ref_time= theoretical_time
            
                #======================================================================
                #       continue with the full data now
                #======================================================================
    
                # we used "variable0" for i=0 and now we are going to use "variable_i_1" (values for i-1)
            a_i_1 = a0
            T_i_1 = T0
            L_i_1 = L0
            W_i_1 = W0
            
            
                # While loop 7 - continue with the full data.
            
            while i < (len(d1)-1):##i-1 because i+=1 is located just below.
                    
                i += 1
                z += 1 
    
                ## We had already seen a strange problem with few sensors. Indeed each time point was duplicated
                ## So we must compare the time for i-1 (a_i_1) and i (a_i) to avoid duplicated data
                
                a_i = d1[i][a]
                
                if a_i == a_i_1:
                    continue
                    # continue with a new i, because we have duplicated values.
                
                ####
    
                T_i = d1[i][T]
                L_i = d1[i][L]
                W_i = d1[i][W]
                
                # update the theoretical time
                theoretical_time += delta
                # convert it to datetime format
                time_a_i = datetime.datetime(int(a_i[:4]),int(a_i[5:7]),int(a_i[8:10]),int(a_i[11:13]),int(a_i[14:16]))
                
                # do a summary regarding to the NaN
                first_date_with_NaN = list()
                last_date_with_NaN = list()
                
                    #Calibrate the time
                y = hom.correction(time_a_i,theoretical_time)
                
                ## We have 3 different possibilities
    
###----------------###----------------###----------------###----------------###----------------###----------------###----------------###
#            
#    # -> the real time of is 0 close to the last time point and we can not have two time the same corrected time = "Skip"
# 
#    # -> the real time is close enough to the theoretical time
#                       
#    # -> the real time is too far from the theoretical time, add a NaN
#                           
###----------------###----------------###----------------###----------------###----------------###----------------###----------------###
 
                
                if y == "Skip":
                    #remove the i
                    print("the values for",i,"was deleted",a_i,"%.1f" %T_i,"%.2f" %L_i,"%.4f" %W_i) 
                    theoretical_time -= delta
                    continue
                
                elif y != "Skip" and y != "NaN": # the real time is close enough to the theoretical time
                    d2[z] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),a_i,T_i,L_i,W_i,"True","True"]
                    # Na, if Na == True, write the presence of missing data in the missing data summary.
                    Na = False
                    
                elif y == "NaN":
                    Na = True
                    nb_NA = 0
                    theoretical_time2 = theoretical_time #to determine the number of NaN required
    
                    first_date_with_NaN.append(a_i)
                    
                    while y == "NaN":
                        # nb_NA, counter for the number of missing data we have.
                        nb_NA += 1
                        theoretical_time2 += delta
                        y = hom.correction(time_a_i,theoretical_time2)
                        
                    imputed += nb_NA # to determine the number of imputations did.
                    
                    ## We have also 3 different possibilities
                    
###----------------###----------------###----------------###----------------###----------------###----------------###----------------###
#            
#    # -> nb_NA less than 4 hours (16 data)                     (a)
#       # impute data for the three variables
#                    
#    # -> nb_NA less than 24 hours (96 data)                    (b)
#       # impute data for only water content
#                   
#  # -> nb_NA is bigger than 24 hours (96 data)                 (c)
#       # don't impute data, but add just 'NaN'
#                           
###----------------###----------------###----------------###----------------###----------------###----------------###----------------###

                    
                    
                    if nb_NA <= 16:                              #(a)
                        for k in range(1,nb_NA+1):
                            Tx = hom.imputation(k,0,T_i_1,nb_NA+1,T_i)
                            Lx = hom.imputation(k,0,L_i_1,nb_NA+1,L_i)
                            Wx = hom.imputation(k,0,W_i_1,nb_NA+1,W_i)
                            d2[z] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),str(theoretical_time),Tx,Lx,Wx,"imputed","True"]
                            theoretical_time += delta
                            z += 1
                        d2[z] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),a_i,T_i,L_i,W_i,"True","True"]
                        
                    elif nb_NA <= 96:                            #(b)
                        for k in range(1,nb_NA+1):
                            Tx = np.NaN
                            Lx = np.NaN
                            Wx = hom.imputation(k,0,W_i_1,nb_NA+1,W_i)
                            d2[z] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),str(theoretical_time),Tx,Lx,Wx,"imputed","True"]
                            theoretical_time += delta
                            z += 1
                        d2[z] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),a_i,T_i,L_i,W_i,"True","True"]
                        
                    else:                                       #(c)
                        for k in range(1,nb_NA+1):
                            Tx = np.NaN
                            Lx = np.NaN
                            Wx = np.NaN
                            d2[z] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),str(theoretical_time),Tx,Lx,Wx,"imputed","True"]
                            theoretical_time += delta
                            z += 1
                        d2[z] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),hom.date_format(theoretical_time),a_i,T_i,L_i,W_i,"True","True"]
                    last_date_with_NaN.append(a_i)
                    
                 # update the variables, 
                    
                a_i_1 = a_i
                T_i_1 = T_i
                L_i_1 = L_i
                W_i_1 = W_i
                
##-----------##-----------##-----------##-----------##-----------##-----------##-----------##-----------##----------- 
##-----------##-----------##-----------##-----------##-----------##-----------##-----------##-----------##-----------                
        #               #####
       # #              #####
      #   #             #####
     #  #  #    #               water_content_correction
    #   #   #           #####
   #    #    #          #####
  #     #     #         #####
 # # # # # # # #  
                
                    # we add a step here to correct the values for water content, indeed we have little variation
                    # due to the measure itself.
                    # As we need to compute delta up and down, it is preferable to remove all the extraordinary
                    # low values to avoid any overestimation
                    # So we consider we could not have for a 15 minutes scale a decrease up to 1 % 
                    # and for a day scale a decrease up to 10 %.
                #  Using these two tresholds we can set up a linear curve allowing to to compute a treshold anywhen.
                #  percent = f(minutes) , 15 minutes scale, slope = slope_tp and intercept = intercept_tp
                if hom.Yes_or_No(delta_down_limit) == True:
                    
                    delta_water = ref_water_content - W_i
                        # compute the delta between the two time point
                    
                    delta_water_time = (theoretical_time - ref_time)
                    delta_water_time = hom.minute(delta_water_time)
                        # compute the delta (time) between the two time point in minutes
                    
                    treshold =  (delta_water_time * slope_tp )+ intercept_tp
                        # compute the treshold using the linear curve determined above.
                    
                    # apply the comparison with the computed treshold
                    if delta_water > treshold:
                        d2[z][9] = 'correction_needed'
                        d2[z][7] = -1
                    else:
                        # just uptade the parameters
                        ref_water_content = W_i
                        ref_time = theoretical_time
                    
##-----------##-----------##-----------##-----------##-----------##-----------##-----------##-----------##----------- 
##-----------##-----------##-----------##-----------##-----------##-----------##-----------##-----------##----------- 
                    
                    # write a dataframe where store the summary about the missing data
                if Na == True:
                    for element in range(len(first_date_with_NaN)):
                        missing_data [element] = {'sensor': Sensor,'first_date_with_NaN':first_date_with_NaN[element],'last_date_with_NaN':last_date_with_NaN[element]}
                        element+=1
                    NaN = pan.DataFrame.from_dict(missing_data,orient = 'index',dtype = None)
                    
                    
                    # now, append the dataframe to have one single file with the data for all the sensors
                    if v == 0:
                        # we don't have to append the dataframe, but just to create a new DataFrame.
                        missing_data_all_sensors = NaN
                    elif v > 1:
                        # we have to append the dataframes in this case
                        frames2 = [missing_data_all_sensors,NaN]
                        missing_data_all_sensors = pan.concat(frames2)   
                    v +=1
    
                    ## complete with /add 'NA'
            while theoretical_time < End:
                z += 1
                theoretical_time += delta
                d2[z] = [str(theoretical_time),Sensor,hom.Time(theoretical_time),
                        hom.date_format(theoretical_time),np.NaN,np.NaN,np.NaN,np.NaN,'no more data',"True"]
    
    
            # end of the While loop 7 - 
    
            
        except : # if we have no values
            x=0
            while theoretical_time <= End:
                d2[x] = [str(theoretical_time),Sensor,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,'True']
                theoretical_time += delta
                x += 1
                
            a0 = np.NaN
            a_i = np.NaN
            a_i_1 = np.NaN
            stop_writing_data = np.NaN
            nb_NA_required = 0
            imputed = 0
            
        #####
        #####
        # The data are now pre-processed
        # they are stored in a dictionnary (d2)
        #####
        #####
    
        #=================================================
        #       write the data into a csv
        #=================================================
            
        Tp_Light_WC = pan.DataFrame.from_dict(d2,orient = 'index',dtype = np.number)
        Tp_Light_WC.rename(columns = {0:'corrected_time',1:'Sensor',2:"Julian_time",3:"date",4:"real_time",5:"temperature",6:"light",
                               7:"water_content",8:"notifications",9:"water_notifications"},inplace = True)
        # subtract 0.1 for the light value.
        # the real 0 for light is equal to 0.1 in the data retrieved from cloud.
        Tp_Light_WC['light'] = Tp_Light_WC['light'] - 0.1  
        
        Tp_Light_WC['corrected_time'] = pan.to_datetime(Tp_Light_WC['corrected_time'])
        Tp_Light_WC.set_index('corrected_time',inplace=True) # corrected_time is now become the index
        
        if hom.Yes_or_No(delta_down_limit) == True:
            Tp_Light_WC_before_cor = Tp_Light_WC
            Tp_Light_WC_before_cor[Tp_Light_WC_before_cor.water_content != np.NaN]
            Tp_Light_WC_before_cor['water_content'] = Tp_Light_WC_before_cor.water_content.replace(np.NaN,-90)
            # we just want to impute water content value which are considered as wrong only
            
            Tp_Light_WC['water_content'] = Tp_Light_WC.water_content.replace(-1,np.NaN)
            
            Tp_Light_WC['water_content'] = pan.to_numeric(Tp_Light_WC.water_content, errors='coerce')
            Tp_Light_WC['water_content'] = Tp_Light_WC.water_content.interpolate(method = 'time')
            
            Tp_Light_WC['water_content'] = Tp_Light_WC['water_content'].combine_first(Tp_Light_WC_before_cor['water_content'])   
            Tp_Light_WC['water_content'] = Tp_Light_WC.water_content.replace(-90,np.NAN)
    
        digits_Tp_Light_WC = pan.Series(digits, index = ["temperature","light",'water_content'])
        Tp_Light_WC.round(digits_Tp_Light_WC)
        
            #write only the data for entire days
        stop_writing_data = Tp_Light_WC.index[-1] #get the last value for index
        
        if stop_writing_data != end:
            last = stop_writing_data - datetime.timedelta(days = 1)
            stop_writing_data = last.replace(hour = 23,minute = 45)
            
            # if we have to do a subset of the data, it is interesting to keep in memory the values
            # for the next time point. Just in case we should use them. Notably, to avoid to paste
            # values (see 'Does the first time point correspond to the theoretical time?', (b) )         
            
            # following value
            following_value = stop_writing_data + delta
            data_following_value = Tp_Light_WC[Tp_Light_WC.index == str(following_value)]
            if len(data_following_value) == 1:
                data_following_value.reset_index(inplace = True)
                data_following_value_notifications= data_following_value['notifications'][0]
                data_following_value_real_time = data_following_value['real_time'][0]
                data_following_value_temperature = data_following_value["temperature"][0]
                data_following_value_light = data_following_value["light"][0]
                data_following_value_water_content = data_following_value["water_content"][0]
            else:
                data_following_value_notifications= 'no_folowing_value'
                data_following_value_real_time = np.NaN
                data_following_value_temperature = np.NaN
                data_following_value_light = np.NaN
                data_following_value_water_content = np.NaN
                
            Tp_Light_WC = Tp_Light_WC[Tp_Light_WC.index <= str(stop_writing_data)]
        else :
            # Here we have no values for the next time point, indeed we didn'to do any subset.
            data_following_value_notifications= 'no_folowing_value'
            data_following_value_real_time = np.NaN
            data_following_value_temperature = np.NaN
            data_following_value_light = np.NaN
            data_following_value_water_content = np.NaN
    
        #=================================================
        #       append data by sensor
        #=================================================    
            
        if append_data == True:
            
            older_file = stored_data[1]
            path5 = path0+'\\'+older_file+'\\'+Sensor+'.txt'
            Tp_Light_WC_ = pan.read_csv(path5,sep = '\t',keep_default_na = True,index_col = 'corrected_time')
            
                
            Tp_Light_WC = Tp_Light_WC_.append(Tp_Light_WC)
            
            
        digits_Tp_Light_WC = pan.Series(digits, index = ["temperature","light",'water_content'])
        Tp_Light_WC = Tp_Light_WC.round(digits_Tp_Light_WC)
        
            
        Tp_Light_WC.to_csv(Sensor_file,sep = '\t', na_rep = np.NaN)
        Sensor_file.close()
            
            
        #=================================================
        #       summary sensor
        #=================================================
    
        sensors[s] = [Sensor,a0,a_i,stop_writing_data,int(nb_NA_required),int(imputed),data_following_value_real_time,data_following_value_temperature,data_following_value_light,data_following_value_water_content,data_following_value_notifications]    
        # a3                -> first time we have data (raw data)
        # a_1               -> last time we have data (raw data)
        # stop_writing_data -> last time for which we want to write the data in to a csv file (not the raw data here)
        # nb_NA_required    -> number of NA required before getting the first real data
        # imputed           -> number of imputed values, excepted before the first real data
        
    #### end of the for loop, retrieve the data sensor by sensor.
    
    #=================================================
    #       write the raw data into a csv
    
    #=================================================
    
    raw_data_all_sensors = raw_data_all_sensors[['Sensor','capture_ts','air_temperature_celsius','par_umole_m2s','vwc_percent']]
    
    raw_data_all_sensors.set_index('Sensor', inplace = True)
        
    raw_data_all_sensors.to_csv(Raw_data,sep = '\t')
    
    #=================================================
    #       write the summary into a csv
    #=================================================  
    
    data2 = pan.DataFrame.from_dict(sensors,orient = 'index',dtype = None)
    data2.rename(columns = {0:'Sensor',1:'first time (raw data)',2:'last time (raw_data)',3:'Time for the last True data',
                          4:'Imputed values before the first real value',5:'number of imputed data or NA',6: 'data_following_value_real_time',
                          7: 'data_following_value_temperature',8: 'data_following_value_light',9: 'data_following_value_WC',
                          10:'data_following_value_notifications'},inplace = True)
    data2.set_index('Sensor', inplace = True)
    data2.to_csv(Summary,sep = '\t')
    Summary.close()
    
    
    # now for the NaN
    if v > 0:
        missing_data_all_sensors = missing_data_all_sensors[['sensor','first_date_with_NaN','last_date_with_NaN']]
        missing_data_all_sensors.set_index('sensor',inplace = True)
        text_file_e = open('summary_NaN'+username+'.txt','a')
        missing_data_all_sensors.to_csv(text_file_e,sep = '\t')
        text_file_e.close()
    
    ##############  ##############  ##############  ##############  ##############  ##############  ##############
    
    #=======================================================================================
    #       Store the date of the last time already retrieved and the name of the folder.
    #=======================================================================================
     # create a tuple to store the summary and the older path
    stored_data = (sensors,folder_name)
    
    with open(path1,'wb') as file:
        pickler = pickle.Pickler(file)
        pickler.dump(stored_data)
    
    #================
    #       end
    #================
    
    Raw_data.close() 
    
    return(subset,Start,End)