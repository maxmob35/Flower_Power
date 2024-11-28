# -*- coding: utf-8 -*-
"""
Created on Wed Aug  3 13:36:49 2016

@author: Maxime Mobailly - Robert Van Loo (supervisor and reviewer) - Wageningen University
"""


#==============================================================================
#               Some packages have to be downloaded
#==============================================================================

import os
import pandas as pan
import datetime
import matplotlib.pyplot as plt
import numpy as np
import utils as hom

def data_processing(path0,path4,path2,experiments_parameters,selected_sensors,experiment_name,folder_name,experiments_details,subset,digits):
#==============================================================================
#               Preparation steps
#==============================================================================
    
    Correction = experiments_parameters.loc['Correction']['Reply']
    treshold = int(experiments_parameters.loc['minimum change']['Reply'])
    Format = experiments_parameters.loc['format']['Reply']
    treshold_NaN = int(experiments_parameters.loc['treshold_NaN']['Reply'])
        
    if (os.path.exists(path4)) == True and hom.Yes_or_No(Correction) == True:
        # open the file where are stored the correction coefficients
        correction_parameters = pan.read_csv(path4,sep='\t',index_col = 'Sensor')
        
        Correction_Temp = experiments_parameters.loc['Correction_Temp']['Reply']
        Correction_Light = experiments_parameters.loc['Correction_Light']['Reply']
        Correction_Water = experiments_parameters.loc['Correction_Water']['Reply']
    
     # Do you want to do the plots
     # Two kinds of plots are possible:
     #  -> for the 15 minutes data
     #  -> for the daily values
     
    plot = experiments_parameters.loc['Plot']['Reply']
    Plot_15_minutes_data = experiments_parameters.loc['Plot_15_minutes_data']['Reply']
    
    path7 = path0+'\\'+'processed_data'
         
    if os.path.exists(path7) == False:
        os.mkdir(path7)  
    
    path_8 = path7+'\\'+folder_name+experiment_name
    if os.path.exists(path_8) == False:
        os.mkdir(path_8)
    
    path_15_minutes_data = path_8+'\\'+'15_minutes_data'
    os.mkdir(path_15_minutes_data)
    
    path_daily_data = path_8+'\\'+'daily_data'
    os.mkdir(path_daily_data)
        
    print('#####    #####')
    
        #==============================================================================
        #               selection process - period selection
        #==============================================================================
    
    growing_season = experiments_parameters.loc['growing_season']['Reply']
    if hom.Yes_or_No(growing_season) == False:
        Start = experiments_parameters.loc['start_season']['Reply']
        Start = hom.datetime_format(Start)
        End = experiments_parameters.loc['end_season']['Reply']
        End = hom.datetime_format(End)
    else:
        Start = experiments_details.loc[experiment_name]['start experiment']
        Start = hom.datetime_format(Start)
        End = experiments_details.loc[experiment_name]['end experiment']
        End = hom.datetime_format(End)
        
    # open 8 new dataframes, in which we will have all the data for one specific variable for all the seleted sensors. 
    
    Tp_average = pan.DataFrame()
    Light_average = pan.DataFrame()
    Light_Cum_Par = pan.DataFrame()
    Water_ave = pan.DataFrame()
    Water_delta_up = pan.DataFrame()
    Water_delta_down = pan.DataFrame()
    Water_Cum_up = pan.DataFrame()
    Water_Cum_down = pan.DataFrame() 
    
    sensor_selection = subset
                #==============================================================================
                #               Control - first part 
                #==============================================================================
    control = experiments_parameters.loc['Control']['Reply']
    
    data_above_crop_sensors = pan.DataFrame()
    data_above_crop_sensors['average_control_light'] = np.nan
    
    daily_data_above_crop_sensors = pan.DataFrame()
    daily_data_above_crop_sensors['average_control_light'] = np.nan
    
    above_crop_sensors_selection = selected_sensors[selected_sensors[experiment_name] == 2 ]
    above_crop_sensors = list(above_crop_sensors_selection[above_crop_sensors_selection.index.isin (sensor_selection)].index)
    number_of_above_crop_sensor = len(above_crop_sensors)-1
    
    if hom.Yes_or_No(control) == True:
        for x in above_crop_sensors:
            sensor_selection.remove(x)
            sensor_selection.insert(0,x)
        above_crop_sensors = list(reversed(above_crop_sensors))
            
    for sensor in sensor_selection:
        
        print(sensor)
        
         # Open a new dictionnary.
        Processed_data = {}
    
        #==============================================================================
        #               Read the data from a csv
        #==============================================================================
    
        path9 = path2+'\\'+sensor+'.txt'
        
        raw_data = pan.read_csv(path9,sep='\t',index_col = 'corrected_time' )

            # convert the index format in datetime format.
        raw_data.index = pan.to_datetime(raw_data.index)
        
        digits_raw_data = pan.Series(digits, index = ["temperature","light",'water_content'])
        raw_data = raw_data.round(digits_raw_data)
        
    
        ###============================================================================
        ###============================================================================
        ###             Correction part
        ###============================================================================
        ###============================================================================
        if (os.path.exists(path4)) == True and hom.Yes_or_No(Correction) == True:
            
            if hom.Yes_or_No(Correction_Temp) == True: 
                try:
                    slope_tp = correction_parameters.loc[sensor]['slope_tp']
                    intercept_tp = correction_parameters.loc[sensor]['Intercept_tp']
                    
                    raw_data['temperature'] = (raw_data['temperature']*slope_tp)+intercept_tp
                    raw_data['temperature'] = raw_data['temperature'].replace(- intercept_tp,0)
                except:
                    print('tp:',sensor,'not present in the correction parameters file')
    
            if hom.Yes_or_No(Correction_Light) == True:  
                try:
                    slope_light = correction_parameters.loc[sensor]['slope_light']
                    raw_data['light'] = (raw_data['light']*slope_light)
                except:
                    print('light:',sensor,'not present in the correction parameters file')

            if hom.Yes_or_No(Correction_Water) == True:  
                try:
                    slope_water = correction_parameters.loc[sensor]['slope_water']
                    intercept_water = correction_parameters.loc[sensor]['Intercept_water']
                    raw_data['water_content'] = (raw_data['water_content']*slope_water) + intercept_water
                    raw_data['water_content'] = raw_data['water_content'].replace(- intercept_water,0)
                except:
                    print('water:',sensor,'not present in the correction parameters file')
###        ###==============================================================================###        ###        
###        ###==============================================================================###        ###
###        ###==============================================================================###        ###
###        ###                                                                              ###        ###
###        ###==============================================================================###        ###
###        ###==============================================================================###        ###      
###        ###==============================================================================###        ###                
    
   ########                             Computation part                                       ########
    
###        ###==============================================================================###        ###        
###        ###==============================================================================###        ###
###        ###==============================================================================###        ###
###        ###                                                                              ###        ###
###        ###==============================================================================###        ###
###        ###==============================================================================###        ###      
###        ###==============================================================================###        ###    
    
    
        #==============================================================================
        #               First, do a subset according to the growing season or to your choice
        #==============================================================================
        
        raw_data = raw_data[raw_data.index >= str(Start)]
        raw_data = raw_data[raw_data.index < str(End)]
        
###        ###==============================================================================###        ###        
###        ###==============================================================================###        ###
###        ###==============================================================================###        ###
###        ###               Computation based on the 15 minutes data                       ###        ###
###        ###==============================================================================###        ###
###        ###==============================================================================###        ###      
###        ###==============================================================================###        ###
        
    #####
    #       temperature
    #####         
         # any computation are needed for now.
        
    #####
    #       Water_content
    #####    
        
        # Compute the running average, only for water content, with a window of 5.
        raw_data['wc_run_ave'] = raw_data['water_content'].rolling(min_periods=1,center=True,window=5).mean()
        raw_data = hom.correct_running_average_result(raw_data,'wc_run_ave','water_content',5)        
        
        # compute delta up or delta down based on 15 minutes raw_data (running average) 
        raw_data['wc_run_ave_shift_+1'] = raw_data['wc_run_ave'].shift(1)
        raw_data['delta'] = raw_data['wc_run_ave'] - raw_data['wc_run_ave_shift_+1']
          
        raw_data['delta_up'] = raw_data['delta']
        raw_data['delta_down'] = raw_data['delta']
        
        raw_data['delta_up'].where(raw_data['delta_up'] >= treshold, inplace = True)
        raw_data['delta_down'].where(raw_data['delta_down'] < treshold, inplace = True)
        
        raw_data['delta_up'] = raw_data['delta_up'].replace(np.NaN,0)
        raw_data['delta_down'] = raw_data['delta_down'].replace(np.NaN,0)
        
        raw_data['cum_delta_down'] = raw_data['delta_down'].cumsum()
        raw_data['cum_delta_up'] = raw_data['delta_up'].cumsum()
        
        del raw_data['wc_run_ave_shift_+1']
        del raw_data['delta']
        
    
    #####
    #       Light
    #####    
        
            #==============================================================================
            #               we need to use the data of the controls
            #==============================================================================
        if sensor in above_crop_sensors:
            if hom.Yes_or_No(control) == True:
                position_in_the_list = above_crop_sensors.index(sensor)
                if position_in_the_list == 0:
                    # create a new dataframe            
                    data_above_crop_sensors = pan.DataFrame(raw_data,index = raw_data.index,columns = raw_data.columns)
                    #   remove several columns
                    #   index:'corrected_time',0:'Sensor',1:"Julian_time",2:"date",3:"real_time",4:"temperature",5:"light",
                    #   6:"water_content",7:"notifications",8:"water_notifications",9:'wc_run_ave',
                    #   10:'delta_up',11:'delta_down',12:'cum_delta_down',13: 'cum_delta_up'
                                              
                    data_above_crop_sensors.drop(data_above_crop_sensors.columns[[0,2,3,6,7,8,10,11,12,13]],axis=1, inplace=True)
                    data_above_crop_sensors.rename(columns = {'temperature':sensor+'_tp','light':sensor+'_light','wc_run_ave':sensor+'_wc_run_ave'},inplace=True)
                
                if position_in_the_list > 0:
                    # create a new dataframe            
                    data_above_crop_sensor_bis = pan.DataFrame(raw_data,index = raw_data.index,columns = raw_data.columns)
                    #   remove several columns
                    #   index:'corrected_time',0:'Sensor',1:"Julian_time",2:"date",3:"real_time",4:"temperature",5:"light",
                    #   6:"water_content",7:"notifications",8:"water_notifications",9:'wc_run_ave',
                    #   10:'delta_up',11:'delta_down',12:'cum_delta_down',13: 'cum_delta_up'
                                              
                    data_above_crop_sensor_bis.drop(data_above_crop_sensor_bis.columns[[0,1,2,3,6,7,8,10,11,12,13]],axis=1, inplace=True)
                    data_above_crop_sensor_bis.rename(columns = {'temperature':sensor+'_tp','light':sensor+'_light','wc_run_ave':sensor+'_wc_run_ave'},inplace=True)
                    data_above_crop_sensors = pan.concat([data_above_crop_sensors,data_above_crop_sensor_bis],axis=1)
                    del (data_above_crop_sensor_bis)
                
                if number_of_above_crop_sensor == position_in_the_list:
                    # compute the average, corresponding to the reference 
                
                    # Just perform a list comprehension to create your columns:
                    temperature = [col for col in list(data_above_crop_sensors) if col.endswith('_tp')]
                    light = [col for col in list(data_above_crop_sensors) if col.endswith('_light')]
                    water = [col for col in list(data_above_crop_sensors) if col.endswith('_wc_run_ave')]

                    data_above_crop_sensors['average_control_temperature'] = data_above_crop_sensors[temperature].mean(axis=1)
                    data_above_crop_sensors['average_control_light'] = data_above_crop_sensors[light].mean(axis=1)
                    data_above_crop_sensors['average_control_water'] = data_above_crop_sensors[water].mean(axis=1)
                    
                    # print the result in a new file 
                    path10 = path_15_minutes_data+'\\'+'above_crop_sensors'
                    os.mkdir(path10)
                    above_crop_sensors_15_minutes_data = open('data_above_crop_sensors.txt','a')
                    data_above_crop_sensors.to_csv(above_crop_sensors_15_minutes_data,sep = '\t',na_rep = np.NaN)
                    above_crop_sensors_15_minutes_data.close()
                    
                    # do the plots for the control
                    
                    plot_above_crop_sensor = data_above_crop_sensors.plot(y =['average_control_water','average_control_temperature','average_control_light'],legend = False,subplots=True,figsize=(13,7),rot = 45,
                                  grid = True)
                                  
                    [plot_above_crop_sensor[0].set_ylabel('wc_run_ave (%)')]
                    [plot_above_crop_sensor[1].set_ylabel('temperature (in C°)')]
                    [plot_above_crop_sensor[2].set_ylabel('PAR irradiance (mole.m-2.d-1)')]
                    # change the free space between subplots
                    plt.subplots_adjust(hspace=.7)
                    # Supported formats: emf, eps, pdf, png, ps, raw, rgba, svg, svgz
                    plt.savefig(path10+'\\'+"plot_above_crop_sensor"+'.'+Format)
                    plt.close()
        
                    
        else:
            
                # compute the Par absorbed
            raw_data['Par_absorbed'] = data_above_crop_sensors['average_control_light'] - raw_data['light']
                    # remove the negative numbers
            raw_data['Par_absorbed'] = raw_data['Par_absorbed'].clip(lower=0)
            
            raw_data = raw_data.round(pan.Series([1,2,4,4,2,4,4,4,4], index = [ "temperature","light",'water_content','wc_run_ave','Par_absorbed','delta_up','delta_down','cum_delta_up','cum_delta_down' ]))
            
    #####       
    #####
    #       Do the plot and write the data in a file
    #####
    #####
            
        # create a new folder for each sensors to store the data and the plots
        
        path11 = path_15_minutes_data+'\\'+sensor
        os.mkdir(path11)
        
        if hom.Yes_or_No(Plot_15_minutes_data) == True:
            # water
            water = raw_data.plot(y =['water_content','wc_run_ave'],legend = False,subplots=True,
                              figsize=(13,7),sharey=True,rot = 45,grid = True)
            # we have to create the legends in a separate step.
            #[water.legend(bbox_to_anchor=(1,1.20)) for water in plt.gcf().axes]
            [water[0].set_ylabel('Water_content (%)')]
            [water[1].set_ylabel('water_content (running average - %)')]
            plt.savefig(path11+'\\'+sensor+"water."+Format) # or svg (vector format)
            plt.close()
                
            # light
            light = raw_data.plot(y =['light','Par_absorbed'], legend = False, subplots=True,
                              figsize=(13,7),sharey=True,rot = 45,grid = True )
            # we have to create the legends in a separate step.
            #[light.legend(bbox_to_anchor=(1,1.20)) for light in plt.gcf().axes]
            [light[0].set_ylabel('Par irradiance (mole m-² d-1)')]
            [light[1].set_ylabel('Par_absorbed (mole m-² d-1)')]
            # change the free space between subplots
            plt.subplots_adjust(hspace=.7)
            plt.savefig(path11+'\\'+sensor+"light."+Format)
            plt.close()
                
            # temperature
            temp = raw_data.plot(y =['temperature'], legend = False,figsize=(13,7),rot = 45,grid = True)
            temp.set_ylabel('temperature (C)')
            plt.savefig(path11+'\\'+sensor+"temp."+Format)
            plt.close()
            
            
        processed_data_15_minutes_data = open(path11+'\\'+sensor+'_15_minutes_data.txt','a')
        raw_data.to_csv(processed_data_15_minutes_data,sep = '\t')
        processed_data_15_minutes_data.close()
        
        
###        ###==============================================================================###        ###        
###        ###==============================================================================###        ###
###        ###==============================================================================###        ###
###        ###               Computation based on the daily data                            ###        ###
###        ###==============================================================================###        ###
###        ###==============================================================================###        ###      
###        ###==============================================================================###        ###    

    
            # determine the last and the fisrt day for which we have data
        first_day = raw_data.index[0]
        last_day = raw_data.index[-1]
            # compute the number of days for which we have data
        number_of_day = last_day - first_day
        number_of_day = number_of_day.days
        
    #####
    #####   
        
        for i in range(0,number_of_day):
            first_day_str = str(first_day)
             # to have only the date, we have to remove the hour. it is the reason we have to select the first 11 characters
            start = first_day_str[0:11]
            one_day = raw_data[start]
                
            ###########   
            # water  
            ###########
            if one_day['water_content'].count() == 0 or one_day['water_content'].isnull().sum() > treshold_NaN: # that means we don't have data or less than 10 NaN
                wc_ave = np.NaN     #
                wc_std = np.NaN     #
                wc_min = np.NaN     #
                wc_max = np.NaN     #
                wc_delta_up = np.NaN     #
                wc_delta_down = np.NaN     #
                wc_15m_delta_up = one_day['delta_up'].sum()     #
                wc_15m_delta_down = one_day['delta_down'].sum()     #
            else:
                wc_midnight = one_day['wc_run_ave'][0]     #
                wc_ave = one_day['wc_run_ave'].mean()      #
                wc_std = one_day['wc_run_ave'].std()     #
                wc_min = one_day['wc_run_ave'].min()     #
                wc_min_ind = one_day['wc_run_ave'].idxmin()     #
                wc_max = one_day['wc_run_ave'].max()     #
                wc_max_ind = one_day['wc_run_ave'].idxmax()     #
                wc_23_45_00 = one_day['wc_run_ave'][-1]     #
                wc_delta = hom.delta_comp (one_day,wc_min_ind,wc_max_ind,wc_min,wc_midnight,wc_23_45_00,wc_max)
                wc_delta_up = wc_delta[0]     #
                wc_delta_down = wc_delta[1]     #
                wc_15m_delta_up = one_day['delta_up'].sum()     #
                wc_15m_delta_down = one_day['delta_down'].sum()     #
                
            ###########
            # light 
            ###########
            if one_day['light'].count() == 0 or one_day['light'].isnull().sum() > treshold_NaN: # that means we don't have data or less than 10 NaN
                light_ave = np.NaN
            
            else: 
                light_ave = "%.2f" %one_day['light'].mean()
                
            ###########
            # temperature
            ###########
                
            if one_day['temperature'].count() == 0 or one_day['temperature'].isnull().sum() > treshold_NaN: # that means we don't have data or less than 10 NaN
                tp_ave = np.NaN 
                tp_std = np.NaN
                tp_min = np.NaN 
                tp_max = np.NaN
            
            else:
                tp_ave = "%.1f" %one_day['temperature'].mean() 
                tp_std = "%.1f" %one_day['temperature'].std()
                tp_min = "%.1f" %one_day['temperature'].min() 
                tp_max = "%.1f" %one_day['temperature'].max()
                
            first_day += datetime.timedelta(days = 1)
            julian_day = hom.Time(first_day)
            
            Processed_data[i] = {'day' : start,'julian_day' : julian_day,'Sensor' : sensor,
            'temperature_average' : tp_ave,'temperature_std' : tp_std, 'temperature_min' : tp_min,
            'temperature_max' : tp_max, 'light_average' : light_ave,'water_average' : wc_ave,
            'water_std' : wc_std, 'water_min' : wc_min, 'water_max' : wc_max, 
            'wc_delta_up' : wc_delta_up, 'wc_delta_down' : wc_delta_down,
            'wc_15m_delta_up' : wc_15m_delta_up,'wc_15m_delta_down' : wc_15m_delta_down }
        
        # convert the dict in a dataframe
        Processed_data = pan.DataFrame.from_dict(Processed_data,orient = 'index',dtype = None)
        Processed_data = Processed_data[['day','julian_day','Sensor','temperature_average','temperature_std','temperature_min','temperature_max',
                       'light_average','water_average','water_std','water_min','water_max',
                       'wc_delta_up','wc_delta_down','wc_15m_delta_up', 'wc_15m_delta_down']]        
    
        Processed_data = Processed_data.set_index('day')
        Processed_data.index = pan.to_datetime(Processed_data.index)
        
            
        if sensor in above_crop_sensors:
            if hom.Yes_or_No(control) == True:
                position_in_the_list = above_crop_sensors.index(sensor)
                if position_in_the_list == 0:
                    # create a new dataframe            
                    daily_data_above_crop_sensors = pan.DataFrame(Processed_data,index = Processed_data.index,columns = Processed_data.columns)
                    # remove several columns
                    # index :'day',0 :'julian_day', 1:'Sensor', 2:'temperature_average', 3:'temperature_std', 4:'temperature_min', 5:'temperature_max',
                    # 6: 'light_average', 7:'water_average', 8:'water_std', 9:'water_min', 10:'water_max',
                    # 11:'wc_delta_up', 12:'wc_delta_down', 13:'wc_15m_delta_up', 14:'wc_15m_delta_down'                      
                    daily_data_above_crop_sensors.drop(daily_data_above_crop_sensors.columns[[1,3,4,5,8,9,10,11,12,13,14]],axis=1, inplace=True)
                    daily_data_above_crop_sensors.rename(columns = {'temperature_average':sensor+'_tp','light_average':sensor+'_light','water_average':sensor+'_wc_run_ave'},inplace=True)
                
                if position_in_the_list > 0:
                    # create a new dataframe            
                    daily_data_above_crop_sensor_bis = pan.DataFrame(Processed_data,index = Processed_data.index,columns = Processed_data.columns)
                    daily_data_above_crop_sensor_bis.drop(daily_data_above_crop_sensor_bis.columns[[0,1,3,4,5,8,9,10,11,12,13,14]],axis=1, inplace=True)
                    daily_data_above_crop_sensor_bis.rename(columns = {'temperature_average':sensor+'_tp','light_average':sensor+'_light','water_average':sensor+'_wc_run_ave'},inplace=True)
                    daily_data_above_crop_sensors = pan.concat([daily_data_above_crop_sensors,daily_data_above_crop_sensor_bis],axis=1)
                
                if number_of_above_crop_sensor == position_in_the_list:
                    # compute the average, corresponding to the reference 
                
                    # Just perform a list comprehension to create your columns:
                    temperature = [col for col in list(daily_data_above_crop_sensors) if col.endswith('_tp')]
                    light = [col for col in list(daily_data_above_crop_sensors) if col.endswith('_light')]
                    water = [col for col in list(daily_data_above_crop_sensors) if col.endswith('_wc_run_ave')]
                    
                    daily_data_above_crop_sensors['average_control_temperature'] = daily_data_above_crop_sensors[temperature].mean(axis=1)
                    daily_data_above_crop_sensors['average_control_light'] = daily_data_above_crop_sensors[light].mean(axis=1)
                    daily_data_above_crop_sensors['average_control_water'] = daily_data_above_crop_sensors[water].mean(axis=1)
                    daily_data_above_crop_sensors['average_control_light_cum'] = daily_data_above_crop_sensors['average_control_light'].cumsum()
                    daily_data_above_crop_sensors['run_average_control_light_cum'] = daily_data_above_crop_sensors['average_control_light_cum'].rolling(min_periods=1,center=True,window=11).mean()
                                     
                    daily_data_above_crop_sensors = hom.correct_running_average_result (daily_data_above_crop_sensors,'run_average_control_light_cum','run_average_control_light_cum',11)
            
                    # print the result in a new file 
                    path12 = path_daily_data+'\\'+'above_crop_sensors'
                    os.mkdir(path12)
                    above_crop_sensors_daily_data = open('daily_data_above_crop_sensors.txt','a')
                    daily_data_above_crop_sensors.to_csv(above_crop_sensors_daily_data,sep = '\t',na_rep = np.NaN)
                    above_crop_sensors_daily_data.close()
                    
                    # do the plots for the control
                    
                    plot_above_crop_sensor = daily_data_above_crop_sensors.plot(y =['average_control_water','average_control_temperature','average_control_light'],legend = False,subplots=True,figsize=(13,7),rot = 45,
                                  grid = True)
                                  
                    [plot_above_crop_sensor[0].set_ylabel('wc_run_ave (%)')]
                    [plot_above_crop_sensor[1].set_ylabel('temperature (in C°)')]
                    [plot_above_crop_sensor[2].set_ylabel('PAR irradiance (mole.m-2.d-1)')]
                    # change the free space between subplots
                    plt.subplots_adjust(hspace=.7)
                    # Supported formats: emf, eps, pdf, png, ps, raw, rgba, svg, svgz
                    plt.savefig(path12+'\\'+"plot_above_crop_sensor"+'.'+Format)
                    plt.close()
        
        
        else:
            
                # compute the Par absorbed
            Processed_data['light_average'] = pan.to_numeric(Processed_data['light_average'], errors='coerce')
                
            Processed_data['light_average_cum'] = Processed_data['light_average'].cumsum()
            
            if number_of_above_crop_sensor > 0 :
        
                Processed_data['run_average_light_average_cum'] = Processed_data['light_average_cum'].rolling(min_periods=1,center=True,window=10).mean()
                Processed_data = hom.correct_running_average_result (Processed_data,'run_average_light_average_cum','run_average_light_average_cum',11)
                
                Processed_data['Fint%'] = 100 *((daily_data_above_crop_sensors['run_average_control_light_cum'] -  Processed_data['run_average_light_average_cum'])/ daily_data_above_crop_sensors['run_average_control_light_cum'])
            else:
                Processed_data['Fint%'] = np.nan
                
            ###########
            # Water 
            ###########
            
            Processed_data['Cum_up'] = Processed_data['wc_delta_up'].cumsum()
            
            Processed_data['Cum_down'] = Processed_data['wc_delta_down'].cumsum()
            
            Processed_data['Cum_up_15_minutes_data'] = Processed_data['wc_15m_delta_up'].cumsum()
            
            Processed_data['Cum_down_15_minutes_data'] = Processed_data['wc_15m_delta_down'].cumsum()
        
        #==============================================================================
        #              
        #==============================================================================
        Processed_data = Processed_data.round(pan.Series([1,1,1,1,2,4,4,4,4,4,4,4,4,2,2], index = ['temperature_average','temperature_std','temperature_min','temperature_max',
                       'light_average','water_average','water_std','water_min','water_max',
                       'wc_delta_up','wc_delta_down','wc_15m_delta_up', 'wc_15m_delta_down','light_average_cum','Fint%'] ))
                       
        # create a new folder for each sensors to store the data and the plots
        
        path_processed_data = path_daily_data+'\\'+sensor
            # 
        os.mkdir(path_processed_data)
            # change the current working directory
        os.chdir(path_processed_data)     
        
        if hom.Yes_or_No(plot) == True:
            if (sensor in above_crop_sensors) == False:
                
                # water
                water = Processed_data.plot(y =['water_average','wc_delta_up','wc_delta_down'],legend = False,subplots=True,
                                  figsize=(13,7),rot = 45,grid = True)
                [water[0].set_ylabel('Water_content (%)')]
                [water[1].set_ylabel('wc_delta_up (%)')]
                [water[1].set_ylabel('wc_delta_up (%)')]
                plt.subplots_adjust(hspace=.7)
                plt.savefig("water"+'.'+Format) # or svg (vector format)
                plt.close()
                
                # light
                light = Processed_data.plot(y =['light_average_cum','Fint%'], legend = False, subplots=True,
                                  figsize=(13,7),sharey=False,rot = 45,grid = True )
                # we have to create the legends in a separate step.
                #[light.legend(bbox_to_anchor=(1,1.20)) for light in plt.gcf().axes]
                [light[0].set_ylabel('light_average_cum (mole m-² d-1)')]
                [light[1].set_ylabel('%_intercepted')]
                [plt.ylim(-10, 100)]
                # change the free space between subplots
                plt.subplots_adjust(hspace=.7)
                plt.savefig("light"+'.'+Format)
                plt.close()
    
        
        # write the dataframe in a text file
        text_file = open(sensor+'_daily.txt','a')
        Processed_data.to_csv(text_file,sep = '\t')
        text_file.close()
    
        if (sensor in above_crop_sensors) == False:
            Tp_average[sensor] = Processed_data.temperature_average
            Light_average[sensor] = Processed_data.light_average
            Light_Cum_Par[sensor] = Processed_data['Fint%']
            Water_ave[sensor] = Processed_data.water_average
            Water_delta_up[sensor] = Processed_data.wc_delta_up
            Water_delta_down[sensor] = Processed_data.wc_delta_down
            Water_Cum_up[sensor] = Processed_data.Cum_up
            Water_Cum_down[sensor] = Processed_data.Cum_down
    
    os.chdir(path_daily_data)       
    
    text_file_tp = open('Tp_average.txt','a')
    Tp_average.to_csv(text_file_tp,sep = '\t',na_rep = np.NaN)
    Tp_average = Tp_average.iloc[-1]
    text_file_tp.close()
    
    text_file_light = open('Light_average.txt','a')
    Light_average.to_csv(text_file_light,sep = '\t',na_rep = np.NaN)
    text_file_light.close()
    
    text_file_light2 = open('Light_Cum_Par.txt','a')
    Light_Cum_Par.to_csv(text_file_light2,sep = '\t',na_rep = np.NaN)
    text_file_light2.close()
    
    text_file_Water_ave = open('Water_average.txt','a')
    Water_ave.to_csv(text_file_Water_ave,sep = '\t',na_rep = np.NaN)
    text_file_Water_ave.close()
    
    text_file_Water_delta_up = open('Water_delta_up.txt','a')
    Water_delta_up.to_csv(text_file_Water_delta_up,sep = '\t',na_rep = np.NaN)
    text_file_Water_delta_up.close()
    
    text_file_Water_delta_down = open('Water_delta_down.txt','a')
    Water_delta_down.to_csv(text_file_Water_delta_down,sep = '\t',na_rep = np.NaN)
    text_file_Water_delta_down.close()
    
    text_file_Water_Cum_up = open('Water_Cum_up.txt','a')
    Water_Cum_up.to_csv(text_file_Water_Cum_up,sep = '\t',na_rep = np.NaN)
    text_file_Water_Cum_up.close()
    
    text_file_Water_Cum_down = open('Water_Cum_down.txt','a')
    Water_Cum_down.to_csv(text_file_Water_Cum_down,sep = '\t',na_rep = np.NaN)
    text_file_Water_Cum_down.close()
    
    
    Summary_dict = {'Sensors' : pan.Series (Tp_average.index.values.tolist()), 
       'Light_Cum' : pan.Series((Light_Cum_Par.iloc[-1]).values.tolist()),
       'Water_Cum_up' :  pan.Series ((Water_Cum_up.iloc[-1]).values.tolist()),
       'Water_Cum_down' : pan.Series ((Water_Cum_down.iloc[-1]).values.tolist())}
       
    summary = pan.DataFrame(Summary_dict)
    summary.set_index ('Sensors',inplace = True)
    summary = summary.transpose()
    summary['Average'] = summary.mean(axis=1)
    summary['Std'] = summary.std(axis=1)
    summary = summary.transpose()
    
    text_file_summary = open('Summary.txt','a')
    summary.to_csv(text_file_summary,sep = '\t',na_rep = np.NaN)
    text_file_summary.close()
