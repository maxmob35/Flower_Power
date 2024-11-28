# -*- coding: utf-8 -*-
"""
Created on  Mon Sep 12 11:15:14 2016

@author: Maxime Mobailly - Robert Van Loo (supervisor and reviewer) - Wageningen University

We have already retrieved the data from the cloud. Nevertheless we must to
standardize the values we get using the average of all the sensors as reference.

This will be done by periods, not on the 15 minutes data.
Each day will be split into 6 periods of 4 hours each.

This scipt require parameters about the experiment (beginning, ending of the standardization)
"""

import pandas as pan
import datetime
import numpy as np
import utils as hom
from statsmodels.formula.api import ols

def standardization_script (sensor_selected,Start,End,path2,digits):
    
    # create three new dataframes (one for each variable)
    tp_av_allsensors = pan.DataFrame()
    light_av_allsensors = pan.DataFrame()
    water_av_allsensors = pan.DataFrame()
    
    # j is used to incremente the dictionarry Standardization.
    j = 0
    
    first_day = Start
    
    for sensor in sensor_selected:
        
        first_day = Start
        
        print(sensor)
        
         # Open a new dictionnary.
        Standardization = {}
    
        #==============================================================================
        #               Read the data from a csv
        #==============================================================================
        # download the data from one sensor at a time
        path6 = path2+'\\'+sensor+'.txt'
        
        standardization_data = pan.read_csv(path6,sep='\t',index_col = 'corrected_time')
        
            # convert the index format in datetime format.
        standardization_data.index = pan.to_datetime(standardization_data.index)
        digits_standardization_data = pan.Series(digits, index = ["temperature","light",'water_content'])
        standardization_data = standardization_data.round(digits_standardization_data)
        
        
    ###============================================================================
    ###============================================================================
    ###             Computation part
    ###============================================================================
    ###============================================================================
        
        #==============================================================================
        #            First, some details about the experiment.   
        #==============================================================================
        
        standardization_data = standardization_data[standardization_data.index >= str(Start)]
        standardization_data = standardization_data[standardization_data.index <= str(End)]
        
        test1 = len(standardization_data)
        if test1 == 0:
            print('no values, no standardization possible')
            continue
        
        ###============================================================================
        ###============================================================================
        ###             Standardization part
        ###============================================================================
        ###============================================================================
        
            # Compute the running average, only for water content, with a window of 5.
        standardization_data['wc_run_ave'] = standardization_data['water_content'].rolling(min_periods=1,center=True,window=5).mean()
        
        standardization_data = hom.correct_running_average_result (standardization_data,'wc_run_ave','water_content',5)
    
            # compute the number of days for which we have data
        number_of_day = End - Start
        number_of_day = (number_of_day.days)
        
    #####
    #####   
        
        for i in range(1,number_of_day):
            # 4*4 = 16 time points by period
            # there are 6 periods of 4 hours in a day.
            first_day_str = str(first_day)
            start = first_day_str[:11]
            one_day = standardization_data[start]
            
            z=0
            # to select 16 time points
            # they are 6 periods by day
            for y in range (1,7):
                
                period = one_day[one_day.index >= one_day.index[z]]
                period = one_day[one_day.index <= one_day.index[z+15]]
                z+=16
                
                ###########
                # water 
                ###########
                # count how many missing data we have in this particular period
                if period['water_content'].count() == 0 or period['water_content'].isnull().sum() > 2: 
                    # that means we don't have data at all or more than 2 NaN (16 values by period -> 
                    # more than 12.5 % of missing data)
                    wc_ave = np.nan
                    wc_std = np.nan
                                         
                else:
                    wc_ave = period['water_content'].mean() 
                    wc_std = period['water_content'].std()
                                        
                    ###########
                    # light 
                    ###########
                    
                if period['light'].count() == 0 or period['light'].isnull().sum() > 2:
                    light_ave = np.nan
                    light_std = np.nan
                    
                else: 
                    light_ave = period['light'].mean()
                    light_std = period['light'].std()
                    
                ###########
                # temperature
                ###########
                    
                if period['temperature'].count() == 0 or period['temperature'].isnull().sum() > 2:
                    tp_ave = np.nan 
                    tp_std = np.nan
                
                else:
                    tp_ave = period['temperature'].mean() 
                    tp_std = period['temperature'].std()
            
                Standardization [j] = {'0' : start+'period'+str(y),'1' : hom.Time(first_day),'2' : sensor, '3' : tp_ave,
                                '4' : tp_std, '5' : light_ave, '6' : light_std, '7' : wc_ave,'8' : wc_std}  
                j+=1
                
                del(period)   
                    
            # this day was complelely analyzed
            first_day += datetime.timedelta(days = 1)
            
           
        # convert the dict in a dataframe
        correction_parameters = pan.DataFrame.from_dict(Standardization,orient = 'index',dtype = None)
        correction_parameters = correction_parameters[['0','1','2','3','4','5','6','7','8']]
                       
        correction_parameters.rename(columns = {'0':'day','1':'julian_day','2':'Sensor','3':sensor+"tp_av",'4':sensor+"tp_std",
                                                '5':sensor+"light_average",'6':sensor+'light_std',
                                                '7':sensor+"water_av",'8':sensor+'water_std'},inplace = True)
        
        tp_av = correction_parameters[['day',sensor+"tp_av"]]
        light_av = correction_parameters[['day',sensor+"light_average"]]
        water_av = correction_parameters[['day',sensor+"water_av"]]
    
        del (correction_parameters)
        
        tp_av.set_index("day",inplace =True)
        light_av.set_index("day",inplace =True)
        water_av.set_index("day",inplace =True)

        tp_av_allsensors = pan.concat([tp_av_allsensors,tp_av],axis = 1 )
        light_av_allsensors = pan.concat([light_av_allsensors,light_av],axis = 1 )
        water_av_allsensors = pan.concat([water_av_allsensors,water_av],axis = 1 )
        
        del (tp_av)
        del (light_av)
        del (water_av)
    
    tp_av_allsensors['average'] = tp_av_allsensors.mean(axis=1)
    light_av_allsensors['average'] = light_av_allsensors.mean(axis=1)
    water_av_allsensors['average'] = water_av_allsensors.mean(axis=1)
    
    # quality check
    water_av_allsensors['std'] = water_av_allsensors.std(axis=1)
    
    water_av_allsensors3 = water_av_allsensors.copy()
    
    treshold = water_av_allsensors3['average'] - 3 * (water_av_allsensors3['std'])
    
    
    for columns in water_av_allsensors3:
        # one column at a time
        if columns == 'average' or columns == 'std':
            continue
         # X correspond to a serie
        ww = water_av_allsensors3[columns] < treshold 
        # it is also a serie
        wnb = ww[ww == True].count()
        # count the number of 'True' result.
        
        if wnb > 10:
            del(water_av_allsensors3[columns])
            print(columns,'has been removed and not used in the average computation')
            
    water_av_allsensors['average'] = water_av_allsensors3.mean(axis=1)
    
    print('Linear regresssions to assess the correction parameters')
    
    temp_reg_result = {}
    i = 0 
    
    for column in tp_av_allsensors:
        sensor_name = column
        sensor_name = sensor_name.replace("tp_av","")
        if column == 'average' or column == 'std':
            continue
        else:
            sensor = tp_av_allsensors[column]
            param = sensor.isnull().sum()
            if param > number_of_day :
                # 25 % of missing data
                continue
            else:
                mod = ols(formula = 'sensor ~ average',data = tp_av_allsensors)
                res = mod.fit()
                intercept_tp = res.params.Intercept
                slope_tp = res.params.average
                R_square_tp = res.rsquared
    
                temp_reg_result[i] = {'Sensor' : sensor_name ,'Intercept_tp' : intercept_tp ,'slope_tp': slope_tp, 'R_square_tp': R_square_tp}
                i+=1
                    
                
    temp_reg_result = pan.DataFrame.from_dict(temp_reg_result,orient = 'index',dtype = None)
    temp_reg_result = temp_reg_result[['Sensor','Intercept_tp','slope_tp','R_square_tp']]  
    temp_reg_result.set_index('Sensor',inplace = True)

    
    light_reg_result = {}
    i = 0 
    
    for column in light_av_allsensors:
        sensor_name = column
        sensor_name = sensor_name.replace("light_average","")
        if column == 'average'or column == 'std':
            continue
        else:
            sensor = light_av_allsensors[column]
            param = sensor.isnull().sum()
            if param > number_of_day :
                # 25 % of missing data
                continue
            else:
                mod = ols(formula = 'sensor ~ average -1' ,data = light_av_allsensors)
                res = mod.fit()
                slope_light = res.params.average
                R_square_light = res.rsquared
                    
                light_reg_result[i] ={'Sensor' : sensor_name ,'slope_light': slope_light, 'R_square_light': R_square_light}
                i+=1
                
    light_reg_result = pan.DataFrame.from_dict(light_reg_result,orient = 'index',dtype = None)
    light_reg_result = light_reg_result[['Sensor','slope_light','R_square_light']]  
    light_reg_result.set_index('Sensor',inplace = True)

    water_reg_result = {}
    i = 0 
    
    for column in water_av_allsensors:
        sensor_name = column
        sensor_name = sensor_name.replace("water_av","")
        
        if column == 'average'or column == 'std':
            continue
        else:
            sensor = water_av_allsensors[column]
            param = sensor.isnull().sum()
            if param > number_of_day :
                continue
            else:
                mod = ols(formula = 'sensor ~ average ' ,data = water_av_allsensors)
                res = mod.fit()
                intercept_water = res.params.Intercept
                slope_water = res.params.average
                R_square_water = res.rsquared
                
                water_reg_result[i] = {'Sensor' : sensor_name ,'Intercept_water' : intercept_water, 'slope_water': slope_water, 'R_square_water': R_square_water}
                i+=1
            
    water_reg_result = pan.DataFrame.from_dict(water_reg_result,orient = 'index',dtype = None)
    water_reg_result = water_reg_result[['Sensor','Intercept_water','slope_water','R_square_water']]  
    water_reg_result.set_index('Sensor',inplace = True)

        
    # merge the three dataframe
    correction_parameters = pan.concat([temp_reg_result,light_reg_result,water_reg_result],axis = 1)
    
    return (correction_parameters)

def standardization_preparation (experiment_name,parameter1,parameter2,account_number,experiments_sensors,subset,number_of_experiments,experiments_details):
    # return a list containing the names and the position of the entries in the directory given by path2 (dates)
    
    global experiment
    
    experiments_details['start standardization'] = pan.to_datetime(experiments_details['start standardization'])
    experiments_details['end standardization'] = pan.to_datetime(experiments_details['end standardization'])
    
    experiment_name_ = experiment_name
    
    if hom.Yes_or_No(parameter1) == True :
        sensor_selected = subset
        Start = experiments_details.loc[experiment_name]['start standardization']
        End = experiments_details.loc[experiment_name]['end standardization']
        Stop = True
        
    else: 
        if parameter2 == True:
            sensor_selected = subset
            Start = experiments_details.loc[experiment_name]['start standardization']
            End = experiments_details.loc[experiment_name]['end standardization']
            Stop = True
            
        else:
            x1 = experiments_details[experiments_details.account == account_number]
            details = experiments_details.loc[experiment_name]['details']
            if details == 'account':
                experiments_details = experiments_details.drop(experiment_name)
            
            if experiment < number_of_experiments:
                experiment_name_ = (list(x1.index))[experiment]
                Start = x1.iloc[experiment]['start standardization']
                End = x1.iloc[experiment]['end standardization']
                sensor_selected = list(experiments_sensors[experiments_sensors[experiment_name_] != 0 ].index)
                experiment+=1
                Stop = False
                print(experiment_name_)
                
            if experiment >= number_of_experiments:
                Stop = True

    return(sensor_selected,Start,End,experiment_name_,Stop,experiments_details)