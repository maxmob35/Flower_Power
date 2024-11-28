# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 08:43:18 2016

@author: Maxime Mobailly
"""

#==============================================================================
#      The time used by the sensors correspond to the Coordinated universal time
#==============================================================================
print('#####    #####')

print(
    'The time used by the sensor corresponds to the Coordinated Universal Time \n'
    'UTC is the time standard commonly used across the world. \n'
    'Coordinated Universal Time (UTC) is the basis for civil time today. \n'
    'UTC+2 for the Netherlands, at UTC = 11:23, the current local time in The Netherlands is 13:23. \n'
    'This is True only for the summer time period, other wise it is UTC+1.'
    )

print('#####    #####')

print('Air temperature : Range: -5°C to +55°C, Accuracy: +/- 1.5°C')
print('Light : Range: 0.13 to 104 (mole m-² d-1), Accuracy: +/- 15%')
print('Soil Moisture : Range: 0 to 50 (%), Accuracy: +/-3')
print('Fertilizer : Range: 0 to 10 (mS/cm), Accuracy: +/-20%')

print('#####    #####')

print('Data can be stored by one Flower Power for up to 80 days without any data loss.')

print('#####    #####')

print('You can control up to 256 Parrot Flower Power products with one account.')

print('#####    #####')

#==============================================================================
#               To run the different script, some packages must be downloaded
#==============================================================================

import os
import time
import pandas as pan

import Loggin as log
from Serial_Number import serial_number
import data_retrieving2 as drt
import data_standardization as dst
import data_processing as dp
import utils as hom

#
# time1
beginning = time.strftime("%Y-%m-%d-%H-%M-%S")
    
    #==================================================================================
    #               Determine several parameters
    #
    #  Part where we determine several parameters, variables and path used in the script
    #==================================================================================
                    
    # return the path of the program used - CWD = « Current Working Directory »
path0 = os.getcwd()
input_file = 'C:\\Users\\Maxime\\Documents\\Doc\\WUR\\completed script\\input_file.xlsx'
experiments_details = pan.read_excel(input_file, sheetname=3, header=0,index_col = 0) 
experiments_sensors = pan.read_excel(input_file, sheetname=2, header=0,index_col = 0)
experiments_parameters = pan.read_excel(input_file, sheetname=1, header=0,index_col = 0)

digits = [int(experiments_parameters.loc['digit_temperature']['Reply']),
                int(experiments_parameters.loc['digit_light']['Reply']),
                int(experiments_parameters.loc['digit_water']['Reply'])]
                                        
#==============================================================================
#               login
#==============================================================================
                                        
Account_list_begining =
Account_reference = ['first_account', 'second_account','third_account']
    
        # it's a list containing the three different accounts we have
Account_list = list(Account_list_begining)

test_while_loop = False

while test_while_loop == False:
    experiment_name = experiments_parameters.loc['experiment_name']['Reply']
    if (experiment_name in experiments_sensors.columns) == True:
         test_while_loop = True
    else:
        continue

details = experiments_details.loc[experiment_name]['details']
Account = experiments_details.loc[experiment_name]['account']
    # process to select the selected account(s) from the Acccount_list
    
Account_list = log.account_selection(Account,Account_list)
# return a list with the selected account(s)

total_account_number = len (Account_list)
i = 0
       
if total_account_number > 1:
    parameter1 = experiments_parameters.loc['parameter1']['Reply']
    # parameter1 == True, means you chosed to consider only one experiment
else:
    parameter1 = 'No'
    
    # to incremente the number of account.
experiment_by_account = 0

# for loop to work on the several account if it is needed. 
 
for username in Account_list:
    account_selected = experiments_sensors [experiments_sensors['account'] == username]
    sensors_number = len(account_selected.index)
    selected_sensors = account_selected [account_selected [experiment_name] != 0]
    selected_sensors_number = len(selected_sensors)
    if sensors_number == selected_sensors_number:
        parameter2 = False
    else:
        parameter2 = True

    print(username)
    
    account_number = Account_list_begining.index(username)+1
    
    if hom.Yes_or_No(parameter1) == False:
        name = username
        rename = True
    else: 
        name = str(Account_list)
        rename = False
        
        # loggin step
    loggin_result = log.loggin(username)
    # this function return different variables:
    
    loc2name = loggin_result[0]
    # ---> Build a dict to link location_identifiers to the sensors' nickname
    auth_header = loggin_result[1]
    
    #==============================================================================
    #               get a dataframe with the sensors' nickname and their serial number
    #==============================================================================
    
    # if the folder are already existing move directly to the newt step 
    condition1 = os.path.exists(path0+'\\serialnumber_'+username+'.txt')
    
    if condition1 == False:
        serial2name = loggin_result[2]
            # ---> Build a dict to link serial number to the sensors' nickname
        nickname_serialnumber = serial_number(serial2name)
        serialnumber = open(path0+'\\serialnumber_'+username+'.txt','a')
        nickname_serialnumber.to_csv(serialnumber,sep = '\t')
        serialnumber.close()
        
    #==============================================================================
    #               retrieve the data from the cloud
    #==============================================================================
    
    # there is three different possibilites:
    # (1) only one account has been selected - the most simple case
    # (2) two or three account has been selected:
    #   (2a) analyzed separately
    #   (2b) analyzed together
    
    # in the case (2b) we have to use the same folder to store the data.
    if rename == True or i == 0:
        
        # we are going to create a new folder automatically called by the current day
            # we have to check if this folder is not already existing.
            # name the new folder year_month_day_hour_minute
        folder_name = time.strftime("%Y_%m_%d_%H_%M")+'_'+name
                # time.strftime -> get the current time
            # return the path of the folder we want to create.
        path2 = path0+'\\'+folder_name
            # Check if the folder is already existing
        condition2 = os.path.exists(path2)
        # if the folder is already existing wait 30 seconds.
        if condition1 == True:
            time.sleep(30)
            folder_name = time.strftime("%Y_%m_%d_%H_%M")+'_'+name
            path2 = path0+'\\'+folder_name
            # then, create the new folder
        os.mkdir(path2)
            # change the directory to write the files inside
        os.chdir(path2)
        
    subset = list(selected_sensors.index)
    retrieved_data = drt.data_retrieving(username,path0,auth_header,loc2name,parameter2,path2,folder_name,subset,name,experiments_parameters,digits)
    # return(subset,Start,End)
    
    i+=1
    # in the case (2b) we have to wait the last account was retrieved before to go to the next step.
    if hom.Yes_or_No(parameter1) == True:
        if i < total_account_number:
            continue
        else:
            subset = os.listdir(path2)
            subset = [item.replace('.txt', '') for item in subset]
            subset = [x for x in subset if not x.startswith('raw')]
            subset = [x for x in subset if not x.startswith('summary')]
    else:
        subset = retrieved_data[0]
    
    print('The retrieving data is now done')
    #==============================================================================
    #               standardize the sensor.
    #==============================================================================
    several_experiment_by_account = True
    
    experiment = 0 
    
    print('experiment_name:',experiment_name)
    
    if details == 'set':
        experiment_name = Account_reference[0]
        del Account_reference[0]
        print('new_experiment_name:',experiment_name)
        
    number_of_experiments = experiments_details.loc[experiment_name]['number of experiments']
    
    if experiments_details.loc[experiment_name]['standardization'] == True:
        
        while several_experiment_by_account == True:
            standardization_parameters = dst.standardization_preparation (experiment_name,parameter1,parameter2,account_number,experiments_sensors,subset,number_of_experiments,experiments_details,experiment)
            # return(sensor_selected,Start,End,experiment_name_,Stop,experiments_details,experiment)
            subset = standardization_parameters[0]
            experiment_name = standardization_parameters[3]
            experiments_details = standardization_parameters[5]
            standardization = experiments_details.loc[experiment_name]['standardization']
            
            test = retrieved_data[1] < standardization_parameters[1]
            test2 = retrieved_data[2] > standardization_parameters[1]
            if test == True and test2 == True:
                test = True
            
            print('experiment_name',experiment_name)
            
            path3 = path0+'\\'+'standardization'
            path4 = path3+'\\'+'correction_parameters'+experiment_name+'.txt' 
            
            if test == False and standardization == False:
                several_experiment_by_account = False
                
                dp.data_processing(path0,path4,path2,experiments_parameters,selected_sensors,experiment_name,folder_name,experiments_details,subset,digits)
            else:
                     
                if (os.path.exists(path3)) == False:
                    os.mkdir(path3)
                os.chdir(path3) 
                
                condition2 = os.path.exists(path4)
                
                if condition2 == True:
                    size = os.path.getsize(path4)
                else:
                    size = 0
                     
                if size == 0:
                    correction_parameters = open(path4,'a')
                    standardization_results = dst.standardization_script (subset,standardization_parameters[1],standardization_parameters[2],path2,digits)    
                    standardization_results.to_csv(correction_parameters,sep = '\t')
                    correction_parameters.close()
                    
                if standardization_parameters [4] == True:
                    several_experiment_by_account = False    
                dp.data_processing(path0,path4,path2,experiments_parameters,selected_sensors,experiment_name,folder_name,experiments_details,subset,digits)
    else:
         dp.data_processing(path0,path4,path2,experiments_parameters,selected_sensors,experiment_name,folder_name,experiments_details,subset,digits)
             
# time2
ending = time.strftime("%Y-%m-%d-%H-%M-%S")

beginning1 = hom.datetime_format(beginning)
ending1 = hom.datetime_format(ending)
consumed_time = (ending1 - beginning1)

print('start:',beginning,'End:',ending,'minutes:',hom.minute(consumed_time) )
print('It is done, you can take some rest well deserved.')