#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import time
import pymysql
import argparse
import requests
import configparser
import random
from helpers.create_logger import create_logger
from helpers.calc_free import calc_free_sim
from datetime import datetime, timedelta

logger = create_logger(f"autodial_marks")
print_log = True

no_sim_sleeptime = 5
mv_sim_sleeptime = 20
no_dialers_sleeptime = 300
step_sleep_time=5

redials_missed_calls = 1
redials_timeout = 300

last_call_ivr_branch = 'support'

config = configparser.ConfigParser()
config.read('/opt/pydialer/config.ini')

operator_list = ['mts', 'ks', 'life']
call_file_dir = '/var/www/html/asterisk/call'
asterisk_outgoing = '/var/spool/asterisk/outgoing'

def calcFree(numbers, cur, mark_type, ivr_branch, dep_id, department_callerid):
    free = calc_free_sim(cur, dep_id, print_log, logger)
    if free['mts'] == 0 and free['ks'] == 0 and free['life'] == 0 and free['all'] == 0 and free['trunk_enable'] == 0:
        step_sleep_time = no_sim_sleeptime
        print("no free sim")
        return

    print(free)
    logger.info(f"free are {free}")

    random.shuffle(operator_list)

    local_operator_list = list(operator_list) + ['all',]
    if free['trunk_enable'] == 1: local_operator_list.append('all_trunk')    

    for oper in local_operator_list:

        if oper == 'all' or oper == 'all_trunk':
            selected_numbers = numbers[:free[oper]]
        else:
            selected_numbers = [num for num in numbers if num['oper'] == oper][:free[oper]]
           
        print(f"selected_numbers {selected_numbers}")
        logger.info(f"selected_numbers {selected_numbers}")
        
        for number in selected_numbers:
            cur.execute("INSERT INTO `operator_marks` (`calldate`, `client_number`, `operator_number`, `billsec`, `queue`, `evaluated_call_id`, `mark_type`, `recordingfile`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (number['calldate'], number['client_number'], number['operator_number'], number['billsec'], number['queue'], number['id'], mark_type, number['recordingfile']))
            con.commit()
            
            if mark_type == 'last_call':
                cur.execute("UPDATE `autodial_marks` SET `callback_status` = 'PROCESSED'  WHERE `mark_type` = 'last_call' AND `client_number` = %s",(number['client_number']))
                con.commit()
            elif mark_type == 'incoming':
                cur.execute("UPDATE `autodial_marks` SET `callback_status` = 'PROCESSED'  WHERE `mark_type` = 'incoming' AND `client_number` = %s",(number['client_number']))
                con.commit()
            else:
                cur.execute("UPDATE `autodial_marks` SET `callback_status` = 'PROCESSED'  WHERE `mark_type` = 'manual_out' AND `client_number` = %s",(number['client_number']))
                con.commit()

            if ivr_branch is not None:
                makeFile(department_callerid, number['client_number'], number['id'], ivr_branch)    
                     
def getLastCallNumbers(start_time, end_time, cur):
    query = f"SELECT DISTINCT client_number FROM autodial_marks WHERE mark_type = 'last_call' AND callback_status = 'NEW' AND calldate BETWEEN '{start_time}' AND '{end_time}';"
    cur.execute(query)
    numbers = cur.fetchall()

    detail_information = []
    for number in numbers:
        cur.execute("SELECT `id`, `calldate`, `client_number`, `operator_number`, `billsec`, `queue`, `recordingfile` FROM `autodial_marks` WHERE `client_number` = %s AND callback_status = 'NEW' ORDER BY `autodial_marks`.`id` DESC LIMIT 1",(number['client_number'],))
        result = cur.fetchone()
        if result:
            detail_information.append(result)
    
    return detail_information

def assign_operators_to_numbers(detail_information):
    if isinstance(detail_information, dict):
        detail_information = [detail_information]

    updated_information = []
    
    for detail in detail_information:
        client_number = detail['client_number']
        
        if client_number.startswith(('+38066', '+38095', '+38050')):  # VF
            oper = 'mts'
        elif client_number.startswith(('+38067', '+38068', '+38096', '+38097', '+38098')):  # KS
            oper = 'kyivstar'
        elif client_number.startswith(('+38063', '+38073', '+38093')):  # Life
            oper = 'lifecell'
        else:
            oper = 'unknown'
        
        updated_detail = detail.copy()  
        updated_detail['oper'] = oper  
        updated_information.append(updated_detail)
    
    return updated_information

def makeFile(dep_cid, number, callid, queue_ivr_branch):
    body = f'''Channel: Local/{number}@from-autodial-marks
MaxRetries: 0
RetryTime: 60
WaitTime: 60
Context: ivr-marks
Extension: s
Priority: 1
Callerid: {dep_cid}
Setvar: callback_callid={callid}
Setvar: queue_branch={queue_ivr_branch}
'''
    # Create call file
    file_name = f"callback_ocinka-{number}.call"
    call_file = os.path.join(call_file_dir, file_name)
    f = open(call_file, "w")
    f.write(body)
    f.close()
    logger.info(f"Created file {file_name}")
    print(f"Created file {file_name}")
    os.popen(f"chown asterisk:asterisk {call_file}")
    time.sleep(0.1)
    os.rename(f"{call_file}", f"{asterisk_outgoing}/{file_name}")
    

while True:
    time.sleep(step_sleep_time)
    con = pymysql.connect(host=config['mysql']['host'],
                          user=config['mysql']['user'],
                          password=config['mysql']['password'],
                          database=config['mysql']['database'],
                          charset='utf8',
                          cursorclass=pymysql.cursors.DictCursor)
    with con:
        cur = con.cursor()
        cur.execute("SELECT MAX(CASE WHEN `calls_type` = 'last_call' THEN `enable` END) AS `enable_last_call`, MAX(CASE WHEN `calls_type` = 'incoming' THEN `enable` END) AS `enable_incoming`, MAX(CASE WHEN `calls_type` = 'manual_out' THEN `enable` END) AS `enable_manual_out` FROM `operator_marks_setting` WHERE `calls_type` IN ('last_call', 'incoming', 'manual_out')")
        markSettings = cur.fetchone()
        
        if markSettings['enable_last_call'] == 1:
            print(f"Last call marks")

            cur.execute("SELECT oms.agent_shift_start, oms.agent_shift_end, oms.never_call_after, d.id AS dep_id, d.callerid FROM operator_marks_setting oms JOIN departaments d ON d.name = oms.dep_name WHERE oms.calls_type = %s LIMIT 1", ('last_call',))
            settings = cur.fetchone()

            mark_type = 'last_call'            
        
            if settings:                            
                agent_shift_start = settings['agent_shift_start']
                agent_shift_end = settings['agent_shift_end']
                never_call_after = settings['never_call_after']                
                department_callerid = settings['callerid']
                dep_id = settings['dep_id']
            
                print(f"Agent Shift Start: {agent_shift_start}, Agent Shift End: {agent_shift_end}, Never Call After: {never_call_after}, Department Caller ID: {department_callerid}")
 
                current_time = datetime.now().time()                

                shift_start_time = datetime.strptime(str(agent_shift_start), '%H:%M:%S').time()
                shift_end_time = datetime.strptime(str(agent_shift_end), '%H:%M:%S').time()
                never_call_after_time = datetime.strptime(str(never_call_after), '%H:%M:%S').time()

                if shift_start_time <= current_time <= shift_end_time:
                    print(f"Current time is within the agent's shift: {current_time}")
                
                    previous_day = (datetime.now() - timedelta(days=1)).date()
                    today = datetime.now().date()
                    start_time = f"{previous_day} {agent_shift_end}"
                    end_time = f"{today} {agent_shift_start}"
                
                    print(f"start_time: {start_time}")
                    print(f"end_time: {end_time}")

                    detail_information = getLastCallNumbers(start_time, end_time, cur)

                    print(f"Detail Information: {detail_information}")

                    if detail_information:
                        logger.info(f"Last call marks")
                        logger.info(f"Detail Information: {detail_information}")

                        updated_information = assign_operators_to_numbers(detail_information)
                        calcFree(updated_information, cur, mark_type, last_call_ivr_branch, dep_id, department_callerid) 

                elif shift_end_time < current_time <= never_call_after_time:
                    print(f"Current time is between the end of the shift and 'never call after' time: {current_time}")
                
                    today = datetime.now().date()
                    start_time = f"{today} {agent_shift_start}"
                    end_time = f"{today} {agent_shift_end}"

                    print(f"start_time: {start_time}")
                    print(f"end_time: {end_time}")

                    detail_information = getLastCallNumbers(start_time, end_time, cur)

                    print(f"Detail Information: {detail_information}")

                    if detail_information:
                        logger.info(f"Last call marks")
                        logger.info(f"Detail Information: {detail_information}")

                        updated_information = assign_operators_to_numbers(detail_information)
                        calcFree(updated_information, cur, mark_type, last_call_ivr_branch, dep_id, department_callerid) 

                else:
                    print(f"Current time is outside of defined time ranges: {current_time}")

            else:
                print(f"No settings found for {mark_type}")
                logger.info(f"No settings found for {mark_type}")

        if markSettings['enable_incoming'] == 1:
            print(f"Incoming marks")

            cur.execute("SELECT oms.sleeptime, oms.steps, d.id AS dep_id, d.callerid FROM operator_marks_setting oms JOIN departaments d ON d.name = oms.dep_name WHERE oms.calls_type = %s LIMIT 1", ('incoming',))
            settings = cur.fetchone()

            mark_type = 'incoming'

            if settings:                            
                sleeptime = settings['sleeptime']
                department_callerid = settings['callerid']
                steps = settings['steps']
                dep_id = settings['dep_id']

                print(f"Sleeptime: {sleeptime}, Department Caller ID: {department_callerid}, steps: {steps} ")

                cur.execute("SELECT `id`, `calldate`, `client_number`, `operator_number`, `billsec`, `queue`, `recordingfile` FROM `autodial_marks` WHERE `mark_type` = %s AND callback_status = 'NEW' AND NOW() > `calldate` + INTERVAL %s SECOND LIMIT 1",(mark_type, sleeptime))
                detail_information = cur.fetchone()

                print(f"Detail Information: {detail_information}")

                if detail_information:
                    logger.info(f"Incoming marks")
                    logger.info(f"Detail Information: {detail_information}")

                    cur.execute("SELECT mark_ivr_menu FROM `config_queue_callbacks` WHERE `queue_name` = %s", (detail_information['queue']))
                    ivr_branch = cur.fetchone()
                    ivr_branch = ivr_branch['mark_ivr_menu']

                    print(f"IVR ivr_branch: {ivr_branch}")
                    logger.info(f"IVR ivr_branch: {ivr_branch}")

                    if ivr_branch:
                        updated_information = assign_operators_to_numbers(detail_information)
                        calcFree(updated_information, cur, mark_type, ivr_branch, dep_id, department_callerid) 

            else:
                print(f"No settings found for {mark_type}")          
                logger.info(f"No settings found for {mark_type}")

        if markSettings['enable_manual_out'] == 1:
            print(f"Manual out marks")
            cur.execute("SELECT oms.sleeptime, oms.steps, d.id AS dep_id, d.callerid FROM operator_marks_setting oms JOIN departaments d ON d.name = oms.dep_name WHERE oms.calls_type = %s LIMIT 1", ('manual_out',))
            settings = cur.fetchone()

            mark_type = 'manual_out'

            if settings:                            
                sleeptime = settings['sleeptime']
                department_callerid = settings['callerid']
                steps = settings['steps']
                dep_id = settings['dep_id']

                print(f"Sleeptime: {sleeptime}, Department Caller ID: {department_callerid}, steps: {steps} ")

                cur.execute("SELECT `id`, `calldate`, `client_number`, `operator_number`, `billsec`, `queue`, `recordingfile` FROM `autodial_marks` WHERE `mark_type` = %s AND callback_status = 'NEW' AND NOW() > `calldate` + INTERVAL %s SECOND LIMIT 1",(mark_type, sleeptime))
                detail_information = cur.fetchone()

                print(f"Detail Information: {detail_information}")

                if detail_information:
                    logger.info(f"Manual out marks")
                    logger.info(f"Detail Information: {detail_information}")

                    cur.execute("SELECT queue_ivr_branch FROM `config_agent_marks` WHERE sip = %s", (detail_information['operator_number']))
                    ivr_branch = cur.fetchone()
                    ivr_branch = ivr_branch['queue_ivr_branch']

                    print(f"IVR ivr_branch: {ivr_branch}")
                    logger.info(f"IVR ivr_branch: {ivr_branch}")

                    if ivr_branch:
                        updated_information = assign_operators_to_numbers(detail_information)
                        calcFree(updated_information, cur, mark_type, ivr_branch, dep_id, department_callerid) 

            else:
                print(f"No settings found for {mark_type}")
                logger.info(f"No settings found for {mark_type}")

        else:
            print(f"Marks is disable")
            logger.info(f"Marks is disable")

        if redials_missed_calls:
            print("Redials missed calls")
            cur.execute("SELECT `client_number`, `operator_number`, `date_callback`, `queue`, `evaluated_call_id`, `mark_type` FROM `operator_marks` WHERE `call_attempts` = 1 AND `callback_status` != 'INITED' AND `callback_status` != 'ANSWERED' AND NOW() > `date_callback` + INTERVAL %s SECOND LIMIT 1", (redials_timeout))
            call = cur.fetchone()

            print(f"Detail Information: {call}")

            branch = None

            if call:
                logger.info(f"Detail Information: {call}")

                cur.execute("SELECT d.callerid FROM operator_marks_setting oms JOIN departaments d ON d.name = oms.dep_name WHERE oms.calls_type = %s",(call['mark_type']))
                department_callerid = cur.fetchone()
                
                if call['mark_type'] == 'incoming':
                    cur.execute("SELECT `mark_ivr_menu` FROM `config_queue_callbacks` WHERE `queue_name` = %s",(call['queue']))
                    branch = cur.fetchone()
                    branch = branch['mark_ivr_menu']

                elif call['mark_type'] == 'manual_out':
                    cur.execute("SELECT `queue_ivr_branch` FROM `config_agent_marks` WHERE `sip` = %s", (call['operator_number']))
                    branch = cur.fetchone()
                    branch = branch['queue_ivr_branch']
                else:
                    branch = last_call_ivr_branch

                if department_callerid and call['client_number'] and call['evaluated_call_id'] and branch:

                    cur.execute("UPDATE `operator_marks` SET `callback_status` = 'INITED' WHERE `evaluated_call_id` = %s",(call['evaluated_call_id']))
                    con.commit()

                    makeFile(department_callerid['callerid'], call['client_number'], call['evaluated_call_id'], branch)
                else:
                    print("Failed to create redials file. One or more parameters are empty")
                    logger.info("Failed to create redials file. One or more parameters are empty")

        else:
            print("Redials missed calls is disable")
            logger.info("Redials missed calls is disable") 


