import os
import time
import pymysql
import configparser
import random
import pwd
import grp
from datetime import datetime, timedelta
from helpers.create_logger import create_logger as file_handler
from helpers.calc_free import calc_free_sim

class Autodialer:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('/opt/pydialer/config.ini')

        self.file_handler = file_handler("autodial_marks")
        self.redial = True
        self.redials_timeout = 600
        self.debug_status = True

        self.con = pymysql.connect(
            host=self.config['mysql']['host'],
            user=self.config['mysql']['user'],
            password=self.config['mysql']['password'],
            database=self.config['mysql']['database'],
            charset='utf8',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )

        self.logger = Logger(self.file_handler, self.debug_status, self.con)
        self.call_process = CallProcess(self.logger, self.con)
        self.last_call_handler = LastCallHandler(self.call_process, self.logger)
        self.call_handler = CallHandler(self.call_process, self.logger)
        self.redial_call_handler = Redial(self.call_process, self.logger, self.con, self.redials_timeout)

    def run(self):
        while True:
            time.sleep(5)
            with self.con.cursor() as cur:
                self.process_marks(cur)

    def process_marks(self, cur):
        mark_settings = self.get_mark_settings(cur)

        if mark_settings['enable_last_call']:
            self.last_call_handler.handle_last_call(cur, 'last_call')
        if mark_settings['enable_last_call_out']:
            self.last_call_handler.handle_last_call(cur, 'last_call_out')
        if mark_settings['enable_incoming']:
            self.call_handler.handle_call(cur, 'incoming')
        if mark_settings['enable_manual_out']:
            self.call_handler.handle_call(cur, 'manual_out')
        if self.redial:
            self.redial_call_handler.redial_handle_call(cur)

    def get_mark_settings(self, cur):
        try:
            query = """
                SELECT MAX(CASE WHEN calls_type = 'last_call' THEN enable END) AS enable_last_call,
                   MAX(CASE WHEN calls_type = 'incoming' THEN enable END) AS enable_incoming,
                   MAX(CASE WHEN calls_type = 'manual_out' THEN enable END) AS enable_manual_out,
                   MAX(CASE WHEN calls_type = 'last_call_out' THEN enable END) AS enable_last_call_out
            FROM operator_marks_setting
            WHERE calls_type IN ('last_call', 'incoming', 'manual_out', 'last_call_out')
            """
            cur.execute(query)
            return cur.fetchone()
        except Exception as e:
            self.logger.error(f"Error while getting the list of active ratings: {e}")
            return None

class Logger:
    def __init__(self, logger, debug_status, con):
        self.logger = logger
        self.debug_status = debug_status
        self.con = con
        self.cur = con.cursor()

    def debug(self, message):
        if self.debug_status:
            print(message)

    def info(self, message):
        print(message)
        self.logger.info(message)

    def warning(self, message):
        print(message)
        self.logger.warning(message)

    def error(self, message):
        print(message)
        self.logger.error(message)

class LastCallHandler:
    def __init__(self, call_process, logger):
        self.logger = logger
        self.call_process = call_process

    def handle_last_call(self, cur, call_type):
        self.logger.debug(f"Mark {call_type} is start")
        settings = self.get_call_settings(cur, call_type)
        if settings:
            current_time = datetime.now().time()
            shift_start_time = datetime.strptime(str(settings['agent_shift_start']), '%H:%M:%S').time()
            shift_end_time = datetime.strptime(str(settings['agent_shift_end']), '%H:%M:%S').time()
            never_call_after_time = datetime.strptime(str(settings['never_call_after']), '%H:%M:%S').time()

            if shift_start_time <= current_time <= shift_end_time:
                start_time, end_time = self.get_shift_time_range(settings)
            elif shift_end_time < current_time <= never_call_after_time:
                start_time, end_time = self.get_after_shift_time_range(settings)

            if start_time and end_time:
                self.process_last_call_details(cur, call_type, settings, start_time, end_time)
        else:
                self.logger.error(f"Failed to get call settings for call type: {call_type}")

    def get_call_settings(self, cur, call_type):
        try:
            query = """
                SELECT oms.agent_shift_start, oms.agent_shift_end, oms.never_call_after, d.id AS dep_id, d.callerid
                FROM operator_marks_setting oms
                JOIN departaments d ON d.name = oms.dep_name
                WHERE oms.calls_type = %s LIMIT 1
            """
            cur.execute(query, (call_type,))
            return cur.fetchone()

        except Exception as e:
            self.logger.error(f"Error while getting rating settings {call_type}: {e}")
            return None

    def get_shift_time_range(self, settings):
        previous_day = (datetime.now() - timedelta(days=1)).date()
        today = datetime.now().date()
        start_time = f"{previous_day} {settings['agent_shift_end']}"
        end_time = f"{today} {settings['agent_shift_start']}"
        return start_time, end_time

    def get_after_shift_time_range(self, settings):
        today = datetime.now().date()
        start_time = f"{today} {settings['agent_shift_start']}"
        end_time = f"{today} {settings['agent_shift_end']}"
        return start_time, end_time

    def process_last_call_details(self, cur, call_type, settings, start_time, end_time):
        detail_information = self.get_last_call_numbers(cur, start_time, end_time, call_type)
        if detail_information:
            self.logger.info(f"Detail information for call type {call_type}: {detail_information}")
            ivr_branch = self.get_ivr_branch(cur, call_type)
            if ivr_branch:
                updated_information = self.call_process.assign_operators_to_numbers(detail_information)
                self.call_process.calc_free_and_process(
                    updated_information, cur, call_type, ivr_branch,
                    settings['dep_id'], settings['callerid'], start_time, end_time)
            else:
                self.logger.error(f"Failed to get IVR branch for call type: {call_type}")
        else:
            self.logger.debug(f"Detail information for call type {call_type} si null")

    def get_last_call_numbers(self, cur, start_time, end_time, call_type):
        try:
            query = f"SELECT DISTINCT client_number FROM autodial_marks WHERE mark_type = %s AND callback_status = 'NEW' AND calldate BETWEEN %s AND %s"
            cur.execute(query, (call_type, start_time, end_time))
            numbers = cur.fetchall()

            detail_information = []
            for number in numbers:
                cur.execute("""
                    SELECT `id`, `calldate`, `client_number`, `operator_number`, `billsec`, `queue`, `uniqueid`, `recordingfile`
                    FROM `autodial_marks`
                    WHERE `client_number` = %s AND callback_status = 'NEW'
                    ORDER BY `autodial_marks`.`id` DESC LIMIT 1
                """, (number['client_number'],))
                result = cur.fetchone()
                if result:
                    detail_information.append(result)

            return detail_information

        except Exception as e:
            self.logger.error(f"Error while getting numbers for {call_type}: {e}")
            return []

    def get_ivr_branch(self, cur, call_type):
        try:
            query = "SELECT ivr_branch FROM `operator_marks_setting` WHERE calls_type = %s"
            cur.execute(query, (call_type,))
            result = cur.fetchone()
            return result['ivr_branch'] if result else None
        except Exception as e:
            self.logger.error(f"Error while getting the IVR branch for rating {call_type}: {e}")
            return None

class CallHandler:
    def __init__(self, call_process, logger):
        self.call_process = call_process
        self.logger = logger

    def handle_call(self, cur, call_type):
        self.logger.debug(f"Mark {call_type} is started")
        settings = self.get_call_settings(cur, call_type)
        if settings:
            detail_information = self.get_number_for_call(cur, call_type, settings['sleeptime'])
            if detail_information:
                self.logger.info(f"Detail information for call type {call_type}: {detail_information}")
                ivr_branch = self.get_ivr_branch(cur, call_type, detail_information)
                if ivr_branch:
                    updated_information = self.call_process.assign_operators_to_numbers(detail_information)
                    self.call_process.calc_free_and_process(updated_information, cur, call_type, ivr_branch, settings['dep_id'], settings['callerid'], None, None)
                else:
                    self.logger.error(f"Failed to get IVR branch for call type: {call_type}")
            else:
                self.logger.debug(f"Detail information for call type {call_type} si null")
        else:
            self.logger.error(f"Failed to get call settings for call type: {call_type}")

    def get_call_settings(self, cur, call_type):
        try:
            query = """
                SELECT oms.sleeptime, d.id AS dep_id, d.callerid 
                FROM operator_marks_setting oms 
                JOIN departaments d ON d.name = oms.dep_name 
                WHERE oms.calls_type = %s LIMIT 1
            """
            cur.execute(query, (call_type,))
            return cur.fetchone()
        except Exception as e:
            self.logger.error(f"Error while getting rating settings {call_type}: {e}")
            return None

    def get_number_for_call(self, cur, call_type, sleeptime):
        try:
            query = """
                SELECT `id`, `calldate`, `client_number`, `operator_number`, `billsec`, `queue`, `uniqueid`, `recordingfile`
                FROM `autodial_marks` 
                WHERE `mark_type` = %s AND callback_status = 'NEW' AND NOW() > `calldate` + INTERVAL %s SECOND LIMIT 1
            """
            cur.execute(query, (call_type, sleeptime,))
            return cur.fetchone()
        except Exception as e:
            self.logger.error(f"Error while getting the number for rating {call_type}: {e}")
            return None

    def get_ivr_branch(self, cur, call_type, detail_information):
        try:
            if call_type == "incoming":
                query = "SELECT mark_ivr_menu AS ivr_branch FROM `config_queue_callbacks` WHERE `queue_name` = %s"
                cur.execute(query, (detail_information['queue'],))
                result = cur.fetchone()
                return result['ivr_branch'] if result else None

            if call_type == "manual_out":
                query = "SELECT queue_ivr_branch AS ivr_branch FROM `config_agent_marks` WHERE sip = %s"
                cur.execute(query, (detail_information['operator_number'],))
                result = cur.fetchone()
                return result['ivr_branch'] if result else None

        except Exception as e:
            self.logger.error(f"Error while getting the IVR branch for rating {call_type}: {e}")
            return None

class Redial:
    def __init__(self, call_process, logger, con, redials_timeout):
        self.call_process = call_process
        self.con = con
        self.logger = logger
        self.redials_timeout = redials_timeout

    def redial_handle_call(self, cur):
        self.logger.debug(f"Mark redial is started")
        detail_information = self.get_redial_number(cur)
        if detail_information:
            self.logger.info(f"Detail information for redial: {detail_information}")
            updated_information = self.call_process.assign_operators_to_numbers(detail_information)
            department_settings = self.get_department_settings(cur, detail_information['mark_type'])
            ivr_branch = self.get_ivr_branch(cur, detail_information)
            if ivr_branch:
                uniqueid = self.get_call_uniqueid(cur, detail_information['evaluated_call_id'])
                audio_filename = self.call_process.get_operator_audio_by_number(cur, detail_information['operator_number'], detail_information['mark_type'])
                if updated_information is not None and department_settings is not None and ivr_branch is not None and uniqueid is not None:
                    self.make_redial_call(cur, updated_information, department_settings, ivr_branch, uniqueid, audio_filename)
                else:
                    self.logger.error("Failed to create redials file. One or more parameters are empty")
            else:
                self.logger.error(f"Failed to get IVR branch for Redial where call type: {detail_information['mark_type']}")
        else:
            self.logger.debug(f"Detail information for Recal si null")

    def get_redial_number(self, cur):
        try:
            query = """
                SELECT `client_number`, `operator_number`, `date_callback`, `queue`, `evaluated_call_id`, `mark_type` 
                FROM `operator_marks` 
                WHERE `call_attempts` = 1 AND `callback_status` != 'INITED' 
                AND `callback_status` != 'ANSWERED' AND NOW() > `date_callback` + INTERVAL %s SECOND LIMIT 1
            """
            cur.execute(query, (self.redials_timeout,))
            return cur.fetchone()
        except Exception as e:
            self.logger.error(f"Error while getting the number for callback: {e}")
            return None

    def get_department_settings(self, cur, call_type):
        try:
            query = """
                SELECT d.callerid, d.id AS dep_id
                FROM operator_marks_setting oms
                JOIN departaments d ON d.name = oms.dep_name
                WHERE oms.calls_type = %s
            """
            cur.execute(query, (call_type,))
            return cur.fetchone()
        except Exception as e:
            self.logger.error(f"Error while getting the department settings during callback for rating '{call_type}': {e}")
            return None

    def get_ivr_branch(self, cur, detail_information):
        try:
            if detail_information['mark_type'] == "incoming":
                query = "SELECT mark_ivr_menu AS ivr_branch FROM `config_queue_callbacks` WHERE `queue_name` = %s"
                cur.execute(query, (detail_information['queue'],))
                result = cur.fetchone()
                return result['ivr_branch'] if result else None

            if detail_information['mark_type'] == "manual_out":
                query = "SELECT queue_ivr_branch AS ivr_branch FROM `config_agent_marks` WHERE sip = %s"
                cur.execute(query, (detail_information['operator_number'],))
                result = cur.fetchone()
                return result['ivr_branch'] if result else None

            if detail_information['mark_type'] == 'last_call' or detail_information['mark_type'] == 'last_call_out':
                query = "SELECT ivr_branch FROM `operator_marks_setting` WHERE calls_type = %s"
                cur.execute(query, (detail_information['mark_type'],))
                result = cur.fetchone()
                return result['ivr_branch'] if result else None

        except Exception as e:
            self.logger.error(f"Error while getting the IVR branch during callback for rating: {e}")
            return None

    def get_call_uniqueid(self, cur, evaluated_call_id):
        try:
            query = "SELECT uniqueid FROM `autodial_marks` WHERE `id` = %s LIMIT 1"
            cur.execute(query, (evaluated_call_id,))
            return cur.fetchone()
        except Exception as e:
            self.logger.error(f"Error while getting the unique call identifier during callback ID '{evaluated_call_id}': {e}")
            return None

    def make_redial_call(self, cur, numbers, department_settings, ivr_branch, uniqueid, audio_filename):
        free = calc_free_sim(cur, department_settings['dep_id'], True, self.logger)
        if any(value > 0 for value in free.values()):
            random.shuffle(self.call_process.operator_list)
            local_operator_list = self.call_process.operator_list.copy()
            if free['trunk_enable']:
                local_operator_list.append('all_trunk')
            for oper in local_operator_list:
                if not numbers:
                    self.logger.debug("No more numbers to process.")
                    break

                if oper in ['all', 'all_trunk']:
                    selected_numbers = numbers[:free[oper]]
                else:
                    selected_numbers = [num for num in numbers if num['oper'] == oper][:free[oper]]

                for number in selected_numbers:
                    self.update_call(cur, number['evaluated_call_id'])
                    self.call_process.make_call_file(department_settings['callerid'], number['client_number'], number['evaluated_call_id'], ivr_branch, uniqueid, audio_filename)
                    numbers.remove(number)

    def update_call(self, cur, evaluated_call_id):
        try:
            query = "UPDATE `operator_marks` SET `callback_status` = 'INITED' WHERE `evaluated_call_id` = %s"
            cur.execute(query, (evaluated_call_id,))
            self.con.commit()
        except Exception as e:
            self.logger.error(f"Error while updating the call status with ID '{evaluated_call_id}': {e}")
            self.con.rollback() 

class CallProcess:
    def __init__(self, logger, con):
        self.logger = logger
        self.con = con
        self.operator_list = ['mts', 'ks', 'life', 'all']
        self.call_file_dir = '/var/www/html/asterisk/call'
        self.asterisk_outgoing = '/var/spool/asterisk/outgoing'

    def calc_free_and_process(self, numbers, cur, call_type, ivr_branch, dep_id, department_callerid, start_time, end_time):
        free = calc_free_sim(cur, dep_id, True, self.logger)
        self.logger.info(free)
        self.logger.info(numbers)
        if any(value > 0 for value in free.values()):
            random.shuffle(self.operator_list)
            local_operator_list = self.operator_list.copy()
            if free['trunk_enable']:
                local_operator_list.append('all_trunk')
            for oper in local_operator_list:
                if not numbers:
                    self.logger.debug("No more numbers to process.")
                    break

                if oper in ['all', 'all_trunk']:
                    selected_numbers = numbers[:free[oper]]
                else:
                    selected_numbers = [num for num in numbers if num['oper'] == oper][:free[oper]]

                self.logger.debug(f"Selected numbers: {selected_numbers} for operator {oper}")

                for number in selected_numbers:
                    self.process_call(cur, number, call_type, ivr_branch, department_callerid, start_time, end_time)
                    numbers.remove(number)
        else:
            self.logger.warning(f"No free sim in dep {department_callerid}, for call type: {call_type}")

    def process_call(self, cur, number, call_type, ivr_branch, department_callerid, start_time, end_time):
        if ivr_branch:
            audio_filename = self.get_operator_audio_by_number(cur, number['operator_number'], call_type)
            success = self.add_call_to_operator_mark(cur, number, call_type)
            if success:
                self.update_call_status(cur, number, call_type, start_time, end_time)
                self.make_call_file(department_callerid, number['client_number'], number['id'], ivr_branch, number['uniqueid'], audio_filename)
            else:
                self.logger.error(f"Failed to add call to operator mark for number: {number['operator_number']} and call type: {call_type}")
        else:
            self.logger.error(f"IVR branch is null for call type: {call_type}")

    def get_operator_audio_by_number(self, cur, operator_number, call_type):
        try:
            cur.execute("SELECT say_fio FROM operator_marks_setting WHERE calls_type = %s", (call_type,))
            settings = cur.fetchone()

            if not settings or settings['say_fio'] != 1:
                return None

            cur.execute("SELECT audio_filename FROM operator_name_audio WHERE operator_number = %s LIMIT 1", (operator_number,))
            audio_filename = cur.fetchone()

            if audio_filename:
                return audio_filename['audio_filename']
            else:
                self.logger.warning(f"Say_fio is enabled, but no audio filename found for operator number: '{operator_number}'")
                return None

        except Exception as e:
            self.logger.error(f"Error while getting settings for call type {call_type} or audio file of the operator's name. Operator's number: '{operator_number}': {e}")
            return None

    def assign_operators_to_numbers(self, detail_information):
        if isinstance(detail_information, dict):
            detail_information = [detail_information]

        updated_information = []
        for detail in detail_information:
            client_number = detail['client_number']
            if client_number.startswith(('+38066', '+38095', '+38050')):
                oper = 'mts'
            elif client_number.startswith(('+38067', '+38068', '+38096', '+38097', '+38098')):
                oper = 'ks'
            elif client_number.startswith(('+38063', '+38073', '+38093')):
                oper = 'life'
            else:
                oper = 'unknown'

            updated_detail = detail.copy()
            updated_detail['oper'] = oper
            updated_information.append(updated_detail)

        return updated_information

    def add_call_to_operator_mark(self, cur, number, call_type):
        try:
            cur.execute("""
                INSERT INTO `operator_marks` 
                (`calldate`, `client_number`, `operator_number`, `billsec`, `queue`, `evaluated_call_id`, `mark_type`, `recordingfile`) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                number['calldate'], 
                number['client_number'], 
                number['operator_number'], 
                number['billsec'], 
                number['queue'], 
                number['id'], 
                call_type, 
                number['recordingfile']
            ))
            self.con.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error while inserting a record into 'operator_marks' for rating '{call_type}': {e}")
            self.con.rollback()
            return False

    def update_call_status(self, cur, number, call_type, start_time=None, end_time=None):
        try:
            if call_type == 'last_call':
                query = """
                    UPDATE `autodial_marks` 
                    SET `callback_status` = 'PROCESSED'
                    WHERE `mark_type` = 'last_call' 
                    AND calldate BETWEEN %s AND %s 
                    AND `client_number` = %s
                """
                params = (start_time, end_time, number['client_number'])
            elif call_type == 'last_call_out':
                query = """
                    UPDATE `autodial_marks` 
                    SET `callback_status` = 'PROCESSED'  
                    WHERE `mark_type` = 'last_call_out' 
                    AND calldate BETWEEN %s AND %s 
                    AND `client_number` = %s
                """
                params = (start_time, end_time, number['client_number'])
            elif call_type == 'incoming':
                query = """
                    UPDATE `autodial_marks` 
                    SET `callback_status` = 'PROCESSED'
                    WHERE `mark_type` = 'incoming' 
                    AND `client_number` = %s
                """
                params = (number['client_number'],)
            else:  # 'manual_out'
                query = """
                    UPDATE `autodial_marks` 
                    SET `callback_status` = 'PROCESSED'  
                    WHERE `mark_type` = 'manual_out' 
                    AND `client_number` = %s
                """
                params = (number['client_number'],)

            cur.execute(query, params)
            self.con.commit()
        except Exception as e:
            self.logger.error(f"Error while updating a record in 'autodial_marks' for rating '{call_type}': {e}")
            self.con.rollback()

    def make_call_file(self, dep_cid, number, callid, queue_ivr_branch, uniqueid_number_evaluated, audio_filename=None):
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
Setvar: uniqueid_number_evaluated={uniqueid_number_evaluated}
Setvar: operator_name_audio={audio_filename}
    '''
        try:
            file_name = f"callback_ocinka-{number}.call"
            call_file_path = os.path.join(self.call_file_dir, file_name)

            with open(call_file_path, "w") as call_file:
                call_file.write(body)

            self.logger.info(f"Created file {file_name}")

            os.chown(call_file_path, pwd.getpwnam('asterisk').pw_uid, grp.getgrnam('asterisk').gr_gid)

            time.sleep(0.1)

            destination_path = os.path.join(self.asterisk_outgoing, file_name)
            os.rename(call_file_path, destination_path)

        except Exception as e:
            self.logger.error(f"Failed to create call file for number {number}: {e}")

if __name__ == "__main__":
    autodialer = Autodialer()
    autodialer.run()


