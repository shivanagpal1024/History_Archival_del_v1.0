import logging
import sys
import configparser as cp
import ibm_db_dbi as dbi
import csv
import sys
import os

global hprtnid
global procprd

del_cnt = 0
counter = 1
cnt = 0
gb_size = 1024 * 1024 * 1024 * 2.0  # 2GB
size = 0
division_counter = 0
threshold = 10000

log = logging.getLogger('root')
FORMAT = "[%(levelname)s: %(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel(logging.DEBUG)

DB2_config = {}


def check_for_configuration():  # reading the configuration property file
    log.info("reading configuration file")
    config = cp.ConfigParser()
    config.read("DB2.properties")
    error_list = []
    for key, value in config['db2'].items():
        if value is None or value.strip() == '':
            error_list.append(f'{key} \"{value}\" not found or incorrect\n')
        else:
            DB2_config[key] = value

    if len(error_list) != 0:
        log.error("something went wrong while reading configuration=%s", error_list)
        sys.exit(-1)

    log.info("configuration file read successfully config_size=%s", len(DB2_config))


def db2_connection():
    global conn
    log.info("Connecting DB2")
    dbase = DB2_config['database'.strip()]
    hstnm = DB2_config['hostname'.strip()]
    port = DB2_config['port'.strip()]
    protocol = DB2_config['protocol'.strip()]
    usrid = DB2_config['uid'.strip()]
    pswd = DB2_config['pwd'.strip()]
    try:
        conn = dbi.connect(f"DATABASE={dbase};HOSTNAME={hstnm};PORT={port};PROTOCOL={protocol};UID={usrid};PWD={pswd};",
                           "", "")
    except Exception as e:
        log.error(
            "something went wrong while creating connection host=%s, username=%s, password=%s, database=%s, message=%s",
            hstnm, usrid, pswd, dbase, str(e))
        sys.exit(-1)
    log.info("connection created")


def h_partn_xref(region):
    sql = "SELECT  PROC_PRD,H_PARTN_ID FROM {}.H_PARTN_XREF ORDER BY PROC_PRD DESC WITH UR;".format(region)
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    hprtnid = []
    procprd = []
    count = 0
    try:
        for row in results:
            count += 1
            procprd.append(row[0])
            hprtnid.append(row[1])
        return hprtnid, procprd
    except OSError as err:
        print("OS error: {0}".format(err))


def fetch_records(region, parm):
    '''
    It fetches the Distinct ECAP_MBR_KEY  from INT_MBR table.
    '''
    try:
        list_hprtn_id, list_proc_prd = h_partn_xref(region)
        if (parm.upper() == 'M'):
            table = 'INT_MBR'
            col_mbrctl = ''
        elif (parm.upper() == 'C'):
            table = 'INT_MBR_COV'
            col_mbrctl = ',INT_MBR_PARTN_ID'
        else:
            log.error("Invalid Parms Pass", parm)
        # check_for_files(table)
        sql = "SELECT DISTINCT ECAP_MBR_KEY FROM {0}.{1} WHERE MBR_PARTN_ID = 9 ;COMMIT;".format(region, table)
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            ecap_mbr_key = row[0]
            print(f'Processing ECAP_MBR_KEY:{ecap_mbr_key}')
            min_begn_mtcn = get_min_bgn_mtcn(region, ecap_mbr_key, col_mbrctl)
            chk_records_to_arcv(region, table, ecap_mbr_key, min_begn_mtcn, list_hprtn_id, list_proc_prd)
    except Exception as e:
        log.error(f"Error While fetching records from {table}")
        sys.exit(-1)


def get_min_bgn_mtcn(region, ecap_mbr_key, col_mbrctl):
    '''
    This function basically get the Earliest MTCN from the database for that particular ECAP_MBR_KEY
    :param ecap_mbr_key:
    :return: min_begn_mtcn
    '''
    try:
        sql = "SELECT CURR_MTCN,DATA_SEG_ID{0} FROM {1}.MBR_CTL WHERE ECAP_MBR_KEY = {2}".format(col_mbrctl, region,
                                                                                                 ecap_mbr_key)
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            curr_mtcn = row[0]
            datsegid = row[1]
            if col_mbrctl != '':
                int_mbr_partn_id = row[2]
            if curr_mtcn != 0:
                min_begn_mtcn = curr_mtcn
            else:
                min_begn_mtcn = 99999
    except Exception as e:
        log.error("Unable to execute query to get curr_mtcn from MBR_CTL table", str(e))

    try:
        sql1 = "SELECT MIN(MBR_BEGN_MTCN) FROM {}.CYC_INVC_ADJ WHERE ECAP_MBR_KEY = {}".format(region, ecap_mbr_key)
        cursor = conn.cursor()
        cursor.execute(sql1)
        results = cursor.fetchall()
        for row in results:
            mbr_begn_mtcn = int(0 if row[0] is None else row[0])
            if mbr_begn_mtcn != 0:
                if mbr_begn_mtcn < min_begn_mtcn:
                    min_begn_mtcn = mbr_begn_mtcn
    except Exception as e:
        log.error("Unable to execute query to get min_begn_mtcn from CYC_INVC_ADJ table", str(e))

    try:
        sql2 = " SELECT MIN(MBR_BEGN_MTCN) FROM {0}.RECYC_MBR_SUBL_LNK WHERE ECAP_MBR_KEY = {1} AND " \
               "PAY_PARTN_ID =(SELECT PAY_PARTN_ID FROM {0}.DAT_SEG WHERE DATA_SEG_ID= {2})".format(region,
                                                                                                    ecap_mbr_key,
                                                                                                    datsegid)
        cursor = conn.cursor()
        cursor.execute(sql2)
        results = cursor.fetchall()
        for row in results:
            mbr_begn_mtcn = int(0 if row[0] is None else row[0])
            if mbr_begn_mtcn != 0:
                if mbr_begn_mtcn < min_begn_mtcn:
                    min_begn_mtcn = mbr_begn_mtcn
    except Exception as e:
        log.error("Unable to execute query to get min_begn_mtcn from RECYC_MBR_SUBL_LNK table", str(e))

    try:
        datsegid1 = (datsegid * 2) - 1
        datsegid2 = datsegid * 2
        sql3 = "SELECT MIN(MBR_BEGN_MTCN) FROM {0}.CYC_MBR_SUBL_LNK WHERE ECAP_MBR_KEY = '{1}'" \
               "AND CURR_PREV_PARTN_ID IN ({2},{3})".format(region, ecap_mbr_key, datsegid1, datsegid2)
        cursor = conn.cursor()
        cursor.execute(sql3)
        results = cursor.fetchall()
        for row in results:
            mbr_begn_mtcn = int(0 if row[0] is None else row[0])
            if mbr_begn_mtcn != 0:
                if mbr_begn_mtcn < min_begn_mtcn:
                    min_begn_mtcn = mbr_begn_mtcn
    except Exception as e:
        log.error("Unable to execute query to get min_begn_mtcn from CYC_MBR_SUBL_LNK table", str(e))

    try:
        sql4 = "SELECT MAX(BEGN_MTCN) FROM {0}.INT_MBR WHERE ECAP_MBR_KEY = {1}".format(region, ecap_mbr_key) -- 5182
        cursor = conn.cursor()
        cursor.execute(sql4)
        results = cursor.fetchall()
        for row in results:
            mbr_begn_mtcn = int(0 if row[0] is None else row[0])
            if mbr_begn_mtcn != 0:
                if mbr_begn_mtcn < min_begn_mtcn:
                    min_begn_mtcn = mbr_begn_mtcn
        return min_begn_mtcn
    except Exception as e:
        log.error("Unable to execute query to get BEGN_MTCN from table", str(e))


def chk_records_to_arcv(region, table, ecap_mbr_key, min_begn_mtcn, list_hprtn_id, list_proc_prd):
    global counter
    global cnt
    global gb_size
    global size
    global division_counter
    global threshold
    global del_cnt
    record_ind = 'N'
    try:
        sql = "SELECT * FROM {0}.{1} WHERE ECAP_MBR_KEY = {2} AND BEGN_MTCN < {3}".format(region, table, ecap_mbr_key,
                                                                                          min_begn_mtcn)
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        if len(results) > 0:
            record_ind = 'Y'
            if table == 'INT_MBR':
                index = 26
            elif table == 'INT_MBR_COV':
                index = 33
            filename = f"H_{table}_{counter}"
            for row in results:
                del_cnt += 1
                updt_tm = row[index]
                yearmo = updt_tm.strftime("%Y%m")
                if yearmo in list_proc_prd:
                    loc = list_proc_prd.index(yearmo)
                    himpartnid = list_hprtn_id[loc]
                elif yearmo < list_proc_prd[-1]:
                    himpartnid = list_hprtn_id[-1]
                row1 = list(row)
                row1.insert(0, himpartnid)
                lst = [elt.strip() if type(elt) is str else elt for elt in row1]

                with open('{}.csv'.format(filename), 'a', newline='') as hintmbr:
                    cnt += 1
                    writer = csv.writer(hintmbr)
                    writer.writerow(lst)
                if division_counter == threshold:
                    division_counter = 0
                    size = os.path.getsize('{}.csv'.format(filename))
                if size >= gb_size:
                    size = 0
                    counter += 1
                    filename = f"H_{table}_{counter}"
                division_counter += 1
        if (record_ind == 'Y'):
            delsql = "DELETE FROM {0}.{1} WHERE ECAP_MBR_KEY = {2} AND BEGN_MTCN < {3}".format(region, table,
                                                                                               ecap_mbr_key,
                                                                                               min_begn_mtcn)
            cursor = conn.cursor()
            cursor.execute(delsql)
            conn.commit()
            cursor.execute(sql)
            records = cursor.fetchall()
            if len(records) == 0:
                print(f'Records Deleted successfully for ECAP_MBR_KEY {ecap_mbr_key}')
    except Exception as err:
        log.error("Unable to execute query to to get the Archival records", str(e))


def printing_values():
    if cnt > 0:
        print("The total Number of file written for the process:", counter)
    print("Records written to archival", cnt)
    print("Total Number of records delete from Source table", del_cnt)


if __name__ == '__main__':
    check_for_configuration()
    db2_connection()
    fetch_records('D6744DBC', 'M')
    printing_values()
else:
    print('no calling function')
