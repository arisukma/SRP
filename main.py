import cx_Oracle
import pandas as pd
import numpy as np
import random
import warnings
from datetime import datetime, date, timedelta

warnings.filterwarnings("ignore")

conn = cx_Oracle.connect('app_iodsc_username', 'xxxx', 'oil3rknp')
sql = """
select distinct t.WELL_NAME from IODSC_SOR.SRP_INFERRED_PRODUCTION t
"""
DynM = pd.read_sql_query(sql, conn)
DynM = DynM[
    ['DYNO_DATE', 'WELL_NAME', 'UWI', 'CARD_AREA', 'GASSY_FACTOR', 'FLUID_STROKE', 'LOAD_SPAN', 'GROSS_DISPLACEMENT',
     'NET_DISPLACEMENT', 'BFPD_PREDICT', 'BFPD_TEST', 'FNORM', 'FNORM_1', 'FNORM_2', 'FNORM_3', 'DNORM_1', 'DNORM_2',
     'CREATED_DATE', 'CREATED_BY', 'DFLUID_STROKE', 'DLOAD_SPAN', 'DGROSS_DISPLACEMENT', 'NETNORM', "PUMP_FILL"]]
dynoC = pd.read_csv(r"D:\SRP\Dynomaster.csv")
dynoC.date = pd.to_datetime(dynoC.date)
dynoC = dynoC.rename(columns={"date": "DYNO_DATE", "name": "WELL_NAME"})
namelist = pd.read_csv(r"D:\SRP\listnamealllast.csv").values
name = []
for item in namelist:
    item = str(item).strip("[").strip("]").strip("'")
    name.append(item)

conn.close()
import warnings
from datetime import datetime, date, timedelta

warnings.filterwarnings("ignore")
dynoMech = DynM[(DynM.WELL_NAME.isin(name)) & (DynM.DYNO_DATE <= datetime.now() - timedelta(1)) & (
            DynM.DYNO_DATE >= datetime.now() - timedelta(4))].reset_index(drop=True)
dynoMech["DPUMP_FIL"] = 1.0
dynoMech["DNETNORM"] = 1.0
dynoMech["DGASSY_FACTOR"] = 1.0
dynoMech["DCARD_AREA"] = 1.0

datedict = {}
for i in range(1, 3):
    sql2 = """
    select uwi, test_date from (select a.*, dense_rank() OVER (PARTITION BY a.uwi ORDER BY a.test_date desc) rank1 from sho.well_test a) where rank1 = {}
    """.format(i)
    conn2 = cx_Oracle.connect('IODSC_DM', 'dmN1h$e0C#21$BiL', 'dmw3rknp')
    RefDate = None
    RefDate = pd.read_sql_query(sql2, conn2)
    RefDate = RefDate[RefDate.UWI.isin(dynoMech.UWI.unique())]
    datedict[i] = RefDate.copy()
conn2.close()

for i in range(len(dynoMech)):
    for k in range(1, 3):
        if dynoMech.iat[i, 0] > datedict[k][datedict[k].UWI == dynoMech.iat[i, 2]].TEST_DATE.values[0]:
            date = pd.to_datetime(datedict[k][datedict[k].UWI == dynoMech.iat[i, 2]].TEST_DATE.values[0])
            try:
                pf_ref = DynM[(DynM.DYNO_DATE == date) & (DynM.UWI == dynoMech.iat[i, 2])].PUMP_FILL
                net_ref = DynM[(DynM.DYNO_DATE == date) & (DynM.UWI == dynoMech.iat[i, 2])].NETNORM
                gas_ref = DynM[(DynM.DYNO_DATE == date) & (DynM.UWI == dynoMech.iat[i, 2])].GASSY_FACTOR
                card_ref = DynM[(DynM.DYNO_DATE == date) & (DynM.UWI == dynoMech.iat[i, 2])].CARD_AREA
                dynoMech.iat[i, -4] = dynoMech.iat[i, 23] / pf_ref
                dynoMech.iat[i, -3] = dynoMech.iat[i, 22] / net_ref
                dynoMech.iat[i, -2] = dynoMech.iat[i, 4] / gas_ref
                dynoMech.iat[i, -1] = dynoMech.iat[i, 3] / card_ref
            except:
                dynoMech.iat[i, -4] = 1
                dynoMech.iat[i, -3] = 1
                dynoMech.iat[i, -2] = 1
                dynoMech.iat[i, -1] = 1
            break
    if np.isnan(dynoMech.iat[i, -2]):
        dynoMech.iat[i, -2] = 1
        dynoMech.iat[i, -1] = 1

dynoMechx = dynoMech[~np.isnan(dynoMech.FNORM_1)].reset_index(drop=True)
import pickle

model = pickle.load(open(r"D:\SRP\srp_lgbm1.sav", 'rb'))

dynoMechx.BFPD_PREDICT = model.predict(
    dynoMechx[["FNORM_1", "FNORM_2", "FNORM_3", "DFLUID_STROKE", "DGROSS_DISPLACEMENT",
               "DLOAD_SPAN", "DGASSY_FACTOR", 'NETNORM', "DCARD_AREA"]].values) * dynoMechx.GROSS_DISPLACEMENT

for i in range(len(dynoMechx)):
    try:
        if dynoMechx.iat[i, 9] > 500:
            if dynoMechx.iat[i, 9] / (dynoMechx.iat[i, 12] * dynoMechx.iat[i, 7]) >= 1.5:
                if state != 1:
                    dynoMechx.iat[i, 9] = dynoMechx.iat[i, 12] * dynoMechx.iat[i, -4] * dynoMechx.iat[i, 7]
        if (dynoMechx.iat[i, 9] > 100):
            if dynoMechx.iat[i, -3] > 1.5:
                if (dynoMechx.iat[i, -1] > 1.1) & (dynoMechx.iat[i, -4] < 5):
                    dynoMechx.iat[i, 9] = dynoMechx.iat[i, -4] * dynoMechx.iat[i, 9]
        if dynoMechx.iat[i, 9] >= 100:
            if dynoMechx.iat[i, -1] > 3:
                dynoMechx.iat[i, 9] = dynoMechx.iat[i, 9] * (dynoMechx.iat[i, -1]) * 0.5
        if dynoMechx.iat[i, 9] < 200:
            if dynoMechx.iat[i, 21] >= 1.2:
                dynoMechx.iat[i, 9] = dynoMechx.iat[i, 9] * dynoMechx.iat[i, -4]
        if (dynoMechx.iat[i, 9] > dynoMechx.iat[i, 7]):
            dynoMechx.iat[i, 9] = dynoMechx.iat[i, 7]

    except:
        continue

import cx_Oracle
from datetime import datetime, date, timedelta

conn3 = cx_Oracle.connect('IODSC_username', 'xxxxx', 'oil3rknp')
cur1 = conn3.cursor()


def update(DYNO_DATE, WELL_NAME, BFPD_PREDICT):
    cur1.execute(
        "Update IODSC_SOR.SRP_INFERRED_PRODUCTION set BFPD_PREDICT = :BFPD_PREDICT where DYNO_DATE = :DYNO_DATE AND WELL_NAME = :WELL_NAME",
        {'DYNO_DATE': (DYNO_DATE), 'WELL_NAME': (WELL_NAME), "BFPD_PREDICT": (BFPD_PREDICT)})
    if cur1.rowcount <= 0:
        print('Update failed.')
    # if cur1.rowcount > 0:
    # print('success')
    conn3.commit()


for i in range(len(dynoMechx)):
    try:
        update(dynoMechx.iat[i, 0].date(), dynoMechx.iat[i, 1], dynoMechx.iat[i, 9])
    except:
        pass

print(datetime.now())