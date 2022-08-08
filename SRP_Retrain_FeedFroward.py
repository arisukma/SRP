import pandas as pd
import numpy as np
import warnings
import cx_Oracle
import random
from datetime import datetime, date, timedelta
warnings.filterwarnings("ignore")

conn = cx_Oracle.connect('app_iodsc_mlds', 'xxxxxx', 'oil3rknp')
sql = """
select t.UWI, t.WELL_NAME, t.DYNO_DATE ,t.CARD_AREA,t.GASSY_FACTOR, t.FLUID_STROKE,
       t.LOAD_SPAN, t.GROSS_DISPLACEMENT, t.NET_DISPLACEMENT,
       t.PUMP_FILL from IODSC_SOR.SRP_INFERRED_PRODUCTION t where t.DYNO_DATE >=SYSDATE-100
"""
main = pd.read_sql_query(sql, conn)
main = main[main.CARD_AREA>=0]

featpack1 = pd.read_csv("historical_feature2.csv", usecols = ['UWI', 'WELL_NAME', 'DYNO_DATE', 'BFPD_TEST', 'BFPD_TEST_Average',
                                                             'BFPD_TEST_SLOPE', 'BFPD_TEST_Intercept'],
                        parse_dates=["DYNO_DATE"])

main["BFPD_TEST_Average"] = None
main["BFPD_TEST_SLOPE"] = 0.0
main["BFPD_TEST_Intercept"] = 0.0
main["RefTestDate"] = None


def nearestlatestdate(date,datelist):
    dates = sorted(datelist)
    if date<=dates[0]:
        selecteddate = None
    else:
        leftmode=0
        tempdate = dates[0]
        for k, seldate in enumerate(dates):
            if date<=seldate :
                leftmode = 1
                if date==seldate:
                    tempdate = seldate
                continue
            else:
                if leftmode==1:
                    selecteddate = tempdate
                    break
                else:
                    selecteddate = seldate
                    continue
    return selecteddate


uwitemp = ""
for i in main.index:
    uwi = main.UWI[i]
    date = main.DYNO_DATE[i]
    if uwi == uwitemp:
        pass
    else:
        reftable = featpack1[featpack1.UWI == uwi].reset_index(drop=True)
        datelist = list(reftable.DYNO_DATE.values)
        uwitemp = uwi
    if len(datelist)!=0:
        selectdate = nearestlatestdate(date, datelist)
    else:
        continue
    if selectdate!=None:
        index = datelist.index(selectdate)
        main["BFPD_TEST_Average"][i] = reftable["BFPD_TEST_Average"][index]
        main["BFPD_TEST_SLOPE"][i] = reftable["BFPD_TEST_SLOPE"][index]
        main["BFPD_TEST_Intercept"][i] = reftable["BFPD_TEST_Intercept"][index]
        main["RefTestDate"][i] = selectdate

#main.to_csv("SRP_DATASETXXXX.csv", index = False)
main["RefTestDate"] = pd.to_datetime(main["RefTestDate"])
main["DAYS_AFTER_TEST"] = main["DYNO_DATE"] - main["RefTestDate"]
main["DAYS_AFTER_TEST"] = main["DAYS_AFTER_TEST"].apply(lambda x : x.days)
import pickle
model = pickle.load(open(r"C:\Users\ari.negara\DSProjects\SRPInfer\SRP_Retrain_LGBM2.sav", 'rb'))

main["BFPD_Predict"] = 0
main.BFPD_PREDICT = model.predict(main[['CARD_AREA','GASSY_FACTOR', 'FLUID_STROKE',
       'LOAD_SPAN', 'GROSS_DISPLACEMENT', 'NET_DISPLACEMENT',
       'PUMP_FILL', 'BFPD_TEST_Average', 'BFPD_TEST_SLOPE',
       'BFPD_TEST_Intercept','DAYS_AFTER_TEST']])
main.to_csv("resultretrain.csv", index = False)
print("complete")
