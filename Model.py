import pandas as pd
import numpy as np
from tqdm import tqdm
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import KFold
from sklearn.metrics import r2_score
import lightgbm as lgb
import warnings
warnings.filterwarnings("ignore")
import gc
pd.set_option("display.max_columns", 500)
pd.set_option("display.max_rows", 500)

data = pd.read_csv("SRP_DATASET6.csv", parse_dates=["DYNO_DATE"])
data = data.sort_values(["DYNO_DATE"]).reset_index()
SEED = 100
parameters = {
    'metric': ['rmse'],
    'is_unbalance': [True],
    'num_class': [5],
    'learning_rate': [0.05],
    'max_depth': [6],
    'max_features': ['sqrt'],
    'random_state': [SEED],
    'n_estimators': [500]

}

cols = ['CARD_AREA','GASSY_FACTOR', 'FLUID_STROKE',
       'LOAD_SPAN', 'GROSS_DISPLACEMENT', 'NET_DISPLACEMENT',
       'PUMP_FILL', 'BFPD_TEST_Average', 'BFPD_TEST_SLOPE',
       'BFPD_TEST_Intercept','DAYS_AFTER_TEST', 'BFPD_TEST_2']

import warnings
warnings.filterwarnings("ignore")

train = data.loc[:700000, cols]
test = data.loc[700000:900000, cols]
blind = data.loc[900000:, cols]

del data
gc.collect()
train = train[train["BFPD_TEST_Average"]>=0]
train = train[train["LOAD_SPAN"]>=0]
train = train[train["PUMP_FILL"]>=0]

test = test[test["BFPD_TEST_Average"]>=0]
test = test[test["LOAD_SPAN"]>=0]
test = test[test["PUMP_FILL"]>=0]
test = test[test["FLUID_STROKE"]>=0]
test  = test.reset_index(drop=True)

X = train.iloc[:,:-1]
y = train["BFPD_TEST_2"].values
gsearch2 = GridSearchCV(estimator= lgb.LGBMClassifier(), param_grid = parameters,
                        scoring='r2',n_jobs=4)
gsearch2.fit(X, y)
print(gsearch2.cv_results_ , gsearch2.best_params_, (-gsearch2.best_score_))
test_predict = gsearch2.predict(test.iloc[:,:-1])
r2 = r2_score(test.iloc[:,-1], test_predict)
print(r2)

from sklearn.metrics import mean_absolute_percentage_error as mape
from sklearn.metrics import mean_squared_error as mse
mape_score = mape(test.iloc[:,-1], test_predict)
rmse = (mse(test.iloc[:,-1], test_predict))**0.5
print("r2 = {} , mape = {} , rmse = {}" . format(r2, mape_score, rmse))


blind = blind[blind["BFPD_TEST_Average"]>=0]
blind = blind[blind["LOAD_SPAN"]>=0]
blind = blind[blind["PUMP_FILL"]>=0]
blind = blind[blind["FLUID_STROKE"]>=0]
blind = blind.reset_index(drop=True)

blind_predict = gsearch2.predict(blind.iloc[:,:-1])
r2blind = r2_score(blind.iloc[:,-1], blind_predict)
mapeblind_score = mape(blind.iloc[:,-1], blind_predict)
rmseblind = (mse(blind.iloc[:,-1], blind_predict))**0.5
print("blind r2 = {} , mape = {} , rmse = {}" . format(r2blind, mapeblind_score, rmseblind))
