# -*- coding: utf-8 -*-
"""
Created on Tue May 19 12:05:30 2020

@author: jardiel.junior
"""

from statsmodels.discrete.discrete_model import Probit
import statsmodels.api  as sm
import pandas as pd
import numpy as np


df = pd.read_excel('Dados_Probit.xlsx')
df = df.dropna().reset_index(drop=True)
X = np.array(df[[ 'PD Credit Grade', 'PD KMV']])
X = sm.add_constant(X)
model = Probit(df['def - 60pct'], X)
probit_model = model.fit()
print(probit_model.summary())