# -*- coding: utf-8 -*-
"""
Created on Wed May 20 15:00:52 2020

@author: jardiel.junior
"""
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from distutils.version import LooseVersion
from scipy.stats import norm
from sklearn.neighbors import KernelDensity
import pandas as pd
import seaborn as sns


df = pd.read_excel('Dados_Probit.xlsx', sheet_name='PD', skiprows=2)

X = np.array(df.loc[df['def - 60pct']!=0, 'z score KMV']*df.loc[df['def - 60pct']!=0, 'def - 60pct'])
#X = np.array(df['z Score Credit Grade'])
Y = np.array(df['def - 60pct'])
#plt.hist(X, bins=150)
#plt.hist(df['z score Merton']*, weights=(df['def - 60pct']/len(df['def - 60pct']))*100,bins=150)
sns.distplot(X, axlabel='Distance to Default', bins=150, norm_hist=False, kde=True, hist = True).set_title('KMV Model')
#ax.set(xlabel='Distance to Default', ylabel='Frequency')

#plt.show()
