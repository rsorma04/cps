# -*- coding: utf-8 -*-
"""
Created on Mon Sep 21 12:14:10 2020

@author: rsorma
"""

import os, re, struct
import pandas as pd
import numpy as np

base_dir = "C:/Users/rsorma/Desktop/cps/cpsb199401.csv"

df = pd.read_csv(base_dir)

cols = list(df.columns)[1:-1]

cols_one = list(df.columns)

melted = pd.melt(df, id_vars=["hrhhid", "source"],
             value_vars=cols)

sas_dat = pd.read_sas(r"C:\Users\rsorma\Desktop\cps\cpsbjan94.sas",
                      format='xport')

import sas7bdat
from sas7bdat import *

file_name = file_path + "cars.sas7bdat"
foo = SAS7BDAT(file_name)
my_df = foo.to_data_frame()
my_df = my_df.head()
print(my_df)


# read data dictionary text file
data_dict = open(r"C:\Users\rsorma\Desktop\cps\January_2017_Record_Layout.txt").read()

# manually list out the IDs for series of interest
var_names = ['PRTAGE', 'PESEX', 'PREMPNOT', 'PWCMPWGT']

# regular expression matching series name and data dict pattern
p = f'\n({"|".join(var_names)})\s+(\d+)\s+.*?\t+.*?(\d\d*).*?(\d\d+)'

# dictionary of variable name: [start, end, and length + 's']
d = {s[0]: [int(s[2])-1, int(s[3]), f'{s[1]}s']
     for s in re.findall(p, data_dict)}

print(d)






# lists of variable starts, ends, and lengths
start, end, width = zip(*d.values())

# create list of which characters to skip in each row
skip = ([f'{s - e}x' for s, e in zip(start, [0] + list(end[:-1]))])

# create format string by joining skip and variable segments
unpack_fmt = ''.join([j for i in zip(skip, width) for j in i])
print(unpack_fmt)

# struct can interpret row bytes with the format string
unpacker = struct.Struct(unpack_fmt).unpack_from




print(open(r"C:\Users\rsorma\Desktop\cps\aug17pub.dat").readline())

# open file (read as binary) and read lines into "raw_data"
raw_data = open(r"C:\Users\rsorma\Desktop\cps\aug17pub.dat", 'rb').readlines()

wgt = d['PWCMPWGT']  # Location of sample weight variable

# unpack and store data of interest if sample weight > 0
data = [[*map(int, unpacker(row))] for row in raw_data
        if int(row[wgt[0]:wgt[1]]) > 0]

print(data[:5])



a = pd.read_fwf("https://www2.census.gov/programs-surveys/cps/datasets/1994/basic/jan94_mar94_dd.txt",
                skiprows=5, widths=[13,6,37, 1000])



b = pd.read_fwf("https://data.nber.org/data/progs/cps-basic/cpsbjan94.sas")


a1 = a[5:]

a2 = a1.dropna(how='all')

a2['Unnamed: 0'] = a2['Unnamed: 0'].interpolate(method='pad')





