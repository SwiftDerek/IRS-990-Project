# -*- coding: utf-8 -*-
"""
Created on Sun Apr 28 14:24:56 2019

@author: Tom
"""

import geopy
import geopy.distance

pdf = pd.read_csv('zipcodes.csv')
pdf = pdf.dropna(axis = 0, how = 'any', subset = ['Lat', 'Long'])


pdf['slat'] = 0
pdf['flat'] = 0
pdf['slong'] = 0
pdf['flong'] =0

d = geopy.distance.distance(miles = 15)

def getn(row):
    return d.destination(point=(geopy.Point(row['Lat'], row['Long'])), bearing=0)[0]
def gets(row):
    return d.destination(point=(geopy.Point(row['Lat'], row['Long'])), bearing=180)[0]
def gete(row):
    return d.destination(point=(geopy.Point(row['Lat'], row['Long'])), bearing=90)[1]
def getw(row):
    return d.destination(point=(geopy.Point(row['Lat'], row['Long'])), bearing=270)[1]


d.destination(point = (geopy.Point(pdf.iloc[0]['Lat'], pdf.iloc[0]['Long'])), bearing =  0)


pdf['slat'] = pdf.apply(getn, axis=1)
pdf['flat'] = pdf.apply(gets, axis=1)
pdf['slong'] = pdf.apply(gete, axis=1)
pdf['flong'] = pdf.apply(getw, axis=1)
    
pdf.to_csv('zipcodes.csv')