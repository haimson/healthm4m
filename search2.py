import nltk
from nltk.tokenize import *
import csv

import os
os.chdir("/Users/oliverhaimson/Dropbox/summer research project/searches")

import sqlite3 as lite
import sys

con = lite.connect('/Users/oliverhaimson/Dropbox/Craigslist Personals/sqlite/week1and2.sqlite')

#read in data from database

with con:
    con.row_factory = lite.Row
    
    cur = con.cursor()
    cur.execute("SELECT * FROM m4m_full")
    con.commit()

    rows = cur.fetchall()

# make dictionary arrays

hivDict = [" hiv","/hiv","-hiv",".hiv",",hiv", \
           " aids","/aids","-aids",".aids",",aids","vaids", \
           " pos","/pos","-pos",".pos",",pos","vpos", \
           " poz","/poz","-poz",".poz",",poz","vpoz", \
           "undetect", "u/+", \
           " neg","/neg","-neg",".neg",",neg"]
hivNot = ["hive", \
          "posa","pose","posing","position","post","poss","posh","poso","poss","posy","posie", \
          "negate","negatin","negatio","negativi","negl","nego","negr","negu"]
diseaseDict = [" d&d","/d&d","-d&d",".d&d",",d&d", \
               " d/d","/d/d","-d/d",".d/d",",d/d", \
               # these won't show up because of space - take care of them with exceptions
               # " d /d","/d /d","-d /d",".d /d",",d /d", \
               # " d/ d","/d/ d","-d/ d",".d/ d",",d/ d", \
               "dd free","d d free","ddf","dd/f", \
               "disease", \
               "std", \
               " bug","/bug","-bug",".bug", \
               " test","/test","-test",".test", \
               "clean"]
diseaseNot = ["buga","bugb","buge","bugg","bugh","bugl","bugo", \
              "testimony", "testes", "testy", "testosterone", \
              "clean-cut", "cleancut", "clean-shaven", "cleanshaven", "cleanse", "cleaned"]
              # "clean cut" and "clean shaven" won't show up because of space - take care of them with exceptions
safeDict = ["safe"]
safeNot = ["safeg"]
protectionDict = ["protect", \
                  "condom"]
protectionNot = ["protective", \
                 "condominium"]
riskyDict = ["bareback", \
             " bb","/bb","-bb",".bb",",bb", \
             " raw","/raw","-raw",".raw",",raw", \
             "breeding"," bred", \
             " seed", \
             "uninhibited"]
riskyNot = ["bbw", \
            "rawh", \
            "seedy"]
healthyDict = ["health"]
healthyNot = []


# create arrays for all categories
zeros = [0]*len(rows)

hiv = [0]*len(rows)
disease = [0]*len(rows)
safe = [0]*len(rows)
risky = [0]*len(rows)
protection = [0]*len(rows)
healthy = [0]*len(rows)

healthRelated = [0]*len(rows)

count = [0]*len(rows)
wordCount = [0]*len(rows)
proportion = [0]*len(rows)

descriptions = [0]*len(rows)
age = [0]*len(rows)
location_id = [0]*len(rows)

var = [0]*len(rows)

# search for health-related terms

def categorySearch(word, categoryDict, categoryDictNot):
    addIt = False
    for entry in categoryDict:
        if entry in word:
            addIt = True
            break
    for entry in categoryDictNot:
        if entry in word:
            addIt = False
            break
    return addIt

def diseaseExceptions(word, j, words, addIt):
    if (j>0):
        if ((word == " free" or word ==" f") and words[j-1].lower() == ("dd" or "d" or "d/d")):
            addIt = True
        elif (word == " d" and words[j-1].lower() == ("d" or "d/")):
            addIt = True
        elif (word == " /d" and words[j-1].lower() == "d"):
            addIt = True
    if (len(words)>j+1):
        if (word == " clean" and words[j+1].lower() == ("shaven" or "cut")):
            addIt = False
    return addIt

def riskExceptions(word, j, words, addIt):
    if (j>0):
        if (words[j-1].lower() == "no"):
            addIt = False
    return addIt

i=0
for line in rows:
    descriptions[i] = line["description"].encode('utf-8')
    age[i] = line["age"]
    location_id[i] = line["channel_id"]
    words = WhitespaceTokenizer().tokenize(line["description"])
    j=0
    for w in words:
        wordCount[i] += 1
        # add blank space to the beginning of each word so that we know it's the beginning of a word
        w = " "+w
        w = w.lower()
        addItHiv = categorySearch(w, hivDict, hivNot)
        if addItHiv == True:
            hiv[i] = 1
            disease[i] = 1 #because HIV is a sub-category of disease
            count[i] += 1
        addItDisease = categorySearch(w, diseaseDict, diseaseNot)
        addItDisease = diseaseExceptions(w, j, words, addItDisease)
        if addItDisease == True:
            disease[i] = 1
            count[i] +=1
        addItSafe = categorySearch(w, safeDict, safeNot)
        if addItSafe == True:
            safe[i] = 1
            count[i] += 1
        addItRisky = categorySearch(w, riskyDict, riskyNot)
        if addItRisky == True:
            protection[i] = 1 #because risky is a subcategory of protection
        addItRisky = riskExceptions(w, j, words, addItRisky)
        if addItRisky == True:
            risky[i] = 1
            count[i] += 1
        addItProtection = categorySearch(w, protectionDict, protectionNot)
        if addItProtection == True:
            protection[i] = 1
            count[i] += 1
        addItHealthy = categorySearch(w, healthyDict, healthyNot)
        if addItHealthy == True:
            healthy[i] = 1
            count[i] += 1
        j += 1
    i += 1


# create general health-related boolean array and calculate proportions array

for i in range(0,len(rows)):
    if count[i]>0:
        healthRelated[i] = 1
    proportion[i] = round((float(count[i]) / float(wordCount[i])), 3)


# get ids

ids = [0]*len(rows)
channel_id = [0]*len(rows)

i=0
for line in rows:
    ids[i] = int(line["id"])
    channel_id[i] = int(line["channel_id"])
    i += 1


# channels
    
import string
cur.execute("SELECT name FROM channel WHERE id>41")
con.commit()
channels = cur.fetchall()

locations = [0]*len(channels)

exclude = set(string.punctuation+" ")
i=0
for channel in channels:
    locations[i] = ''.join(ch for ch in channel["name"] if ch not in exclude)
    i+=1

channelsDict = {}
for i in range(42,137):
    j=0
    channelsDict[str(i)] = [0]*len(rows)
    for line in rows:
        if line["channel_id"] == i:
            channelsDict[str(i)][j] = 1
        j+=1

   
# organize data and write to database

cur.execute("DROP TABLE IF EXISTS searchResults;")

locationsString = ""
for location in locations:
    locationsString=locationsString+location+" int, "
locationsString=locationsString[:-2]

cur.execute("CREATE TABLE searchResults (ids int, channel_id int, wordCount int, healthRelated int, hiv int, disease int, safe int, \
            risky int, protection int, healthy int, age int, "+locationsString+");")

qs = "?,"*106
qs = qs[:-1]
for i in range(0,len(rows)):
    query = "INSERT INTO searchResults VALUES ("+qs+")"
    values = [ids[i], channel_id[i], wordCount[i], healthRelated[i], hiv[i], disease[i], safe[i], risky[i], protection[i], \
                healthy[i], age[i]]
    for j in range(42,137):
        values.append(channelsDict[str(j)][i])
    cur.execute(query, values)
    con.commit()


# print out results

def toPercentage(category):
    percent = round(100*float(sum(category))/float(len(rows)), 2)
    string = "% = "+str(percent)
    return string

def toAvg(array):
    avg = round(float(sum(array))/float(len(rows)), 2)
    string = "avg = "+str(avg)
    return string

print("Overall SHR percentage: "+toPercentage(healthRelated))
print("HIV percentage: "+toPercentage(hiv))
print("Disease percentage: "+toPercentage(disease))
print("Safe percentage: "+toPercentage(safe))
print("Risky percentage: "+toPercentage(risky))
print("Protection percentage: "+toPercentage(protection))
print("Healthy percentage: "+toPercentage(healthy))
print("Average word count: "+toAvg(wordCount))
