#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt


# In[8]:


from getpass import getpass
from mysql.connector import connect, Error

try:
    with connect(
        host="127.0.0.1",
        user=input("Enter username: "),
        password=getpass("Enter password: "),
    ) as connection:
     
        print(connection)
except Error as e:
    print(e)


# In[68]:


connection.reconnect();
cursor = connection.cursor();
cursor.execute("CREATE DATABASE Gun_Violence;")


# In[10]:


connection.reconnect();
cursor = connection.cursor();
cursor.execute("USE Gun_Violence");


# In[70]:


sql = "CREATE TABLE Incident (incident_id VARCHAR(50) NOT NULL Primary kEY , date date not null);"
cursor.execute(sql);


# In[71]:


sql = "DESCRIBE Incident"
cursor.execute(sql);
result = cursor.fetchall()
print(result)


# In[72]:


sql = "CREATE TABLE Location(location_id VARCHAR(50) NOT NULL Primary kEY , state varchar(50) not null,city_or_country varchar(50) not null, address varchar(100) not null,latitude decimal(10,6) not null,longitude decimal(10,6) not null,incident_id varchar(50) not null,CONSTRAINT FK_Incident_incident_id FOREIGN KEY (incident_id) REFERENCES Incident(incident_id));"
cursor.execute(sql)


# In[73]:


sql = "DESCRIBE Location"
cursor.execute(sql);
result = cursor.fetchall()
print(result)


# In[74]:


sql = "CREATE TABLE Demographic(adults int(50) NOT NULL, teen int(50) not null, child int(50) not null, male int(50) not null, female int(50) not null,demographic_id varchar(50) not null primary key, incident_id varchar(50) not null,CONSTRAINT FK_Demographic_incident_id FOREIGN KEY (incident_id) REFERENCES Incident(incident_id));"
cursor.execute(sql)


# In[75]:


sql = "DESCRIBE Demographic"
cursor.execute(sql);
result = cursor.fetchall()
print(result)


# In[80]:


sql="CREATE TABLE Participant (Victim int(50) NOT NULL, suspect int(50) not null, participant_id varchar(50) not null primary key, incident_id varchar(50) not null,  Demographic_id varchar(50) not null, CONSTRAINT FK_Participant_Demographic_id FOREIGN KEY(Demographic_id) REFERENCES Demographic(demographic_id), CONSTRAINT FK_Participant_incident_id FOREIGN KEY (incident_id) REFERENCES Incident(incident_id));"
cursor.execute(sql);


# In[81]:


sql = "DESCRIBE Participant"
cursor.execute(sql);
result = cursor.fetchall()
print(result)


# In[82]:


sql="CREATE TABLE Status(unharmed int(50) not null, n_killed int(50) not null,n_injured int(50) not null,status_id varchar(50) not null primary key,incident_id varchar(50) not null, constraint FK_Status_incident_id FOREIGN KEY(incident_id) REFERENCES Incident(incident_id));"
cursor.execute(sql)


# In[83]:


sql = "DESCRIBE Status"
cursor.execute(sql);
result = cursor.fetchall()
print(result)


# In[84]:


Incident=pd.read_csv('/Users/poojamanjunatha/Desktop/tables/Incident.csv', index_col=False)


# In[85]:


for i, row in Incident.iterrows():
    sql = "INSERT INTO Incident VALUES (%s, %s)"
    cursor.execute(sql, tuple(row))
    print("Record inserted")


# In[86]:


Location=pd.read_csv('/Users/poojamanjunatha/Desktop/tables/Location.csv', index_col=False)


# In[87]:


Location.head()


# In[88]:


for i, row in Location.iterrows():
    sql = "INSERT INTO Location VALUES (%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
    print("Record inserted")


# In[89]:


Participant=pd.read_csv('/Users/poojamanjunatha/Desktop/tables/Participant.csv', index_col=False)


# In[90]:


Participant.head()


# In[95]:


for i, row in Participant.iterrows():
    sql = "INSERT INTO Participant VALUES (%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
    print("Record inserted")


# In[92]:


Demographic=pd.read_csv('/Users/poojamanjunatha/Desktop/tables/Demographic.csv', index_col=False)


# In[98]:


Demographic.head()


# In[94]:


for i, row in Demographic.iterrows():
    sql = "INSERT INTO Demographic VALUES (%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
    print("Record inserted")


# In[99]:


Status=pd.read_csv('/Users/poojamanjunatha/Desktop/tables/Status.csv', index_col=False)


# In[100]:


Status.head()


# In[101]:


for i, row in Status.iterrows():
    sql = "INSERT INTO Status VALUES (%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
    print("Record inserted")


# In[102]:


connection.commit()


# In[105]:


sql = "CREATE TABLE Participation (incident_id VARCHAR(50) NOT NULL, participant_id VARCHAR(50) NOT NULL, CONSTRAINT PK_Participation PRIMARY KEY (incident_id, participant_id), CONSTRAINT FK_Participation_incident_id FOREIGN KEY (incident_id) REFERENCES Incident (incident_id), CONSTRAINT FK_Participation_participant_id FOREIGN KEY (participant_id) REFERENCES Participant (participant_id))"
cursor.execute(sql)


# In[107]:


Participation=pd.read_csv('/Users/poojamanjunatha/Desktop/tables/Participation.csv', index_col=False)


# In[108]:


for i, row in Participation.iterrows():
    sql = "INSERT INTO Participation VALUES (%s,%s)"
    cursor.execute(sql, tuple(row))
    print("Record inserted")


# In[4]:


print("The Highest number of teenage shootings has been in:")
cursor.execute("SELECT l.State, COUNT(d.Teen) AS num_shootings "
               "FROM Demographic d "
               "JOIN Incident i ON d.Incident_ID = i.Incident_ID "
               "JOIN Location l ON i.Incident_ID = l.Incident_ID "
               "WHERE d.Teen > 0 "
               "GROUP BY l.State "
               "ORDER BY num_shootings DESC "
               "LIMIT 1")
result = cursor.fetchone()
print(result)
cursor.close()


# In[9]:


print('The number of incidents where at least one person was killed and the corresponding state:')
cursor.execute("SELECT l.State, COUNT(DISTINCT s.Incident_ID) AS num_incidents "
                "FROM Status s "
                "JOIN Location l ON s.Incident_ID = l.Incident_ID "
                "WHERE s.n_killed > 0 OR s.n_injured > 0 "
                "GROUP BY l.State "
                "ORDER BY num_incidents DESC")
result = cursor.fetchone()
print(result)


# In[4]:


print("The number of incidents where there were no suspects:")
cursor2 = connection.cursor()
cursor2.execute("SELECT COUNT(*) "
                "FROM Incident i "
                "LEFT JOIN Participant p ON i.Incident_ID = p.Incident_ID AND p.suspect = 'Yes' "
                "WHERE p.participant_id IS NULL")
result = cursor2.fetchone()
print(result[0])
cursor2.close()


# In[6]:


print("The number of incidents where there were at least one female participant:")
cursor = connection.cursor()
query = "SELECT l.State, COUNT(DISTINCT p.Incident_ID) AS num_incidents          FROM Participant p          JOIN Demographic d ON p.Demographic_id = d.Demographic_id          JOIN Incident i ON p.Incident_ID = i.Incident_ID          JOIN Location l ON p.Incident_ID = l.Incident_ID          WHERE d.Female > 0          GROUP BY l.State          ORDER BY num_incidents DESC"
cursor.execute(query)
results = cursor.fetchall()
for row in results:
    print(row)
cursor.close()
connection.close()


# In[1]:


pip install snowflake-connector-python


# In[1]:


import snowflake.connector

conn = snowflake.connector.connect(
    user='<pooja255>',
    password='<Dhavala@123>',
    account='<POOJAM255 - US Central 1 (Iowa)>',
    database='<gv>',
    schema='<my_schema>')


# In[ ]:




