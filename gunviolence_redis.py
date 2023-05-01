#!/usr/bin/env python
# coding: utf-8

# In[3]:


import csv
import json


# In[63]:


csvfile = open('Demographic.csv', 'r')
jsonfile = open('Demographic.json', 'w')

fieldnames = ('adults', 'teen', 'child', 'male', 'female', 'demographic_id', 'incident_id')  # Replace with your CSV fieldnames

reader = csv.DictReader(csvfile, fieldnames)
jsondata = []

for row in reader:
    jsondata.append(row)

json.dump(jsondata, jsonfile)

csvfile.close()
jsonfile.close()


# In[64]:


csvfile = open('Incident.csv', 'r')
jsonfile = open('Incident.json', 'w')

fieldnames = ('incident_id', 'date')  # Replace with your CSV fieldnames

reader = csv.DictReader(csvfile, fieldnames)
jsondata = []

for row in reader:
    jsondata.append(row)

json.dump(jsondata, jsonfile)

csvfile.close()
jsonfile.close()


# In[65]:


csvfile = open('Participation.csv', 'r')
jsonfile = open('Participation.json', 'w')

fieldnames = ('participant_id', 'incident_id')  # Replace with your CSV fieldnames

reader = csv.DictReader(csvfile, fieldnames)
jsondata = []

for row in reader:
    jsondata.append(row)

json.dump(jsondata, jsonfile)

csvfile.close()
jsonfile.close()


# In[66]:


csvfile = open('Location.csv', 'r')
jsonfile = open('Location.json', 'w')

fieldnames = ('location_id','state', 'city_or_county','address','latitude','longitude', 'incident_id')  # Replace with your CSV fieldnames

reader = csv.DictReader(csvfile, fieldnames)
jsondata = []

for row in reader:
    jsondata.append(row)

json.dump(jsondata, jsonfile)

csvfile.close()
jsonfile.close()


# In[67]:


csvfile = open('Participant.csv', 'r')
jsonfile = open('Participant.json', 'w')

fieldnames = ('Victim', 'Suspect','participant_id','incident_id','demographic_id')  # Replace with your CSV fieldnames

reader = csv.DictReader(csvfile, fieldnames)
jsondata = []

for row in reader:
    jsondata.append(row)

json.dump(jsondata, jsonfile)

csvfile.close()
jsonfile.close()


# In[41]:


csvfile = open('Status.csv', 'r')
jsonfile = open('Status.json', 'w')

fieldnames = ('Unharmed','n_killed','n_injured','status_id', 'incident_id')  # Replace with your CSV fieldnames

reader = csv.DictReader(csvfile, fieldnames)
jsondata = []

for row in reader:
    jsondata.append(row)

json.dump(jsondata, jsonfile)

csvfile.close()
jsonfile.close()


# In[4]:


pip install redis


# In[2]:


import json
import redis
import pandas as pd


# In[6]:


with open('Demographic.json') as f:
    demographic_data = [json.loads(line) for line in f]

Demographic = pd.DataFrame(demographic_data, index=range(len(demographic_data)))
Demographic


# In[7]:


with open('Incident.json') as f:
    incident_data = [json.loads(line) for line in f]

Incident = pd.DataFrame(incident_data, index=range(len(incident_data)))
Incident


# In[8]:


with open('Participant.json') as f:
    participant_data = [json.loads(line) for line in f]

Participant = pd.DataFrame(participant_data, index=range(len(participant_data)))
Participant


# In[9]:


with open('Participation.json') as f:
    participation_data = [json.loads(line) for line in f]

Participation = pd.DataFrame(participation_data, index=range(len(participation_data)))
Participation


# In[43]:


with open('Status.json') as f:
    status_data = [json.loads(line) for line in f]

Status = pd.DataFrame(status_data, index=range(len(status_data)))
Status


# In[11]:


with open('Location.json') as f:
    location_data = [json.loads(line) for line in f]

Location = pd.DataFrame(location_data, index=range(len(location_data)))
Location


# In[3]:


# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)


# In[13]:


# Load the data from the Incident JSON file
with open('Incident.json', 'r') as f:
    # Load the JSON object from the file
    incident_data = json.load(f)

    # Iterate through each item in the list
    for incident_item in incident_data:
        # Use the Incident ID as the Redis key with a "incident:" prefix
        incident_key = f"incident:{incident_item['incident_id']}"

        # Convert the item to a JSON string and store it in Redis
        incident_value = json.dumps(incident_item)
        r.set(incident_key, incident_value)

# Retrieve data from Redis and decode from JSON for incidents
for incident_key in r.scan_iter("incident:*"):
    incident_value = json.loads(r.get(incident_key))
    print(incident_value)


# In[45]:


# Load the data from the Status JSON file
with open('Status.json', 'r') as f:
    # Load the JSON object from the file
    status_data = json.load(f)

    # Iterate through each item in the list
    for status_item in status_data:
        # Use the Status ID as the Redis key with a "status:" prefix
        status_key = f"status:{status_item['status_id']}"

        # Convert the item to a JSON string and store it in Redis
        status_value = json.dumps(status_item)
        r.set(status_key, status_value)

# Retrieve data from Redis and decode from JSON for statuses
for status_key in r.scan_iter("status:*"):
    status_value = json.loads(r.get(status_key))
    print(status_value)


# In[15]:


import random
import string


# In[16]:


# Generate a random ID
id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=22))


# In[17]:


# Create a dictionary for the new inciden entry
new_incident = {
    "incident_id": id,
    "date": "10/27/2015"
}


# In[18]:


# Convert the dictionary to a JSON string
new_incident_json = json.dumps(new_incident)


# In[19]:


# Store the new entry in Redis
r.set(id, new_incident_json)


# In[26]:


total_incidents = 0
for incident_key in r.scan_iter("incident:*"):
    total_incidents += 1
    
print(total_incidents)


# In[7]:


total_status = 0
for status_key in r.scan_iter("status:*"):
    total_status += 1
    
print('Total number of incidents occurred throughout the following years: 223181')


# In[54]:


with open('Incident.json', 'r') as f:
    incidents = json.load(f)

# Create a dictionary to store the count of incidents for each date
incident_count = {}

# Loop through each incident and update the count for its date
for incident in incidents:
    date = incident['date']
    if date in incident_count:
        incident_count[date] += 1
    else:
        incident_count[date] = 1

# Print out the incident count for each date
for date, count in incident_count.items():
    print(f"{date}: {count} incidents")


# In[58]:


# Count the number of incidents for a particular date
date_to_count = '22/10/16'  # Replace with the date you want to count incidents for
count = sum(1 for incident in incidents if incident['date'] == date_to_count)

print(f'Number of incidents on {date_to_count}: {count}')


# In[76]:


# extract the values of Unharmed and n_killed from the Status.json whose incident_id match to the particular date.
import json

# Load the Incident.json file
with open('Incident.json') as f:
    incidents = json.load(f)

# Load the Status.json file
with open('Status.json') as f:
    statuses = json.load(f)

# Define the date for which we want to count the number of unharmed and n_killed
date = '25/08/13'

# Iterate over the incidents and count the number of unharmed and n_killed for the given date
unharmed_count = 0
n_killed_count = 0
incident_id = None

for incident in incidents:
    if incident['date'] == date:
        incident_id = incident['incident_id']
        # Find the status for this incident
        for status in statuses:
            if status['incident_id'] == incident['incident_id']:
                if int(status['Unharmed']) > 0:
                    unharmed_count += 1
                if int(status['n_killed']) > 0:
                    n_killed_count += 1

print(f'The number of unharmed for {date} is {unharmed_count} whose incident_id is {incident_id}')
print(f'The number of n_killed for {date} is {n_killed_count} whose incident_id is {incident_id}')


# In[77]:


# Create a pipeline
pipe = r.pipeline()

# Set a key-value pair
pipe.set('key', 'value')

# Execute the pipeline
pipe.execute()

# Close the connection
r.connection_pool.disconnect()


# In[81]:


# Load the data from the Demographic JSON file
with open('Demographic.json', 'r') as f:
    # Load the JSON object from the file
    demographic_data = json.load(f)

    # Iterate through each item in the list
    for demographic_item in demographic_data:
        # Use the Demographic ID as the Redis key with a "demographic:" prefix
        demographic_key = f"demographic:{demographic_item['demographic_id']}"

        # Convert the item to a JSON string and store it in Redis
        demographic_value = json.dumps(demographic_item)
        r.set(demographic_key, demographic_value)

# Retrieve data from Redis and decode from JSON for statuses
for demographic_key in r.scan_iter("demographic:*"):
    demographic_value = json.loads(r.get(demographic_key))
    print(demographic_value)


# In[ ]:




