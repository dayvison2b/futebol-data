from sqlalchemy import create_engine

with open('settings/mysql_credentials.py', 'r') as file:
    file_content = file.readlines()

# Parse the content to extract the values
mysql_credentials = {}
for line in file_content:
    key, value = line.strip().split(' = ')
    mysql_credentials[key] = value.strip("'")
    
username = mysql_credentials['username']
password = mysql_credentials['password']
host = mysql_credentials['host']
port = mysql_credentials['port']
database = mysql_credentials['database']

def connect():
    engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}")
    return engine

def close(engine):
    engine.dispose()
    return