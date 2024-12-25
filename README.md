# Analyzing Seattle

Analyzing Seattle is a fun project i'm working on where I take open datasets from government agencies (with a focus on Seattle) and analyze them for fun! I have currently worked on:
- Moving Seattle Parking Transactions to a `mySQL` database
- Moving EVERY Licenced Pet since 2016 to a `mySQL` database
- Moved a list of every Washington state-licensed electric vehicle into a `mySQL` db

I will be working on graphing these and analyzing the data but for now I can make some pretty simple cool facts:

- Did you know Luna is more popular with Seattleites?
- Seattleites perfer Dogs ~10% more than Cats!
- The average Seattle citizen pays $4.30 for 83 minutes of parking! Thats ~$3.10/hr!

More will be done with this soon, all you need to make your own db is to create a .env file with the following schema:
```env
DATABASE_NAME=db_name
DATABASE_USR=db_user
DATABASE_PWD=database_password
DATABASE_HST=host.database.com
DB_TABLE_NAME=pets
DATA_FILE_PATH=/path/to/folder/pets.csv
# ^ that one is dependant on what your moving to mySQL
```

Then run either `pets_init_db.py` or `parking_init_db.py` to create the db
