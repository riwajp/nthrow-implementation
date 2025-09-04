import os
import asyncio

from datetime import datetime
import time
from nthrow.utils import create_db_connection, create_store, utcnow
from nthrow.utils import uri_clean, uri_row_count

from src.monitoring.extractor import Extractor

# from nthrow.source.StorageHelper import Storage

# table name and postgres credentials
table = "nthrows"
creds = {
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "database": os.environ["DB"],
    "host": os.environ["DB_HOST"],
    "port": os.environ["DB_PORT"],
}

conn = create_db_connection(**creds)
create_store(conn, table)  # creates table


def monitor_activity():
    extractor = Extractor(conn, table)
    # extractor.query_args.update({"limit": 100, "start":"2025-09-04-06:00:00"})  # default args for your dataset
    extractor.query_args.update({"limit": 100})

    # url of your dataset, this effectively becomes id of this dataset
    # use l.get_list_row() to return this record from database later
    extractor.set_list_info("https://www.seismicportal.eu/fdsnws/event/1/")

    # sets the CustomStorage if redis can't be found
    # extractor.storage = Storage(conn, table)


   
    extractor.settings = {
        "remote": {
            "refresh_interval": 1,
            "run_period": "18-2",  # 24 hrs format
            "timezone": "Asis/Kathmandu",
        }
    }

    async def call():
        async with await extractor.create_session() as session:
            extractor.session =  session

            _= await extractor.collect_rows(extractor.get_list_row()) 

            # collect_rows calls fetch_rows on your extractor class and
            # puts the returned rows in postgres table
            while True:           
               
                if extractor.should_run_again():
                    extractor._reset_run_times()
                    await extractor.collect_rows(extractor.get_list_row())
                else:            
                    print("Waiting...")
                time.sleep(5)
                
                
                
            
           
         

    asyncio.run(call())


if __name__ == "__main__":
    monitor_activity()
