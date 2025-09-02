import os
import asyncio
from datetime import datetime
import nepali_datetime
from nthrow.utils import create_db_connection, create_store, utcnow
from nthrow.utils import uri_clean, uri_row_count
from src.simple.extractor import Extractor



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


def extract_cases():
    extractor = Extractor(conn, table)
 
    extractor.set_list_info("https://supremecourt.gov.np/weekly_dainik/pesi/daily/39")

   
    extractor.settings = {
        "remote": {
            "refresh_interval": 15,
            "run_period": "18-2",  # 24 hrs format
            "timezone": "Asis/Kathmandu",
        }
    }

    """
        extractor.settings = {
            'remote': {
                # how often to refresh this dataset (in miutes)
                # you can leave it None but this extractor will only run once
                # (will still run until pagination ends)
                'refresh_interval': None,

                # number if remote url accepts a limit parameter
                'limit': None
            }
        }
    """

    async def call():
        async with await extractor.create_session() as session:
            extractor.session =  session

            # collect_rows calls fetch_rows on your extractor class and
            # puts the returned rows in postgres table

            while True:
                           
                _ = await extractor.collect_rows(extractor.get_list_row())
                
                row = extractor.get_list_row()               

                next_page=row["state"]["pagination"]["to"]
               
                if nepali_datetime.datetime.strptime(next_page, '%Y-%m-%d').date() < nepali_datetime.date(2082, 5, 10):
                    print("=====pagination ended")
                    break
               
           
            # l.del_garbage()

    asyncio.run(call())


if __name__ == "__main__":
    extract_cases()
