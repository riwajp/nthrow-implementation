import os
import asyncio
import time
from nthrow.utils import create_db_connection, create_store
from src.case_extractor.extractor import Extractor



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


def state(pagi={}, config={}):
		return {
			'pagination': {
				'from': {'date': None, 'cursor': None},
				'to': {'date': None, 'cursor': None},
				'config': {
					'timezone': None,
					'start': '2025-09-05', 
					'end': '2025-09-02', 
					'step': 1,
					**config
				},
				**pagi
			}
		}

def extract_cases():
    extractor = Extractor(conn, table)

    extractor.query_args.update({"before":"2025-09-05"})
 
    extractor.set_list_info("https://supremecourt.gov.np/weekly_dainik/pesi/daily/")

      
    extractor.settings = {
        "remote": {
            "refresh_interval": 15,
            "run_period": "18-2",  # 24 hrs format
            "timezone": "Asis/Kathmandu",
        }
    }

   

    async def call():
        async with await extractor.create_session() as session:
            extractor.session =  session
            row=extractor.make_a_row('/','https://supremecourt.gov.np/weekly_dainik/pesi/daily/',None,state(),_list=True)

            # collect_rows calls fetch_rows on your extractor class and
            # puts the returned rows in postgres table
            
            district_num = 1
            district_path=district_num+19 if district_num>=76 else district_num+17
       
            extractor.query_args.update({"district_path": district_path})

            while True:
               
                            
                _ = await extractor.collect_rows(extractor.get_list_row(row))
                
                row = extractor.get_list_row()      
                                                

                if not row["state"]["pagination"]["to"]:
                    print("===== completed scraping all dates")
                    break
                
                time.sleep(2)

               
           
            # l.del_garbage()

    asyncio.run(call())


if __name__ == "__main__":
    extract_cases()
