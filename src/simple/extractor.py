from bs4 import BeautifulSoup
from nthrow.utils import sha1
from nthrow.source import SimpleSource
import nepali_datetime
import datetime 
from nthrow.source.http import create_session


"""
extractor.make_a_row method
	make_a_row takes 3 parameters here
	1.) url of the dataset that you put in extractor.set_list_info
	2.) url of a row,
			always pass it through self.mini_uri method,
			this replaces https with http and
			removes www. from urls to reduce duplicate rows
	- hash of urls from 1 & 2 becomes id of the row
	3.) the row data, it's stored in a JSONB column

extractor.make_error method
	make_error takes 3 parameters
	1.) _type = HTTP, Exception etc.
	2.) code = 404, 403 etc.
	3.) message = None (Any text message)
"""


class Extractor(SimpleSource):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	async def create_session(self, session=None):		
		return await create_session(timeout=24)
	
	async def fetch_rows(self, row, _type="to"):
		# row is info about this dataset
		# it is what was returned with extractor.get_list_row method
		# it holds pagination, errors, retry count, next update time etc.
		try:
			url ="https://supremecourt.gov.np/weekly_dainik/pesi/daily/39"
			args = self.prepare_request_args(row, _type)
			
			today_date=nepali_datetime.date.today()
			page =  nepali_datetime.datetime.strptime(args["cursor"] , '%Y-%m-%d') if args["cursor"] else today_date

	
			form_data = {
			"todays_date": today_date.strftime('%K-%n-%D'),
			"pesi_date": page.strftime('%K-%n-%D'),
			"submit": "खोज्नु होस्",
			}

			res = await self.http_post(url,data=form_data)  
			if res.status_code == 200:
				rows = []
				content = res.text
				soup = BeautifulSoup(content, "html.parser")

			
				tables = soup.find_all("table", class_="record_display")[1:]
				

				for table in tables:
					for tr in table.find_all("tr")[1:]:
						tds = tr.find_all("td")
						if len(tds)!=10:
							continue

						row_data = {
							"uri":"https://supremecourt.gov.np/weekly_dainik/pesi/daily/39#" + sha1(tr.get_text(strip=True)),
							"hearing_date":form_data["pesi_date"],
							"case_num": tds[1].get_text(strip=True),
							"registration_date": tds[2].get_text(strip=True),
							"case_type": tds[3].get_text(strip=True),
							"plantiff": tds[4].get_text(strip=True),
							"defendant": tds[5].get_text(strip=True),
						}

						rows.append(row_data)

					
				# slice rows length to limit from extractor.query_args or
				# extractor.settings[remote]
				rows = self.clamp_rows_length(rows)
				return {
					"rows": [
						self.make_a_row(
							row["uri"], self.mini_uri(r["uri"], keep_fragments=True), r
						)
						for r in rows
					],
					"state": {
						"pagination": {
							# value for next page, return None when pagination ends
							_type: (page - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
						}
					},
				}
			else:
				self.logger.error("Non-200 HTTP response: %s : %s" % (res.status_code, url))
				return self.make_error("HTTP", res.status_code, url)
		except Exception as e:
			self.logger.exception(e)
			return self.make_error("Exception", type(e), str(e))
