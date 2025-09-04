from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from nthrow.utils import sha1
from nthrow.source import SimpleSource
from nthrow.utils import utcnow

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

	def make_url(self, row, _type):
		args = self.prepare_request_args(row, _type)
		

		limit=args['q']["limit"]

		start=args['q'].get("start",None)

		if not start:
			refresh_interval=self.settings["remote"]["refresh_interval"]
			start= row.get("updated_at",datetime.now() - timedelta(minutes=refresh_interval)).strftime("%Y-%m-%d-%H:%M:%S")

			
		print(start)
		return f"https://www.seismicportal.eu/fdsnws/event/1/query?limit={limit}&start={start}&format=json"


	
	async def fetch_rows(self, row, _type="to"):
		# row is info about this dataset
		# it is what was returned with extractor.get_list_row method
		# it holds pagination, errors, retry count, next update time etc.
		try:
			url = self.make_url(row, _type)
			
			res = await self.http_get(url)  # wrapper around aiohttp session's get

			if res.status_code == 200:
				rows = []
				content = res.json()
				
				
				
				for event in content["features"]:
					rows.append({
						"uri": f'https://www.seismicportal.eu/fdsnws/event/1/#{sha1(event["id"])}',  # noqa:E501
						"event":event
						
					})
				
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
							
						}
					
					}
				}
			else:
				self.logger.error("Non-200 HTTP response: %s : %s" % (res.status_code, url))
				return self.make_error("HTTP", res.status_code, url)
		except Exception as e:
			self.logger.exception(e)
			return self.make_error("Exception", type(e), str(e))
