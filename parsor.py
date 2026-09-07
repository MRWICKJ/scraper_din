import json
import re
from bs4 import BeautifulSoup

def parse_zauba_html(html, cin=None):
    soup = BeautifulSoup(html, "html.parser")

    master_data = {"cin": cin}
    title_elem = soup.find("h1", id="title")
    master_data["Company Name"] = (
        title_elem.text.strip() if title_elem else None
    )

    basic_info_sec = soup.find("div", id="company-information")
    if basic_info_sec:
        rows = basic_info_sec.find_all("li", class_="row")
        for row in rows:
            label = row.find("span")
            val = row.find("label")
            if label and val:
                master_data[label.text.strip()] = val.text.strip()

    directors = []
    json_ld = soup.find("script", type="application/ld+json")
    if json_ld:
        try:
            data = json.loads(json_ld.string)
            master_data["cin"] = master_data["cin"] or data.get("identifier", {}).get("value")
            master_data["Registered Address"] = data.get("address")
            master_data["Email"] = data.get("email")

            for person in data.get("alumni", []):
                directors.append({
                    "cin": master_data["cin"],
                    "din": person.get("identifier"),
                    "name": person.get("name"),
                    "designation": person.get("jobTitle"),
                })
        except Exception:
            pass

    charges = []
    charges_sec = soup.find("div", id="charges") or soup.find("div", id="charges-details")

    if not charges_sec:
        for table in soup.find_all("table"):
            if "charge id" in table.text.lower():
                charges_sec = table
                break

    if charges_sec:
        table = charges_sec if charges_sec.name == "table" else charges_sec.find("table")
        if table:
            rows = table.find_all("tr")
            if len(rows) > 1:
                raw_headers = [th.text.strip().lower() for th in rows[0].find_all(["th", "td"])]
                for row in rows[1:]:
                    cols = [td.text.strip() for td in row.find_all("td")]
                    if len(cols) == len(raw_headers):
                        row_dict = dict(zip(raw_headers, cols))
                        amount_str = row_dict.get("amount") or "0"
                        try:
                            amount = float(amount_str.replace(",", "").replace("₹", "").strip())
                        except ValueError:
                            amount = 0.0

                        charges.append({
                            "cin": cin or master_data.get("cin"),
                            "charge_id": row_dict.get("charge id") or row_dict.get("id"),
                            "creation_date": row_dict.get("date of creation") or row_dict.get("creation date"),
                            "modification_date": row_dict.get("date of modification") or row_dict.get("modification date"),
                            "closure_date": row_dict.get("date of satisfaction") or row_dict.get("closure date"),
                            "amount": amount,
                            "charge_holder": row_dict.get("holder name") or row_dict.get("charge holder") or row_dict.get("bank"),
                            "status": row_dict.get("status") or ("CLOSED" if row_dict.get("date of satisfaction") else "OPEN"),
                        })

    return master_data, directors, charges

