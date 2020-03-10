import csv
from collections import defaultdict


def location_key(row):
    return f"{row.get('addr')}{row.get('post_code')}"

def poll_loc_key(row):
    return f"{row.get('ed_num')}-{row.get('pd_num')}-{row.get('pd_sufx')}"

infile = "GEOCODED_polling_locations_42nd_GE_19-10-2015_sean_format.csv"
outfile = "GEOCODED_polling_locations_42nd_GE_19-10-2015_sean_format_different_pollocs.csv"
lost_outfile = "lost_data.csv"


with open (infile, encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    data = [row for row in reader if row.get('province') == "ON"]


sorted_by_pd_key = defaultdict(list)
for row in data:
    sorted_by_pd_key[poll_loc_key(row)].append(row)

new_data = []
lost_data = []

for key, rows in sorted_by_pd_key.items():
    if len(rows) == 1:
        new_data.append(rows[0])
    else:
        if len({location_key(row) for row in rows}) == 1:
            letters = set()
            for row in rows:
                letters.update(row.get("pd_ltr"))
            row = rows[0]
            row['pd_ltr'] = "".join(letters)
            new_data.append(rows[0])
        else:
            for row in rows:
                lost_data.append(row)

headers = data[0].keys()
with open(outfile, "w", newline="") as  csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=headers)
    writer.writeheader()
    writer.writerows(new_data)

with open(lost_outfile, "w", newline="") as  csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=headers)
    writer.writeheader()
    writer.writerows(lost_data)