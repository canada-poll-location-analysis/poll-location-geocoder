import json

import geocoder
import pandas as pd
from pyproj import Proj, transform
from tqdm import tqdm


def geocode_address(infile, outfile):
    """
    This program takes the :param infile: and finds the ArcGIS geocoded address.  It writes the result
    to :param outfile: and to results.json.  if the address exists in results.josn, a call to ArcGIS isn't made
    """
    print(f"Geocoding Polling Location addresses")
    addrs = pd.read_csv(infile)
    print(f"Read Addresses {infile} ({len(addrs)})")
    try:
        found = result_file()
    except FileNotFoundError:
        found = dict()
    else:
        if found is None:
            found = dict()
    pbar = tqdm(desc="Geocoding Addresses", total=len(addrs), unit=' addrs')
    for idx, address in addrs.iterrows():
        if address['province'].lower() not in {"on"}:
            pbar.update()
            continue
        address_1 = f"{address['addr']} {address['municipality']}, {address['province']} {address['post_code']} CANADA"
        pbar.set_postfix_str(address_1[:40])
        if address_1 in found:
            result = found[address_1]
        else:
            geocoder_object = geocoder.arcgis(address_1)
            result = geocoder_object.json
            found[address_1] = result
            result_file(found)
        try:
            addrs.at[idx, 'lat'], addrs.at[idx, 'lon'], addrs.at[idx, 'confidence'], addrs.at[idx, 'quality'], addrs.at[
                idx, 'score'] = \
                result['lat'], result['lng'], result['confidence'], result['quality'], result['score']
            easting, northing = to_lambert(result['lat'], result['lng'])
            addrs.at[idx, 'easting'], addrs.at[idx, 'northing'] = easting, northing
        except TypeError:
            print(f"Address: \n{address_1}\n\twas not found")
        pbar.update()
    addrs.to_csv(outfile)
    print("Fin")


def to_lambert(lat, lon=None):
    inProj = Proj('EPSG:4326')
    outProj = Proj('EPSG:3347')
    if lon is None:
        lat, lon = lat
    new_pt = transform(inProj, outProj, lon, lat)
    return new_pt


def to_wgs(lambert_x, lambert_y=None):
    outProj = Proj(init='EPSG:4326')
    inProj = Proj(init='EPSG:3347')
    if lambert_y is None:
        lambert_x, lambert_y = lambert_x
    new_pt = transform(inProj, outProj, lambert_x, lambert_y)
    return new_pt


def result_file(data=None, result_filename=None):
    if result_filename is None:
        result_filename = "results.json"
    if data is None:
        with open(result_filename) as jsonfile:
            results = json.load(jsonfile)
            return results
    else:
        with open(result_filename, "w", encoding="UTF-8") as jsonfile:
            json.dump(data, jsonfile, indent=4)


if __name__ == '__main__':
    geocode_address("polling_locations_42nd_GE_19-10-2015_sean_format.csv",
                    "GEOCODED_polling_locations_42nd_GE_19-10-2015_sean_format.csv")
