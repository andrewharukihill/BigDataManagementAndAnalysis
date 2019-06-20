import sys
from pyspark import SparkContext
import geopandas as gpd
import fiona.crs

def createIndex(tracts):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(tracts).to_crs(fiona.crs.from_epsg(5070))
    zones = zones.loc[(zones['plctrpop10'] > 0) & (zones.geometry.is_valid)].reset_index()
    index = rtree.Rtree()
    for idx, geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return (zones.plctract10[idx], zones.plctrpop10[idx])
    return None

def processTweets(records):
    import re
    import pyproj
    import rtree
    import csv
    import shapely.geometry as geom
    proj = pyproj.Proj(init='epsg:5070', preserve_units=True)
    index, zones = createIndex('500cities_tracts.geojson')


    #Get drug terms in a set
    drugs = []
    with open('drug_illegal.txt', 'r') as fi:
        for line in fi:
            if(len(line.strip().split('\n')[0]) > 3):
                drugs.append(line.strip().split('\n')[0])
    with open('drug_sched2.txt', 'r') as fi:
        for line in fi:
            if(len(line.strip().split('\n')[0]) > 3):
                drugs.append(line.strip().split('\n')[0])
    counts = {}
    for record in csv.reader(records, delimiter="|"):
        latitude, longitude, body = float(record[1]), float(record[2]), record[6].strip()
        for word in drugs:

            if word in body:
                point = geom.Point(proj(longitude, latitude))
                try:
                    tract, pop = findZone(point, index, zones)
                except:
                    continue
                if tract:
                    if  pop > 0:
                        counts[tract] = counts.get(tract, 0) + (1.0 / pop)
    return counts.items()


if __name__=='__main__':
    sc = SparkContext()
    tweets = sc.textFile(sys.argv[-1]).cache()
    output = tweets.mapPartitions(processTweets)\
                        .reduceByKey(lambda x,y: x+y)\
                        .sortBy(lambda x: x[0]) \
                        .saveAsTextFile('output')
