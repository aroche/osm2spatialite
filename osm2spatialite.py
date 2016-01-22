## The MIT License (MIT)

## Copyright (c) 2016 Augustin Roche

## Permission is hereby granted, free of charge, to any person obtaining a copy
## of this software and associated documentation files (the "Software"), to deal
## in the Software without restriction, including without limitation the rights
## to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
## copies of the Software, and to permit persons to whom the Software is
## furnished to do so, subject to the following conditions:

## The above copyright notice and this permission notice shall be included in
## all copies or substantial portions of the Software.

## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
## IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
## FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
## AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
## LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
## OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
## THE SOFTWARE.



# imports OSM data into a SqLite database, with more elements than the default sqlite_osm_*
# uses a style file like osm2pgsql


import os, sys
import pyspatialite.dbapi2 as db
import argparse
import json
try:
    from imposm.parser import OSMParser
except:
    from OSMParser import OSMParser
    

class Operations:
    def __init__(self, options):
        self.options = options
        self.connection = db.connect(options.dbname)
        self.connection.row_factory = db.Row
        cur = self.connection.cursor()
        cur.execute("SELECT InitSpatialMetadata(1)")
        cur.execute("CREATE TABLE \"%s_nodes\" (id INTEGER PRIMARY KEY, tags TEXT)" % options.prefix)
        cur.execute("CREATE TABLE \"%s_ways\" (id INTEGER PRIMARY KEY, tags TEXT)" % options.prefix)
        cur.execute("CREATE TABLE \"%s_coords\" (id INTEGER PRIMARY KEY, lat REAL, lng REAL)" % options.prefix)
        cur.execute("CREATE TABLE \"%s_ways_coords\" (id_way INTEGER, id_node INTEGER)" % options.prefix)
        cur.execute("CREATE TABLE \"%s_relations\" (id INTEGER PRIMARY KEY, type TEXT, tags TEXT)" % options.prefix)
        cur.execute("CREATE TABLE \"%s_relations_refs\" (id_relation INTEGER, id_elt INTEGER, type_elt TEXT, role TEXT)" % options.prefix)
        self.connection.commit()
        
        self.style = DBStyle(options.style)
        
    def ways(self, ways):
        cur = self.connection.cursor()
        for id, tags, refs in ways:
            cur.execute("INSERT INTO \"%s_ways\" VALUES (?, ?)" % self.options.prefix, (id, json.dumps(tags, ensure_ascii=False)))
            way_coords = [(id, n) for n in refs]
            cur.executemany("INSERT INTO \"%s_ways_coords\" VALUES (?, ?)"  % self.options.prefix, way_coords)
            
        self.connection.commit()
        
    def nodes(self, nodes):
        cur = self.connection.cursor()
        for id, tags, coords in nodes:
            cur.execute("INSERT INTO \"%s_nodes\" VALUES (?, ?)"  % self.options.prefix, 
                (id, json.dumps(tags, ensure_ascii=False)))
        self.connection.commit()
        
    def relations(self, rels):
        cur = self.connection.cursor()
        for id, tags, refs in rels:
            cur.execute("INSERT INTO \"%s_relations\" VALUES (?, ?, ?)" % self.options.prefix, 
                (id, tags.get('type'), json.dumps(tags, ensure_ascii=False)))
            rel_elts = []
            for n in refs:
                rel_elts.append((id,) + n)
            cur.executemany("INSERT INTO \"%s_relations_refs\" VALUES (?, ?, ?, ?)" % self.options.prefix, rel_elts)
        self.connection.commit()
        
    def coords(self, coords):
        cur = self.connection.cursor()
        for id, x, y in coords:
            cur.execute("INSERT INTO \"%s_coords\" VALUES (?, ?, ?)" % self.options.prefix, (id, y, x))
        self.connection.commit()
        
    def osm2tables(self):
        """Creates true geom tables and populates them"""
        cur = self.connection.cursor()
        tagField = ''
        if self.options.json:
            tagField = ', tags TEXT'
        cur.execute("CREATE TABLE \"{}_line\" (osm_id INTEGER {})".format(self.options.prefix, tagField))
        cur.execute("SELECT AddGeometryColumn('%s_line', 'way', 4326, 'LINESTRING', 'XY')" % self.options.prefix)
        cur.execute("CREATE TABLE \"{}_polygon\" (osm_id INTEGER {})".format(self.options.prefix, tagField))
        cur.execute("SELECT AddGeometryColumn('%s_polygon', 'way', 4326, 'MULTIPOLYGON', 'XY')" % self.options.prefix)
        cur.execute("CREATE TABLE \"{}_point\" (osm_id INTEGER {})".format(self.options.prefix, tagField))
        cur.execute("SELECT AddGeometryColumn('%s_point', 'way', 4326, 'POINT', 'XY')" % self.options.prefix)
        self.connection.commit()
        self.createTagColumns()
        
        # points
        print("Points...")
        req = """SELECT nodes.id, tags, asBinary(makepoint(lng, lat)) geom 
            FROM \"{0}_nodes\" nodes JOIN {0}_coords coords ON nodes.id=coords.id""".format(self.options.prefix)
        self.insert_geoms(req, 'point')
        # lines, polygons
        print("Ways...")
        req = """SELECT id, tags, asBinary(way) geom, asBinary(CastToMulti(BuildArea(way))) geom2, isClosed(way) isClosed
        FROM (SELECT ways.id, tags, makeline(makepoint(lng, lat)) way
            FROM \"{0}_ways\" ways JOIN \"{0}_ways_coords\" ON id_way=ways.id
            JOIN \"{0}_coords\" coords ON id_node=coords.id
            GROUP BY ways.id) WHERE st_isValid(way)""".format(self.options.prefix)
        self.insert_geoms(req, 'way')
        
        if not self.options.keepRaw:
            self.deleteTempTables()
        
    def insert_geoms(self, query, geomType):
        cur = self.connection.cursor()
        cur.execute(query)
        cur_insert = self.connection.cursor()
        table = self.options.prefix + '_point'
        for li in cur:
            tags = json.loads(li['tags'])
            fields = ['osm_id', 'way']
            if geomType == 'way':
                if self.style.is_polygon(tags.keys()) and tags.get('area') != 'no':
                    geom = li['geom2']
                    table = self.options.prefix + '_polygon'
                else:
                    geom = li['geom']
                    table = self.options.prefix + '_line'
            else:
                geom = li['geom']   
            
            vals = [li['id'], geom]
            if self.options.json:
                tags2 = tags.copy()
            for (tag, val) in tags.iteritems():
                if self.style.is_field(tag):
                    fields.append('"' + tag + '"')
                    vals.append(val)
                elif self.options.json and self.style.has_tag(tag) and self.style.get(tag)['flag'] == 'delete':
                    del tags2[tag]
                    
            # if no tags left, object is not created
            if len(fields) <= 2 and not self.options.keepAll:
                continue
            
            if self.options.json:
                tags = tags2
                vals.append(json.dumps(tags, ensure_ascii=False))
                fields.append('tags')
                
            nbtags = len(fields) - 2

            req = """INSERT INTO "{}" ({})
                               VALUES ({}) """.format(table, ','.join(fields),
               ','.join(['?', 'setSRID(geomFromWKB(?), 4326)'] + (['?'] * nbtags)))
            cur_insert.execute(req, vals)
        self.connection.commit()
        
        
    def createTagColumns(self):
        if not self.style: 
            return
        cur = self.connection.cursor()
        for (tag, s) in self.style:
            if self.style.is_field(tag):
                for table in ('_line', '_point', '_polygon'):
                    cur.execute('ALTER TABLE "{}{}" ADD COLUMN "{}" {}'.format(self.options.prefix, table, tag, s['dataType']))
        self.connection.commit()

    def deleteTempTables(self):
        cur = self.connection.cursor()
        for table in ('coords', 'ways', 'nodes', 'ways_coords', 'relations', 'relations_refs'):
            cur.execute('DROP TABLE {}_{}'.format(self.options.prefix, table))
        self.connection.commit()

    def createIndex(self):
        print("Creating spatial indexes...")
        cur = self.connection.cursor()
        for table in ('polygon', 'point', 'line'):
            cur.execute("SELECT CreateSpatialIndex('{}_{}', 'way')".format(self.options.prefix, table))
        self.connection.commit()
                                
            
        
        
class DBStyle:
    """Extracts styles"""
    def __init__(self, styleFile):
        self.styleFile=styleFile
        self.tags = {}
        if styleFile:
            with open(styleFile, 'r') as f:
                for li in f:
                    li = li.split('#', 1)[0].strip()
                    if li:
                        try:
                            elts = li.split()
                            if len(elts) == 3:
                                elts.append(None)
                            osmTypes, tag, dataType, flag = elts
                        except:
                            print("Error parsing %s" % li)
                            continue
                        self.tags[tag] = {
                                        'osmTypes': osmTypes.split(','), 
                                        'dataType': dataType,
                                        'flag': flag
                                        }
              
    def __iter__(self):
        for (k, tag) in self.tags.iteritems():
            yield (k, tag)
            
    def get(self, tagname):
        return self.tags.get(tagname)
        
    def has_tag(self, tagname):
        return (tagname in self.tags)
    
    def is_field(self, tagname):
        return ((tagname in self.tags) and (self.tags[tagname]['flag'] not in ('phstore', 'delete')))
    
    def is_polygon(self, tags):
        """Checks if any of the given tags if flagged as polygon"""
        for tagname in tags:
            if tagname in self.tags and self.tags[tagname]['flag'] in ('polygon', 'phstore'):
                return True


argParser = argparse.ArgumentParser(description="A tool for converting OpenStreetMap data files into a spatialite database. OSM XML and PBF formats accepted.")
argParser.add_argument("inputFile", help="Input .osm or .pbf file")
argParser.add_argument("dbname", help="The SQLite file name used for output data")
argParser.add_argument("-p", "--prefix", help="prepend this prefix to the tables (default: 'osm')",
                       default='osm')
argParser.add_argument("-j", "--json", help="Stores all tags in a column as JSON string",
                       action='store_true')
argParser.add_argument("-k", "--keep-raw", help="Keep intermediary tables, matching, matching OSM structure",
                       dest='keepRaw', action='store_true')
argParser.add_argument("-i", "--index", help="Create spatial index for geometry columns",
                       action='store_true')
argParser.add_argument("-s", "--style", help="use specified style file", default='default.style')
argParser.add_argument("-a", "--keep-all", dest='keepAll', help="Do not filter out objects that have not tags in style file",
                       action='store_true')
options = argParser.parse_args()
        
                           
if os.path.exists(options.dbname):
    os.remove(options.dbname)
print("Creating DB...")
op = Operations(options)
p = OSMParser(concurrency=4, ways_callback=op.ways, nodes_callback=op.nodes, 
          relations_callback=op.relations, coords_callback=op.coords)
print("Parsing data...")
p.parse(options.inputFile)
print("Creating geometries...")
op.osm2tables()

## TODO
## - warning before deleting existing db (with option for that)
## - custom SRID
## -implement relations (multipolygons...)
## - possible multie flags