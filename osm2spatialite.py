# imports OSM data into a SqLite database, with more elements than the default sqlite_osm_*
# uses a style file like osm2pgsql
# options (to implement) : stylefile, add other tags, srid, keep raw tables, add geometry indices

import os, sys
import pyspatialite.dbapi2 as db
from imposm.parser import OSMParser
import argparse
import json
import pdb

class Operations:
    def __init__(self, options):
        self.options = options
        self.connection = db.connect(options.dbname)
        self.connection.row_factory = db.Row
        cur = self.connection.cursor()
        cur.execute("SELECT InitSpatialMetadata(1)")
        cur.execute("CREATE TABLE %s_nodes (id INTEGER PRIMARY KEY, tags TEXT)" % options.prefix)
        cur.execute("CREATE TABLE %s_ways (id INTEGER PRIMARY KEY, tags TEXT)" % options.prefix)
        cur.execute("CREATE TABLE %s_coords (id INTEGER PRIMARY KEY, lat REAL, lng REAL)" % options.prefix)
        cur.execute("CREATE TABLE %s_ways_coords (id_way INTEGER, id_node INTEGER)" % options.prefix)
        cur.execute("CREATE TABLE %s_relations (id INTEGER PRIMARY KEY, tags TEXT)" % options.prefix)
        cur.execute("CREATE TABLE %s_relations_refs (id_relation INTEGER, id_elt INTEGER, type_elt TEXT, role TEXT)" % options.prefix)
        self.connection.commit()
        
        self.style = DBStyle(options.style)
        self.tableFields = []
        
        
    def ways(self, ways):
        cur = self.connection.cursor()
        for id, tags, refs in ways:
            cur.execute("INSERT INTO %s_ways VALUES (?, ?)" % self.options.prefix, (id, json.dumps(tags, ensure_ascii=False)))
            way_coords = []
            for n in refs:
                way_coords.append((id, n))
            cur.executemany("INSERT INTO %s_ways_coords VALUES (?, ?)"  % self.options.prefix, way_coords)
            
        self.connection.commit()
        
    def nodes(self, nodes):
        cur = self.connection.cursor()
        # pdb.set_trace()
        for id, tags, coords in nodes:
            cur.execute("INSERT INTO %s_nodes VALUES (?, ?)"  % self.options.prefix, 
                (id, json.dumps(tags, ensure_ascii=False)))
        self.connection.commit()
        
    def relations(self, rels):
        cur = self.connection.cursor()
        for id, tags, refs in rels:
            cur.execute("INSERT INTO %s_relations VALUES (?, ?)" % self.options.prefix, 
                (id, json.dumps(tags, ensure_ascii=False)))
            rel_elts = []
            for n in refs:
                rel_elts.append((id,) + n)
            cur.executemany("INSERT INTO %s_relations_refs VALUES (?, ?, ?, ?)" % self.options.prefix, rel_elts)
        self.connection.commit()
        
    def coords(self, coords):
        cur = self.connection.cursor()
        for id, x, y in coords:
            cur.execute("INSERT INTO %s_coords VALUES (?, ?, ?)" % self.options.prefix, (id, y, x))
        self.connection.commit()
        
    def osm2tables(self):
        """Creates true geom tables and populates them"""
        cur = self.connection.cursor()
        tagFields = ''
        if self.options.json:
            tagField = ', tags TEXT'
        cur.execute("CREATE TABLE {}_line (osm_id INTEGER {})".format(self.options.prefix, tagField))
        cur.execute("SELECT AddGeometryColumn('%s_line', 'way', 4326, 'LINESTRING')" % self.options.prefix)
        cur.execute("CREATE TABLE {}_polygon (osm_id INTEGER {})".format(self.options.prefix, tagField))
        cur.execute("SELECT AddGeometryColumn('%s_polygon', 'way', 4326, 'MULTIPOLYGON')" % self.options.prefix)
        cur.execute("CREATE TABLE {}_point (osm_id INTEGER {})".format(self.options.prefix, tagField))
        cur.execute("SELECT AddGeometryColumn('%s_point', 'way', 4326, 'POINT')" % self.options.prefix)
        self.connection.commit()
        self.createTagColumns()
        
        # points
        req = """SELECT nodes.id, tags, asBinary(makepoint(lng, lat)) geom 
            FROM {0}_nodes nodes JOIN {0}_coords coords ON nodes.id=coords.id""".format(self.options.prefix)
        self.insert_geoms(req, 'point')
        # lines, polygons
        req = """SELECT id, tags, asBinary(way) geom, asBinary(CastToMulti(BuildArea(way))) geom2, isClosed(way) isClosed
        FROM (SELECT ways.id, tags, makeline(makepoint(lng, lat)) way
            FROM {0}_ways ways JOIN {0}_ways_coords ON id_way=ways.id
            JOIN {0}_coords coords ON id_node=coords.id
            GROUP BY id_way)""".format(self.options.prefix)
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
            
            nbtags = len(fields) - 2
            if self.options.json:
                vals.append(json.dumps(tags2))
                fields.append('tags')

            req = """INSERT INTO %s (%s)
                               VALUES (%s) """ % (table, ','.join(fields),
               ','.join(['?', 'setSRID(geomFromWKB(?), 4326)'] + (['?'] * nbfields)))
            cur_insert.execute(req, vals)
        self.connection.commit()
        
        
    def createTagColumns(self):
        if not self.style: 
            return
        cur = self.connection.cursor()
        for (tag, s) in self.style:
            if self.style.is_field(tag):
                self.tableFields.append(tag)
                for table in ('_line', '_point', '_polygon'):
                    cur.execute('ALTER TABLE %s%s ADD COLUMN "%s" %s' % (self.options.prefix, table, tag, s['dataType']))
        self.connection.commit()

    def deleteTempTables(self):
        cur = self.connection.cursor()
        for table in ('coords', 'ways', 'nodes', 'ways_coords', 'relations', 'relations_refs'):
            cur.execute('DROP TABLE {}_{}'.format(self.options.prefix, table))
        self.connection.commit()

    def createIndex(self):
        print "Creating spatial indices..."
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
            f = open(styleFile, 'r')
            for li in f:
                li = li.split('#')[0].strip()
                if li:
                    try:
                        osmTypes, tag, dataType, flag = li.split()
                    except:
                        print "Error parsing %s" % li
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
            if tagname in self.tags and self.tags[tagname]['flag'] == 'polygon':
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
options = argParser.parse_args()
        
                           
if os.path.exists(options.dbname):
    os.remove(options.dbname)
print "Creating DB..."
op = Operations(options)
p = OSMParser(concurrency=2, ways_callback=op.ways, nodes_callback=op.nodes, 
          relations_callback=op.relations, coords_callback=op.coords)
print "Parsing data..."
p.parse('data.osm')
print "Creating geometries..."
op.osm2tables()

## TODO
## - warning before deleting existing db (with option for that)
## - custom SRID
## -implement relations (multipolygons...)