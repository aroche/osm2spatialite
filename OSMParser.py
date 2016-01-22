# OSM XML parser that acts in the same way as imposm.parser
# TODO: implement tag filters

import xml.etree.ElementTree as etree
import pdb


class OSMParser:
    pack_size = 100 # number of elements to be processed at the same time
    
    def __init__(self, concurrency=2, ways_callback=None, nodes_callback=None, 
          relations_callback=None, coords_callback=None):
        self.ways_callback=ways_callback
        self.nodes_callback = nodes_callback
        self.relations_callback = relations_callback
        self.coords_callback = coords_callback
        
    def parse(self, source):
        with open(source, 'r') as xml:
            nodes = []
            coords = []
            ways = []
            rels = []
            for evt, elt in etree.iterparse(xml, events=('end',)):
                if elt.tag == 'node':
                    tags = self.extract_tags(elt)
                    if len(tags) > 0:
                        nodes.append((elt.get('id'), tags, (elt.get('lon'), elt.get('lat'))))
                    else:
                        coords.append((elt.get('id'), elt.get('lon'), elt.get('lat')))
                if elt.tag == 'way':
                    refs = [nd.get('ref') for nd in elt.findall('nd')]
                    tags = self.extract_tags(elt)
                    ways.append((elt.get('id'), tags, refs))
                if elt.tag == 'relation':
                    refs = []
                    for member in elt.findall('member'):
                        refs.append((member.get('ref'), member.get('type'), member.get('role')))
                    tags = self.extract_tags(elt)
                    rels.append((elt.get('id'), tags, refs))
                    
                if self.nodes_callback and len(nodes) >= self.pack_size:
                    self.nodes_callback(nodes)
                    nodes = []
                if self.coords_callback and len(coords) >= self.pack_size:
                    self.coords_callback(coords)
                    coords = []
                if self.ways_callback and len(ways) >= self.pack_size:
                    self.ways_callback(ways)
                    ways = []
                if self.relations_callback and len(rels) >= self.pack_size:
                    self.relations_callback(rels)
                    rels = []
                if elt.tag in ('node', 'relation', 'way'):
                    elt.clear()
                    
            if self.nodes_callback:
                self.nodes_callback(nodes)
            if self.coords_callback:
                self.coords_callback(coords)
            if self.ways_callback:
                self.ways_callback(ways)
            if self.relations_callback:
                self.relations_callback(rels)
            if elt.tag in ('node', 'relation', 'way'):
                    elt.clear()                     
                    
    def extract_tags(self, elt):
        tags = {}
        for tag in elt.findall('tag'):
            k = tag.get('k')
            v = tag.get('v')
            tags[k] = v
        return tags
                    
