'''Python script for extracting metadata fields from sentinel manifests or from opensearch API
Sentinel 1
Sentinel 2
Assumes there is a location where several manifests are stored
February 2016'''
import lxml
from lxml import etree
import os
import re
from xml.dom import minidom
import requests
from requests.auth import HTTPBasicAuth
import time
from datetime import date, timedelta
import sys

#TODO add logging instead of printing
#import logging
#logging.basicConfig(level=logging.DEBUG)
#https://docs.python.org/2/howto/logging.html

DHUS_USER ='guest'
DHUS_PASS ='guest'
FILEPATH = "/tmp/harvested/manifests/"
class SentinelMetadataExtractor:
  filepath = ''
  tree = ""
  root = ""
  file_error_count=0
  filenames_error=[]
  total_files=0
  productMetadata={}
  productMetadataEtrees={}

  
  def __init__(self, filepath=FILEPATH):
    self.filepath = filepath
    
  def _transformSolrCoordsToSAFECoords(self, coords):
    '''receives coordinates in solr format and parses to the one in SAFE format for further processing
    FROM #POLYGON ((123.3108 8.0611,123.0589 9.3117,120.8469 9.0181,121.1061 7.7648,123.3108 8.0611,123.3108 8.0611))
    TO 8.0611,123.3108 9.3117,123.0589 9.0181,120.8469 7.7648,121.1061 8.0611,123.3108
    '''
    
    #remove (( and )), split by commas
    coords_split = str(re.findall("POLYGON\s\D\D(.*)\D\D", coords)).split(',')
    
    #add commas in whitespaces, and a whitespace to separate each pair, result is something like this 8.0611,123.3108 9.3117,123.0589 9.0181,120.8469 7.7648,121.1061 8.0611,123.3108
    coords_united = ''
    for c in range(0,len(coords_split)):
      replaced = coords_split[c].replace(" ", ",")
      
      coords_united = coords_united + replaced
      if c != (len(coords_split)-1):
        coords_united = coords_united +" "

    return coords_united
  
  
  def _downloadProduct(self,filename,link,outputFolder,user=DHUS_USER, password=DHUS_PASS):
    '''download product and shows a progress bar'''
    print 'Download product from ' + link

    with open(filename, "wb") as f:
      print "Downloading %s" % filename
      response = requests.get(link, stream=True, auth=HTTPBasicAuth(user, password))
      total_length = response.headers.get('content-length')

      if total_length is None: # no content length header
          f.write(response.content)
      else:
          dl = 0
          total_length = int(total_length)
          for data in response.iter_content():
              dl += len(data)
              f.write(data)
              done = int(50 * dl / total_length)
              sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50-done)) )    
              sys.stdout.flush()
    print 'Finished Download'
  
  def extractMetadataFromAPIForToday(self,downloadManifests = False,user=DHUS_USER, password=DHUS_PASS, outputFolder=FILEPATH):
    '''queries the dhus api and uses its metadata,
    can download manifests and stores the manifests at specified location (filepath on init)'''
    #odata - url = "https://scihub.copernicus.eu/apihub/odata/v1/Products?$filter=year(IngestionDate) eq 2016 and month(IngestionDate) eq 02 and day(IngestionDate) eq 1&$skip=0&$top=99"
 
    if downloadManifests: #if folder doesnt exist and we want to write to file, exit
      if not self._folderExists(outputFolder):
        return None 
 
    today = time.strftime('%Y-%m-%d')
    yesterday_d = date.today() - timedelta(1)
    yesterday = yesterday_d.strftime('%Y-%m-%d')
    NOW = str(today)+'T00:00:00.000Z'#Thh:mm:ss.SSSZ
    YESTERDAY = str(yesterday)+'T00:00:00.000Z'#Thh:mm:ss.SSSZ
    metadata = {}
    
   
    ##first request to get total results
    url = "https://scihub.copernicus.eu/dhus/search?q=ingestionDate:["+YESTERDAY+" TO "+NOW+"]"

    r = requests.get(url, auth=HTTPBasicAuth(user, password))
    if r.status_code != 200:
      print 'Wrong authentication? status code != 200'
      return None
    
    #print r.text
    xml = minidom.parseString(r.text)
    total_results = xml.getElementsByTagName("opensearch:totalResults")[0].firstChild.data
    
    print 'found ' +total_results +' results'

    for i in range(0, int(total_results), 100):
      url = "https://scihub.copernicus.eu/dhus/search?q=ingestionDate:["+YESTERDAY+" TO "+NOW+"]&start="+str(i)+"&rows=99"

      r = requests.get(url, auth=HTTPBasicAuth(user, password))
      xml = minidom.parseString(r.text)

      products=xml.getElementsByTagName("entry")

      for prod in products:
          ident=prod.getElementsByTagName("id")[0].firstChild.data
          #print ident
          link=prod.getElementsByTagName("link")[0].attributes.items()[0][1]
          #print link
          title=prod.getElementsByTagName("title")[0].firstChild.data
          #print title
          
          metadata['uuid'] = ident
          metadata['downloadLink'] = link
          metadata['thumbnail'] = "https://"+user+":"+password+"@scihub.copernicus.eu/apihub/odata/v1/Products('"+ident+"')/Products('Quicklook')/$value"
          
          if downloadManifests:
            self._downloadProduct(title.lower()+'.SAFE.zip',link,outputFolder,user,password)
          
          for node in prod.getElementsByTagName("str"):
              (name,value)=node.attributes.items()[0]
              #metadata[value]=''
              
              #print name,value
              if value=="filename":
                  #filename= str(node.toxml()).split('>')[1].split('<')[0]#ugly, but minidom is not straightforward
                  filename = node.firstChild.data
                  metadata['manifestLink'] = "https://scihub.copernicus.eu/apihub/odata/v1/Products('" + ident +"')/Nodes('"+filename+"')/Nodes('manifest.safe')/$value"
                  
              if value=="footprint": 
                  #assuming all footprints are polygons
                  footprint = node.firstChild.data
                  metadata['Coordinates'] = self._parseCoordinates(self._transformSolrCoordsToSAFECoords(footprint))
                  
              if value=="beginposition": 
                  metadata['StartTime'] = node.firstChild.data

              if value=="endposition": 
                  metadata['StopTime'] = node.firstChild.data
                
              if value=="platformname": 
                metadata['FamilyName'] = node.firstChild.data

              if value=="instrumentshortname": 
                metadata['InstrumentFamilyName'] = node.firstChild.data
                       
              if value=="instrumentname": 
                metadata['instrumentname'] = node.firstChild.data
                       
              if value=="footprint": 
                metadata['StartTime'] = node.firstChild.data     
                
              if value=="polarisationmode": 
                metadata['TransmitterReceiverPolarisation'] = node.firstChild.data
                
              if value=="sensoroperationalmode": 
                metadata['InstrumentMode'] = node.firstChild.data  
                
              if value=="productclass": 
                metadata['ProductClass'] = node.firstChild.data   
                
              if value=="producttype": 
                metadata['ProductType'] = node.firstChild.data   
                
              if value=="productconsolidation": 
                metadata['ProductConsolidation'] = node.firstChild.data   
                
              if value=="acquisitiontype": 
                metadata['AcquisitionType'] = node.firstChild.data  
                
              if value=="orbitdirection": 
                metadata['OrbitDirection'] = node.firstChild.data     
                
              if value=="swathidentifier": 
                metadata['Swath'] = node.firstChild.data                    
                

          self.productMetadata[title.lower()+'.manifest.safe'] =metadata #.manifest.safe to mantain consistency across class

    print 'finished parsing api results'
    
    
  def extractMetadataFromManifestFiles(self):
    '''main method for metadata extarction'''
    for filename in os.listdir(self.filepath):
      self.total_files = self.total_files +1
      
      if filename.endswith(".safe") or filename.endswith(".SAFE"): 
          #print "processing file - "+str(filename)
          
          try: 
            self.tree = etree.parse(self.filepath+"/"+str(filename))
            self.root = self.tree.getroot()
          except lxml.etree.XMLSyntaxError:
            print 'File XML Syntax Error'
            self.file_error_count = self.file_error_count+1
            self.filenames_error.append(str(filename))
            continue
          
          #####all this names represent files succesfully parsed with this program
          raw = ['S1A_S3_RAW__0SDH','S1A_IW_RAW__0SSV','S1A_IW_RAW__0SSH','S1A_IW_RAW__0SDV','S1A_IW_RAW__0SDH','S1A_EW_RAW__0SDH','S1A_EW_RAW__0SSH',
                 'S1A_EW_RAW__0SDV','S1A_S1_RAW__0SSV','S1A_S6_RAW__0SSV','S1A_S5_RAW__0SSV','S1A_S3_RAW__0SDV','S1A_S1_RAW__0SDH','S1A_S4_RAW__0SSV',
                 'S1A_S3_RAW__0SSV']
          
          gr = ['S1A_S3_GRDH_1SDH','S1A_IW_SLC__1SSV','S1A_IW__1SSH','S1A_IW_SLC__1SDV','S1A_IW_SLC__1SDH','S1A_IW_GRDH_1SSV','S1A_IW_GRDH_1SSH',
                'S1A_IW_GRDH_1SDV','S1A_IW_GRDH_1SDH','S1A_EW_GRDM_1SSH','S1A_EW_GRDM_1SDV','S1A_EW_GRDH_1SDH','S1A_EW_GRDM_1SDH',
                'S1A_IW_SLC__1SSH','S1A_S5_GRDH_1SSV','S1A_EW_GRDH_1SSH','S1A_S5_SLC__1SSV','S1A_IW_GRDH_1SDV','S1A_S4_GRDH_1SSV','S1A_S3_GRDH_1SSV',
                'S1A_S3_GRDH_1SDV','S1A_S4_SLC__1SSV','S1A_S3_SLC__1SSV','S1A_S1_GRDH_1SDH','S1A_S3_SLC__1SDV']
          
          ocn = ['S1A_IW_OCN__2SDV','S1A_WV_OCN__2SSV','S1A_IW_OCN__2SSV','S1A_EW_OCN__2SDH','S1A_IW_OCN__2SDH']
          
          s2 = ['S2A_OPER_PRD_MSIL1C_PDMC']
          
          processed = False
          for sentinel_name in gr:
            if filename.startswith(sentinel_name):
              processed = True
              self.productMetadata[filename.lower()] = self._extractGR()
              break
            
          for sentinel_name in raw:
            if filename.startswith(sentinel_name):
              processed = True
              self.productMetadata[filename.lower()] = self._extractRAW()
              break
            
          for sentinel_name in ocn:  
            if filename.startswith(sentinel_name):
                processed = True
                self.productMetadata[filename.lower()] = self._extractIWOCN()
                break
              
          for sentinel_name in s2:  
            if filename.startswith(sentinel_name):
                processed = True
                self.productMetadata[filename.lower()] = self._extractS2()
                break
          if not processed:
              self.file_error_count = self.file_error_count+1
              self.filenames_error.append(str(filename))
              print "FILE NOT IN KNOWN FILES - "+str(filename)
      else:
        print "File not ending with .SAFE or .safe - "+str(filename)
        
    print "All Done"

  def _parseCoordinates(self,coordinates):
    '''parse coordinates to a json format[[[lat1,long1],[lat2,long2],...]]'''
    final_list = []
    coordsx=[] #lat
    coordsy=[] #long
    
    split = re.split(' |,',coordinates.strip())
    #print split
    
    for c in range(0,len(split)):
      if c % 2 == 0: #pair
        coordsx.append(split[c])
      else:
        coordsy.append(split[c])
    
    for c in range(0,len(coordsx)):
      final_list.append([str(coordsx[c]),str(coordsy[c])])
    
    #print final_list
    return final_list#lat,long
     
    
    
  def _extractS2(self):
    metadata = {}
    ###############S1A_S3_GRDH_1SDH###############S1A_IW_SLC__1SSV###############S1A_IW__1SSH###############S1A_IW_SLC__1SDV
    ###############S1A_IW_SLC__1SDH###############S1A_IW_GRDH_1SSV###############S1A_IW_GRDH_1SSH###############S1A_IW_GRDH_1SDV
    ###############S1A_IW_GRDH_1SDH###############S1A_EW_GRDM_1SSH###############S1A_EW_GRDM_1SDV###############S1A_EW_GRDH_1SDH
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:startTime',self.root.nsmap)
    metadata['StartTime'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:familyName',self.root.nsmap)
    metadata['FamilyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:number',self.root.nsmap)
    metadata['FamilyNameNumber'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:familyName',self.root.nsmap)
    metadata['InstrumentFamilyName'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel/1.1}frameSet/{http://www.esa.int/safe/sentinel/1.1}footPrint/{http://www.opengis.net/gml}coordinates',self.root.nsmap)
    metadata['Coordinates'] = self._parseCoordinates(extracted[0].text)
    #
    return metadata
    
  def _extractGR(self):
    metadata = {}
    ###############S1A_S3_GRDH_1SDH###############S1A_IW_SLC__1SSV###############S1A_IW__1SSH###############S1A_IW_SLC__1SDV
    ###############S1A_IW_SLC__1SDH###############S1A_IW_GRDH_1SSV###############S1A_IW_GRDH_1SSH###############S1A_IW_GRDH_1SDV
    ###############S1A_IW_GRDH_1SDH###############S1A_EW_GRDM_1SSH###############S1A_EW_GRDM_1SDV###############S1A_EW_GRDH_1SDH
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:startTime',self.root.nsmap)
    metadata['StartTime'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:stopTime',self.root.nsmap)
    metadata['StopTime'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:familyName',self.root.nsmap)
    metadata['FamilyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:number',self.root.nsmap)
    metadata['FamilyNameNumber'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:familyName',self.root.nsmap)
    metadata['InstrumentFamilyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:extension/s1sarl1:instrumentMode/s1sarl1:mode',self.root.nsmap)
    metadata['InstrumentMode'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productClass',self.root.nsmap)
    metadata['ProductClass'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productClassDescription',self.root.nsmap)
    metadata['ProductClassDescription'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productComposition',self.root.nsmap)
    metadata['ProductComposition'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productType',self.root.nsmap)
    metadata['ProductType'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:transmitterReceiverPolarisation',self.root.nsmap)
    if len(extracted) > 1:
      metadata['TransmitterReceiverPolarisation'] = extracted[0].text + "/" + extracted[1].text
    else:
      metadata['TransmitterReceiverPolarisation'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:frameSet/safe:frame/safe:footPrint/gml:coordinates',self.root.nsmap)
    metadata['Coordinates'] = self._parseCoordinates(extracted[0].text)
    #
    return metadata
    
  def _extractRAW(self):
    metadata = {}
    ###############S1A_S3_RAW__0SDH###############S1A_IW_RAW__0SSV###############S1A_IW_RAW__0SSH###############S1A_IW_RAW__0SDV
    ###############S1A_IW_RAW__0SDH###############S1A_EW_RAW__0SDH
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}acquisitionPeriod/{http://www.esa.int/safe/sentinel-1.0}startTime',self.root.nsmap)
    metadata['StartTime'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}acquisitionPeriod/{http://www.esa.int/safe/sentinel-1.0}stopTime',self.root.nsmap)
    metadata['StopTime'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}familyName',self.root.nsmap)
    metadata['FamilyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}number',self.root.nsmap)
    metadata['FamilyNameNumber'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}instrument/{http://www.esa.int/safe/sentinel-1.0}familyName',self.root.nsmap)
    metadata['InstrumentFamilyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}instrument/{http://www.esa.int/safe/sentinel-1.0}extension/s1sar:instrumentMode/s1sar:mode',self.root.nsmap)
    metadata['InstrumentMode'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:productClass',self.root.nsmap)
    metadata['ProductClass'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:productClassDescription',self.root.nsmap)
    metadata['ProductClassDescription'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:productConsolidation',self.root.nsmap)
    metadata['ProductConsolidation'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:transmitterReceiverPolarisation',self.root.nsmap)
    if len(extracted) > 1:
      metadata['TransmitterReceiverPolarisation'] = extracted[0].text + "/" + extracted[1].text
    else:
      metadata['TransmitterReceiverPolarisation'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}frameSet/{http://www.esa.int/safe/sentinel-1.0}frame/{http://www.esa.int/safe/sentinel-1.0}footPrint/{http://www.opengis.net/gml}coordinates',self.root.nsmap)
    metadata['Coordinates'] = self._parseCoordinates(extracted[0].text)
    
    return metadata
  
  def _extractIWOCN(self):
    metadata = {}
    ###############S1A_IW_OCN__2SDV
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:startTime',self.root.nsmap)
    metadata['StartTime'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:stopTime',self.root.nsmap)
    metadata['StopTime'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:familyName',self.root.nsmap)
    metadata['FamilyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:number',self.root.nsmap)
    metadata['FamilyNameNumber'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:familyName',self.root.nsmap)
    metadata['InstrumentFamilyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:extension/s1sarl2:instrumentMode/s1sarl2:mode',self.root.nsmap)
    metadata['InstrumentMode'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productClass',self.root.nsmap)
    metadata['ProductClass'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productClassDescription',self.root.nsmap)
    metadata['ProductClassDescription'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productComposition',self.root.nsmap)
    metadata['ProductComposition'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productType',self.root.nsmap)
    metadata['ProductType'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:transmitterReceiverPolarisation',self.root.nsmap)
    if len(extracted) > 1:
      metadata['TransmitterReceiverPolarisation'] = extracted[0].text + "/" + extracted[1].text
    else:
      metadata['TransmitterReceiverPolarisation'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:frameSet/safe:frame/safe:footPrint/gml:coordinates',self.root.nsmap)
    metadata['Coordinates'] = self._parseCoordinates(extracted[0].text)
    
    return metadata

  
  def checkAllFilesParsed(self):
    ''' check if any files processed had errors and outputs their name, along with the number of total files processed'''
    print 'Processed ' + str(self.total_files) + ' files'
    print '\n\n'
    print str(self.file_error_count) + " files had errors"
    print 'Printing filenames'
    
    for j in range(0,len(self.filenames_error)):
      print self.filenames_error[j]
 
 
  def getProductsMetadata(self):
    '''return all ingested products metadata
    the actual metadata, dict in which keys are the filenames in lowercase, values are a dict of keys,values
    '''
    return self.productMetadata

  def getXMLStringMetadataInspire(self, template='inspire_template.xml'):
    '''return all ingested products metadata in etree
    the actual metadata, dict in which keys are the filenames in lowercase, values are an lxml etree
    '''
    for productName in self.productMetadata.keys(): #generate inspires for all metadata harvested
        #write to manifests-inspire, based on the template, a xml inspire metadata document
        inspireetree = self.generateInspireFromTemplate(productName,template,'', False)
        self.productMetadataEtrees[productName] = etree.tostring(inspireetree,encoding='utf-8', pretty_print=True)
        
        
    return self.productMetadataEtrees
  
  def _getBoundingBox(self,coords):
    '''returns a bounding box for a given set of coordinates
    '''
    ##parse coords
    coordsx=[] #lat
    coordsy=[] #long
    
    for c in range(0,len(coords)):
      coordsx.append(coords[c][0])
      coordsy.append(coords[c][1])
    
    xmax = max(coordsx);
    xmin = min(coordsx);
    ymax = max(coordsy);
    ymin = min(coordsy);
    
    #lat max = north bound lat
    #lat min = south bound lat
    #long max = east bound long
    #ong min = west bound long
    return [xmax,xmin,ymax,ymin]
  
  
  def _folderExists(self,outputFolder):
    '''check if path exists and then check if path given is a folder, returns boolean'''
    if not os.path.exists(outputFolder):
      print 'Folder does not exist, creating '+outputFolder +' .'
      try:
        os.makedirs(outputFolder)
        return True
      except:
        print 'Folder creation not succesfull, maybe you do not have permissions'
        return False
    
    try:
      os.path.isdir(outputFolder)
      return True
    except:
      print 'given outputFolder path is not a folder'   
      return False
    return False
  
  
  def generateInspireFromTemplate(self, metadata_key, template='inspire_template.xml', outputFolder = '/tmp/harvested/manifests-inspire/',writeToFile=False):
    '''glued with spit and hammered code to generate inspire xml based on a custom template
    general idea is to replace the values on the template with the ones from our metadata
    check http://inspire-geoportal.ec.europa.eu/editor/
    returns the etree
    outputFolder can be empty if writeToFile is false
    '''
    
    #print metadata_key
    
    if writeToFile: #if folder doesnt exist and we want to write to file, exit
      if not self._folderExists(outputFolder):
        return None
      
      
    if metadata_key in self.productMetadata.keys():
      try: 
        template_tree = etree.parse(template)
        template_root = template_tree.getroot()
        
        current_metadata = self.productMetadata[metadata_key]

        #uuid
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}fileIdentifier/{http://www.isotc211.org/2005/gco}CharacterString',template_root.nsmap)
        find[0].text = current_metadata['uuid'] 
        
        #org name
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}contact/{http://www.isotc211.org/2005/gmd}CI_ResponsibleParty/{http://www.isotc211.org/2005/gmd}organisationName/{http://www.isotc211.org/2005/gco}CharacterString',template_root.nsmap)
        find[0].text = 'ESA' 
        
        #org contact email
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}contact/{http://www.isotc211.org/2005/gmd}CI_ResponsibleParty/{http://www.isotc211.org/2005/gmd}contactInfo/{http://www.isotc211.org/2005/gmd}CI_Contact/{http://www.isotc211.org/2005/gmd}address/{http://www.isotc211.org/2005/gmd}CI_Address/{http://www.isotc211.org/2005/gmd}electronicMailAddress/{http://www.isotc211.org/2005/gco}CharacterString',template_root.nsmap)
        find[0].text = 'esapub@esa.int'
        
        #date created 
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}dateStamp/{http://www.isotc211.org/2005/gco}Date',template_root.nsmap)
        find[0].text = current_metadata['StartTime'][:10]    
        
        #filename
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}citation/{http://www.isotc211.org/2005/gmd}CI_Citation/{http://www.isotc211.org/2005/gmd}title/{http://www.isotc211.org/2005/gco}CharacterString',template_root.nsmap)
        find[0].text = metadata_key.upper()
        
        #date for metadata creation (this xml)
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}citation/{http://www.isotc211.org/2005/gmd}CI_Citation/{http://www.isotc211.org/2005/gmd}date/{http://www.isotc211.org/2005/gmd}CI_Date/{http://www.isotc211.org/2005/gmd}date/{http://www.isotc211.org/2005/gco}Date',template_root.nsmap)
        find[0].text = current_metadata['StartTime'][:10]        

        #uuid
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}citation/{http://www.isotc211.org/2005/gmd}CI_Citation/{http://www.isotc211.org/2005/gmd}identifier/{http://www.isotc211.org/2005/gmd}RS_Identifier/{http://www.isotc211.org/2005/gmd}code/{http://www.isotc211.org/2005/gco}CharacterString',template_root.nsmap)
        find[0].text = current_metadata['uuid']     
        
        #abstract
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}abstract/{http://www.isotc211.org/2005/gco}CharacterString',template_root.nsmap)
        find[0].text = 'ESA Sentinel Product Metadata'  
        
        #responsible org
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}pointOfContact/{http://www.isotc211.org/2005/gmd}CI_ResponsibleParty/{http://www.isotc211.org/2005/gmd}organisationName/{http://www.isotc211.org/2005/gco}CharacterString',template_root.nsmap)
        find[0].text = 'ESA'
        
        #responsible org contact
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}pointOfContact/{http://www.isotc211.org/2005/gmd}CI_ResponsibleParty/{http://www.isotc211.org/2005/gmd}contactInfo/{http://www.isotc211.org/2005/gmd}CI_Contact/{http://www.isotc211.org/2005/gmd}address/{http://www.isotc211.org/2005/gmd}CI_Address/{http://www.isotc211.org/2005/gmd}electronicMailAddress/{http://www.isotc211.org/2005/gco}CharacterString',template_root.nsmap)
        find[0].text = 'esapub@esa.int'
        
        #keywords from INSPIER Data Themes
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}descriptiveKeywords/{http://www.isotc211.org/2005/gmd}MD_Keywords/{http://www.isotc211.org/2005/gmd}keyword/{http://www.isotc211.org/2005/gco}CharacterString',template_root.nsmap)
        find[0].text = 'Orthoimagery'       
        
        #keywors from repositories
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}descriptiveKeywords/{http://www.isotc211.org/2005/gmd}MD_Keywords/{http://www.isotc211.org/2005/gmd}thesaurusName/{http://www.isotc211.org/2005/gmd}CI_Citation/{http://www.isotc211.org/2005/gmd}title/{http://www.isotc211.org/2005/gco}CharacterString',template_root.nsmap)
        find[0].text = 'GEMET - INSPIRE themes, version 1.0'
        
        #licenese conditions
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}resourceConstraints/{http://www.isotc211.org/2005/gmd}MD_Constraints/{http://www.isotc211.org/2005/gmd}useLimitation/{http://www.isotc211.org/2005/gco}CharacterString',template_root.nsmap)
        find[0].text = 'Conditions unknown'        
        
        #topic category code
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}topicCategory/{http://www.isotc211.org/2005/gmd}MD_TopicCategoryCode',template_root.nsmap)
        find[0].text = 'imageryBaseMapsEarthCover'
        
        #start time of data
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}extent/{http://www.isotc211.org/2005/gmd}EX_Extent/{http://www.isotc211.org/2005/gmd}temporalElement/{http://www.isotc211.org/2005/gmd}EX_TemporalExtent/{http://www.isotc211.org/2005/gmd}extent/{http://www.opengis.net/gml}TimePeriod/{http://www.opengis.net/gml}beginPosition',template_root.nsmap)
        find[0].text = current_metadata['StartTime'][:10]  

        #coords
        bb = self._getBoundingBox(current_metadata['Coordinates'])
        #lat max = north bound lat
        #lat min = south bound lat
        #long max = east bound long
        #ong min = west bound long
        #return [xmax,xmin,ymax,ymin]
      
        #westBoundLongitude
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}extent/{http://www.isotc211.org/2005/gmd}EX_Extent/{http://www.isotc211.org/2005/gmd}geographicElement/{http://www.isotc211.org/2005/gmd}EX_GeographicBoundingBox/{http://www.isotc211.org/2005/gmd}westBoundLongitude/{http://www.isotc211.org/2005/gco}Decimal',template_root.nsmap)
        find[0].text = bb[0]
        
        #eastBoundLongitude
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}extent/{http://www.isotc211.org/2005/gmd}EX_Extent/{http://www.isotc211.org/2005/gmd}geographicElement/{http://www.isotc211.org/2005/gmd}EX_GeographicBoundingBox/{http://www.isotc211.org/2005/gmd}eastBoundLongitude/{http://www.isotc211.org/2005/gco}Decimal',template_root.nsmap)
        find[0].text = bb[1]
        
        #southBoundLatitude
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}extent/{http://www.isotc211.org/2005/gmd}EX_Extent/{http://www.isotc211.org/2005/gmd}geographicElement/{http://www.isotc211.org/2005/gmd}EX_GeographicBoundingBox/{http://www.isotc211.org/2005/gmd}southBoundLatitude/{http://www.isotc211.org/2005/gco}Decimal',template_root.nsmap)
        find[0].text = bb[2]
        
        #northBoundLatitude
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}extent/{http://www.isotc211.org/2005/gmd}EX_Extent/{http://www.isotc211.org/2005/gmd}geographicElement/{http://www.isotc211.org/2005/gmd}EX_GeographicBoundingBox/{http://www.isotc211.org/2005/gmd}northBoundLatitude/{http://www.isotc211.org/2005/gco}Decimal',template_root.nsmap)
        find[0].text = bb[3]
        
        #end time of data
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}identificationInfo/{http://www.isotc211.org/2005/gmd}MD_DataIdentification/{http://www.isotc211.org/2005/gmd}extent/{http://www.isotc211.org/2005/gmd}EX_Extent/{http://www.isotc211.org/2005/gmd}temporalElement/{http://www.isotc211.org/2005/gmd}EX_TemporalExtent/{http://www.isotc211.org/2005/gmd}extent/{http://www.opengis.net/gml}TimePeriod/{http://www.opengis.net/gml}endPosition',template_root.nsmap)
        if 'StopTime' in current_metadata.keys():#sentinel 2 products sometimes dont have stoptime
          find[0].text = current_metadata['StopTime'][:10]
        else:
          find[0].text = current_metadata['StartTime'][:10]
        
        #link for the resource described in the metadata
        find = template_root.findall('./{http://www.isotc211.org/2005/gmd}distributionInfo/{http://www.isotc211.org/2005/gmd}MD_Distribution/{http://www.isotc211.org/2005/gmd}transferOptions/{http://www.isotc211.org/2005/gmd}MD_DigitalTransferOptions/{http://www.isotc211.org/2005/gmd}onLine/{http://www.isotc211.org/2005/gmd}CI_OnlineResource/{http://www.isotc211.org/2005/gmd}linkage/{http://www.isotc211.org/2005/gmd}URL',template_root.nsmap)
        find[0].text = current_metadata['downloadLink']      
        

            
        if writeToFile:
          output_filename = output_folder+metadata_key.upper().split('.')[0]+'.xml' #remove .manifest.safe , add .xml
          template_tree.write(output_filename, pretty_print=True) 
          
        return template_tree
        
      except lxml.etree.XMLSyntaxError:
        print 'File XML Syntax Error'
        return None
      
      #except Exception:
        #print 'Unspecified error occured, maybe the file doesn\'t exist'
        #return None

    else:
      print 'Wrong metadata key'
      return None
    