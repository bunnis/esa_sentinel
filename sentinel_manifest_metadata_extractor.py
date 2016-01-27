'''Python script for extracting metadata fields from sentinel manifests
Sentinel 1
Sentinel 2
Assumes there is a location where several manifests are stored
January 2016'''
import lxml
from lxml import etree
import os

class SentinelMetadataExtractor:
  filepath = ''
  tree = ""
  root = ""
  file_error_count=0
  filenames_error=[]
  total_files=0
  productMetadata={}
  
  def __init__(self, Filepath="/tmp/harvested/manifests/"):
    self.filepath = Filepath;
    
    
  def extractMetadata(self):
    for filename in os.listdir(self.filepath):
      self.total_files = self.total_files +1
      
      if filename.endswith(".safe") or filename.endswith(".SAFE"): 
          #print "processing file - "+str(filename)
          
          try: 
            self.tree = etree.parse(self.filepath+"/"+str(filename))
            self.root = self.tree.getroot()
          except lxml.etree.XMLSyntaxError:
            #print 'File XML Syntax Error'
            self.file_error_count = self.file_error_count+1
            self.filenames_error.append(str(filename))
            continue
          
          raw = ['S1A_S3_RAW__0SDH','S1A_IW_RAW__0SSV','S1A_IW_RAW__0SSH','S1A_IW_RAW__0SDV','S1A_IW_RAW__0SDH','S1A_EW_RAW__0SDH','S1A_EW_RAW__0SSH',
                 'S1A_EW_RAW__0SDV','S1A_S1_RAW__0SSV','S1A_S6_RAW__0SSV','S1A_S5_RAW__0SSV']
          
          gr = ['S1A_S3_GRDH_1SDH','S1A_IW_SLC__1SSV','S1A_IW__1SSH','S1A_IW_SLC__1SDV','S1A_IW_SLC__1SDH','S1A_IW_GRDH_1SSV','S1A_IW_GRDH_1SSH',
                'S1A_IW_GRDH_1SDV','S1A_IW_GRDH_1SDH','S1A_EW_GRDM_1SSH','S1A_EW_GRDM_1SDV','S1A_EW_GRDH_1SDH','S1A_EW_GRDM_1SDH',
                'S1A_IW_SLC__1SSH','S1A_S5_GRDH_1SSV','S1A_EW_GRDH_1SSH','S1A_S5_SLC__1SSV']
          
          ocn = ['S1A_IW_OCN__2SDV','S1A_WV_OCN__2SSV','S1A_IW_OCN__2SSV','S1A_EW_OCN__2SDH']
          
          s2 = ['S2A_OPER_PRD_MSIL1C_PDMC']
          
          processed = False
          for sentinel_name in gr:
            if filename.startswith(sentinel_name):
              processed = True
              self.productMetadata[filename] = self.extractGR()
              break
            
          for sentinel_name in raw:
            if filename.startswith(sentinel_name):
              processed = True
              self.productMetadata[filename] = self.extractRAW()
              break
            
          for sentinel_name in ocn:  
            if filename.startswith(sentinel_name):
                processed = True
                self.productMetadata[filename] = self.extractIWOCN()
                break
              
          for sentinel_name in s2:  
            if filename.startswith(sentinel_name):
                processed = True
                self.productMetadata[filename] = self.extractS2()
                break
          if not processed:
              self.file_error_count = self.file_error_count+1
              self.filenames_error.append(str(filename))
              print "FILE NOT IN KNOWN FILES - "+str(filename)
      else:
        print "File not ending with .SAFE or .safe - "+str(filename)

  def parseCoordinates(self,coordinates):
    final_list = []
    coordsx=[] #lat
    coordsy=[] #long
    
    split = coordinates.strip().split(' ')
    
    for c in range(0,len(split)):
      if c % 2 == 0: #pair
        coordsx.append(split[c])
      else:
        coordsy.append(split[c])
    
    for c in range(0,len(coordsx)):
      final_list.append(str(coordsx[c])+","+str(coordsy[c]))

    return final_list#lat,long
     
    
    
  def extractS2(self):
    metadata = {}
    ###############S1A_S3_GRDH_1SDH###############S1A_IW_SLC__1SSV###############S1A_IW__1SSH###############S1A_IW_SLC__1SDV
    ###############S1A_IW_SLC__1SDH###############S1A_IW_GRDH_1SSV###############S1A_IW_GRDH_1SSH###############S1A_IW_GRDH_1SDV
    ###############S1A_IW_GRDH_1SDH###############S1A_EW_GRDM_1SSH###############S1A_EW_GRDM_1SDV###############S1A_EW_GRDH_1SDH
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:startTime',self.root.nsmap)
    metadata['startTime'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:familyName',self.root.nsmap)
    metadata['familyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:number',self.root.nsmap)
    metadata['familyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:familyName',self.root.nsmap)
    metadata['instrumentFamilyName'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel/1.1}frameSet/{http://www.esa.int/safe/sentinel/1.1}footPrint/{http://www.opengis.net/gml}coordinates',self.root.nsmap)
    metadata['coordinates'] = self.parseCoordinates(extracted[0].text)
    #
    return metadata
    
  def extractGR(self):
    metadata = {}
    ###############S1A_S3_GRDH_1SDH###############S1A_IW_SLC__1SSV###############S1A_IW__1SSH###############S1A_IW_SLC__1SDV
    ###############S1A_IW_SLC__1SDH###############S1A_IW_GRDH_1SSV###############S1A_IW_GRDH_1SSH###############S1A_IW_GRDH_1SDV
    ###############S1A_IW_GRDH_1SDH###############S1A_EW_GRDM_1SSH###############S1A_EW_GRDM_1SDV###############S1A_EW_GRDH_1SDH
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:startTime',self.root.nsmap)
    metadata['startTime'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:stopTime',self.root.nsmap)
    metadata['stopTime'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:familyName',self.root.nsmap)
    metadata['familyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:number',self.root.nsmap)
    metadata['familyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:familyName',self.root.nsmap)
    metadata['instrumentFamilyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:extension/s1sarl1:instrumentMode/s1sarl1:mode',self.root.nsmap)
    metadata['instrumentMode'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productClass',self.root.nsmap)
    metadata['productClass'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productClassDescription',self.root.nsmap)
    metadata['productClassDescription'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productComposition',self.root.nsmap)
    metadata['productComposition'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:productType',self.root.nsmap)
    metadata['productType'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl1:standAloneProductInformation/s1sarl1:transmitterReceiverPolarisation',self.root.nsmap)
    if len(extracted) > 1:
      metadata['transmitterReceiverPolarisation'] = extracted[0].text + "/" + extracted[1].text
    else:
      metadata['transmitterReceiverPolarisation'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:frameSet/safe:frame/safe:footPrint/gml:coordinates',self.root.nsmap)
    metadata['coordinates'] = self.parseCoordinates(extracted[0].text)
    #
    return metadata
    
  def extractRAW(self):
    metadata = {}
    ###############S1A_S3_RAW__0SDH###############S1A_IW_RAW__0SSV###############S1A_IW_RAW__0SSH###############S1A_IW_RAW__0SDV
    ###############S1A_IW_RAW__0SDH###############S1A_EW_RAW__0SDH
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}acquisitionPeriod/{http://www.esa.int/safe/sentinel-1.0}startTime',self.root.nsmap)
    metadata['startTime'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}acquisitionPeriod/{http://www.esa.int/safe/sentinel-1.0}stopTime',self.root.nsmap)
    metadata['stopTime'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}familyName',self.root.nsmap)
    metadata['familyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}number',self.root.nsmap)
    metadata['familyNameNumber'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}instrument/{http://www.esa.int/safe/sentinel-1.0}familyName',self.root.nsmap)
    metadata['instrumentFamilyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}platform/{http://www.esa.int/safe/sentinel-1.0}instrument/{http://www.esa.int/safe/sentinel-1.0}extension/s1sar:instrumentMode/s1sar:mode',self.root.nsmap)
    metadata['instrumentMode'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:productClass',self.root.nsmap)
    metadata['productClass'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:productClassDescription',self.root.nsmap)
    metadata['productClassDescription'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:productConsolidation',self.root.nsmap)
    metadata['productConsolidation'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sar:standAloneProductInformation/s1sar:transmitterReceiverPolarisation',self.root.nsmap)
    if len(extracted) > 1:
      metadata['transmitterReceiverPolarisation'] = extracted[0].text + "/" + extracted[1].text
    else:
      metadata['transmitterReceiverPolarisation'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/{http://www.esa.int/safe/sentinel-1.0}frameSet/{http://www.esa.int/safe/sentinel-1.0}frame/{http://www.esa.int/safe/sentinel-1.0}footPrint/{http://www.opengis.net/gml}coordinates',self.root.nsmap)
    metadata['coordinates'] = self.parseCoordinates(extracted[0].text)
    
    return metadata
  
  def extractIWOCN(self):
    metadata = {}
    ###############S1A_IW_OCN__2SDV
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:startTime',self.root.nsmap)
    metadata['startTime'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:acquisitionPeriod/safe:stopTime',self.root.nsmap)
    metadata['stopTime'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:familyName',self.root.nsmap)
    metadata['familyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:number',self.root.nsmap)
    metadata['familyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:familyName',self.root.nsmap)
    metadata['instrumentFamilyName'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:platform/safe:instrument/safe:extension/s1sarl2:instrumentMode/s1sarl2:mode',self.root.nsmap)
    metadata['instrumentMode'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productClass',self.root.nsmap)
    metadata['productClass'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productClassDescription',self.root.nsmap)
    metadata['productClassDescription'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productComposition',self.root.nsmap)
    metadata['productComposition'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:productType',self.root.nsmap)
    metadata['productType'] = extracted[0].text
    
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/s1sarl2:standAloneProductInformation/s1sarl2:transmitterReceiverPolarisation',self.root.nsmap)
    if len(extracted) > 1:
      metadata['transmitterReceiverPolarisation'] = extracted[0].text + "/" + extracted[1].text
    else:
      metadata['transmitterReceiverPolarisation'] = extracted[0].text
    #
    extracted = self.root.findall('./metadataSection/metadataObject/metadataWrap/xmlData/safe:frameSet/safe:frame/safe:footPrint/gml:coordinates',self.root.nsmap)
    metadata['coordinates'] = self.parseCoordinates(extracted[0].text)
    
    return metadata
  
  def checkAllFilesParsed(self):
    print 'Processed ' + str(self.total_files) + ' files'
    print '\n\n'
    print str(self.file_error_count) + " files had errors"
    print 'Printing filenames'
    
    for j in range(0,len(self.filenames_error)):
      print self.filenames_error[j]
    
  def getProductsMetadata(self):
    return self.productMetadata
    
sentinel = SentinelMetadataExtractor()
sentinel.extractMetadata()
sentinel.checkAllFilesParsed()