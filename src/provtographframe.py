from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *

from prov.model import (
    ProvDocument,
    ProvRecord,
    ProvElement,
    ProvEntity,
    ProvActivity,
    ProvAgent,
    ProvRelation,
    PROV_ATTR_ENTITY,
    PROV_ATTR_ACTIVITY,
    PROV_ATTR_AGENT,
    PROV_ATTR_TRIGGER,
    PROV_ATTR_GENERATED_ENTITY,
    PROV_ATTR_USED_ENTITY,
    PROV_ATTR_DELEGATE,
    PROV_ATTR_RESPONSIBLE,
    PROV_ATTR_SPECIFIC_ENTITY,
    PROV_ATTR_GENERAL_ENTITY,
    PROV_ATTR_ALTERNATE1,
    PROV_ATTR_ALTERNATE2,
    PROV_ATTR_COLLECTION,
    PROV_ATTR_INFORMED,
    PROV_ATTR_INFORMANT,
    PROV_ATTR_BUNDLE,
    PROV_ATTR_PLAN,
    PROV_ATTR_ENDER,
    PROV_ATTR_STARTER,
    ProvBundle,
)

INFERRED_ELEMENT_CLASS = {
    PROV_ATTR_ENTITY: ProvEntity,
    PROV_ATTR_ACTIVITY: ProvActivity,
    PROV_ATTR_AGENT: ProvAgent,
    PROV_ATTR_TRIGGER: ProvEntity,
    PROV_ATTR_GENERATED_ENTITY: ProvEntity,
    PROV_ATTR_USED_ENTITY: ProvEntity,
    PROV_ATTR_DELEGATE: ProvAgent,
    PROV_ATTR_RESPONSIBLE: ProvAgent,
    PROV_ATTR_SPECIFIC_ENTITY: ProvEntity,
    PROV_ATTR_GENERAL_ENTITY: ProvEntity,
    PROV_ATTR_ALTERNATE1: ProvEntity,
    PROV_ATTR_ALTERNATE2: ProvEntity,
    PROV_ATTR_COLLECTION: ProvEntity,
    PROV_ATTR_INFORMED: ProvActivity,
    PROV_ATTR_INFORMANT: ProvActivity,
    PROV_ATTR_BUNDLE: ProvBundle,
    PROV_ATTR_PLAN: ProvEntity,
    PROV_ATTR_ENDER: ProvEntity,
    PROV_ATTR_STARTER: ProvEntity,
}

SHORT_NAMES = {
    "prov:Entity": "ent",
    "prov:Activity": "act",
    "prov:Generation": "gen",
    "prov:Usage": "usd",
    "prov:Communication": "wib",
    "prov:Start": "wsb",
    "prov:End": "web",
    "prov:Invalidation": "inv",
    "prov:Derivation": "der",
    "prov:Agent": "agt",
    "prov:Attribution": "att",
    "prov:Association": "waw",
    "prov:Delegation": "del",
    "prov:Influence": "inf",
    "prov:Alternate": "alt",
    "prov:Specialization": "spe",
    "prov:Mention": "men",
    "prov:Membership": "mem",
}
#The below 2 functions took inspiration from the prov_to_graph function from the prov library:
#https://github.com/trungdong/prov

def prov_to_graphframe_pairmatch(prov_document):
      unified = prov_document.unified() #unify the information in the PROV document
      node_map = {}
      vList = [] #a list to collect all the vertices
      eList = [] # a list to collect all the edges

      #for loop and collect specific information from PROV element and add them to the vertices list
      for element in unified.get_records(ProvElement):
        vList.append(((str(element)),str(element.identifier),SHORT_NAMES[str(element.get_type())],str(element.get_asserted_types())))
        node_map[element.identifier] = element
      #similar process but for edges
      for relation in unified.get_records(ProvRelation):
              attr_pair_1, attr_pair_2 = relation.formal_attributes[:2]
              qn1, qn2 = attr_pair_1[1], attr_pair_2[1]
              if qn1 and qn2:  
                  try:
                      if qn1 not in node_map:
                          node_map[qn1] = INFERRED_ELEMENT_CLASS[attr_pair_1[0]](None, qn1)
                      if qn2 not in node_map:
                          node_map[qn2] = INFERRED_ELEMENT_CLASS[attr_pair_2[0]](None, qn2)
                  except KeyError:
                      continue  
                  eList.append((str(node_map[qn1].identifier), str(node_map[qn2].identifier), str(relation),SHORT_NAMES[str(relation.get_type())]))
      #create a dataframe from the vertices list
      v = sqlContext.createDataFrame(vList,["element","id","type","primitive type"])
      #create a dataframe from the edges list
      e = sqlContext.createDataFrame(eList, ["src","dst","relation","relation type"])
      #create a Graphframe with the two dataframes
      g= GraphFrame(v, e)
      return g

#This is the same function as the one above but only with the option of whether to include primitive types earlier
def prov_to_graphframe_motiffind(prov_document,primitive):
      unified = prov_document.unified()
      node_map = {}
      vList = []
      eList = []
      #we extract the primitive type here if primitive is set to True
      if primitive:
        for element in unified.get_records(ProvElement):
          vList.append(((str(element)),str(element.identifier),"$".join([SHORT_NAMES[str(element.get_type())],str(element.get_asserted_types())])))
          node_map[element.identifier] = element
      #else we extract only the generic types
      else:
        for element in unified.get_records(ProvElement):
          vList.append(((str(element)),str(element.identifier),SHORT_NAMES[str(element.get_type())]))
          node_map[element.identifier] = element

      for relation in unified.get_records(ProvRelation):
              
              attr_pair_1, attr_pair_2 = relation.formal_attributes[:2]
              
              qn1, qn2 = attr_pair_1[1], attr_pair_2[1]
              if qn1 and qn2:  
                  try:
                      if qn1 not in node_map:
                          node_map[qn1] = INFERRED_ELEMENT_CLASS[attr_pair_1[0]](None, qn1)
                      if qn2 not in node_map:
                          node_map[qn2] = INFERRED_ELEMENT_CLASS[attr_pair_2[0]](None, qn2)
                  except KeyError:
                      
                      continue  
                  eList.append((str(node_map[qn1].identifier), str(node_map[qn2].identifier),SHORT_NAMES[str(relation.get_type())]))
      v = sqlContext.createDataFrame(vList,["element","id","type"])
      e = sqlContext.createDataFrame(eList, ["src","dst","relation_type"])
      g= GraphFrame(v, e) 
      return g