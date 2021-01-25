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

def prov_to_graphframe(prov_document):
      unified = prov_document.unified()
      node_map = {}
      vList = []
      eList = []
      for element in unified.get_records(ProvElement):
        vList.append(((str(element)),str(element.identifier),str(element.get_type())))
        node_map[element.identifier] = element

      for relation in unified.get_records(ProvRelation):
              # taking the first two elements of a relation
              attr_pair_1, attr_pair_2 = relation.formal_attributes[:2]
              # only need the QualifiedName (i.e. the value of the attribute)
              qn1, qn2 = attr_pair_1[1], attr_pair_2[1]
              if qn1 and qn2:  # only proceed if both ends of the relation exist
                  try:
                      if qn1 not in node_map:
                          node_map[qn1] = INFERRED_ELEMENT_CLASS[attr_pair_1[0]](None, qn1)
                      if qn2 not in node_map:
                          node_map[qn2] = INFERRED_ELEMENT_CLASS[attr_pair_2[0]](None, qn2)
                  except KeyError:
                      # Unsupported attribute; cannot infer the type of the element
                      continue  # skipping this relation
                  eList.append((str(node_map[qn1].identifier), str(node_map[qn2].identifier), str(relation),str(relation.get_type())))
      v = sqlContext.createDataFrame(vList,["element","id","type"])
      e = sqlContext.createDataFrame(eList, ["src","dst","relation","relation type"])
      g= GraphFrame(v, e) 
      return g