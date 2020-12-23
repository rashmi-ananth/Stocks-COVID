from confluent_kafka import avro
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
import os
import io
import json
import logging
import avro as av
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
#import kafka
#from time import sleep
#from kafka import KafkaProducer
#from kafka import KafkaClient
# from confluent_kafka import Producer
# from confluent_kafka.avro import AvroProducer
#from kafka.errors import KafkaError
#import avro.schema
#from avro.datafile import DataFileReader, DataFileWriter
#from avro.io import DatumReader, DatumWriter
import csv
#from schema_registry.client import SchemaRegistryClient, schema

class Case(object):
    def __init__(self, cdc_report_dt, pos_spec_dt, onset_dt, current_status, sex, age_group, race_and_ethnicity,
                 hosp_yn, icu_yn, death_yn, medcond_yn):
        self.cdc_report_dt = cdc_report_dt
        self.pos_spec_dt = pos_spec_dt
        self.onset_dt = onset_dt
        self.current_status = current_status
        self.sex = sex
        self.age_group = age_group
        self.race_and_ethnicity = race_and_ethnicity
        self.hosp_yn = hosp_yn
        self.icu_yn = icu_yn
        self.death_yn = death_yn
        self.medcond_yn = medcond_yn

def dictToCase(case, ctx):
    if case is None:
        return None

    return Case(cdc_report_dt=case["cdc_report_dt"], pos_spec_dt=case["pos_spec_dt"],
                onset_dt=case["onset_dt"], current_status=case["current_status"], sex=case["sex"], age_group=case["age_group"],
                race_and_ethnicity=case["race_and_ethnicity"], hosp_yn=case["hosp_yn"], icu_yn=case["icu_yn"], death_yn=case[
            "death_yn"], medcond_yn=case["medcond_yn"])

# c = AvroConsumer({
#     'bootstrap.servers': 'kafka:29092',
#     'group.id': 'test1',
#     'schema.registry.url': 'http://schema-registry:8081'
#     })


#c.subscribe(['topic_test'])

value_schema_str = """
{
  "name": "pythonTest",
  "type": "record",
  "namespace": "cases.avro",
  "fields": [
     {
       "name": "cdc_report_dt",
       "type": ["string", "null"]
     },
     {
       "name": "pos_spec_dt",
       "type": ["string", "null"]
     },
     {
       "name": "onset_dt",
       "type": ["string", "null"]
     },
     {
       "name": "current_status",
       "type": ["string", "null"]
     },
     {
       "name": "sex",
       "type": ["string", "null"]
     },
     {
       "name": "age_group",
       "type": ["string", "null"]
     },
     {
       "name": "race_and_ethnicity",
       "type": ["string", "null"]
     },
     {
       "name": "hosp_yn",
       "type": ["string", "null"]
     },
     {
       "name": "icu_yn",
       "type": ["string", "null"]
     },
     {
       "name": "death_yn",
       "type": ["string", "null"]
     },
     {
       "name": "medcond_yn",
       "type": ["string", "null"]
     }
   ]
 }
 """
schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_deserializer = AvroDeserializer(value_schema_str, schema_registry_client, dictToCase)
string_deserializer = StringDeserializer('utf_8')


consumer_conf = {'bootstrap.servers': 'kafka:29092',
                 'key.deserializer': string_deserializer,
                 'value.deserializer': avro_deserializer,
                 'group.id': "group1"}
#'auto.offset.reset': "earliest"}
consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(['topic_test'])
consumeList = []
consumer.close()