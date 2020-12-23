from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import os
import io
import json
import logging
import avro as av
import csv
from uuid import uuid4
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

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

def caseToDict(case, ctx): return dict(cdc_report_dt=case["cdc_report_dt"], pos_spec_dt=case["pos_spec_dt"],
onset_dt=case["onset_dt"], current_status=case["current_status"], sex=case["sex"], age_group=case["age_group"],
race_and_ethnicity=case["race_and_ethnicity"], hosp_yn=case["hosp_yn"], icu_yn=case["icu_yn"], death_yn=case[
"death_yn"], medcond_yn=case["medcond_yn"])

def CSVRead(csvfilename):
    reader = csv.DictReader(open(csvfilename))

    return reader


def AvroSerialize(schema, msg):
    writer = av.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = av.io.BinaryEncoder(bytes_writer)
    writer.write(dict(msg), encoder)
    raw_data = bytes_writer.getvalue()
    print(type(raw_data))
    return raw_data

def CSVProducer(filename, schema):
    reader = CSVRead(filename)
    j = 1
    for i in reader:
        #print(i)
        #raw_data = AvroSerialize(schema, i)
        key = {"id": j}
        avroProducer.produce(topic='topic_test', value=i, key=key)
        j+=1

    avroProducer.flush()

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

value_schema = avro.loads(value_schema_str)



def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_serializer = AvroSerializer(value_schema_str, schema_registry_client, caseToDict)
producer_conf = {'bootstrap.servers': 'kafka:29092',
                    'key.serializer': StringSerializer('utf_8'),
                    'value.serializer': avro_serializer}

producer = SerializingProducer(producer_conf)
        



# avroProducer = AvroProducer({
#     'bootstrap.servers': 'kafka:29092',
#     'on_delivery': delivery_report,
#     'schema.registry.url': 'http://schema-registry:8081'
#     }, default_key_schema=key_schema, default_value_schema=value_schema)

#CSVProducer('../customers.csv', value_schema)
