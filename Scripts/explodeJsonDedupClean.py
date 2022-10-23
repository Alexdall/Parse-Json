import re
import sys, os
import json

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, Catalog
from pyspark.sql import DataFrame, DataFrameStatFunctions, DataFrameNaFunctions
from pyspark.sql import functions as f
from pyspark.sql import types as T
from pyspark.sql.types import Row

spark_conf = SparkConf()
spark_conf.setAll([
    ('spark.master', 'spark://172.19.0.1:7077'),
    ('spark.app.name', 'myApp'),
    ('spark.submit.deployMode', 'client'),
    ('spark.ui.showConsoleProgress', 'true'),
    ('spark.eventLog.enabled', 'false'),
    ('spark.logConf', 'false'),
    ('spark.driver.bindAddress', '0.0.0.0'),
    ('spark.driver.host', '172.19.0.1'),
])

spark_sess = SparkSession.builder.config(conf=spark_conf).getOrCreate()
spark_ctxt = spark_sess.sparkContext
spark_reader = spark_sess.read
spark_streamReader = spark_sess.readStream
spark_ctxt.setLogLevel("WARN")


# COMMAND ----------

class ExplodeJson:

    def __init__(self, config):
        self.partition = config.__getitem__('partition')
        self.partitionReferenceColumn = config.__getitem__('partitionReferenceColumn')
        self.idTable = config.__getitem__('idTable')
        self.principalTableName = config.__getitem__('principalTableName')
        self.listTable = []
        self.listRefineTables = []
        self._regex = '[^a-zA-Z0-9_-]'

    def recursiveParseJson(self, _json):
        def parseJson(_json, listJson=[]):
            try:
                index = 0
                items = []
                if len(listJson) == 0:
                    for item in _json:
                        for k, v in item.items():
                            if type(v) == list or type(v) == dict:
                                if self.partition != "" and self.partitionReferenceColumn != "":
                                    items.append((item[self.idTable], index, re.sub(self._regex, '', k), v,
                                                  item[self.partitionReferenceColumn]))
                                else:
                                    items.append((item[self.idTable], index, re.sub(self._regex, '', k), v))

                        if self.partition != "" and self.partitionReferenceColumn != "":
                            item[self.partition] = item[self.partitionReferenceColumn]

                        index = index + 1

                    for it in items:
                        _json[it[1]].pop(it[2])

                    self.listTable.append((self.principalTableName, _json))

                    if len(items) > 0:
                        parseJson(None, items)
                else:
                    for lj in listJson:
                        j = lj[3]
                        nj = {}
                        if type(j) == dict:
                            items = []
                            for k, v in j.items():
                                if type(v) == list or type(v) == dict:
                                    if self.partition != "" and self.partitionReferenceColumn != "":
                                        items.append((lj[0], None, lj[2] + re.sub(self._regex, '', k), v, lj[4]))
                                    else:
                                        items.append((lj[0], None, lj[2] + re.sub(self._regex, '', k), v))
                                else:
                                    nj[re.sub(self._regex, '', k)] = v
                                    nj[self.idTable] = lj[0]
                                    if self.partition != "" and self.partitionReferenceColumn != "":
                                        nj[self.partition] = lj[4]
                            if len(nj.keys()) > 0:
                                self.listTable.append((lj[2], nj))

                            if len(items) > 0:
                                parseJson(None, items)
                        elif type(j) == list:
                            items = []
                            for item in j:
                                nj = {}
                                try:
                                    for k, v in item.items():
                                        if type(v) == list or type(v) == dict:
                                            if self.partition != "" and self.partitionReferenceColumn != "":
                                                items.append(
                                                    (lj[0], None, lj[2] + re.sub(self._regex, '', k), v, lj[4]))
                                            else:
                                                items.append((lj[0], None, lj[2] + re.sub(self._regex, '', k), v))
                                        else:
                                            nj[re.sub(self._regex, '', k)] = v
                                            nj[self.idTable] = lj[0]
                                            if self.partition != "" and self.partitionReferenceColumn != "":
                                                nj[self.partition] = lj[4]
                                    self.listTable.append((lj[2], nj))
                                    nj = {}
                                except AttributeError as error:
                                    _str = ""
                                    for item in lj[3]:
                                        _str = _str + str(item) + "|"

                                    nj[self.idTable] = lj[0]
                                    nj[lj[2]] = _str[:len(_str) - 1]
                                    if self.partition != "" and self.partitionReferenceColumn != "":
                                        nj[self.partition] = lj[4]
                                    self.listTable.append((lj[2], nj))
                                    break
                            if len(items) > 0:
                                parseJson(None, items)
            except Exception as e:
                strError = 'Error in parseJson(): ' + str(e)
                strError = strError.replace("'", "")
                raise Exception(strError)

        parseJson(_json)

    def refineTables(self):
        try:
            listaTB = self.listTable
            listNmTb = []

            for l in listaTB:
                if type(l[1]) == list:
                    self.listRefineTables.append((l))

            for tb in self.listRefineTables:
                listaTB.remove(tb)

            for tb in listaTB:
                listNmTb.append(tb[0])

            listNmTb = list(dict.fromkeys(listNmTb))

            for nmTb in listNmTb:
                tb = []
                for ltb in listaTB:
                    if ltb[0] == nmTb:
                        tb.append(ltb[1])
                self.listRefineTables.append((nmTb, tb))
        except Exception as e:
            strError = 'Error in refineTables(): ' + str(e)
            strError = strError.replace("'", "")
            raise Exception(strError)

'''
from google.cloud import storage


def read_json(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    j = blob.download_as_string(client=None)
    # j = str(j).replace('\\','').replace('"body" : "{','"body" : {').replace('"}",','"},').replace('true','"true"').replace('false','"false"')
    # j = bytes(j,'utf-8')
    return j

k = read_json("daia-datalake-landing","vendas-fgpinheiros-grandcru.json")
print(type(k))
print(k[:1000])
#j = j.replace('\\','')#.replace('"body" : "{','"body" : {').replace('"}",','"},').replace('true','"true"').replace('false','"false"')
'''

path = "/home/alexandre/VisualCodeProjects/Parse_json/Fontes/vendas-fgpinheiros-grandcru.json"

def loadReplaceJson(path):
    f = open(path,'r')
    j = f.read()
    js = json.loads(j)
    for i in js.values():
        i = str(i)
        i = i.replace('''body': '{"''','''body': {"''').replace("\"}'","\"}").replace('false','"false"').replace('true','"true"').replace("'",'"')
        return (i)

file = loadReplaceJson(path)

config = {
    "partition":"",
    "partitionReferenceColumn":"",
    "idTable":"id",
    "principalTableName":"tbprincipal",

}


e = ExplodeJson(config)

e.recursiveParseJson(json.loads(file))

e.refineTables()

for tb in e.listRefineTables:
    print(tb[0])
    #print(tb[1])

    ls = tb[1]
    # print(type(ls))
    schema = []
    conta = 0
    valida_conta = 0

    # print('valida_conta: '+str(valida_conta))
    for l in ls:
        # print('l :'+str(l))
        # print((l))
        columns = list(l.keys())
        conta = len(columns)
        # print(conta)
        # print(valida_conta)
        if conta > valida_conta:
            # data = list(l.values())
            schema = []
            #   print('passou aqui')
            for m in range(len(columns)):
                schema.append(columns[m] + ' string')
                valida_conta = conta
                sss = ', '.join(schema)
                schema_final = sss
            rdd = spark_sess.sparkContext.parallelize([ls])
            # df = spark_sess.createDataFrame(rdd,schema_final)
            df = spark_sess.read.json(rdd)
            df.show()
            #df.write\
            #  .mode('overwrite')\
            #  .parquet('gs://daia-datalake-raw/' + tb[0])
