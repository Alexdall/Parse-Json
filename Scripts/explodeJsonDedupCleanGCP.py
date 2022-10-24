#--------------- CODE ------------------"
import re
import sys, os
import json
from google.cloud import storage


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

    def read_gcp_blob(self, bucket_name, blob_name):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.get_blob(blob_name)
        j = blob.download_as_string(client=None)
        return j

    def loadReplaceJson(self, bucket, file):
        j = self.read_gcp_blob(bucket, file)
        js = json.loads(j)
        for i in js.values():
            i = str(i)
            i = i.replace('''body': '{"''', '''body': {"''').replace("\"}'", "\"}").replace('false', '"false"').replace(
                'true', '"true"').replace("'", '"')
            return (i)

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

    def createParquet(self):
        try:
            for tb in self.listRefineTables:
                print(tb[0])
                ls = tb[1]
                # print(type(ls))
                schema = []
                conta = 0
                valida_conta = 0
                for l in ls:
                    columns = list(l.keys())
                    conta = len(columns)
                    if conta > valida_conta:
                        schema = []
                        for m in range(len(columns)):
                            schema.append(columns[m] + ' string')
                            valida_conta = conta
                            sss = ', '.join(schema)
                            schema_final = sss
                        rdd = spark.sparkContext.parallelize([ls])
                        # df = spark_sess.createDataFrame(rdd,schema_final)
                        df = spark.read.json(rdd)
                        df.show()
                        # df.write\
                        #  .mode('overwrite')\
                        #  .parquet(f'gs://daia-datalake-raw/{e.principalTableName}-' + tb[0])

        except Exception as e:
            strError = 'Error in createParquet(): ' + str(e)
            strError = strError.replace("'", "")
            raise Exception(strError)



#----------------- RUN ------------------#

config = {
    "partition": "",
    "partitionReferenceColumn": "",
    "idTable": "id",
    "principalTableName": "vendas",

}

e = ExplodeJson(config)

j = e.loadReplaceJson("daia-datalake-landing", "produto-envio_caspian.json")

e.recursiveParseJson(json.loads(j))

e.refineTables()

e.createParquet()