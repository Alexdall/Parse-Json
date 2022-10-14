import sys, os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, Catalog
from pyspark.sql import DataFrame, DataFrameStatFunctions, DataFrameNaFunctions
from pyspark.sql import functions as F
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

import re

class ExplodeJson:

    def __init__(self, config):
        self.partition = config.__getitem__('partition')
        self.partitionReferenceColumn = config.__getitem__('partitionReferenceColumn')
        self.idTable = config.__getitem__('idTable')
        self.principalTableName = config.__getitem__('principalTableName')
        self.listTable = []
        self.listRefineTables = []
        self._regex = '[^a-zA-Z0-9_-]'

    def recursiveParseJson(self,_json):
        def parseJson(_json,listJson=[]):
            try:
                index = 0
                items = []
                if len(listJson) == 0:
                    for item in _json:
                        for k, v in item.items():
                            if type(v) == list or type(v) == dict:
                            #    if self.partition != "" and self.partitionReferenceColumn != "":
                            #        items.append((item[self.idTable],index,re.sub(self._regex, '', k),v,item[self.partitionReferenceColumn]))
                            #    else:
                                 items.append((item[self.idTable],index,re.sub(self._regex, '', k),v))

                        #if self.partition != "" and self.partitionReferenceColumn != "":
                        #    item[self.partition] = item[self.partitionReferenceColumn]

                        index = index + 1

                    for it in items:
                        _json[it[1]].pop(it[2])

                    self.listTable.append((self.principalTableName,_json))

                    if len(items) > 0:
                        parseJson(None,items)
                else:
                    for lj in listJson:
                        j = lj[3]
                        nj = {}
                        if type(j) == dict:
                            j[self.idTable] = lj[0]
                            for k, v in j.items():
                                print(k)
                                print(v)
                                print(type(v))
                                if type(v) == list or type(v) == dict:
                                    #if self.partition != "" and self.partitionReferenceColumn != "":
                                    #    items.append((lj[0],None,re.sub(self._regex, '', k),v,lj[4]))
                                    #else:
                                    items.append((lj[0],index,re.sub(self._regex, '', k),v))
                                    if type(v) == list:
                                        parseJson(None,items)
                                else:
                                    nj[re.sub(self._regex, '', k)]=v
                                #    if self.partition != "" and self.partitionReferenceColumn != "":
                                #            nj[self.partition] = lj[4]

                            self.listTable.append((lj[2],nj))

                        elif type(j) == list:
                            for item in j:
                                nj = {}
                                try:
                                    for k,v in item.items():
                                        if type(v) == list or type(v) == dict:
                                        #    if self.partition != "" and self.partitionReferenceColumn != "":
                                         #       items.append((lj[0],None,re.sub(self._regex, '', k),v,lj[4]))
                                            items.append((lj[0], None, re.sub(self._regex, '', k), v))
                                        else:
                                            nj[re.sub(self._regex, '', k)]=v
                                            nj[self.idTable] = lj[0]
                                            if self.partition != "" and self.partitionReferenceColumn != "":
                                                nj[self.partition] = lj[4]

                                    self.listTable.append((lj[2],nj))
                                except AttributeError as error:
                                    _str = ""
                                    for item in lj[3]:
                                        _str = _str+str(item)+"|"

                                    nj[self.idTable] = lj[0]
                                    nj[lj[2]] = _str[:len(_str)-1]
                                    if self.partition != "" and self.partitionReferenceColumn != "":
                                                nj[self.partition] = lj[4]
                                    self.listTable.append((lj[2],nj))

                if len(items) > 0:
                    parseJson(None,items)
            except Exception as e:
                print(item)
                strError = 'Error in parseJson(): '+str(e)
                strError = strError.replace("'","")
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
                tb=[]
                for ltb in listaTB:
                    if ltb[0] == nmTb:
                        tb.append(ltb[1])
                self.listRefineTables.append((nmTb,tb))
        except Exception as e:
            strError = 'Error in refineTables(): '+str(e)
            strError = strError.replace("'","")
            raise Exception(strError)

import json

j = '''[
        {
            "id" : 5993,
            "identificador" : "VENDA",
            "operacao" : "ATUALIZACAO",
            "prioridade" : 170,
            "body" : {"id":"000004-001-1-2022-08-01","sequencial":"000004","numeroCaixa":"001","data":"2022-08-01","dataHoraInicio":"2022-08-01T09:39:43","dataHoraFim":"2022-08-01T09:43:07","lojaId":"1","dataHoraVenda":"2022-08-01T09:43:07","funcionarioId":"999","funcionario":{"nome":"cm"},"efetiva":"true","tipoPreco":"1","tipoDesconto":"","valor":574.10,"acrescimo":0.00,"desconto":0.00,"servico":0.000,"quantidadeItens":6,"quantidadeItensCancelados":2,"valorItensCancelados":3474.90,"itens":[{"produtoId":"4","funcionarioId":"0","efetivo":"true","quantidade":1.000,"valorUnitario":55.500,"valorTotal":55.50,"acrescimo":0.00,"desconto":0.00,"preco":55.500,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":42.61000,"markup":30.2500,"lucro":2.90000},{"produtoId":"3","funcionarioId":"0","efetivo":"true","quantidade":1.000,"valorUnitario":52.100,"valorTotal":52.10,"acrescimo":0.00,"desconto":0.00,"preco":52.100,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.000,"markup":0,"lucro":42.7220},{"produtoId":"2","funcionarioId":"0","efetivo":"true","quantidade":1.000,"valorUnitario":64.800,"valorTotal":64.80,"acrescimo":0.00,"desconto":0.00,"preco":64.800,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.000,"markup":0,"lucro":53.1360},{"produtoId":"1","funcionarioId":"0","efetivo":"true","quantidade":1.000,"valorUnitario":61.900,"valorTotal":61.90,"acrescimo":0.00,"desconto":0.00,"preco":61.900,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.000,"markup":0,"lucro":50.7580},{"produtoId":"20004","funcionarioId":"0","efetivo":"False","quantidade":1.000,"valorUnitario":2940.000,"valorTotal":2940.00,"acrescimo":0.00,"desconto":0.00,"preco":2940.000,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.00000,"markup":0,"lucro":2410.80000},{"produtoId":"20003","funcionarioId":"0","efetivo":"False","quantidade":1.000,"valorUnitario":534.900,"valorTotal":534.90,"acrescimo":0.00,"desconto":0.00,"preco":534.900,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.00000,"markup":0,"lucro":438.61800},{"produtoId":"20002","funcionarioId":"0","efetivo":"true","quantidade":1.000,"valorUnitario":144.900,"valorTotal":144.90,"acrescimo":0.00,"desconto":0.00,"preco":144.900,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.00000,"markup":0,"lucro":118.81800},{"produtoId":"20001","funcionarioId":"0","efetivo":"true","quantidade":1.000,"valorUnitario":194.900,"valorTotal":194.90,"acrescimo":0.00,"desconto":0.00,"preco":194.900,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.00000,"markup":0,"lucro":159.81800}],"pagamentos":[{"sequencial":"1","descricao":"DINHEIRO","formaPagamentoId":"1","valor":574.10,"codigoAutorizacao":"","quantidadeParcelas":0}],"ecf":{"modelo":"CFE SAT","numeroSerie":"904466","ccf":"009256"},"numeroNota":"009256","origem":"SYSPDV","canalVendaId":"1"},
            "entidade_id" : "000004-001-1-2022-08-01-1",
            "objeto_id" : "000004-001-1-2022-08-01-1",
            "origem" : "VAREJOFACIL"
        },
        {
            "id" : 5994,
            "identificador" : "VENDA",
            "operacao" : "ATUALIZACAO",
            "prioridade" : 170,
            "body" : {"id":"000004-001-1-2022-08-01","sequencial":"000004","numeroCaixa":"001","data":"2022-08-01","dataHoraInicio":"2022-08-01T09:39:43","dataHoraFim":"2022-08-01T09:43:07","lojaId":"1","dataHoraVenda":"2022-08-01T09:43:07","funcionarioId":"999","funcionario":{"nome":"cm"},"efetiva":"False","tipoPreco":"1","tipoDesconto":"","valor":574.10,"acrescimo":0.00,"desconto":0.00,"servico":0.000,"quantidadeItens":6,"quantidadeItensCancelados":2,"valorItensCancelados":3474.90,"itens":[{"produtoId":"4","funcionarioId":"0","efetivo":"true","quantidade":1.000,"valorUnitario":55.500,"valorTotal":55.50,"acrescimo":0.00,"desconto":0.00,"preco":55.500,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":42.61000,"markup":30.2500,"lucro":2.90000},{"produtoId":"3","funcionarioId":"0","efetivo":"true","quantidade":1.000,"valorUnitario":52.100,"valorTotal":52.10,"acrescimo":0.00,"desconto":0.00,"preco":52.100,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.00000,"markup":0,"lucro":42.72200},{"produtoId":"2","funcionarioId":"0","efetivo":"true","quantidade":1.000,"valorUnitario":64.800,"valorTotal":64.80,"acrescimo":0.00,"desconto":0.00,"preco":64.800,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.00000,"markup":0,"lucro":53.13600},{"produtoId":"1","funcionarioId":"0","efetivo":"true","quantidade":1.000,"valorUnitario":61.900,"valorTotal":61.90,"acrescimo":0.00,"desconto":0.00,"preco":61.900,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.00000,"markup":0,"lucro":50.75800},{"produtoId":"20004","funcionarioId":"0","efetivo":"False","quantidade":1.000,"valorUnitario":2940.000,"valorTotal":2940.00,"acrescimo":0.00,"desconto":0.00,"preco":2940.000,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.00000,"markup":0,"lucro":2410.80000},{"produtoId":"20003","funcionarioId":"0","efetivo":"False","quantidade":1.000,"valorUnitario":534.900,"valorTotal":534.90,"acrescimo":0.00,"desconto":0.00,"preco":534.900,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.00000,"markup":0,"lucro":438.61800},{"produtoId":"20002","funcionarioId":"0","efetivo":"true","quantidade":1.000,"valorUnitario":144.900,"valorTotal":144.90,"acrescimo":0.00,"desconto":0.00,"preco":144.900,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.00000,"markup":0,"lucro":118.81800},{"produtoId":"20001","funcionarioId":"0","efetivo":"true","quantidade":1.000,"valorUnitario":194.900,"valorTotal":194.90,"acrescimo":0.00,"desconto":0.00,"preco":194.900,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.00000,"markup":0,"lucro":159.81800}],"pagamentos":[{"sequencial":"1","descricao":"DINHEIRO","formaPagamentoId":"1","valor":574.10,"codigoAutorizacao":"","quantidadeParcelas":0}],"cancelamento":{"numeroCaixa":"001","sequencial":"000005","data":"2022-08-01","dataHoraInicio":"2022-08-01T09:43:52","dataHoraFim":"2022-08-01T09:44:38","funcionarioId":"999","funcionario":{"nome":"cm"}},"ecf":{"modelo":"CFE SAT","numeroSerie":"904466","ccf":"009256"},"numeroNota":"009256","origem":"SYSPDV","canalVendaId":"1"},
            "entidade_id" : "000004-001-1-2022-08-01-7",
            "objeto_id" : "000004-001-1-2022-08-01-7",
            "origem" : "VAREJOFACIL"
        },
        {
            "id" : 6016,
            "identificador" : "VENDA",
            "operacao" : "ATUALIZACAO",
            "prioridade" : 170,
            "body" : {"id":"000012-001-1-2022-08-01","sequencial":"000012","numeroCaixa":"001","data":"2022-08-01","dataHoraInicio":"2022-08-01T14:29:35","dataHoraFim":"2022-08-01T14:30:34","lojaId":"1","dataHoraVenda":"2022-08-01T14:30:34","funcionarioId":"999","funcionario":{"nome":"cm"},"efetiva":"true","tipoPreco":"1","tipoDesconto":"","valor":4.00,"acrescimo":0.00,"desconto":0.00,"servico":0.000,"quantidadeItens":1,"quantidadeItensCancelados":0,"valorItensCancelados":0.00,"itens":[{"produtoId":"20022","funcionarioId":"31","funcionario":{"nome":" Marta Valeria Amorim de Melo","numeroDocumento":"10655486801"},"efetivo":"true","quantidade":1.000,"valorUnitario":4.000,"valorTotal":4.00,"acrescimo":0.00,"desconto":0.00,"preco":4.000,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.000,"markup":0,"lucro":3.2800}],"pagamentos":[{"sequencial":"1","descricao":"DINHEIRO","formaPagamentoId":"1","valor":4.00,"codigoAutorizacao":"","quantidadeParcelas":0}],"ecf":{"modelo":"CFE SAT","numeroSerie":"904466","ccf":"009258"},"numeroNota":"009258","origem":"SYSPDV","canalVendaId":"1"},
            "entidade_id" : "000012-001-1-2022-08-01-1",
            "objeto_id" : "000012-001-1-2022-08-01-1",
            "origem" : "VAREJOFACIL"
        },
	{
		"id" : 178914,
		"identificador" : "VENDA",
		"operacao" : "ATUALIZACAO",
		"prioridade" : 170,
		"body" : {"id":"001146-001-1-2022-09-16","sequencial":"001146","numeroCaixa":"001","data":"2022-09-16","dataHoraInicio":"2022-09-16T20:48:18","dataHoraFim":"2022-09-16T20:53:36","lojaId":"1","dataHoraVenda":"2022-09-16T20:53:36","funcionarioId":"3","funcionario":{"nome":"Cassila Teles Inasawa Santos","numeroDocumento":"06107294627"},"clienteId":"473","efetiva":"true","tipoPreco":"1","tipoDesconto":"","valor":318.05,"acrescimo":0.00,"desconto":201.65,"servico":0.000,"quantidadeItens":2,"quantidadeItensCancelados":0,"valorItensCancelados":0.00,"itens":[{"produtoId":"20802","funcionarioId":"20","funcionario":{"nome":"ANTONIO LUCAS COIMBRA BEZERRA","numeroDocumento":"39923362809"},"efetivo":"true","quantidade":2.000,"valorUnitario":179.900,"valorTotal":220.20,"acrescimo":0.00,"desconto":139.60,"preco":179.900,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.00000,"markup":0,"lucro":180.56400,"descontos":[{"valor":71.96,"codigoMotivoDesconto":"2","justificativaMotivoDesconto":"CONFRADE FLVHW92Z","promocaoId":"","tipo":"ITEM","fidelidade":"False"},{"valor":67.64,"codigoMotivoDesconto":"68","justificativaMotivoDesconto":"CONFRADE FLVHW92Z","promocaoId":"","tipo":"SUBTOTAL","fidelidade":"False"}]},{"produtoId":"20196","funcionarioId":"20","funcionario":{"nome":"ANTONIO LUCAS COIMBRA BEZERRA","numeroDocumento":"39923362809"},"efetivo":"true","quantidade":1.000,"valorUnitario":159.900,"valorTotal":97.85,"acrescimo":0.00,"desconto":62.05,"preco":159.900,"servico":0.0000,"tipoPreco":"1","promocao":"False","custo":0.00000,"markup":0,"lucro":80.23700,"descontos":[{"valor":31.98,"codigoMotivoDesconto":"2","justificativaMotivoDesconto":"CONFRADE FLVHW92Z","promocaoId":"","tipo":"ITEM","fidelidade":"False"},{"valor":30.07,"codigoMotivoDesconto":"68","justificativaMotivoDesconto":"CONFRADE FLVHW92Z","promocaoId":"","tipo":"SUBTOTAL","fidelidade":"False"}]}],"pagamentos":[{"sequencial":"1","descricao":"POS Crédito","formaPagamentoId":"4","valor":318.05,"codigoBandeira":"00002","codigoOperadora":"00005","codigoAutorizacao":"089254","quantidadeParcelas":0}],"ecf":{"modelo":"CFE SAT","numeroSerie":"904466","ccf":"010077"},"numeroNota":"010077","origem":"SYSPDV","canalVendaId":"1"},
		"entidade_id" : "001146-001-1-2022-09-16-1",
		"objeto_id" : "001146-001-1-2022-09-16-1",
		"origem" : "VAREJOFACIL"
	}
	]
'''

#with open ("/home/alexandre/VisualCodeProjects/Parse_json/Fontes/vendas-fgpinheiros-grandcru_4.json",'r' ) as f:
#    j = f.read()


type(j)

config = {
    "partition":"",
    "partitionReferenceColumn":"",
    "idTable":"id",
    "principalTableName":"tbprincipal",

}

#del e
e = ExplodeJson(config)

e.recursiveParseJson(json.loads(j))

e.refineTables()

for tb in e.listRefineTables:
    print(tb[0])
    print(tb[1])

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
           # df.write\
           #   .mode('overwrite')\
           #   .parquet("/home/alexandre/PycharmProjects/Parse-Json/Destino/"+tb[0])

