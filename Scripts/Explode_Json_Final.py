
import re
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

    def recursiveParseJson(self,_json):
        def parseJson(_json,listJson=[]):
            try:
                index = 0
                items = []
                if len(listJson) == 0:
                    for item in _json:
                        for k, v in item.items():
                            if type(v) == list or type(v) == dict:
                                if self.partition != "" and self.partitionReferenceColumn != "":
                                    items.append((item[self.idTable],index,re.sub(self._regex, '', k),v,item[self.partitionReferenceColumn]))
                                else:
                                    items.append((item[self.idTable],index,re.sub(self._regex, '', k),v))

                        if self.partition != "" and self.partitionReferenceColumn != "":
                            item[self.partition] = item[self.partitionReferenceColumn]

                        index = index + 1

                    for it in items:
                        _json[it[1]].pop(it[2])

                    self.listTable.append((self.principalTableName,_json))

                    if len(items) > 0:
                        parseJson(None,items)
                else:
                    for lj in listJson:
                        print('lj :'+str(lj))
                        j = lj[3]
                        nj = {}
                        ls = []
                        if type(j) == dict:
                            for k, v in j.items():
                                print('k :'+str(k))
                                print('lj[0] :'+ str(lj[0]))
                                print('v :'+str(v))
                                print(type(v))
                                #print('lj[4] :'+str(lj[4]))
                                if type(v) == list:# or type(v) == dict:
                                    print('oassou list')
                                    if self.partition != "" and self.partitionReferenceColumn != "":
                                        items.append((lj[0],None,re.sub(self._regex, '', k),v,lj[4]))
                                    else:
                                        items.append((lj[0],None,re.sub(self._regex, '', k),v))
                                elif type(v) == dict:
                                    print('passou dict')
                                    if self.partition != "" and self.partitionReferenceColumn != "":
                                        ls.append(v)
                                        print(type(ls))
                                        print('ls :'+str(ls))
                                        items.append((lj[0],None,re.sub(self._regex, '', k),ls))
                                    else:
                                        ls.append(v)
                                        print(type(ls))
                                        print('ls :' + str(ls))
                                        items.append((lj[0],None,re.sub(self._regex, '', k),ls))
                                    ls = []
                                else:
                                    nj[re.sub(self._regex, '', k)]=v
                                    print('v :'+str(v))
                                    print('nj :'+str(nj))
                                    nj[self.idTable] = lj[0]
                                    if self.partition != "" and self.partitionReferenceColumn != "":
                                            nj[self.partition] = lj[4]
                                    self.listTable.append((lj[2],nj))
                                    nj = {}
                            if len(items) > 0:
                                parseJson(None,items)
                        elif type(j) == list:
                            items=[]
                            for item in j:
                                nj = {}
                                try:
                                    for k,v in item.items():
                                        if type(v) == list or type(v) == dict:
                                            if self.partition != "" and self.partitionReferenceColumn != "":
                                                items.append((lj[0],None,re.sub(self._regex, '', k),v,lj[4]))
                                            else:
                                                items.append((lj[0],None,re.sub(self._regex, '', k),v))
                                        else:
                                            nj[re.sub(self._regex, '', k)]=v
                                            nj[self.idTable] = lj[0]
                                            if self.partition != "" and self.partitionReferenceColumn != "":
                                                nj[self.partition] = lj[4]
                                    self.listTable.append((lj[2],nj))
                                    nj={}
                                except AttributeError as error:
                                    _str = ""
                                    for item in lj[3]:
                                        _str = _str+str(item)+"|"

                                    nj[self.idTable] = lj[0]
                                    nj[lj[2]] = _str[:len(_str)-1]
                                    if self.partition != "" and self.partitionReferenceColumn != "":
                                                nj[self.partition] = lj[4]
                                    self.listTable.append((lj[2],nj))
                                    break
                            if len(items) > 0:
                                parseJson(None,items)
            except Exception as e:
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
		"id" : 178914,
		"identificador" : "VENDA",
		"operacao" : "ATUALIZACAO",
		"prioridade" : 170,
		"body" : {"id":"001146-001-1-2022-09-16","sequencial":"001146","numeroCaixa":"001","data":"2022-09-16","dataHoraInicio":"2022-09-16T20:48:18","dataHoraFim":"2022-09-16T20:53:36","lojaId":"1","dataHoraVenda":"2022-09-16T20:53:36","funcionarioId":"3","funcionario":{"nome":"Cassila Teles Inasawa Santos","numeroDocumento":"06107294627"},"clienteId":"473","efetiva":true,"tipoPreco":"1","tipoDesconto":"","valor":318.05,"acrescimo":0.00,"desconto":201.65,"servico":0.000,"quantidadeItens":2,"quantidadeItensCancelados":0,"valorItensCancelados":0.00,"itens":[{"produtoId":"20802","funcionarioId":"20","funcionario":{"nome":"ANTONIO LUCAS COIMBRA BEZERRA","numeroDocumento":"39923362809"},"efetivo":true,"quantidade":2.000,"valorUnitario":179.900,"valorTotal":220.20,"acrescimo":0.00,"desconto":139.60,"preco":179.900,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":0.00000,"markup":0,"lucro":180.56400,"descontos":[{"valor":71.96,"codigoMotivoDesconto":"2","justificativaMotivoDesconto":"CONFRADE FLVHW92Z","promocaoId":"","tipo":"ITEM","fidelidade":false},{"valor":67.64,"codigoMotivoDesconto":"68","justificativaMotivoDesconto":"CONFRADE FLVHW92Z","promocaoId":"","tipo":"SUBTOTAL","fidelidade":false}]},{"produtoId":"20196","funcionarioId":"20","funcionario":{"nome":"ANTONIO LUCAS COIMBRA BEZERRA","numeroDocumento":"39923362809"},"efetivo":true,"quantidade":1.000,"valorUnitario":159.900,"valorTotal":97.85,"acrescimo":0.00,"desconto":62.05,"preco":159.900,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":0.00000,"markup":0,"lucro":80.23700,"descontos":[{"valor":31.98,"codigoMotivoDesconto":"2","justificativaMotivoDesconto":"CONFRADE FLVHW92Z","promocaoId":"","tipo":"ITEM","fidelidade":false},{"valor":30.07,"codigoMotivoDesconto":"68","justificativaMotivoDesconto":"CONFRADE FLVHW92Z","promocaoId":"","tipo":"SUBTOTAL","fidelidade":false}]}],"pagamentos":[{"sequencial":"1","descricao":"POS Crédito","formaPagamentoId":"4","valor":318.05,"codigoBandeira":"00002","codigoOperadora":"00005","codigoAutorizacao":"089254","quantidadeParcelas":0}],"ecf":{"modelo":"CFE SAT","numeroSerie":"904466","ccf":"010077"},"numeroNota":"010077","origem":"SYSPDV","canalVendaId":"1"},
		"entidade_id" : "001146-001-1-2022-09-16-1",
		"objeto_id" : "001146-001-1-2022-09-16-1",
		"origem" : "VAREJOFACIL"
	},
{
		"id" : 178915,
		"identificador" : "VENDA",
		"operacao" : "ATUALIZACAO",
		"prioridade" : 170,
		"body" : {"id":"001147-001-1-2022-09-16","sequencial":"001147","numeroCaixa":"001","data":"2022-09-16","dataHoraInicio":"2022-09-16T20:54:57","dataHoraFim":"2022-09-16T20:58:25","lojaId":"1","dataHoraVenda":"2022-09-16T20:58:25","funcionarioId":"3","funcionario":{"nome":"Cassila Teles Inasawa Santos","numeroDocumento":"06107294627"},"efetiva":true,"tipoPreco":"1","tipoDesconto":"","valor":300.00,"acrescimo":0.00,"desconto":19.90,"servico":0.000,"quantidadeItens":1,"quantidadeItensCancelados":0,"valorItensCancelados":0.00,"itens":[{"produtoId":"21093","funcionarioId":"20","funcionario":{"nome":"ANTONIO LUCAS COIMBRA BEZERRA","numeroDocumento":"39923362809"},"efetivo":true,"quantidade":1.000,"valorUnitario":319.900,"valorTotal":300.00,"acrescimo":0.00,"desconto":19.90,"preco":319.900,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":93.56000,"markup":241.9200,"lucro":152.44000,"descontos":[{"valor":19.90,"codigoMotivoDesconto":"67","justificativaMotivoDesconto":"","promocaoId":"","tipo":"SUBTOTAL","fidelidade":false}]}],"pagamentos":[{"sequencial":"1","descricao":"POS Débito","formaPagamentoId":"5","valor":300.00,"codigoBandeira":"20001","codigoOperadora":"00000","codigoAutorizacao":"533695","quantidadeParcelas":0}],"ecf":{"modelo":"CFE SAT","numeroSerie":"904466","ccf":"010078"},"numeroNota":"010078","origem":"SYSPDV","canalVendaId":"1"},
		"entidade_id" : "001147-001-1-2022-09-16-1",
		"objeto_id" : "001147-001-1-2022-09-16-1",
		"origem" : "VAREJOFACIL"
	},
	{
		"id" : 178916,
		"identificador" : "VENDA",
		"operacao" : "ATUALIZACAO",
		"prioridade" : 170,
		"body" : {"id":"001148-001-1-2022-09-16","sequencial":"001148","numeroCaixa":"001","data":"2022-09-16","dataHoraInicio":"2022-09-16T20:59:30","dataHoraFim":"2022-09-16T21:06:12","lojaId":"1","dataHoraVenda":"2022-09-16T21:06:12","funcionarioId":"3","funcionario":{"nome":"Cassila Teles Inasawa Santos","numeroDocumento":"06107294627"},"clienteId":"189","efetiva":true,"tipoPreco":"1","tipoDesconto":"","valor":1357.99,"acrescimo":0.00,"desconto":651.41,"servico":0.000,"quantidadeItens":4,"quantidadeItensCancelados":0,"valorItensCancelados":0.00,"itens":[{"produtoId":"21727","funcionarioId":"20","funcionario":{"nome":"ANTONIO LUCAS COIMBRA BEZERRA","numeroDocumento":"39923362809"},"efetivo":true,"quantidade":3.000,"valorUnitario":369.900,"valorTotal":749.97,"acrescimo":0.00,"desconto":359.73,"preco":369.900,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":292.08000,"markup":279.9300,"lucro":322.89540,"descontos":[{"valor":166.46,"codigoMotivoDesconto":"1","justificativaMotivoDesconto":"CUPOM350 .......","promocaoId":"","tipo":"ITEM","fidelidade":false},{"valor":193.27,"codigoMotivoDesconto":"68","justificativaMotivoDesconto":"CUPOM350 .......","promocaoId":"","tipo":"SUBTOTAL","fidelidade":false}]},{"produtoId":"21755","funcionarioId":"20","funcionario":{"nome":"ANTONIO LUCAS COIMBRA BEZERRA","numeroDocumento":"39923362809"},"efetivo":true,"quantidade":1.000,"valorUnitario":204.900,"valorTotal":138.47,"acrescimo":0.00,"desconto":66.43,"preco":204.900,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":59.92000,"markup":241.9600,"lucro":53.62540,"descontos":[{"valor":30.74,"codigoMotivoDesconto":"1","justificativaMotivoDesconto":"CUPOM350 .......","promocaoId":"","tipo":"ITEM","fidelidade":false},{"valor":35.69,"codigoMotivoDesconto":"68","justificativaMotivoDesconto":"CUPOM350 .......","promocaoId":"","tipo":"SUBTOTAL","fidelidade":false}]},{"produtoId":"20183","funcionarioId":"20","funcionario":{"nome":"ANTONIO LUCAS COIMBRA BEZERRA","numeroDocumento":"39923362809"},"efetivo":true,"quantidade":1.000,"valorUnitario":599.900,"valorTotal":405.44,"acrescimo":0.00,"desconto":194.46,"preco":599.900,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":0.00000,"markup":0,"lucro":332.46080,"descontos":[{"valor":89.98,"codigoMotivoDesconto":"1","justificativaMotivoDesconto":"CUPOM350 .......","promocaoId":"","tipo":"ITEM","fidelidade":false},{"valor":104.48,"codigoMotivoDesconto":"68","justificativaMotivoDesconto":"CUPOM350 .......","promocaoId":"","tipo":"SUBTOTAL","fidelidade":false}]},{"produtoId":"20158","funcionarioId":"20","funcionario":{"nome":"ANTONIO LUCAS COIMBRA BEZERRA","numeroDocumento":"39923362809"},"efetivo":true,"quantidade":1.000,"valorUnitario":94.900,"valorTotal":64.11,"acrescimo":0.00,"desconto":30.79,"preco":94.900,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":0.00000,"markup":0,"lucro":52.57020,"descontos":[{"valor":14.24,"codigoMotivoDesconto":"1","justificativaMotivoDesconto":"CUPOM350 .......","promocaoId":"","tipo":"ITEM","fidelidade":false},{"valor":16.55,"codigoMotivoDesconto":"68","justificativaMotivoDesconto":"CUPOM350 .......","promocaoId":"","tipo":"SUBTOTAL","fidelidade":false}]}],"pagamentos":[{"sequencial":"1","descricao":"POS Crédito","formaPagamentoId":"4","valor":1357.99,"codigoBandeira":"00002","codigoOperadora":"00000","codigoAutorizacao":"087295","quantidadeParcelas":0}],"ecf":{"modelo":"CFE SAT","numeroSerie":"904466","ccf":"010079"},"numeroNota":"010079","origem":"SYSPDV","canalVendaId":"1"},
		"entidade_id" : "001148-001-1-2022-09-16-1",
		"objeto_id" : "001148-001-1-2022-09-16-1",
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
    #for l in tb[1]:
    #    print(l)
    #    print(type(l))
 #

#print(l)

