import json
#import findspark
#import boto3

#findspark.init()

#import pyspark
#import pyspark.sql.functions as f
#from pyspark.sql.types import *
from datetime import datetime
#from pyspark.sql import SparkSession


#spark = SparkSession.builder.getOrCreate()

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
                        j = lj[3]
                        nj = {}
                        if type(j) == dict:
                            j[self.idTable] = lj[0]
                            for k, v in j.items():
                                if type(v) == list or type(v) == dict:
                                    if self.partition != "" and self.partitionReferenceColumn != "":
                                        items.append((lj[0],None,re.sub(self._regex, '', k),v,lj[4]))
                                    else:
                                        items.append((lj[0],index,re.sub(self._regex, '', k),v))
                                    if type(v) == list:
                                        parseJson(None,items)
                                else:
                                    nj[re.sub(self._regex, '', k)]=v
                                    if self.partition != "" and self.partitionReferenceColumn != "":
                                            nj[self.partition] = lj[4]

                            self.listTable.append((lj[2],nj))

                        elif type(j) == list:
                            for item in j:
                                nj = {}
                                try:
                                    for k,v in item.items():
                                        if type(v) == list or type(v) == dict:
                                            if self.partition != "" and self.partitionReferenceColumn != "":
                                                items.append((lj[0],None,re.sub(self._regex, '', k),v,lj[4]))
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
		"id" : 5994,
		"identificador" : "VENDA",
		"operacao" : "ATUALIZACAO",
		"prioridade" : 170,
		"body" : {"id":"000004-001-1-2022-08-01","sequencial":"000004","numeroCaixa":"001","data":"2022-08-01","dataHoraInicio":"2022-08-01T09:39:43","dataHoraFim":"2022-08-01T09:43:07","lojaId":"1","dataHoraVenda":"2022-08-01T09:43:07","funcionarioId":"999","funcionario":{"nome":"cm"},"efetiva":false,"tipoPreco":"1","tipoDesconto":"","valor":574.10,"acrescimo":0.00,"desconto":0.00,"servico":0.000,"quantidadeItens":6,"quantidadeItensCancelados":2,"valorItensCancelados":3474.90,"itens":[{"produtoId":"4","funcionarioId":"0","efetivo":true,"quantidade":1.000,"valorUnitario":55.500,"valorTotal":55.50,"acrescimo":0.00,"desconto":0.00,"preco":55.500,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":42.61000,"markup":30.2500,"lucro":2.90000},{"produtoId":"3","funcionarioId":"0","efetivo":true,"quantidade":1.000,"valorUnitario":52.100,"valorTotal":52.10,"acrescimo":0.00,"desconto":0.00,"preco":52.100,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":0.00000,"markup":0,"lucro":42.72200},{"produtoId":"2","funcionarioId":"0","efetivo":true,"quantidade":1.000,"valorUnitario":64.800,"valorTotal":64.80,"acrescimo":0.00,"desconto":0.00,"preco":64.800,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":0.00000,"markup":0,"lucro":53.13600},{"produtoId":"1","funcionarioId":"0","efetivo":true,"quantidade":1.000,"valorUnitario":61.900,"valorTotal":61.90,"acrescimo":0.00,"desconto":0.00,"preco":61.900,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":0.00000,"markup":0,"lucro":50.75800},{"produtoId":"20004","funcionarioId":"0","efetivo":false,"quantidade":1.000,"valorUnitario":2940.000,"valorTotal":2940.00,"acrescimo":0.00,"desconto":0.00,"preco":2940.000,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":0.00000,"markup":0,"lucro":2410.80000},{"produtoId":"20003","funcionarioId":"0","efetivo":false,"quantidade":1.000,"valorUnitario":534.900,"valorTotal":534.90,"acrescimo":0.00,"desconto":0.00,"preco":534.900,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":0.00000,"markup":0,"lucro":438.61800},{"produtoId":"20002","funcionarioId":"0","efetivo":true,"quantidade":1.000,"valorUnitario":144.900,"valorTotal":144.90,"acrescimo":0.00,"desconto":0.00,"preco":144.900,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":0.00000,"markup":0,"lucro":118.81800},{"produtoId":"20001","funcionarioId":"0","efetivo":true,"quantidade":1.000,"valorUnitario":194.900,"valorTotal":194.90,"acrescimo":0.00,"desconto":0.00,"preco":194.900,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":0.00000,"markup":0,"lucro":159.81800}],"pagamentos":[{"sequencial":"1","descricao":"DINHEIRO","formaPagamentoId":"1","valor":574.10,"codigoAutorizacao":"","quantidadeParcelas":0}],"cancelamento":{"numeroCaixa":"001","sequencial":"000005","data":"2022-08-01","dataHoraInicio":"2022-08-01T09:43:52","dataHoraFim":"2022-08-01T09:44:38","funcionarioId":"999","funcionario":{"nome":"cm"}},"ecf":{"modelo":"CFE SAT","numeroSerie":"904466","ccf":"009256"},"numeroNota":"009256","origem":"SYSPDV","canalVendaId":"1"},
		"entidade_id" : "000004-001-1-2022-08-01-7",
		"objeto_id" : "000004-001-1-2022-08-01-7",
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
    "principalTableName":"tbpessoal",

}

#del e
e = ExplodeJson(config)

e.recursiveParseJson(json.loads(j))

e.refineTables()

for tb in e.listRefineTables:
    print(tb[0])
    print(tb[1])

