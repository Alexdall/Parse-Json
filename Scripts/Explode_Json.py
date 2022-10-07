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
		"id" : 178914,
		"identificador" : "VENDA",
		"operacao" : "ATUALIZACAO",
		"prioridade" : 170,
		"body" : {"id":"001146-001-1-2022-09-16","sequencial":"001146","numeroCaixa":"001","data":"2022-09-16","dataHoraInicio":"2022-09-16T20:48:18","dataHoraFim":"2022-09-16T20:53:36","lojaId":"1","dataHoraVenda":"2022-09-16T20:53:36","funcionarioId":"3","funcionario":{"nome":"Cassila Teles Inasawa Santos","numeroDocumento":"06107294627"},"clienteId":"473","efetiva":true,"tipoPreco":"1","tipoDesconto":"","valor":318.05,"acrescimo":0.00,"desconto":201.65,"servico":0.000,"quantidadeItens":2,"quantidadeItensCancelados":0,"valorItensCancelados":0.00,"itens":[{"produtoId":"20802","funcionarioId":"20","funcionario":{"nome":"ANTONIO LUCAS COIMBRA BEZERRA","numeroDocumento":"39923362809"},"efetivo":true,"quantidade":2.000,"valorUnitario":179.900,"valorTotal":220.20,"acrescimo":0.00,"desconto":139.60,"preco":179.900,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":0.00000,"markup":0,"lucro":180.56400,"descontos":[{"valor":71.96,"codigoMotivoDesconto":"2","justificativaMotivoDesconto":"CONFRADE FLVHW92Z","promocaoId":"","tipo":"ITEM","fidelidade":false},{"valor":67.64,"codigoMotivoDesconto":"68","justificativaMotivoDesconto":"CONFRADE FLVHW92Z","promocaoId":"","tipo":"SUBTOTAL","fidelidade":false}]},{"produtoId":"20196","funcionarioId":"20","funcionario":{"nome":"ANTONIO LUCAS COIMBRA BEZERRA","numeroDocumento":"39923362809"},"efetivo":true,"quantidade":1.000,"valorUnitario":159.900,"valorTotal":97.85,"acrescimo":0.00,"desconto":62.05,"preco":159.900,"servico":0.0000,"tipoPreco":"1","promocao":false,"custo":0.00000,"markup":0,"lucro":80.23700,"descontos":[{"valor":31.98,"codigoMotivoDesconto":"2","justificativaMotivoDesconto":"CONFRADE FLVHW92Z","promocaoId":"","tipo":"ITEM","fidelidade":false},{"valor":30.07,"codigoMotivoDesconto":"68","justificativaMotivoDesconto":"CONFRADE FLVHW92Z","promocaoId":"","tipo":"SUBTOTAL","fidelidade":false}]}],"pagamentos":[{"sequencial":"1","descricao":"POS Crédito","formaPagamentoId":"4","valor":318.05,"codigoBandeira":"00002","codigoOperadora":"00005","codigoAutorizacao":"089254","quantidadeParcelas":0}],"ecf":{"modelo":"CFE SAT","numeroSerie":"904466","ccf":"010077"},"numeroNota":"010077","origem":"SYSPDV","canalVendaId":"1"},
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

