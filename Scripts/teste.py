import json
import re

f = open('/home/alexandre/PycharmProjects/Parse_json/Fontes/vendas-fgpinheiros-grandcru_3.json','r')
f = f.read()
f = "'''"+f+"'''"
print(f)
#f.close()
#print(type(f))
#q = str(f)
#f = str(open('/home/alexandre/PycharmProjects/Parse_json/Fontes/vendas-fgpinheiros-grandcru_2.json'))

f1 = '''{"a":[
       {
        "id" : 5993,
        "identificador" : "VENDA",
        "operacao" : "ATUALIZACAO",
        "prioridade" : 170,
        "body" : {\"id\":\"000004-001-1-2022-08-01\",\"sequencial\":\"000004\",\"numeroCaixa\":\"001\",\"data\":\"2022-08-01\",\"dataHoraInicio\":\"2022-08-01T09:39:43\",\"dataHoraFim\":\"2022-08-01T09:43:07\",\"lojaId\":\"1\",\"dataHoraVenda\":\"2022-08-01T09:43:07\",\"funcionarioId\":\"999\",\"funcionario\":{\"nome\":\"cm\"},\"efetiva\":false,\"tipoPreco\":\"1\",\"tipoDesconto\":\"\",\"valor\":574.10,\"acrescimo\":0.00,\"desconto\":0.00,\"servico\":0.000,\"quantidadeItens\":6,\"quantidadeItensCancelados\":2,\"valorItensCancelados\":3474.90,\"itens\":[{\"produtoId\":\"4\",\"funcionarioId\":\"0\",\"efetivo\":true,\"quantidade\":1.000,\"valorUnitario\":55.500,\"valorTotal\":55.50,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":55.500,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":42.61000,\"markup\":30.2500,\"lucro\":2.90000},{\"produtoId\":\"3\",\"funcionarioId\":\"0\",\"efetivo\":true,\"quantidade\":1.000,\"valorUnitario\":52.100,\"valorTotal\":52.10,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":52.100,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":0.00000,\"markup\":0,\"lucro\":42.72200},{\"produtoId\":\"2\",\"funcionarioId\":\"0\",\"efetivo\":true,\"quantidade\":1.000,\"valorUnitario\":64.800,\"valorTotal\":64.80,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":64.800,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":0.00000,\"markup\":0,\"lucro\":53.13600},{\"produtoId\":\"1\",\"funcionarioId\":\"0\",\"efetivo\":true,\"quantidade\":1.000,\"valorUnitario\":61.900,\"valorTotal\":61.90,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":61.900,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":0.00000,\"markup\":0,\"lucro\":50.75800},{\"produtoId\":\"20004\",\"funcionarioId\":\"0\",\"efetivo\":false,\"quantidade\":1.000,\"valorUnitario\":2940.000,\"valorTotal\":2940.00,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":2940.000,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":0.00000,\"markup\":0,\"lucro\":2410.80000},{\"produtoId\":\"20003\",\"funcionarioId\":\"0\",\"efetivo\":false,\"quantidade\":1.000,\"valorUnitario\":534.900,\"valorTotal\":534.90,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":534.900,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":0.00000,\"markup\":0,\"lucro\":438.61800},{\"produtoId\":\"20002\",\"funcionarioId\":\"0\",\"efetivo\":true,\"quantidade\":1.000,\"valorUnitario\":144.900,\"valorTotal\":144.90,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":144.900,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":0.00000,\"markup\":0,\"lucro\":118.81800},{\"produtoId\":\"20001\",\"funcionarioId\":\"0\",\"efetivo\":true,\"quantidade\":1.000,\"valorUnitario\":194.900,\"valorTotal\":194.90,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":194.900,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":0.00000,\"markup\":0,\"lucro\":159.81800}],\"pagamentos\":[{\"sequencial\":\"1\",\"descricao\":\"DINHEIRO\",\"formaPagamentoId\":\"1\",\"valor\":574.10,\"codigoAutorizacao\":\"\",\"quantidadeParcelas\":0}],\"cancelamento\":{\"numeroCaixa\":\"001\",\"sequencial\":\"000005\",\"data\":\"2022-08-01\",\"dataHoraInicio\":\"2022-08-01T09:43:52\",\"dataHoraFim\":\"2022-08-01T09:44:38\",\"funcionarioId\":\"999\",\"funcionario\":{\"nome\":\"cm\"}},\"ecf\":{\"modelo\":\"CFE SAT\",\"numeroSerie\":\"904466\",\"ccf\":\"009256\"},\"numeroNota\":\"009256\",\"origem\":\"SYSPDV\",\"canalVendaId\":\"1\"},
        "entidad                                                                                                                                                                                                                                                                                                e_id" : "000004-001-1-2022-08-01-7",
        "objeto_id" : "000004-001-1-2022-08-01-7",
        "origem" : "VAREJOFACIL"
       },
	   {
	    "id" : 5994,
		"identificador" : "VENDA",
		"operacao" : "ATUALIZACAO",
		"prioridade" : 170,
		"body" : {\"id\":\"000004-001-1-2022-08-01\",\"sequencial\":\"000004\",\"numeroCaixa\":\"001\",\"data\":\"2022-08-01\",\"dataHoraInicio\":\"2022-08-01T09:39:43\",\"dataHoraFim\":\"2022-08-01T09:43:07\",\"lojaId\":\"1\",\"dataHoraVenda\":\"2022-08-01T09:43:07\",\"funcionarioId\":\"999\",\"funcionario\":{\"nome\":\"cm\"},\"efetiva\":false,\"tipoPreco\":\"1\",\"tipoDesconto\":\"\",\"valor\":574.10,\"acrescimo\":0.00,\"desconto\":0.00,\"servico\":0.000,\"quantidadeItens\":6,\"quantidadeItensCancelados\":2,\"valorItensCancelados\":3474.90,\"itens\":[{\"produtoId\":\"4\",\"funcionarioId\":\"0\",\"efetivo\":true,\"quantidade\":1.000,\"valorUnitario\":55.500,\"valorTotal\":55.50,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":55.500,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":42.61000,\"markup\":30.2500,\"lucro\":2.90000},{\"produtoId\":\"3\",\"funcionarioId\":\"0\",\"efetivo\":true,\"quantidade\":1.000,\"valorUnitario\":52.100,\"valorTotal\":52.10,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":52.100,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":0.00000,\"markup\":0,\"lucro\":42.72200},{\"produtoId\":\"2\",\"funcionarioId\":\"0\",\"efetivo\":true,\"quantidade\":1.000,\"valorUnitario\":64.800,\"valorTotal\":64.80,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":64.800,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":0.00000,\"markup\":0,\"lucro\":53.13600},{\"produtoId\":\"1\",\"funcionarioId\":\"0\",\"efetivo\":true,\"quantidade\":1.000,\"valorUnitario\":61.900,\"valorTotal\":61.90,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":61.900,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":0.00000,\"markup\":0,\"lucro\":50.75800},{\"produtoId\":\"20004\",\"funcionarioId\":\"0\",\"efetivo\":false,\"quantidade\":1.000,\"valorUnitario\":2940.000,\"valorTotal\":2940.00,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":2940.000,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":0.00000,\"markup\":0,\"lucro\":2410.80000},{\"produtoId\":\"20003\",\"funcionarioId\":\"0\",\"efetivo\":false,\"quantidade\":1.000,\"valorUnitario\":534.900,\"valorTotal\":534.90,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":534.900,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":0.00000,\"markup\":0,\"lucro\":438.61800},{\"produtoId\":\"20002\",\"funcionarioId\":\"0\",\"efetivo\":true,\"quantidade\":1.000,\"valorUnitario\":144.900,\"valorTotal\":144.90,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":144.900,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":0.00000,\"markup\":0,\"lucro\":118.81800},{\"produtoId\":\"20001\",\"funcionarioId\":\"0\",\"efetivo\":true,\"quantidade\":1.000,\"valorUnitario\":194.900,\"valorTotal\":194.90,\"acrescimo\":0.00,\"desconto\":0.00,\"preco\":194.900,\"servico\":0.0000,\"tipoPreco\":\"1\",\"promocao\":false,\"custo\":0.00000,\"markup\":0,\"lucro\":159.81800}],\"pagamentos\":[{\"sequencial\":\"1\",\"descricao\":\"DINHEIRO\",\"formaPagamentoId\":\"1\",\"valor\":574.10,\"codigoAutorizacao\":\"\",\"quantidadeParcelas\":0}],\"cancelamento\":{\"numeroCaixa\":\"001\",\"sequencial\":\"000005\",\"data\":\"2022-08-01\",\"dataHoraInicio\":\"2022-08-01T09:43:52\",\"dataHoraFim\":\"2022-08-01T09:44:38\",\"funcionarioId\":\"999\",\"funcionario\":{\"nome\":\"cm\"}},\"ecf\":{\"modelo\":\"CFE SAT\",\"numeroSerie\":\"904466\",\"ccf\":\"009256\"},\"numeroNota\":\"009256\",\"origem\":\"SYSPDV\",\"canalVendaId\":\"1\"},
		"entidade_id" : "000004-001-1-2022-08-01-7",
		"objeto_id" : "000004-001-1-2022-08-01-7",
		"origem" : "VAREJOFACIL"
	   }
	   ]}'''

y=json.loads(f)
print('y='+str(type(y)))
print(y)
#for key, val in y.items():
    #if isinstance(val, str):
    #    val = val.replace('{','@')
#        print(val)
#        print(type(val))
#print(type(y))
#print(y)


d1 = {}
for i in y:
    # print(i)
    d1 = i
print(type(d1))

# for k,v in d1.items():
#    print(str(k)+':'+str(v))
#    print(type(k))
#    print(type(v))

# print(d1)

def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            print(0)
            for k, v in obj.items():
                # print(k)
                #print(v)
                #print(type(v))

                if isinstance(v, (dict, list)):
                    # print(v)
                    # print(type(v))
                    print('1')
                    extract(v, arr, key)
                elif k == key:
                    print('2')
                    arr.append(v)
        elif isinstance(obj, list):
            print('3')
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values


names = json_extract(y, 'funcionarioId')
print(names)
print('names' + str(type(names)))

#dic = {}
#for x in names:
#    print('aqui')
#    print(x)
#    dic = x

#print(type(dic))

#dic2 = '{"id":"000004-001-1-2022-08-01","sequencial":"000004","canalVendaId":"1"}'
#print(type(dic2))
