# Este script faz a leitura dos dados estruturados dos processos de uma base de dados Oracle,
# enriquece o dataset com os dados não estruturados dos conteúdos textuais dos principais documentos
# dos processos que ficam armazenados numa base Elasticsearch e, por fim, grava os dados num arquivo json.

import json
from json.decoder import JSONDecodeError
import os
import cx_Oracle
import pandas as pd
from io import StringIO
from elasticsearch import Elasticsearch
import ast

# Exclusivo para printar na tela conteúdo do doc elastic
import sys
sys.stdout = open(1, 'w', encoding='utf-8', closefd=False)

class DocNotInCacheError(Exception):
  pass

docsCache          = []
idasAoCacheEs      = 0;
idasAoCacheArquivo = 0;
es                 = Elasticsearch('informação sensível omitida')

def obter_lista_processos_banco():
  dsn_tns_prd = cx_Oracle.makedsn('informação sensível omitida')
  conn        = cx_Oracle.connect('informação sensível omitida', dsn=dsn_tns_prd)
  c           = conn.cursor()
  
  c.execute(r'''
    SELECT json_object ( v.id
                       , v.numero_antigo
                       , v.hash
                       , v.tipo_documento_id
                       , v.classe_id
                       , v.classe_descricao
                       , v.competencia_id
                       , v.competencia_descricao
                       , v.numero_partes_polo_ativo
                       , v.numero_partes_polo_passivo
                       , v.cod_tipo_protocolo
                       , v.assistencia_judiciaria
                       , v.urgente_efeito_suspensivo
                       , v.urgente_tutela_provisoria
                       , v.envolve_estado_minas_gerais
                       , v.envolve_ministerio_publico
                       , v.envolve_municipio
                       , key 'assuntos' VALUE (SELECT JSON_ARRAYAGG ( JSON_OBJECT ( a.id
                                                                                  , a.descricao
                                                                                  )
                                                                    )
                                                  FROM 'informação sensível omitida' a
                                              )
                       ) as json
      FROM V_DADOS v
        ''')

  result = {'processos': []}
  
  for row in c:
    linha = json.loads(row[0])
    result['processos'].append(linha)
  
  conn.close()
  return result

def gerar_caminho_completo(nome_arquivo):
  return os.path.join(os.path.dirname(__file__), nome_arquivo)

def inicializar_cache_documentos(nome_arquivo):
  global docsCache
  with open(gerar_caminho_completo(nome_arquivo), "r+", encoding="utf-8") as f:
    try:
      data      = json.load(f)
      docsCache = data['documentos']
    except JSONDecodeError:
      print(f.read())
      docsCache = []

def gravar_cache_documentos(nome_arquivo):
  global docsCache
  gravar_arquivo_json({"documentos": docsCache}, nome_arquivo)

def obter_doc_no_cache(hash):
  global docsCache
  try:
    doc = list(filter(lambda doc: doc['hash'] == hash, docsCache))[0]
  except IndexError:
    raise DocNotInCacheError
  return doc

def obter_documento_es(hash):
  global es
  res = es.search( index="informação sensível omitida"
                 , query={"term": {"_id": {"value": hash}}}
                 )
  if res['hits']['total']['value'] == 1:
    return res['hits']['hits'][0]['_source']['conteudos'][0]

def obter_texto_documento(hash):
  global idasAoCacheEs
  global idasAoCacheArquivo
  global docsCache
  
  try:
    doc = obter_doc_no_cache(hash)
  except DocNotInCacheError:
    doc = None
  
  if not doc:
    doc = obter_documento_es(hash)
    idasAoCacheEs += 1
    docsCache.append({"hash":hash, "conteudo": doc['conteudo']})
  else:
    idasAoCacheArquivo += 1
  return doc['conteudo']

def enriquecer_processos_com_documentos(result):
  global docsCache
  lista_temp = []
  for i, processo in enumerate(result['processos']):
    doc = obter_texto_documento(processo['hash'])
    result['processos'][i]['documento'] = doc

def gravar_arquivo_json(conteudo, nome_arquivo):
  print('Gravando arquivo %s...' % gerar_caminho_completo(nome_arquivo))
  with open(gerar_caminho_completo(nome_arquivo), 'w', encoding="utf-8") as f:
    json.dump(conteudo, f, indent=2, sort_keys=True, ensure_ascii=False)
  
### Início do processamento

inicializar_cache_documentos('cache_documentos.json')
result = obter_lista_processos_banco()
enriquecer_processos_com_documentos(result)
gravar_arquivo_json(result, 'dados.json')
if idasAoCacheEs > 0: gravar_cache_documentos('cache_documentos.json')
print("Quantidade de idas ao cache de documentos do ES: %d" % idasAoCacheEs)
print("Quantidade de idas ao cache de documentos do arquivo: %d" % idasAoCacheArquivo)
