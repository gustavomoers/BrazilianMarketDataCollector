#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import os
import time

import warnings
warnings.filterwarnings("ignore")


this_year = datetime.datetime.today().strftime('%Y')
today = datetime.datetime.today().strftime('%Y-%m-%d')


# # SCRAPPING B3

# In[ ]:


import ssl

ssl._create_default_https_context = ssl._create_unverified_context

## total shares at B3 (updates each saturday)

capital_social = pd.read_html('http://bvmf.bmfbovespa.com.br/CapitalSocial/', decimal=',', thousands='.')[0]

capital_social.drop_duplicates('Código',inplace=True)
capital_social.set_index('Código',inplace=True)



if os.path.exists('raw_data_B3/capital_social/capitalsocial.pkl'):
    os.remove('raw_data_B3/capital_social/capitalsocial.pkl')


capital_social.to_pickle('raw_data_B3/capital_social/capitalsocial.pkl')

##backup

capital_social.to_pickle('BACKUPS/capital_social/capitalsocial%s.pkl'%today)


# In[ ]:


## scrapping all dividends data from B3

info = pd.read_pickle('clean_data/info_companies/info_companies_geral_comSetor.pkl')

codigo = info['Codigo_CVM'].unique()


# ## SCRAPPING PREVIOUS DIVIDENDS PAGE

# In[ ]:


import threading, urllib.request
import queue

urls_to_load = []
for i in codigo:
    urls_to_load.append("http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/ResumoProventosDinheiro.aspx?codigoCvm=%s&tab=3.1&idioma=pt-br"%i)

for x in range(30,391,30):
    def read_url(url, queue,cvm):
        data = pd.read_html(url, decimal=',', thousands='.')[0]
        data.to_pickle("raw_data_B3/proventos/historico/%s.pkl"%cvm)
        queue.put(data)

    def fetch_parallel():
        result = queue.Queue()
        threads = [threading.Thread(target=read_url, args = (url,result,cvm)) for url,cvm in zip(urls_to_load[x-30:x],codigo[x-30:x])]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return result
    
    
    fetch_parallel()
    time.sleep(5)


# In[ ]:


all_divd = pd.DataFrame()
for x in codigo:
    try:
        comp = pd.read_pickle("raw_data_b3/proventos/historico/%s.pkl"%x)
        comp['Codigo_CVM'] = x
        all_divd = pd.concat([all_divd,comp])
    except:
        continue


# In[ ]:


all_divd["Últ. Dia 'Com'"] = pd.to_datetime(all_divd["Últ. Dia 'Com'"], format='%d/%m/%Y')

all_divd = all_divd.loc[all_divd["Últ. Dia 'Com'"].dt.year >2006]

all_divd.drop(['Data da Aprovação (I)','Proventos por unidade ou mil','Preço por unidade ou mil',
          "Data do Últ. Preço 'Com' (III)"],inplace=True,axis=1)

all_divd.rename({'Tipo do Provento (II)':'Tipo Provento',"Últ. Dia 'Com'":"Data COM","Últ. Preço 'Com'":"Preço Data COM",
            'Valor do Provento (R$)':'Valor','Tipo de Ativo':'CLASSE'},inplace=True,axis=1)

all_divd.to_pickle('raw_data_b3/proventos/historico/all_proventos_hist.pkl')
all_divd.to_pickle('BACKUPS/proventos/all_proventos_hist%s.pkl'%today)


# ## SCRAPPING NEW DIVIDENDS PAGE

# In[ ]:


import threading, urllib.request
import queue

urls_to_load = []
for i in codigo:
    urls_to_load.append("http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/ResumoEventosCorporativos.aspx?codigoCvm=%s&tab=3&idioma=pt-br"%i)

for x in range(30,391,30):
    def read_url(url, queue,cvm):
        data = pd.read_html(url, decimal=',', thousands='.')
        eventos_corp = pd.DataFrame()
        for x in range(2,len(data)):
            resum = data[x]
            eventos_corp = pd.concat([eventos_corp,resum])
        eventos_corp.to_pickle("raw_data_B3/proventos/eventos_corporativos/%s.pkl"%cvm)
        queue.put(eventos_corp)

    def fetch_parallel():
        result = queue.Queue()
        threads = [threading.Thread(target=read_url, args = (url,result,cvm)) for url,cvm in zip(urls_to_load[x-30:x],codigo[x-30:x])]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return result
    
    
    fetch_parallel()
    time.sleep(5)


# In[ ]:


all_event = pd.DataFrame()
for x in codigo:
    try:
        comp = pd.read_pickle("raw_data_B3/proventos/eventos_corporativos/%s.pkl"%x)
        comp['Codigo_CVM'] = x
        all_event = pd.concat([all_event,comp])
    except:
        continue


# In[ ]:


## cleaning all events b3

all_event.rename({'% / Fator de Grupamento':'Fator','Negócios com até':'Data COM','Deliberado em':'Aprovado',
                 'Valor (R$)':'Valor','Início de Pagamento':'Pagamento'},inplace=True,axis=1)

all_event.drop([ 'Relativo a','Observações','Aprovado'],axis=1,inplace=True)

all_event['Data COM']=pd.to_datetime(all_event['Data COM'], format='%d/%m/%Y')

all_event['CLASSE'] = [x[-3:-1] for x in all_event['Código ISIN']]

conditions = [all_event['CLASSE']=='OR',all_event['CLASSE']=='PR',all_event['CLASSE']=='PA',all_event['CLASSE']=='PB',
             all_event['CLASSE']=='PC',all_event['CLASSE']=='PD',all_event['CLASSE']=='PE',all_event['CLASSE']=='PF',
             all_event['CLASSE']=='PG']
choices = ['ON','PN','PNA','PNB','PNC','PND','PNE','PNF','PNG']
all_event['CLASSE'] = np.select(conditions,choices,'UNT')

all_event = all_event.loc[(all_event['Data COM'].dt.year > 2006)]


# In[ ]:


## cleaning and adjusting GRUPAMENTO

grupamento = all_event.loc[all_event['Proventos'] == 'GRUPAMENTO']

grupamento.drop(grupamento.columns.difference(['Proventos', 'Código ISIN',
       'Data COM', 'Fator', 'Ativo Emitido', 'Codigo_CVM','CLASSE']),axis=1,inplace=True)


grupamento['Fator'] = grupamento['Fator'].map(int)

grupamento['str_fator'] = grupamento['Fator'].map(str)

grupamento['count'] = grupamento['str_fator'].map(len)

def divide(row):
    vdd = '1'
    ldd = vdd+'0'*row
    
    return ldd

grupamento['divider'] = grupamento['count'].map(divide)

grupamento['divider'] = grupamento['divider'].map(int)

grupamento['Fator Grupamento'] = grupamento['Fator']/grupamento['divider']

grupamento = grupamento.loc[(grupamento['Data COM'].dt.year > 2006)]


grupamento.drop(['Fator', 'Ativo Emitido','str_fator', 'count', 'divider'],axis=1,inplace=True)


# In[ ]:


## cleaning and adjusting DESDOBRAMENTO
desd = all_event.loc[all_event['Proventos'] == 'DESDOBRAMENTO']

desd.drop(desd.columns.difference(['Proventos', 'Código ISIN',
       'Data COM', 'Fator', 'Ativo Emitido', 'Codigo_CVM','CLASSE']),axis=1,inplace=True)

desd['Fator'] = desd['Fator'].map(int)

desd = desd.loc[(desd['Data COM'].dt.year >2005)]

desd = desd.loc[(desd['Fator'] <3000)]

desd['Fator Desdobramento'] = (desd['Fator']+100)/100
desd['Fator Desdobramento'] = desd['Fator Desdobramento'].map(int)

desd.drop(['Fator', 'Ativo Emitido'],axis=1,inplace=True)


# In[ ]:


## cleaning proventos
div = all_event.loc[all_event['Proventos'].isin(['DIVIDENDO', 'JRS CAP PROPRIO','RENDIMENTO'])]

div.drop(div.columns.difference(['Proventos', 'Código ISIN',
       'Data COM', 'Codigo_CVM','CLASSE','Valor','Pagamento']),axis=1,inplace=True)


# In[ ]:


## concating and saving dados de proventos, grupamentos e desdobramentos
tudo = pd.concat([div,grupamento,desd])

tudo.to_pickle('raw_data_B3/proventos/eventos_corporativos/proventos_atuais_b3.pkl')
tudo.to_pickle('BACKUPS/proventos/proventos_atuais_b3%s.pkl'%today)


# # Merging previous and new dividends info

# In[ ]:


## criando df PROVENTOS atuais e historicos AJUSTADO

tudo= pd.read_pickle("raw_data_B3/proventos/eventos_corporativos/proventos_atuais_b3.pkl")

tudo.reset_index(inplace=True, drop=True)

divih = pd.read_pickle("raw_data_B3/proventos/historico/all_proventos_hist.pkl")


divih.rename({"Tipo Provento":"Proventos"},axis=1,inplace=True)


divih['Data COM']=pd.to_datetime(divih['Data COM'], format='%Y-%m-%d')

divih.reset_index(inplace=True, drop=True)

all_div = pd.concat([divih,tudo])

##ajustando desdobramentos magalu
all_div=all_div.append({'Proventos':"DESDOBRAMENTO",'Data COM':'2019-07-31','CLASSE':'ON','Fator Desdobramento':8,'Codigo_CVM':22470},
              ignore_index=True)

all_div=all_div.append({'Proventos':"DESDOBRAMENTO",'Data COM':'2017-09-04','CLASSE':'ON','Fator Desdobramento':8,'Codigo_CVM':22470},
              ignore_index=True)


## excluindo grupamento da LEVE3
all_div.drop(all_div.loc[(all_div['Codigo_CVM'] == 8575) & (all_div['Proventos'] =='GRUPAMENTO')].index,inplace=True)



all_div['Data COM']=pd.to_datetime(all_div['Data COM'], format='%Y-%m-%d')

all_div.sort_values(['Data COM'],inplace=True,ascending=False)

all_div['Fator Desdobramento'] = all_div.groupby(['Codigo_CVM', 'CLASSE'])['Fator Desdobramento'].cumprod()

all_div['Fator Grupamento'] = all_div.groupby(['Codigo_CVM', 'CLASSE'])['Fator Grupamento'].cumprod()

all_div.sort_values(['Data COM'],inplace=True)

all_div['Fator Grupamento'] = all_div.groupby(['Codigo_CVM', 'CLASSE'])['Fator Grupamento'].bfill().fillna(1)

all_div['Fator Desdobramento'] = all_div.groupby(['Codigo_CVM', 'CLASSE'])['Fator Desdobramento'].bfill().fillna(1)

all_div['Valor ajustado'] = (all_div['Valor'])/(all_div['Fator Desdobramento']*all_div['Fator Grupamento'])

all_div['Preço COM ajustado'] = (all_div['Preço Data COM'])/(all_div['Fator Desdobramento']*all_div['Fator Grupamento'])

all_div['Provento/Preço(%) ajustado'] = all_div['Valor ajustado']/all_div['Preço COM ajustado']

all_div['ano'] = all_div['Data COM'].dt.year

all_div['Pagamento']=pd.to_datetime(all_div['Pagamento'], format='%d/%m/%Y', errors='coerce')

all_div.drop_duplicates(['Codigo_CVM','CLASSE', 'Valor','Data COM'],inplace=True)


# In[ ]:


all_div.to_pickle("clean_data/dividends/proventos_b3.pkl")
all_div.to_pickle("clean_data/final_data/proventos_b3.pkl")

#backup

all_div.to_pickle("BACKUPS/proventos/proventos_b3%s.pkl"%today)

