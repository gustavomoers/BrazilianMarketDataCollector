#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import os
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta



this_year = datetime.datetime.today().strftime('%Y')


# In[ ]:


directories = ['BACKUPS',
'BACKUPS/capital_social',
'BACKUPS/final_data',
'BACKUPS/info_companies',
'BACKUPS/last_results',
'BACKUPS/medias',
'BACKUPS/pivoted_data',
'BACKUPS/price',
'BACKUPS/proventos',
'BACKUPS/results',
'BACKUPS/results/bpa',
'BACKUPS/results/bpp',
'BACKUPS/results/dfcmi',
'BACKUPS/results/dmpl',
'BACKUPS/results/dre',
'BACKUPS/results/dva',
'clean_data',
'clean_data/companies_results',
'clean_data/companies_results/BPA',
'clean_data/companies_results/BPP',
'clean_data/companies_results/DFCMI',
'clean_data/companies_results/DMPL',
'clean_data/companies_results/DRE',
'clean_data/companies_results/DVA',
'clean_data/dividends',
'clean_data/final_data',
'clean_data/info_companies',
'clean_data/pivoted_data',
'merged_data_cvm',
'merged_data_cvm/new_data',
'merged_data_cvm/new_data/DFP',
'merged_data_cvm/new_data/FCA',
'merged_data_cvm/new_data/FRE',
'merged_data_cvm/new_data/ITR',
'merged_data_cvm/old_data',
'merged_data_cvm/old_data/DFP',
'merged_data_cvm/old_data/ITR',
'PRICES',
'PRICES/monthly',
'raw_data_B3',
'raw_data_B3/capital_social',
'raw_data_B3/proventos',
'raw_data_B3/proventos/eventos_corporativos',
'raw_data_B3/proventos/historico',
'raw_data_cvm',
'raw_data_cvm/dfp',
'raw_data_cvm/dfp/DFP',
'raw_data_cvm/fca',
'raw_data_cvm/fca/FCA',
'raw_data_cvm/fre',
'raw_data_cvm/fre/FRE',
'raw_data_cvm/itr',
'raw_data_cvm/itr/ITR',
'VALUATION']


# In[ ]:


for path in directories:
    isExist = os.path.exists(path)

    if not isExist:
  
      os.makedirs(path)


# # DOWNLOADING ZIP FROM CVM
# 

# In[ ]:


# Downloading all annual results
"""## Acessando a base de dados e criando arquivos históricos"""
    
from zipfile import ZipFile
import os
import wget

url_base = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/'

"""* criando uma lista com o nome de todos os arquivos"""

arquivos_zip = []
for ano in range(2010,2023):
    arquivos_zip.append(f'dfp_cia_aberta_{ano}.zip')

arquivos_zip

"""* fazendo o download de todos os arquivos"""

path='raw_data_cvm/dfp'
isExist = os.path.exists(path)

if not isExist:
  
  os.makedirs(path)

    
    
for arq in arquivos_zip:
    wget.download(url_base+arq, out=path)
        
"""* extraindo os arquivos zip"""

for arq in arquivos_zip:
    ZipFile(path+'/'+arq, 'r').extractall(path+'/DFP')


# In[ ]:


# Downloading all quarter results (it takes time)
"""## Acessando a base de dados e criando arquivos históricos"""
    
from zipfile import ZipFile
import os
import wget

url_base = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/'

"""* criando uma lista com o nome de todos os arquivos"""

arquivos_zip = []
for ano in range(2011,2023):
    arquivos_zip.append(f'itr_cia_aberta_{ano}.zip')

arquivos_zip

"""* fazendo o download de todos os arquivos"""

path='raw_data_cvm/itr'
isExist = os.path.exists(path)

if not isExist:
  
  os.makedirs(path)

    
    
for arq in arquivos_zip:
    wget.download(url_base+arq, out=path)
        
"""* extraindo os arquivos zip"""

for arq in arquivos_zip:
    ZipFile(path+'/'+arq, 'r').extractall(path+'/ITR')


# In[ ]:


# Downloading all FRE
"""## Acessando a base de dados e criando arquivos históricos"""
    
from zipfile import ZipFile
import os
import wget

url_base = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/'

"""* criando uma lista com o nome de todos os arquivos"""

arquivos_zip = []
for ano in range(2010,2023):
    arquivos_zip.append(f'fre_cia_aberta_{ano}.zip')

arquivos_zip

"""* fazendo o download de todos os arquivos"""

path='raw_data_cvm/fre'
isExist = os.path.exists(path)

if not isExist:
  
  os.makedirs(path)

    
    
for arq in arquivos_zip:
    wget.download(url_base+arq, out=path)
        
"""* extraindo os arquivos zip"""

for arq in arquivos_zip:
    ZipFile(path+'/'+arq, 'r').extractall(path+'/FRE')


# In[ ]:


# Dowloading all FCA
"""## Acessando a base de dados e criando arquivos históricos"""
    
from zipfile import ZipFile
import os
import wget

url_base = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FCA/DADOS/'

"""* criando uma lista com o nome de todos os arquivos"""

arquivos_zip = []
for ano in range(2010,2023):
    arquivos_zip.append(f'fca_cia_aberta_{ano}.zip')

arquivos_zip

"""* fazendo o download de todos os arquivos"""

path='raw_data_cvm/fca'
isExist = os.path.exists(path)

if not isExist:
  
  os.makedirs(path)

    
    
for arq in arquivos_zip:
    wget.download(url_base+arq, out=path)
        
"""* extraindo os arquivos zip"""

for arq in arquivos_zip:
    ZipFile(path+'/'+arq, 'r').extractall(path+'/FCA')


# # Concating 2010-2016 data

# In[ ]:


## CONCATING DFP 2010-2016
nomes = ['BPA_con', 'BPA_ind', 'BPP_con', 'BPP_ind', 'DFC_MI_con', 'DFC_MI_ind', 'DRE_con', 'DRE_ind', 'DVA_con', 'DVA_ind', 'DFC_MD_con', 'DFC_MD_ind']
for nome in nomes:
    arquivo = pd.DataFrame()
    for ano in range(2010,2017):
        arquivo = pd.concat([arquivo, pd.read_csv(f'raw_data_cvm/dfp/DFP/dfp_cia_aberta_{nome}_{ano}.csv', sep=';', decimal=',', encoding='ISO-8859-1')])
    
    arquivo['VL_CONTA'] = arquivo['VL_CONTA'].apply(lambda x: float(x))
    arquivo.to_pickle(f'merged_data_cvm/old_data/DFP/dfp_cia_aberta_{nome}_2010-2016.pkl')


# In[ ]:


## CONCATING DMPL 2010-2016
nomes = ['DMPL_con','DMPL_ind']
for nome in nomes:
    arquivo = pd.DataFrame()
    for ano in range(2010,2017):
        arquivo = pd.concat([arquivo, pd.read_csv(f'raw_data_cvm/dfp/DFP/dfp_cia_aberta_{nome}_{ano}.csv', sep=';', decimal=',', encoding='ISO-8859-1')])
    
    arquivo['COLUNA_DF'] =np.select([arquivo['COLUNA_DF'].isna()],['PL CON'],arquivo['COLUNA_DF'])
    
    dmplass = ['Patrimônio Líquido Consolidado','Participação dos Não Controladores','Patrimônio Líquido','Reservas de Lucro',
          'Lucros ou Prejuízos Acumulados','PL CON']
    arquivo = arquivo.query("COLUNA_DF == @dmplass")
    arquivo = arquivo.query("DS_CONTA == ['Saldos Iniciais','Saldos Finais']")
    arquivo['VL_CONTA'] = arquivo['VL_CONTA'].apply(lambda x: float(x))
    
    arquivo.to_pickle(f'merged_data_cvm/old_data/DFP/dfp_cia_aberta_{nome}_2010-2016.pkl')


# In[ ]:


## CONCATING ITR 2011-2016
nomes = ['BPA_con', 'BPA_ind', 'BPP_con', 'BPP_ind', 'DFC_MI_con', 'DFC_MI_ind', 'DRE_con', 'DRE_ind', 'DVA_con', 'DVA_ind', 'DFC_MD_con', 'DFC_MD_ind']
for nome in nomes:
    arquivo = pd.DataFrame()
    for ano in range(2011,2017):
        arquivo = pd.concat([arquivo, pd.read_csv(f'raw_data_cvm/itr/ITR/itr_cia_aberta_{nome}_{ano}.csv', sep=';', decimal=',', encoding='ISO-8859-1')])
    
    arquivo['VL_CONTA'] = arquivo['VL_CONTA'].apply(lambda x: float(x))    
    arquivo.to_pickle(f'merged_data_cvm/old_data//ITR/itr_cia_aberta_{nome}_2011-2016.pkl')


# In[ ]:


## CONCATING DMPL QUARTELY 2011-2016

nomes = ['DMPL_con','DMPL_ind']
for nome in nomes:
    arquivo = pd.DataFrame()
    for ano in range(2011,2017):
        arquivo = pd.concat([arquivo, pd.read_csv(f'raw_data_cvm/itr/ITR/itr_cia_aberta_{nome}_{ano}.csv', sep=';', decimal=',', encoding='ISO-8859-1')])
    
    arquivo['COLUNA_DF'] =np.select([arquivo['COLUNA_DF'].isna()],['PL CON'],arquivo['COLUNA_DF'])
    
    dmplass = ['Patrimônio Líquido Consolidado','Participação dos Não Controladores','Patrimônio Líquido','Reservas de Lucro',
          'Lucros ou Prejuízos Acumulados','PL CON']
    arquivo = arquivo.query("COLUNA_DF == @dmplass")
    arquivo = arquivo.query("DS_CONTA == ['Saldos Iniciais','Saldos Finais']")   
    
    arquivo['VL_CONTA'] = arquivo['VL_CONTA'].apply(lambda x: float(x))
    arquivo.to_pickle(f'merged_data_cvm/old_data/ITR/itr_cia_aberta_{nome}_2011-2016.pkl')


# In[ ]:


## Getting ALL FRE'S FROM 2010 TO 2021
nomes = ['posicao_acionaria','distribuicao_capital']
for nome in nomes:
    arquivo = pd.DataFrame()
    for ano in range(2010,int(this_year)+1):
        arquivo = pd.concat([arquivo, pd.read_csv(f'raw_data_cvm/fre/FRE/fre_cia_aberta_{nome}_{ano}.csv', sep=';', decimal=',', encoding='ISO-8859-1')])
    arquivo.to_pickle(f'merged_data_cvm/new_data/FRE/fre_cia_aberta_{nome}_tudo.pkl')


# In[ ]:


## Getting ALL FCA'S FROM 2010 TO 2021
nomes = ['geral']
for nome in nomes:
    arquivo = pd.DataFrame()
    for ano in range(2010,int(this_year)+1):
        arquivo = pd.concat([arquivo, pd.read_csv(f'raw_data_cvm/fca/FCA/fca_cia_aberta_{nome}_{ano}.csv', sep=';', decimal=',', encoding='ISO-8859-1')])
    arquivo.to_pickle(f'merged_data_cvm/new_data/FCA/fca_cia_aberta_{nome}_tudo.pkl')


# In[ ]:




