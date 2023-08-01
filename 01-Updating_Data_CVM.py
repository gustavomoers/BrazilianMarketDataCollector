#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import datetime
import os
import wget
from zipfile import ZipFile

this_year = datetime.datetime.today().strftime('%Y')


# # ATUALIZA ULTIMOS DADOS LANÇADOS NA CVM

# In[ ]:


# Updating last years results


url_base = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/'

"""* criando uma lista com o nome de todos os arquivos"""

arquivos_zip = []
for ano in range(2017,int(this_year)+1):
    arquivos_zip.append(f'dfp_cia_aberta_{ano}.zip')

arquivos_zip

"""* fazendo o download de todos os arquivos"""

path='raw_data_cvm/dfp'
isExist = os.path.exists(path)

if not isExist:
  
  os.makedirs(path)

    
    
for arq in arquivos_zip:
    if os.path.exists(path+'/'+arq):
        os.remove(path+'/'+arq)
    wget.download(url_base+arq, out=path)
        
"""* extraindo os arquivos zip"""

for arq in arquivos_zip:
    ZipFile(path+'/'+arq, 'r').extractall(path+'/DFP')


# In[ ]:


## Updating quartely results


url_base = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/'

"""* criando uma lista com o nome de todos os arquivos"""

arquivos_zip = [] 
for ano in range(2017,int(this_year)+1): 
    arquivos_zip.append(f'itr_cia_aberta_{ano}.zip')



"""* fazendo o download de todos os arquivos"""

path='raw_data_cvm/itr'
isExist = os.path.exists(path)

if not isExist:
  
  os.makedirs(path)

    
    
for arq in arquivos_zip:
    if os.path.exists(path+'/'+arq):
        os.remove(path+'/'+arq)
    wget.download(url_base+arq, out=path)

"""* extraindo os arquivos zip"""

for arq in arquivos_zip: 
    ZipFile(path+'/'+arq, 'r').extractall(path+'/ITR')


# In[ ]:


## Updating FRE

url_base = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/'

"""* criando uma lista com o nome de todos os arquivos"""

arquivos_zip = []
for ano in[2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2021,2023]:
  arquivos_zip.append(f'fre_cia_aberta_{ano}.zip')

arquivos_zip

"""* fazendo o download de todos os arquivos"""

path='raw_data_cvm/fre'
isExist = os.path.exists(path)

if not isExist:
  
  os.makedirs(path)

    
    
for arq in arquivos_zip:
    if os.path.exists(path+'/'+arq):
        os.remove(path+'/'+arq)
    wget.download(url_base+arq, out=path)

"""* extraindo os arquivos zip"""

for arq in arquivos_zip:
  ZipFile(path+'/'+arq, 'r').extractall(path+'/FRE')


# In[ ]:


## Updating FCA


url_base = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FCA/DADOS/'

"""* criando uma lista com o nome de todos os arquivos"""

arquivos_zip = []
for ano in range(2011,int(this_year)+1):
  arquivos_zip.append(f'fca_cia_aberta_{ano}.zip')

arquivos_zip

"""* fazendo o download de todos os arquivos"""

path='raw_data_cvm/fca'
isExist = os.path.exists(path)

if not isExist:
  
  os.makedirs(path)

    
    
for arq in arquivos_zip:
    if os.path.exists(path+'/'+arq):
        os.remove(path+'/'+arq)
    wget.download(url_base+arq, out=path)

"""* extraindo os arquivos zip"""

for arq in arquivos_zip:
  ZipFile(path+'/'+arq, 'r').extractall(path+'/FCA')


# In[ ]:





# In[ ]:




