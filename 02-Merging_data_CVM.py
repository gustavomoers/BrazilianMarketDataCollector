#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import os
import time



this_year = datetime.datetime.today().strftime('%Y')
today = datetime.datetime.today().strftime('%Y-%m-%d')


# # Merging old data  with updated ones

# In[ ]:


## Getting ALL DFP'S FROM 2017 TO LAST YEAR AND CONCATING WITH 2010-2016
nomes = ['BPA_con', 'BPA_ind', 'BPP_con', 'BPP_ind', 'DFC_MI_con', 'DFC_MI_ind', 'DRE_con', 'DRE_ind', 'DVA_con', 'DVA_ind', 'DFC_MD_con', 'DFC_MD_ind']
for nome in nomes:
    arquivo = pd.DataFrame()
    for ano in range(2017,int(this_year)+1):
        arquivo = pd.concat([arquivo, pd.read_csv(f'raw_data_cvm/dfp/DFP/dfp_cia_aberta_{nome}_{ano}.csv', sep=';', decimal=',', encoding='ISO-8859-1')])
    
    arquivo['VL_CONTA'] = arquivo['VL_CONTA'].apply(lambda x: float(x))
    hist_results = pd.read_pickle(f'merged_data_cvm/old_data/DFP/dfp_cia_aberta_{nome}_2010-2016.pkl')
    all_res = pd.concat([hist_results,arquivo])
    all_res.to_pickle(f'merged_data_cvm/new_data/DFP/dfp_cia_aberta_{nome}_tudo.pkl')


# In[ ]:


## Getting ALL DMPL ANNUAL FROM 2017 to TODAY and concating with 2010-2016
nomes = ['DMPL_con','DMPL_ind']
for nome in nomes:
    arquivo = pd.DataFrame()
    for ano in range(2017,int(this_year)+1):
        arquivo = pd.concat([arquivo, pd.read_csv(f'raw_data_cvm/dfp/DFP/dfp_cia_aberta_{nome}_{ano}.csv', sep=';', decimal=',', encoding='ISO-8859-1')])
    
    arquivo['COLUNA_DF'] =np.select([arquivo['COLUNA_DF'].isna()],['PL CON'],arquivo['COLUNA_DF'])
    
    dmplass = ['Patrimônio Líquido Consolidado','Participação dos Não Controladores','Patrimônio Líquido','Reservas de Lucro',
          'Lucros ou Prejuízos Acumulados','PL CON']
    arquivo = arquivo.query("COLUNA_DF == @dmplass")
    arquivo = arquivo.query("DS_CONTA == ['Saldos Iniciais','Saldos Finais']")
    arquivo['VL_CONTA'] = arquivo['VL_CONTA'].apply(lambda x: float(x))
    
    hist_results = pd.read_pickle(f'merged_data_cvm/old_data/DFP/dfp_cia_aberta_{nome}_2010-2016.pkl')
    all_res = pd.concat([hist_results,arquivo])
    all_res.to_pickle(f'merged_data_cvm/new_data/DFP/dfp_cia_aberta_{nome}_tudo.pkl')


# In[ ]:


## Getting ALL ITR'S FROM 2017 TO LAST YEAR AND CONCATING WITH 2011-2016
nomes = ['BPA_con', 'BPA_ind', 'BPP_con', 'BPP_ind', 'DFC_MI_con', 'DFC_MI_ind', 'DRE_con', 'DRE_ind', 'DVA_con', 'DVA_ind', 'DFC_MD_con', 'DFC_MD_ind']
for nome in nomes:
    arquivo = pd.DataFrame()
    for ano in range(2017,int(this_year)+1):
        arquivo = pd.concat([arquivo, pd.read_csv(f'raw_data_cvm/itr/ITR/itr_cia_aberta_{nome}_{ano}.csv', sep=';', decimal=',', encoding='ISO-8859-1')])
    
    arquivo['VL_CONTA'] = arquivo['VL_CONTA'].apply(lambda x: float(x))    
    hist_results = pd.read_pickle(f'merged_data_cvm/old_data//ITR/itr_cia_aberta_{nome}_2011-2016.pkl')
    all_res = pd.concat([hist_results,arquivo])
    all_res.to_pickle(f'merged_data_cvm/new_data/ITR/itr_cia_aberta_{nome}_tudo.pkl')


# In[ ]:


## GETTING ALL DMPL QUARTELY FROM 2017 TO today and concating with 2011-2016

nomes = ['DMPL_con','DMPL_ind']
for nome in nomes:
    arquivo = pd.DataFrame()
    for ano in range(2017,int(this_year)+1):
        arquivo = pd.concat([arquivo, pd.read_csv(f'raw_data_cvm/itr/ITR/itr_cia_aberta_{nome}_{ano}.csv', sep=';', decimal=',', encoding='ISO-8859-1')])
    
    arquivo['COLUNA_DF'] =np.select([arquivo['COLUNA_DF'].isna()],['PL CON'],arquivo['COLUNA_DF'])
    
    dmplass = ['Patrimônio Líquido Consolidado','Participação dos Não Controladores','Patrimônio Líquido','Reservas de Lucro',
          'Lucros ou Prejuízos Acumulados','PL CON']
    arquivo = arquivo.query("COLUNA_DF == @dmplass")
    arquivo = arquivo.query("DS_CONTA == ['Saldos Iniciais','Saldos Finais']")   
    
    arquivo['VL_CONTA'] = arquivo['VL_CONTA'].apply(lambda x: float(x))
    hist_results = pd.read_pickle(f'merged_data_cvm/old_data/ITR/itr_cia_aberta_{nome}_2011-2016.pkl')
    all_res = pd.concat([hist_results,arquivo])
    all_res.to_pickle(f'merged_data_cvm/new_data/ITR/itr_cia_aberta_{nome}_tudo.pkl')


# In[ ]:


## Getting ALL FRE'S FROM 2010 TO 2021
nomes = ['posicao_acionaria','distribuicao_capital']
for nome in nomes:
    arquivo = pd.DataFrame()
    for ano in [2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2021,2023]:
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


# # Creating dataframe with companies information

# In[ ]:


## merging companies_data com FCA


companies_data = pd.read_excel('info_brazilian_companies.xlsx')


fca = pd.read_pickle('merged_data_cvm/new_data/FCA/fca_cia_aberta_geral_tudo.pkl')
fca['Data_Referencia'] = pd.to_datetime(fca['Data_Referencia'])
fca = fca.sort_values('Data_Referencia').drop_duplicates('Codigo_CVM',keep='last')
fca['Codigo_CVM'] = fca['Codigo_CVM'].astype(np.int64)

info_all = fca.merge(companies_data, left_on='Codigo_CVM', right_on='CD_CVM', how='inner')


info_all.to_pickle('clean_data/info_companies/info_companies_geral_comSetor.pkl')

##backup

info_all.to_pickle('BACKUPS/info_companies/info_companies_geral_comSetor%s.pkl'%today)

