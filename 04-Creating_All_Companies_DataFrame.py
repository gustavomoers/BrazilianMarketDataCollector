#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import datetime
import numpy as np
from yahooquery import Ticker
from dateutil.relativedelta import relativedelta

this_year = datetime.datetime.today().strftime('%Y')
last_year = (datetime.datetime.now() - relativedelta(years=1)).strftime('%Y')
two_years_ago = (datetime.datetime.now() - relativedelta(years=2)).strftime('%Y')
today = datetime.datetime.today().strftime('%Y-%m-%d')
one_year_ago = (datetime.datetime.today() - relativedelta(years=1)).strftime('%Y-%m-%d')


# In[ ]:


pd.set_option('display.max_rows', 60000)
pd.set_option('display.float_format', lambda x: '%.4f' % x)

pd.options.mode.chained_assignment = None  # default='warn'


# In[ ]:


def label_year(row):
    if row['DT_INI_EXERC'].month == 1 and row['DT_FIM_EXERC'].month == 12:
        return str(row['DT_FIM_EXERC'].year)

    if row['DT_INI_EXERC'].month == 1 and row['DT_FIM_EXERC'].month == 3:
        return '1T'+str(row['DT_FIM_EXERC'].year)

    if row['DT_INI_EXERC'].month == 1 and row['DT_FIM_EXERC'].month == 6:
        return '2T'+str(row['DT_FIM_EXERC'].year)+'CON'

    if row['DT_INI_EXERC'].month == 4 and row['DT_FIM_EXERC'].month == 6:
        return '2T'+str(row['DT_FIM_EXERC'].year)

    if row['DT_INI_EXERC'].month == 1 and row['DT_FIM_EXERC'].month == 9:
        return '3T'+str(row['DT_FIM_EXERC'].year)+'CON'

    if row['DT_INI_EXERC'].month == 7 and row['DT_FIM_EXERC'].month == 9:
        return '3T'+str(row['DT_FIM_EXERC'].year)
    
    if row['DT_INI_EXERC'].month == 10 and row['DT_FIM_EXERC'].month == 12:
        return '4T'+str(row['DT_FIM_EXERC'].year)    
    
    if row['DT_INI_EXERC'].month == 4 and row['DT_FIM_EXERC'].month == 3: ##BIOSEV
        return str(row['DT_INI_EXERC'].year)
    
    if row['DT_INI_EXERC'].month == 7 and row['DT_FIM_EXERC'].month == 6: ##BRASIL AGRO
        return str(row['DT_INI_EXERC'].year)

    if row['DT_INI_EXERC'].month == 3 and row['DT_FIM_EXERC'].month == 1: ##CAMIL
        return str(row['DT_INI_EXERC'].year)
    
    if row['DT_INI_EXERC'].month == 12 and row['DT_FIM_EXERC'].month == 12: ##COSAN,GAFISA
        return str(row['DT_INI_EXERC'].year)
    
    if row['DT_INI_EXERC'].month == 5 and row['DT_FIM_EXERC'].month == 12: ##VIVARA
        return str(row['DT_INI_EXERC'].year)
    
    


# In[ ]:


def label_bp(row):
    if row['DT_FIM_EXERC'].month == 12:
        return str(row['DT_FIM_EXERC'].year)

    if row['DT_FIM_EXERC'].month == 3:
        return '1T'+str(row['DT_FIM_EXERC'].year)

    if row['DT_FIM_EXERC'].month == 6:
        return '2T'+str(row['DT_FIM_EXERC'].year)

    if row['DT_FIM_EXERC'].month == 9:
        return '3T'+str(row['DT_FIM_EXERC'].year)


# # Pivoting, selecting columns, cleaning, creating TTM results for all companies

# In[ ]:


### PIVOTING, MANIPULATING, CREATING TTM FOR DRE


## reading all dre
a = pd.read_pickle('merged_data_cvm/new_data/DFP/dfp_cia_aberta_DRE_con_tudo.pkl',)
b = pd.read_pickle('merged_data_cvm/new_data/DFP/dfp_cia_aberta_DRE_ind_tudo.pkl')
c = pd.read_pickle('merged_data_cvm/new_data/ITR/itr_cia_aberta_DRE_con_tudo.pkl')
d = pd.read_pickle('merged_data_cvm/new_data/ITR/itr_cia_aberta_DRE_ind_tudo.pkl')


drepiv = pd.concat([a,b,c,d])


## selecting important values
dreass = ['Receita de Venda de Bens e/ou Serviços',
        'Custo dos Bens e/ou Serviços Vendidos',
        'Resultado Bruto',
        'Despesas/Receitas Operacionais',
        'Resultado Antes do Resultado Financeiro e dos Tributos',
        'Resultado Financeiro',
        'Resultado Antes dos Tributos sobre o Lucro',
        'Resultado antes dos Tributos sobre o Lucro',
        'Imposto de Renda e Contribuição Social sobre o Lucro',
        'Resultado Líquido das Operações Continuadas',
        'Resultado Líquido de Operações Descontinuadas',
        'Lucro/Prejuízo Consolidado do Período',
        'Atribuído a Sócios da Empresa Controladora',
        'Atribuído a Sócios Não Controladores',
        'Receitas de Intermediação Financeira',
        'Receitas da Intermediação Financeira',
        'Despesas de Intermediação Financeira',
        'Despesas da Intermediação Financeira',
        'Resultado Bruto de Intermediação Financeira',
        'Resultado Bruto Intermediação Financeira', 
        'Receitas das Operações',
        'Sinistros e Despesas das Operações',
        'Despesas Administrativas',
        'Lucro/Prejuízo do Período',
        'Lucro ou Prejuízo Líquido do Período',
        'Lucro ou Prejuízo Líquido Consolidado do Período',
        'Atribuído aos Sócios da Empresa Controladora',
        'Atribuído aos Sócios não Controladores',
        'Outras Despesas e Receitas Operacionais',
        'Outras Despesas/Receitas Operacionais',
        'Resultado Operacional',
        'Resultado Antes Tributação/Participações',
        'IR Diferido']
drepiv = drepiv.query("DS_CONTA == @dreass")
drepiv.loc[(drepiv.GRUPO_DFP == 'DF Consolidado - Demonstração do Resultado'),'GRUPO_DFP'] = 'DF_CON'
drepiv.loc[(drepiv.GRUPO_DFP == 'DF Individual - Demonstração do Resultado'),'GRUPO_DFP'] = 'DF_IND'
drepiv.drop_duplicates(['CD_CVM','GRUPO_DFP','DS_CONTA','DT_INI_EXERC','DT_FIM_EXERC'],inplace=True)
drepiv.reset_index(inplace=True)
drepiv['DT_INI_EXERC'] = pd.to_datetime(drepiv['DT_INI_EXERC'])
drepiv['DT_FIM_EXERC'] = pd.to_datetime(drepiv['DT_FIM_EXERC'])
drepiv['VL_CONTA'] = np.select([drepiv['ESCALA_MOEDA'] == 'MIL'], [drepiv['VL_CONTA']*1000], drepiv['VL_CONTA']*1)


## creating labels to quartelys and annuals
drepiv['LABEL'] = drepiv.apply(lambda row: label_year(row), axis=1)


## ajustando POWE3 2018 E 2019
 
drepiv['VL_CONTA'] = np.select([((drepiv['CD_CVM'] == 25488) & (drepiv['LABEL'].isin(['2019','2018'])))], [(drepiv['VL_CONTA']*1000)],
                               drepiv['VL_CONTA'])


## pivotando to do calculations
drepiv = drepiv.pivot_table(index=['CD_CVM','GRUPO_DFP','DS_CONTA'],columns = ['LABEL'],values='VL_CONTA')

## creating 4T
for year in range(2010,int(this_year)):
    drepiv['4T'+str(year)] = drepiv[str(year)] - drepiv['3T'+str(year)] - drepiv['2T'+str(year)] - drepiv['1T'+str(year)]

    
    
columns_list = drepiv.columns.to_list()

drepiv.reset_index(inplace=True)


## Calculating TTM
drepiv['TTM'] = drepiv[last_year]
drepiv['Anterior TTM'] = drepiv[two_years_ago]


if '1T'+this_year in columns_list:
    conditions = [drepiv['1T'+this_year].notna()]
    choices = [drepiv['1T'+this_year]+drepiv[last_year]-drepiv['1T'+last_year]]
    drepiv['TTM'] = np.select(conditions, choices, drepiv['TTM'])
    drepiv['Anterior TTM'] = np.select(conditions,
                                       [drepiv['1T'+last_year]+drepiv[two_years_ago]-drepiv['1T'+two_years_ago]],
                                       drepiv['Anterior TTM'])

if '2T'+this_year in columns_list:
    conditions = [drepiv['2T'+this_year].notna()]
    choices = [drepiv['2T'+this_year]+drepiv['1T'+this_year]+drepiv['4T'+last_year]+drepiv['3T'+last_year]]
    drepiv['TTM'] = np.select(conditions, choices, drepiv['TTM'])
    drepiv['Anterior TTM'] = np.select(conditions,
                                       [drepiv['2T'+last_year]+drepiv['1T'+last_year]+drepiv['4T'+two_years_ago]+drepiv['3T'+two_years_ago]],
                                       drepiv['Anterior TTM'])

    
if '3T'+this_year in columns_list:
    conditions = [drepiv['3T'+this_year].notna()]
    choices = [drepiv['3T'+this_year]+drepiv['2T'+this_year]+drepiv['1T'+this_year]+drepiv['4T'+last_year]]
    drepiv['TTM'] = np.select(conditions, choices, drepiv['TTM'])
    drepiv['Anterior TTM'] = np.select(conditions,
                                       [drepiv['3T'+last_year]+drepiv['2T'+last_year]+drepiv['1T'+two_years_ago]+drepiv['4T'+two_years_ago]],
                                       drepiv['Anterior TTM'])
        
if this_year in columns_list:
    conditions = [drepiv[this_year].notna()]
    choices = [drepiv[this_year]]
    drepiv['TTM'] = np.select(conditions, choices, drepiv['TTM'])
    drepiv['Anterior TTM'] = np.select(conditions,
                                       [drepiv[last_year]],
                                       drepiv['Anterior TTM'])
# dropping and melting  
drepiv.drop(drepiv.columns[drepiv.columns.str.endswith('CON')], axis=1,inplace=True)


drepiv= drepiv.melt(id_vars=['CD_CVM','GRUPO_DFP','DS_CONTA'],var_name='LABEL',value_name='VL_CONTA')
drepiv.drop_duplicates(['CD_CVM','GRUPO_DFP','DS_CONTA','LABEL'],inplace=True)

drepiv = drepiv.pivot_table(index=['CD_CVM','GRUPO_DFP','LABEL'],columns = ['DS_CONTA'],values='VL_CONTA')



drepiv.to_pickle('clean_data/companies_results/DRE/all_dre_pivotado.pkl')

##backup

drepiv.to_pickle('BACKUPS/results/dre/all_dre_pivotado%s.csv'%today)


# In[ ]:


### PIVOTING, MANIPULATING, CREATING TTM FOR BPP



## reading all bpp
a = pd.read_pickle('merged_data_cvm/new_data/DFP/dfp_cia_aberta_BPP_con_tudo.pkl',)
b = pd.read_pickle('merged_data_cvm/new_data/DFP/dfp_cia_aberta_BPP_ind_tudo.pkl')
c = pd.read_pickle('merged_data_cvm/new_data/ITR/itr_cia_aberta_BPP_con_tudo.pkl')
d = pd.read_pickle('merged_data_cvm/new_data/ITR/itr_cia_aberta_BPP_ind_tudo.pkl')

bpppiv = pd.concat([a,b,c,d])


# getting patrimonio

bpppiv['CD_CONTA'] = np.select([bpppiv['DS_CONTA']=='Patrimônio Líquido Consolidado'], ['150'],bpppiv['CD_CONTA'])
bpppiv['CD_CONTA'] = np.select([bpppiv['DS_CONTA']=='Patrimônio Líquido'], ['1501'],bpppiv['CD_CONTA'])
bpppiv['CD_CONTA'] = np.select([bpppiv['DS_CONTA']=='Participação dos Acionistas Não Controladores'], ['1502'],
                               bpppiv['CD_CONTA'])



## selecting important values

bppass = ['2',
        '2.01',
        '2.01.04',
        '2.02',
        '2.02.01','150','1501','1502']


bpppiv = bpppiv.query("CD_CONTA == @bppass")
bpppiv.loc[(bpppiv.GRUPO_DFP == 'DF Consolidado - Balanço Patrimonial Passivo'),'GRUPO_DFP'] = 'DF_CON'
bpppiv.loc[(bpppiv.GRUPO_DFP == 'DF Individual - Balanço Patrimonial Passivo'),'GRUPO_DFP'] = 'DF_IND'
bpppiv.drop_duplicates(['CD_CVM','GRUPO_DFP','CD_CONTA','DT_FIM_EXERC'],inplace=True)
bpppiv.reset_index(inplace=True)
bpppiv['DT_FIM_EXERC'] = pd.to_datetime(bpppiv['DT_FIM_EXERC'])
bpppiv['VL_CONTA'] = np.where(bpppiv['ESCALA_MOEDA'] == 'MIL', bpppiv['VL_CONTA']*1000, bpppiv['VL_CONTA']*1)


## creating labels to quartelys and annuals
bpppiv['LABEL'] = bpppiv.apply(lambda row: label_bp(row), axis=1)


## pivotando to do calculations
bpppiv = bpppiv.pivot_table(index=['CD_CVM','GRUPO_DFP','CD_CONTA'],columns = ['LABEL'],values='VL_CONTA')

## creating 4T
for year in range(2010,int(this_year)):
    bpppiv['4T'+str(year)] = bpppiv[str(year)]

    
    
columns_list = bpppiv.columns.to_list()
bpppiv.reset_index(inplace=True)


## Calculating TTM
bpppiv['TTM'] = bpppiv[last_year]
bpppiv['Anterior TTM'] = bpppiv[two_years_ago]

if '1T'+this_year in columns_list:
    conditions = [bpppiv['1T'+this_year].notna()]
    choices = [bpppiv['1T'+this_year]]
    bpppiv['TTM'] = np.select(conditions, choices, bpppiv['TTM'])
    bpppiv['Anterior TTM'] = np.select(conditions, [bpppiv['1T'+last_year]], bpppiv['Anterior TTM'])
    
if '2T'+this_year in columns_list:
    conditions = [bpppiv['2T'+this_year].notna()]
    choices = [bpppiv['2T'+this_year]]
    bpppiv['TTM'] = np.select(conditions, choices, bpppiv['TTM'])
    bpppiv['Anterior TTM'] = np.select(conditions, [bpppiv['2T'+last_year]], bpppiv['Anterior TTM'])
        
if '3T'+this_year in columns_list:
    conditions = [bpppiv['3T'+this_year].notna()]
    choices = [bpppiv['3T'+this_year]]
    bpppiv['TTM'] = np.select(conditions, choices, bpppiv['TTM'])
    bpppiv['Anterior TTM'] = np.select(conditions, [bpppiv['3T'+last_year]], bpppiv['Anterior TTM'])
        
if this_year in columns_list:
    conditions = [bpppiv[this_year].notna()]
    choices = [bpppiv[this_year]]
    bpppiv['TTM'] = np.select(conditions, choices, bpppiv['TTM'])
    bpppiv['Anterior TTM'] = np.select(conditions, [bpppiv[last_year]], bpppiv['Anterior TTM'])
    

    
## dropping unnecessary data and melting  
bpppiv= bpppiv.melt(id_vars=['CD_CVM','GRUPO_DFP','CD_CONTA'],var_name='LABEL',value_name='VL_CONTA')
bpppiv.drop_duplicates(['CD_CVM','GRUPO_DFP','CD_CONTA','LABEL'],inplace=True)

bpppiv = bpppiv.pivot_table(index=['CD_CVM','GRUPO_DFP','LABEL'],columns = ['CD_CONTA'],values='VL_CONTA')

bpppiv.rename({'2':'Passivo Total','2.01':'Passivo Circulante','2.01.04':'Dívida curto prazo',
              '2.02':'Passivo Não Circulante','2.02.01':'Dívida longo prazo',
              '1502':'Participação dos Acionistas Não Controladores',
              '150': 'Patrimônio Líquido Consolidado',
              '1501':'Patrimônio Líquido'},axis=1,inplace=True)

bpppiv.to_pickle('clean_data/companies_results/BPP/all_bpp_pivotado.pkl')

##backup

bpppiv.to_pickle('BACKUPS/results/bpp/all_bpp_pivotado%s.pkl'%today)


# In[ ]:


### PIVOTING, MANIPULATING, CREATING TTM FOR BPA


## reading all bpa
a = pd.read_pickle('merged_data_cvm/new_data/DFP/dfp_cia_aberta_BPA_con_tudo.pkl',)
b = pd.read_pickle('merged_data_cvm/new_data/DFP/dfp_cia_aberta_BPA_ind_tudo.pkl')
c = pd.read_pickle('merged_data_cvm/new_data/ITR/itr_cia_aberta_BPA_con_tudo.pkl')
d = pd.read_pickle('merged_data_cvm/new_data/ITR/itr_cia_aberta_BPA_ind_tudo.pkl')
bpapiv = pd.concat([a,b,c,d])


## selecting important values

bpaass = ['Ativo Total',
        'Ativo Circulante',
        'Caixa e Equivalentes de Caixa',
        'Aplicações Financeiras',
        'Ativo Não Circulante',
        'Ativo Realizável a Longo Prazo',
        'Investimentos']
bpapiv = bpapiv.query("DS_CONTA == @bpaass")
bpapiv.loc[(bpapiv.GRUPO_DFP == 'DF Consolidado - Balanço Patrimonial Ativo'),'GRUPO_DFP'] = 'DF_CON'
bpapiv.loc[(bpapiv.GRUPO_DFP == 'DF Individual - Balanço Patrimonial Ativo'),'GRUPO_DFP'] = 'DF_IND'
bpapiv.drop_duplicates(['CD_CVM','GRUPO_DFP','DS_CONTA','DT_FIM_EXERC'],inplace=True)
bpapiv.reset_index(inplace=True)
bpapiv['DT_FIM_EXERC'] = pd.to_datetime(bpapiv['DT_FIM_EXERC'])
bpapiv['VL_CONTA'] = np.where(bpapiv['ESCALA_MOEDA'] == 'MIL', bpapiv['VL_CONTA']*1000, bpapiv['VL_CONTA']*1)

## creating labels to quartelys and annuals
bpapiv['LABEL'] = bpapiv.apply(lambda row: label_bp(row), axis=1)


## pivotando to do calculations
bpapiv = bpapiv.pivot_table(index=['CD_CVM','GRUPO_DFP','DS_CONTA'],columns = ['LABEL'],values='VL_CONTA')

## creating 4T
for year in range(2010,int(this_year)):
    bpapiv['4T'+str(year)] = bpapiv[str(year)]

    
    
columns_list = bpapiv.columns.to_list()
bpapiv.reset_index(inplace=True)

## Calculating TTM
bpapiv['TTM'] = bpapiv[last_year]
bpapiv['Anterior TTM'] = bpapiv[two_years_ago]

if '1T'+this_year in columns_list:
    conditions = [bpapiv['1T'+this_year].notna()]
    choices = [bpapiv['1T'+this_year]]
    bpapiv['TTM'] = np.select(conditions, choices, bpapiv['TTM'])
    bpapiv['Anterior TTM'] = np.select(conditions, [bpapiv['1T'+last_year]], bpapiv['Anterior TTM'])
    
if '2T'+this_year in columns_list:
    conditions = [bpapiv['2T'+this_year].notna()]
    choices = [bpapiv['2T'+this_year]]
    bpapiv['TTM'] = np.select(conditions, choices, bpapiv['TTM'])
    bpapiv['Anterior TTM'] = np.select(conditions, [bpapiv['2T'+last_year]], bpapiv['Anterior TTM'])
    
if '3T'+this_year in columns_list:
    conditions = [bpapiv['3T'+this_year].notna()]
    choices = [bpapiv['3T'+this_year]]
    bpapiv['TTM'] = np.select(conditions, choices, bpapiv['TTM'])
    bpapiv['Anterior TTM'] = np.select(conditions, [bpapiv['3T'+last_year]], bpapiv['Anterior TTM'])
    
if this_year in columns_list:
    conditions = [bpapiv[this_year].notna()]
    choices = [bpapiv[this_year]]
    bpapiv['TTM'] = np.select(conditions, choices, bpapiv['TTM'])
    bpapiv['Anterior TTM'] = np.select(conditions, [bpapiv[last_year]], bpapiv['Anterior TTM'])

## dropping unnecessary data and melting  
bpapiv= bpapiv.melt(id_vars=['CD_CVM','GRUPO_DFP','DS_CONTA'],var_name='LABEL',value_name='VL_CONTA')
bpapiv.drop_duplicates(['CD_CVM','GRUPO_DFP','DS_CONTA','LABEL'],inplace=True)
bpapiv = bpapiv.pivot_table(index=['CD_CVM','GRUPO_DFP','LABEL'],columns = ['DS_CONTA'],values='VL_CONTA')


bpapiv.to_pickle('clean_data/companies_results/BPA/all_bpa_pivotado.pkl')

##backup

bpapiv.to_pickle('BACKUPS/results/bpa/all_bpa_pivotado%s.pkl'%today)


# In[ ]:


## reading all dfcmi and getting JUROS PAGOS
a = pd.read_pickle('merged_data_cvm/new_data/DFP/dfp_cia_aberta_DFC_MI_con_tudo.pkl')
b = pd.read_pickle('merged_data_cvm/new_data/DFP/dfp_cia_aberta_DFC_MI_ind_tudo.pkl')
e = pd.read_pickle('merged_data_cvm/new_data/DFP/dfp_cia_aberta_DFC_MD_con_tudo.pkl')
f = pd.read_pickle('merged_data_cvm/new_data/DFP/dfp_cia_aberta_DFC_MD_ind_tudo.pkl')

dfcmipiv = pd.concat([a,b,e,f])

int_exp = ['Juros pagos','Juros Pagos','Pagamento de Juros','Pagamento de juros','Juros sobre Financiamentos',
          'Pagamento de Empréstimos e Financiamentos - Juros','Pagamento de Juros de Emp. e Financiamentos',
          'PAgamento Juros','juros pagos','Juros Pago','Juro Pagos','Pagamento juros','Juros pagos','Juros Pagos',
          'Pagamentos juros','Despesa de juros','Juros sobre Financiamentos',
             'Juros sobre Financiamento',
             'Juros sobre financiamentos',
             'Juros Pagos sobre Financiamentos',
             'Juros s/ Financiamentos',
             'Juros e financiamentos',
             'Juros Financiamentos',
             'Juros sobre financiamentos pagos',
             'Juros pagos sobre financiamentos',
             'juros sobre parcelamentos',
             'Juros e Variações sobre Financiamentos',
             'Pagamento juros sobre financiamentos',
             'Pagamento de Juros sobre Financiamentos',
             'Juros sobre Financiamentos e Debêntures',
             'Juros/encargos sobre financiamento',
             'Juros/Encargos sobre financiamento',
             'Juros sobre encargos e financiamentos',
             'Pagamentos de Juros sobre Financiamentos',
             'Juros sobre Empréstimos e Financiamentos',
             'Juros sobre Emprestimos e Financiamentos',
             'Pagamento de Juros sobre Financiamento',
             'Despesa com juros sobre financiamentos',
             'Juros s/encargos e financiamentos',
             'Juros pagos s/financiamentos',
             'Juros / encargos sobre financiamento',
             'Valor justo sobre financiamento',
             'Pagamento de juros sobre financiamentos',
             'Juros sobre empréstimos e financiamentos',
             'Juros sobre emprestimos e financiamentos',
             'Juros Sobre Empréstimos e Financiamentos',
             'Pagamento de juros sobre financiamento',
             'Juros pagos por financiamentos',
             'Juros pagos com financiamentos',
             'Juros sobre empréstimos e financimentos',
             'Juros pagos no financiamento',
             'Juros sobre Emprestimo e Financiamentos Pagos',
             'Juros pg. sobre Emprestimos e Fiananciamentos',
             'Pagamento de Juros por Financiamentos',
             'Juros s/ empréstimos e financiamentos',
             'Juros de Empréstimos e Financiamentos',
             'Juros sobre imprestimos',
             'Juros Sobre empréstimos e financiamentos',
             'Juros Pago sobre Empréstimo e Financiamento',
             'Juros sobre Empréstimos e Financiamentos Pagos',
             'Juros pagos sobre Empréstimos e Financiamentos','Juros Pagos de Empréstimos','Pagamento de Juros de Emprestimos e financiamentos',
          'Juros Pagos de Empréstimos',
         'Juros pagos de Empréstimos',
         'Juros pagos de empréstimos',
         'Juros Pagos sobre Empréstimos',
         'Juros Pagos Sobre Empréstimos',
         'Juros Pagos por Empréstimos',
         'Juros Pagos com Empréstimos',
         'Juros pagos de empréstimo',
         'Juros pagos - Empréstimos',
         'Juros pagos de emprestimos',
         'Juros Pagos por Empréstimo',
         'Juros Pagos sobre Emrpéstimos',
         'Juros de Empréstimos',
         'Juros pagos por Empréstimos',
         'Juros dos Empréstimos',
         'Juros pagos s/ empréstimos',
         'Juros passivos de empréstimos',
         'Juros pagos sobre empréstimos',
         'Juros Provisionados de Empréstimos',
         'Juros pagos por empréstimos',
         'Juros s/ empréstimos',
         'Juros s/ Empréstimos',
         'Juros de empréstimos',
         'Juros sobre Empréstimos',
         'Juros Sobre Empréstimos',
         'Juros pagos sobre empréstimo',
         'Juros sob empréstimos',
         'Juros dos Emprestimos',
         'Custos de Empréstimos',
         'Juros s/Empréstimos',
         'Juros s/ empréstimo',
         'Juros pagos sobre emprestimos',
         'Juros e Encargos sobre Empréstimos',
         'Juros de empréstimo',
         'Juros Empréstimos',
         'Juros recebidos de empréstimos',
         'Juros pagos - Empréstimo ponte',
         'Pagamentos de Empréstimos',
         'Pagto de empréstimos',
         'Juros s/ Emprestimos',
         'Juros de emprestimos',
         'Custo de Empréstimos',
         'Recursos de empréstimos',
         'Juros sobre empréstimos',
         'Juros sobre Emprestimos',
         'Juros pagos de Empréstimos e Debentures',
         'Juros de Empréstimos Pagos',
         'Juros pagos de empréstimos e repasses',
         'Proventos de Empréstimos',
         'Pagamentos e Empréstimos',
         'Pagamento de Empréstimos',
         'Juros pagos sobre empréstimo CCB',
         'Juros S/Empréstimos',
         'Juros Pagos de Empréstimos e Financiamentos',
         'Juros sobre empréstimo',
         'Juros pagos de empréstimos no exterior',
         'Pagamentos de empréstimos',
         'Pagamentos de Emprestimos',
         'Custo de empréstimos',
         'Juros Pagos sobre Empréstimos e Debêntures',
         'Juros Pagos Sobre Empréstimos e Debêntures',
         'Pagamento de Empréstimo',
         'Juros sobre imprestimos',
         'Juros sobre emprestimos',
         'Juros s/ Empréstimos Pagos',
         'Juros de empréstimos pagos',
         'Juros sobre Empréstimos Pagos',
         'Juros e Encargos sobre Empréstimos Pagos',
         'Juros Sobre Empréstimos pagos',
         'Juros Sobre Empréstimos Pagos',
         'Juros pagos de Empréstimos e Financiamentos',
         'Juros Pagos sobre Emprésimos e Debêntures',
         'Juros pagos empréstimos e debêntures',
         'Juros sobre emprestimo',
         'Juros pagos de emprestimos e debentures',
         'Juros s/empréstimos pagos',
         'Juros s/Empréstimos Pagos',
         'Juros e empréstimos pagos',
         'Juros Pagos - Empréstimos e Financiamentos',
         'Juros Pagos sobre Empréstimos e Finaciamentos',
         'Juros provisionados sobre empréstimos',
         'Juros pagos de empréstimos e financiamentos',
         'Juros e variações monetárias de Empréstimos',
         'Juros e Variações Monetárias de Empréstimos',
         'Juros Pagos sobre empréstimos com terceiros',
         'Juros Pagos sobre Empréstimos e Financiamentos',
         'Juros Pagos Sobre Empréstimos e Financiamentos',
         'Pagamento de empréstimo','Juros e variação cambial sobre empréstimos e financiamentos e outros passivos']

dfcmipiv['DS_CONTA'] = np.select([dfcmipiv['DS_CONTA'].isin(int_exp)],['Juros Pagos'],dfcmipiv['DS_CONTA'])


# In[ ]:


### PIVOTING, MANIPULATING, CREATING TTM FOR DFCMI



## selecting important values

dfcmiass = ['Caixa Líquido Atividades Operacionais',
            'Caixa Gerado nas Operações',
            'Variações nos Ativos e Passivos',
            'Caixa Líquido Atividades de Investimento',
            'Caixa Líquido Atividades de Financiamento',
            'Variação Cambial s/ Caixa e Equivalentes',
            'Aumento (Redução) de Caixa e Equivalentes',
            'Saldo Inicial de Caixa e Equivalentes',
            'Saldo Final de Caixa e Equivalentes','Juros Pagos']
dfcmipiv = dfcmipiv.query("DS_CONTA == @dfcmiass")
dfcmipiv.loc[(dfcmipiv.GRUPO_DFP == 'DF Consolidado - Demonstração do Fluxo de Caixa (Método Indireto)'),'GRUPO_DFP'] = 'DF_CON'
dfcmipiv.loc[(dfcmipiv.GRUPO_DFP == 'DF Individual - Demonstração do Fluxo de Caixa (Método Indireto)'),'GRUPO_DFP'] = 'DF_IND'
dfcmipiv.loc[(dfcmipiv.GRUPO_DFP == 'DF Consolidado - Demonstração do Fluxo de Caixa (Método Direto)'),'GRUPO_DFP'] = 'DF_CON'
dfcmipiv.loc[(dfcmipiv.GRUPO_DFP == 'DF Individual - Demonstração do Fluxo de Caixa (Método Direto)'),'GRUPO_DFP'] = 'DF_IND'

dfcmipiv.drop_duplicates(['CD_CVM','GRUPO_DFP','DS_CONTA','DT_INI_EXERC','DT_FIM_EXERC'],inplace=True)
dfcmipiv['VL_CONTA'] = np.where(dfcmipiv['ESCALA_MOEDA'] == 'MIL', dfcmipiv['VL_CONTA']*1000, dfcmipiv['VL_CONTA']*1)


dfcmipiv = dfcmipiv.pivot_table(index=['CD_CVM','GRUPO_DFP','DT_INI_EXERC','DT_FIM_EXERC'],columns = ['DS_CONTA'],values='VL_CONTA')


get_capex = pd.concat([a,b,e,f])
get_capex =get_capex.loc[(get_capex['CD_CONTA'].str.startswith('6.02.'))]
get_capex.loc[(get_capex.GRUPO_DFP == 'DF Consolidado - Demonstração do Fluxo de Caixa (Método Indireto)'),'GRUPO_DFP'] = 'DF_CON'
get_capex.loc[(get_capex.GRUPO_DFP == 'DF Individual - Demonstração do Fluxo de Caixa (Método Indireto)'),'GRUPO_DFP'] = 'DF_IND'
get_capex.loc[(get_capex.GRUPO_DFP == 'DF Consolidado - Demonstração do Fluxo de Caixa (Método Direto)'),'GRUPO_DFP'] = 'DF_CON'
get_capex.loc[(get_capex.GRUPO_DFP == 'DF Individual - Demonstração do Fluxo de Caixa (Método Direto)'),'GRUPO_DFP'] = 'DF_IND'

get_capex.drop_duplicates(['CD_CVM','GRUPO_DFP','CD_CONTA','DT_INI_EXERC','DT_FIM_EXERC'],inplace=True)
get_capex['VL_CONTA'] = np.where(get_capex['ESCALA_MOEDA'] == 'MIL', get_capex['VL_CONTA']*1000, get_capex['VL_CONTA']*1)
get_capex = get_capex.pivot_table(index=['CD_CVM','GRUPO_DFP','DT_INI_EXERC','DT_FIM_EXERC'],columns = ['CD_CONTA'],values='VL_CONTA')
sum_neg = get_capex[get_capex<0].sum(1)
capex = sum_neg.to_frame(name='Capex')
dfcmipiv = dfcmipiv.join(capex)



dfcmipiv.reset_index(inplace=True)
dfcmipiv= dfcmipiv.melt(id_vars=['CD_CVM','GRUPO_DFP','DT_INI_EXERC','DT_FIM_EXERC'],var_name='DS_CONTA',value_name='VL_CONTA')

dfcmipiv['DT_INI_EXERC'] = pd.to_datetime(dfcmipiv['DT_INI_EXERC'])
dfcmipiv['DT_FIM_EXERC'] = pd.to_datetime(dfcmipiv['DT_FIM_EXERC'])

## creating labels to quartelys and annuals
dfcmipiv['LABEL'] = dfcmipiv.apply(lambda row: label_year(row), axis=1)

dfcmipiv = dfcmipiv[dfcmipiv['LABEL'].notna()]

## pivotando to do calculations
dfcmipiv = dfcmipiv.pivot_table(index=['CD_CVM','GRUPO_DFP','DS_CONTA'],columns = ['LABEL'],values='VL_CONTA')

  
columns_list = dfcmipiv.columns.to_list()

dfcmipiv.reset_index(inplace=True)


## Calculating TTM
dfcmipiv['TTM'] = dfcmipiv[last_year]
dfcmipiv['Anterior TTM'] = dfcmipiv[two_years_ago]
    

# dropping and melting  
dfcmipiv= dfcmipiv.melt(id_vars=['CD_CVM','GRUPO_DFP','DS_CONTA'],var_name='LABEL',value_name='VL_CONTA')
dfcmipiv.drop_duplicates(['CD_CVM','GRUPO_DFP','DS_CONTA','LABEL'],inplace=True)
dfcmipiv = dfcmipiv.pivot_table(index=['CD_CVM','GRUPO_DFP','LABEL'],columns = ['DS_CONTA'],values='VL_CONTA')


dfcmipiv.to_pickle('clean_data/companies_results/DFCMI/all_dfcmi_pivotado.pkl')

##backup

dfcmipiv.to_pickle('BACKUPS/results/dfcmi/all_dfcmi_pivotado%s.pkl'%today)


# In[ ]:


### PIVOTING, MANIPULATING, CREATING TTM FOR DVA



## reading all dva
a = pd.read_pickle('merged_data_cvm/new_data/DFP/dfp_cia_aberta_DVA_con_tudo.pkl')
b = pd.read_pickle('merged_data_cvm/new_data/DFP/dfp_cia_aberta_DVA_ind_tudo.pkl')
c = pd.read_pickle('merged_data_cvm/new_data/ITR/itr_cia_aberta_DVA_con_tudo.pkl')
d = pd.read_pickle('merged_data_cvm/new_data/ITR/itr_cia_aberta_DVA_ind_tudo.pkl')
dvapiv = pd.concat([a,b,c,d])


## selecting important values

dvaass = ['Depreciação, Amortização e Exaustão',
            'Juros sobre o Capital Próprio',
            'Dividendos',
            'Lucro retido']
dvapiv = dvapiv.query("DS_CONTA == @dvaass")
dvapiv.loc[(dvapiv.GRUPO_DFP == 'DF Consolidado - Demonstração de Valor Adicionado'),'GRUPO_DFP'] = 'DF_CON'
dvapiv.loc[(dvapiv.GRUPO_DFP == 'DF Individual - Demonstração de Valor Adicionado'),'GRUPO_DFP'] = 'DF_IND'
dvapiv.drop_duplicates(['CD_CVM','GRUPO_DFP','DS_CONTA','DT_INI_EXERC','DT_FIM_EXERC'],inplace=True)
dvapiv['VL_CONTA'] = np.where(dvapiv['ESCALA_MOEDA'] == 'MIL', dvapiv['VL_CONTA']*1000, dvapiv['VL_CONTA']*1)

dvapiv.reset_index(inplace=True)


dvapiv['DT_INI_EXERC'] = pd.to_datetime(dvapiv['DT_INI_EXERC'])
dvapiv['DT_FIM_EXERC'] = pd.to_datetime(dvapiv['DT_FIM_EXERC'])

## creating labels to quartelys and annuals
dvapiv['LABEL'] = dvapiv.apply(lambda row: label_year(row), axis=1)

dvapiv = dvapiv[dvapiv['LABEL'].notna()]

## pivotando to do calculations
dvapiv = dvapiv.pivot_table(index=['CD_CVM','GRUPO_DFP','DS_CONTA'],columns = ['LABEL'],values='VL_CONTA')



## creating 4T
for year in range(2010,int(this_year)):
    dvapiv['4T'+str(year)] = dvapiv[str(year)] - dvapiv['3T'+str(year)+'CON']

## creating 2T
for year in range(2010,int(this_year)):   
    conditions = [dvapiv['2T'+str(year)].isna()]
    choices = [dvapiv['2T'+str(year)+'CON']-dvapiv['1T'+str(year)]]
    dvapiv['2T'+str(year)] = np.select(conditions, choices, dvapiv['2T'+str(year)])
    
## creating 3T
for year in range(2010,int(this_year)):   
    conditions = [dvapiv['3T'+str(year)].isna()]
    choices = [dvapiv['3T'+str(year)+'CON']-dvapiv['2T'+str(year)+'CON']]
    dvapiv['3T'+str(year)] = np.select(conditions, choices, dvapiv['3T'+str(year)])   
    
    
    
columns_list = dvapiv.columns.to_list()
dvapiv.reset_index(inplace=True)

## Calculating TTM
dvapiv['TTM'] = dvapiv[last_year]
dvapiv['Anterior TTM'] = dvapiv[two_years_ago]

if '1T'+this_year in columns_list:
    conditions = [dvapiv['1T'+this_year].notna()]
    choices = [dvapiv['1T'+this_year]+dvapiv[last_year]-dvapiv['1T'+last_year]]
    dvapiv['TTM'] = np.select(conditions, choices, dvapiv['TTM'])
    dvapiv['Anterior TTM'] = np.select(conditions,
                                        [dvapiv['1T'+last_year]+dvapiv[two_years_ago]-dvapiv['1T'+two_years_ago]],
                                        dvapiv['Anterior TTM'])
    
if '2T'+this_year in columns_list:
    conditions = [dvapiv['2T'+this_year].notna()]
    choices = [dvapiv['2T'+this_year+'CON']+dvapiv[last_year]-dvapiv['2T'+last_year+'CON']]
    dvapiv['TTM'] = np.select(conditions, choices, dvapiv['TTM'])
    dvapiv['Anterior TTM'] = np.select(conditions,
                                       [dvapiv['2T'+last_year+'CON']+dvapiv[two_years_ago]-dvapiv['2T'+two_years_ago+'CON']],
                                       dvapiv['Anterior TTM'])
    
if '3T'+this_year in columns_list:
    conditions = [dvapiv['3T'+this_year].notna()]
    choices = [dvapiv['3T'+this_year+'CON']+dvapiv['4T'+last_year]]
    dvapiv['TTM'] = np.select(conditions, choices, dvapiv['TTM'])
    dvapiv['Anterior TTM'] = np.select(conditions,
                                       [dvapiv['3T'+last_year+'CON']+dvapiv[two_years_ago]-dvapiv['3T'+two_years_ago+'CON']],
                                       dvapiv['Anterior TTM'])
    
if this_year in columns_list:
    conditions = [dvapiv[this_year].notna()]
    choices = [dvapiv[this_year]]
    dvapiv['TTM'] = np.select(conditions, choices, dvapiv['TTM'])
    dvapiv['Anterior TTM'] = np.select(conditions,
                                       [dvapiv[last_year]],
                                       dvapiv['Anterior TTM'])

# dropping and melting  
dvapiv.drop(dvapiv.columns[dvapiv.columns.str.endswith('CON')], axis=1,inplace=True) 
dvapiv= dvapiv.melt(id_vars=['CD_CVM','GRUPO_DFP','DS_CONTA'],var_name='LABEL',value_name='VL_CONTA')
dvapiv.drop_duplicates(['CD_CVM','GRUPO_DFP','DS_CONTA','LABEL'],inplace=True)
dvapiv = dvapiv.pivot_table(index=['CD_CVM','GRUPO_DFP','LABEL'],columns = ['DS_CONTA'],values='VL_CONTA')


dvapiv.to_pickle('clean_data/companies_results/DVA/all_dva_pivotado.pkl')

##backup

dvapiv.to_pickle('BACKUPS/results/dva/all_dva_pivotado%s.pkl'%today)


# In[ ]:


### PIVOTING, MANIPULATING, CREATING TTM FOR DMPL


## reading all dmpl
a = pd.read_pickle('merged_data_cvm/new_data/DFP/dfp_cia_aberta_DMPL_con_tudo.pkl')
b = pd.read_pickle('merged_data_cvm/new_data/DFP/dfp_cia_aberta_DMPL_ind_tudo.pkl')
c = pd.read_pickle('merged_data_cvm/new_data/ITR/itr_cia_aberta_DMPL_con_tudo.pkl')
d = pd.read_pickle('merged_data_cvm/new_data/ITR/itr_cia_aberta_DMPL_ind_tudo.pkl')
dmplpiv = pd.concat([a,b,c,d])



## AJUSTANDO ERRO PTNT
dmplpiv['VL_CONTA'] = np.select([((dmplpiv['ESCALA_MOEDA'] == 'UNIDADE') & (dmplpiv['CD_CVM']==25488))], [dmplpiv['VL_CONTA']*1000], dmplpiv['VL_CONTA']*1)


## selecting important values

dmplpiv.loc[(dmplpiv.GRUPO_DFP == 'DF Consolidado - Demonstração das Mutações do Patrimônio Líquido'),'GRUPO_DFP'] = 'DF_CON'
dmplpiv.loc[(dmplpiv.GRUPO_DFP == 'DF Individual - Demonstração das Mutações do Patrimônio Líquido'),'GRUPO_DFP'] = 'DF_IND'
dmplpiv.drop_duplicates(['CD_CVM','GRUPO_DFP',"COLUNA_DF",'DS_CONTA','DT_INI_EXERC','DT_FIM_EXERC'],inplace=True)

dmplpiv['VL_CONTA'] = np.where(dmplpiv['ESCALA_MOEDA'] == 'MIL', dmplpiv['VL_CONTA']*1000, dmplpiv['VL_CONTA']*1)

dmplpiv.reset_index(inplace=True)


dmplpiv['DT_INI_EXERC'] = pd.to_datetime(dmplpiv['DT_INI_EXERC'])
dmplpiv['DT_FIM_EXERC'] = pd.to_datetime(dmplpiv['DT_FIM_EXERC'])

## creating labels to quartelys and annuals
dmplpiv['LABEL'] = dmplpiv.apply(lambda row: label_year(row), axis=1)

dmplpiv = dmplpiv[dmplpiv['LABEL'].notna()]

## pivotando to do calculations
dmplpiv = dmplpiv.pivot_table(index=['CD_CVM','GRUPO_DFP',"COLUNA_DF",'DS_CONTA'],columns = ['LABEL'],values='VL_CONTA')



## creating 4T
for year in range(2010,int(this_year)):
    dmplpiv['4T'+str(year)] = dmplpiv[str(year)]

## creating 2T
for year in range(2010,int(this_year)):   
    conditions = [dmplpiv['2T'+str(year)].isna()]
    choices = [dmplpiv['2T'+str(year)+'CON']-dmplpiv['1T'+str(year)]]
    dmplpiv['2T'+str(year)] = np.select(conditions, choices, dmplpiv['2T'+str(year)])
    
## creating 3T
for year in range(2010,int(this_year)):   
    conditions = [dmplpiv['3T'+str(year)].isna()]
    choices = [dmplpiv['3T'+str(year)+'CON']-dmplpiv['2T'+str(year)+'CON']]
    dmplpiv['3T'+str(year)] = np.select(conditions, choices, dmplpiv['3T'+str(year)])      
    
    
columns_list = dmplpiv.columns.to_list()
dmplpiv.reset_index(inplace=True)


## Calculating TTM
dmplpiv['TTM'] = dmplpiv[last_year]
dmplpiv['Anterior TTM'] = dmplpiv[two_years_ago]

if '1T'+this_year in columns_list:
    conditions = [dmplpiv['1T'+this_year].notna()]
    choices = [dmplpiv['1T'+this_year]]
    dmplpiv['TTM'] = np.select(conditions, choices, dmplpiv['TTM'])
    dmplpiv['Anterior TTM'] = np.select(conditions,
                                       [dmplpiv['1T'+last_year]],
                                       dmplpiv['Anterior TTM'])
if '2T'+this_year in columns_list:
    conditions = [dmplpiv['2T'+this_year].notna()]
    choices = [dmplpiv['2T'+this_year]]
    dmplpiv['TTM'] = np.select(conditions, choices, dmplpiv['TTM'])
    dmplpiv['Anterior TTM'] = np.select(conditions,
                                       [dmplpiv['2T'+last_year]],
                                       dmplpiv['Anterior TTM'])
        
if '3T'+this_year in columns_list:
    conditions = [dmplpiv['3T'+this_year].notna()]
    choices = [dmplpiv['3T'+this_year]]
    dmplpiv['TTM'] = np.select(conditions, choices, dmplpiv['TTM'])
    dmplpiv['Anterior TTM'] = np.select(conditions,
                                       [dmplpiv['3T'+last_year]],
                                       dmplpiv['Anterior TTM'])
    
if this_year in columns_list:
    conditions = [dmplpiv[this_year].notna()]
    choices = [dmplpiv[this_year]]
    dmplpiv['TTM'] = np.select(conditions, choices, dmplpiv['TTM'])
    dmplpiv['Anterior TTM'] = np.select(conditions,
                                       [dmplpiv[last_year]],
                                       dmplpiv['Anterior TTM'])
    
    
# dropping and melting  
dmplpiv.drop(dmplpiv.columns[dmplpiv.columns.str.endswith('CON')], axis=1,inplace=True)
dmplpiv= dmplpiv.melt(id_vars=['CD_CVM','GRUPO_DFP',"COLUNA_DF",'DS_CONTA'],var_name='LABEL',value_name='VL_CONTA')
dmplpiv.drop_duplicates(['CD_CVM','GRUPO_DFP',"COLUNA_DF",'DS_CONTA','LABEL'],inplace=True)
dmplpiv = dmplpiv.pivot_table(index=['CD_CVM','GRUPO_DFP','LABEL'],columns = ["COLUNA_DF",'DS_CONTA'],values='VL_CONTA')


dmplpiv.to_pickle('clean_data/companies_results/DMPL/all_dmpl_pivotado.pkl')

##backup

dmplpiv.to_pickle('BACKUPS/results/dmpl/all_dmpl_pivotado%s.pkl'%today)


# In[ ]:





# # Merging data, creating new columns

# In[ ]:


## merging all data

from functools import reduce

drepiv = pd.read_pickle('clean_data/companies_results/DRE/all_dre_pivotado.pkl')
bpppiv = pd.read_pickle('clean_data/companies_results/BPP/all_bpp_pivotado.pkl')
bpapiv = pd.read_pickle('clean_data/companies_results/BPA/all_bpa_pivotado.pkl')
dfcmipiv = pd.read_pickle('clean_data/companies_results/DFCMI/all_dfcmi_pivotado.pkl')
dvapiv = pd.read_pickle('clean_data/companies_results/DVA/all_dva_pivotado.pkl')


data_frames = [drepiv,bpppiv,bpapiv,dfcmipiv,dvapiv]
df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['CD_CVM','GRUPO_DFP','LABEL'],
                                            how='outer'), data_frames)
df_merged.reset_index(inplace=True)



# In[ ]:


seguradoras = [24180, 23159,  3115]
bancos = [22616,  1155, 20958,  1309,  8540,  3891,  1325,  1210, 20532,
        6076, 20567, 21199, 24600,   906,  1384, 19348,   922,  1171,
        1023, 14206, 24406,  1120,  1228]


# In[ ]:


## renaming columns
conditions = [(df_merged['Patrimônio Líquido Consolidado'].isna()) & (df_merged['Patrimônio Líquido'].notna())]
choices = [df_merged['Patrimônio Líquido']]

df_merged['Patrimônio Líquido Consolidado'] = np.select(conditions,choices,
                                                          df_merged['Patrimônio Líquido Consolidado']) 




df_merged['Receita Líquida'] = df_merged[['Receita de Venda de Bens e/ou Serviços',
                                    'Receitas da Intermediação Financeira',
                                    'Receitas das Operações',
                                    'Receitas de Intermediação Financeira']].mean(axis=1)
                                        
conditions = [df_merged['CD_CVM'].isin(bancos),
             df_merged['CD_CVM'].isin(seguradoras)]
choices = [(df_merged[['Despesas de Intermediação Financeira','Despesas da Intermediação Financeira']].mean(axis=1)),
          (df_merged['Sinistros e Despesas das Operações'])]

df_merged['Custos'] = np.select(conditions,choices,df_merged['Custo dos Bens e/ou Serviços Vendidos']) 

df_merged['Resultado Bruto'] = np.select([df_merged['CD_CVM'].isin(bancos)],
                                             [(df_merged[['Resultado Bruto de Intermediação Financeira',
                                              'Resultado Bruto Intermediação Financeira']].mean(axis=1))],
                                             df_merged['Resultado Bruto']) 

df_merged['Despesas/Receitas Operacionais'] = np.select(conditions,
                                             [(df_merged[['Outras Despesas/Receitas Operacionais',
                                              'Outras Despesas e Receitas Operacionais']].mean(axis=1)),
                                             df_merged['Despesas Administrativas']],
                                             df_merged['Despesas/Receitas Operacionais']) 

df_merged['EBIT'] = np.select([df_merged['CD_CVM'].isin(bancos)],
                                             [(df_merged[['Resultado Antes dos Tributos sobre o Lucro',
                                              'Resultado antes dos Tributos sobre o Lucro','Resultado Operacional']].mean(axis=1))],
                                          df_merged['Resultado Antes do Resultado Financeiro e dos Tributos'])    

df_merged['EBITDA'] = df_merged['EBIT']+np.abs(df_merged['Depreciação, Amortização e Exaustão'])

df_merged['Imposto'] = (df_merged[['Imposto de Renda e Contribuição Social sobre o Lucro','IR Diferido']].mean(axis=1))


df_merged['Lucro Líquido'] = (df_merged[['Lucro ou Prejuízo Líquido Consolidado do Período',
                                    'Lucro ou Prejuízo Líquido do Período',
                                    'Lucro/Prejuízo Consolidado do Período',
                                    'Lucro/Prejuízo do Período']].mean(axis=1))
                                    
df_merged['Lucro Atribuído a não controladores'] = (df_merged[['Atribuído a Sócios Não Controladores',
                                    'Atribuído aos Sócios não Controladores']].mean(axis=1))
 
df_merged['Lucro Atribuído a Controladores'] = (np.nan_to_num(df_merged['Lucro Líquido'])-
                                    np.nan_to_num(df_merged['Lucro Atribuído a não controladores']))
    
    
df_merged['Dividendos'] = np.abs(df_merged['Dividendos'])    
df_merged['Juros sobre o Capital Próprio'] = np.abs(df_merged['Juros sobre o Capital Próprio'])



# In[ ]:


keep_columns = ['CD_CVM','GRUPO_DFP','LABEL','Participação dos Acionistas Não Controladores',
       'Patrimônio Líquido', 
       'Patrimônio Líquido Consolidado',
       'Reservas de Lucro Final', 'Reservas de Lucro Incial',
       'Receita Líquida', 'Custos', 'EBIT', 'EBITDA', 'Imposto',
       'Lucro Líquido', 'Lucro Atribuído a não controladores',
       'Lucro Atribuído a Controladores', 'Passivo Total',
       'Passivo Circulante', 'Dívida curto prazo', 'Passivo Não Circulante',
       'Dívida longo prazo', 'Aplicações Financeiras', 'Ativo Circulante',
       'Ativo Não Circulante', 'Ativo Realizável a Longo Prazo', 'Ativo Total',
       'Caixa e Equivalentes de Caixa', 'Investimentos',
       'Aumento (Redução) de Caixa e Equivalentes',
       'Caixa Gerado nas Operações', 'Caixa Líquido Atividades Operacionais',
       'Caixa Líquido Atividades de Financiamento',
       'Caixa Líquido Atividades de Investimento', 'Capex',
       'Saldo Final de Caixa e Equivalentes',
       'Saldo Inicial de Caixa e Equivalentes',
       'Variação Cambial s/ Caixa e Equivalentes',
       'Variações nos Ativos e Passivos',
       'Depreciação, Amortização e Exaustão', 'Dividendos',
       'Juros sobre o Capital Próprio', 'Lucros ou Prejuízos Acumulados Final',
       'Lucros ou Prejuízos Acumulados Inicial','Resultado Bruto','Despesas/Receitas Operacionais','Resultado Financeiro',
               'Juros Pagos']


# In[ ]:


df_merged.drop(df_merged.columns.difference(keep_columns),axis = 1,inplace=True)


# In[ ]:


df_merged = df_merged[df_merged['Receita Líquida'] != 0]


# In[ ]:


df_merged.sort_values(['CD_CVM','LABEL','GRUPO_DFP'],inplace=True)


# In[ ]:


df_merged.drop_duplicates(['CD_CVM','LABEL'],keep='first',inplace=True)


# In[ ]:


df_merged.drop('GRUPO_DFP',inplace=True, axis=1)


# In[ ]:


df_merged.to_pickle('clean_data/companies_results/resultado_todos.pkl')

##backup

df_merged.to_pickle('BACKUPS/results/resultado_todos%s.pkl'%today)


# # Adding capital distribution and companies informations

# In[ ]:


## merging all data, adding capital distribution and on...

dfff = pd.read_pickle('clean_data/info_companies/info_companies_geral_comSetor.pkl')

df_merged = pd.read_pickle('clean_data/companies_results/resultado_todos.pkl')


pivot_setor_cvm_cd_conta = dfff.merge(df_merged, left_on='Codigo_CVM',right_on='CD_CVM', how='left').reset_index()
pivot_setor_cvm_cd_conta.drop(['index', 'Data_Nome_Empresarial','Unnamed: 0','Versao','ID_Documento',
                               'Nome_Empresarial_Anterior', 'Data_Categoria_Registro_CVM', 'Data_Especie_Controle_Acionario',
                               'Dia_Encerramento_Exercicio_Social', 'Mes_Encerramento_Exercicio_Social', 
                               'Data_Alteracao_Exercicio_Social','Categoria_Registro_CVM',
                              'Data_Situacao_Registro_CVM','Situacao_Emissor', 'Data_Situacao_Emissor'], axis=1, inplace=True)


# In[ ]:


pivot_setor_cvm_cd_conta = pivot_setor_cvm_cd_conta[pivot_setor_cvm_cd_conta['LABEL'].notna()]


# In[ ]:


## fixing labels and creatind dates
conditions = [(pivot_setor_cvm_cd_conta['LABEL'].str.startswith('20')),
              (pivot_setor_cvm_cd_conta['LABEL'].str.contains('TTM')),
              (pivot_setor_cvm_cd_conta['LABEL'].str.contains('T'))
              ]
choices = ['anual','TTM','trimestral']

pivot_setor_cvm_cd_conta['tipo_resultado'] = np.select(conditions,choices,None)

conditions = [(pivot_setor_cvm_cd_conta['LABEL'].str.startswith('1')),
 ((pivot_setor_cvm_cd_conta['LABEL'].str.startswith('2')) & 
 (pivot_setor_cvm_cd_conta['tipo_resultado']=='trimestral')),
 (pivot_setor_cvm_cd_conta['LABEL'].str.startswith('3')),
 (pivot_setor_cvm_cd_conta['LABEL'].str.startswith('4'))]

choices = ['1 trim','2 trim','3 trim','4 trim']

pivot_setor_cvm_cd_conta['trimestre'] = np.select(conditions,choices,'annual')

this_year = datetime.datetime.today().strftime('%Y')
last_year = (datetime.datetime.now() - relativedelta(years=1)).strftime('%Y')
hoje = datetime.datetime.today().strftime('%Y-%m-%d')
ttm_ant = (datetime.datetime.today() - relativedelta(years=1)).strftime('%Y-%m-%d')

pivot_setor_cvm_cd_conta['DT_FIM_EXERC'] = None
for year in range(2009,int(this_year)+1):
    conditions = [(pivot_setor_cvm_cd_conta['LABEL'].str.startswith('1T'+str(year))),
                 (pivot_setor_cvm_cd_conta['LABEL'].str.startswith('2T'+str(year))),
                 (pivot_setor_cvm_cd_conta['LABEL'].str.startswith('3T'+str(year))),
                 (pivot_setor_cvm_cd_conta['LABEL'].str.startswith('4T'+str(year))),
                 (pivot_setor_cvm_cd_conta['LABEL'] == str(year)),
                 (pivot_setor_cvm_cd_conta['LABEL'] == 'TTM'),
                 (pivot_setor_cvm_cd_conta['LABEL'] == 'Anterior TTM')]
    choices = [str(year)+'-03-31',str(year)+'-06-30',str(year)+'-09-30',str(year)+'-12-31',
               str(year)+'-12-31',hoje,ttm_ant]
    
    pivot_setor_cvm_cd_conta['DT_FIM_EXERC'] = np.select(conditions,choices,pivot_setor_cvm_cd_conta['DT_FIM_EXERC'] )  

pivot_setor_cvm_cd_conta['DT_FIM_EXERC'] = pd.to_datetime( pivot_setor_cvm_cd_conta['DT_FIM_EXERC'])
pivot_setor_cvm_cd_conta['mês'] = pivot_setor_cvm_cd_conta['DT_FIM_EXERC'].dt.month
pivot_setor_cvm_cd_conta['ano'] = pivot_setor_cvm_cd_conta['DT_FIM_EXERC'].dt.year


# In[ ]:


# distribuição acionaria
fca = pd.read_pickle('merged_data_cvm/new_data/FCA/fca_cia_aberta_geral_tudo.pkl')
cnpj = fca.drop(fca.columns.difference(['CNPJ_Companhia','Codigo_CVM']),axis=1).drop_duplicates('CNPJ_Companhia')
fre_dist = pd.read_pickle('merged_data_cvm/new_data/FRE/fre_cia_aberta_distribuicao_capital_tudo.pkl')
fre_dist = fre_dist.merge(cnpj,on='CNPJ_Companhia')
fre_dist['Data_Referencia'] = pd.to_datetime(fre_dist['Data_Referencia'])
fre_dist['ano'] = fre_dist['Data_Referencia'].dt.year


capital_social = pd.read_pickle('raw_data_B3/capital_social/capitalsocial.pkl')



pivot_setor_cvm_cd_conta = pivot_setor_cvm_cd_conta.merge(fre_dist,on=['Codigo_CVM','ano'], 
                                                          how='left', suffixes =[None,'_hist'])


capital_social['tipo_resultado']= 'TTM'
pivot_setor_cvm_cd_conta = pivot_setor_cvm_cd_conta.merge(capital_social,
                                                          left_on=['CDO_STRIP','LABEL'],
                                                          right_on=['Código','tipo_resultado'], 
                                                          how='left', 
                                                          suffixes =[None,'_atual'])
pivot_setor_cvm_cd_conta.drop_duplicates(['CODIGO','tipo_resultado','trimestre',
                                         'DT_FIM_EXERC'],inplace=True)


# In[ ]:


## fixing distribução acionara

drop = ['Data_Ultima_Assembleia',
'Nome do Pregão',
'Denominação Social',
'Segmento de Mercado',
'Tipo de Capital',
'Capital R$',
'Aprovado em',
'CNPJ_Companhia_hist',
'Data_Referencia_hist',
'Versao',
'ID_Documento',
'Data_Referencia','CD_CVM_x']

pivot_setor_cvm_cd_conta.drop(drop,axis=1,inplace=True)

pivot_setor_cvm_cd_conta['Percentual_Total_Acoes_Circulacao'] = pivot_setor_cvm_cd_conta['Percentual_Total_Acoes_Circulacao'].apply(lambda x: float(x))
pivot_setor_cvm_cd_conta['Percentual_Acoes_Ordinarias_Circulacao'] = pivot_setor_cvm_cd_conta['Percentual_Acoes_Ordinarias_Circulacao'].apply(lambda x: float(x))
pivot_setor_cvm_cd_conta['Percentual_Acoes_Preferenciais_Circulacao'] = pivot_setor_cvm_cd_conta['Percentual_Acoes_Preferenciais_Circulacao'].apply(lambda x: float(x))

pivot_setor_cvm_cd_conta['Total Ações'] = (pivot_setor_cvm_cd_conta['Quantidade_Total_Acoes_Circulacao']
                                * 100)/pivot_setor_cvm_cd_conta['Percentual_Total_Acoes_Circulacao']

pivot_setor_cvm_cd_conta['Total ON'] = (pivot_setor_cvm_cd_conta['Quantidade_Acoes_Ordinarias_Circulacao']
                                * 100)/pivot_setor_cvm_cd_conta['Percentual_Acoes_Ordinarias_Circulacao']


pivot_setor_cvm_cd_conta['Total PN'] = (pivot_setor_cvm_cd_conta['Quantidade_Acoes_Preferenciais_Circulacao']
                                * 100)/pivot_setor_cvm_cd_conta['Percentual_Acoes_Preferenciais_Circulacao']

pivot_setor_cvm_cd_conta['Total PN'] = np.select([pivot_setor_cvm_cd_conta['LABEL'] == 'TTM'],
                                                [0],
                                                pivot_setor_cvm_cd_conta['Total PN'])

pivot_setor_cvm_cd_conta['Total ON'] = np.select([pivot_setor_cvm_cd_conta['LABEL'] == 'TTM'],
                                                [0],
                                                pivot_setor_cvm_cd_conta['Total ON'])

pivot_setor_cvm_cd_conta['Total Ações'] = np.select([pivot_setor_cvm_cd_conta['LABEL'] == 'TTM'],
                                                [0],
                                                pivot_setor_cvm_cd_conta['Total Ações'])

pivot_setor_cvm_cd_conta['Total Ações'] = np.nan_to_num(pivot_setor_cvm_cd_conta['Total Ações']) + np.nan_to_num(pivot_setor_cvm_cd_conta['Qtde Total de Ações'])

pivot_setor_cvm_cd_conta['Total ON'] = np.nan_to_num(pivot_setor_cvm_cd_conta['Total ON']) + np.nan_to_num(pivot_setor_cvm_cd_conta['Qtde Ações Ordinárias'])

pivot_setor_cvm_cd_conta['Total PN'] = np.nan_to_num(pivot_setor_cvm_cd_conta['Total PN']) + np.nan_to_num(pivot_setor_cvm_cd_conta['Qtde Ações Preferenciais'])

drop2 = ['Qtde Ações Ordinárias',
'Qtde Ações Preferenciais',
'Qtde Total de Ações',
'tipo_resultado_atual',
'Quantidade_Total_Acoes_Circulacao',
'Quantidade_Acoes_Preferenciais_Circulacao',
'Quantidade_Acoes_Ordinarias_Circulacao']

pivot_setor_cvm_cd_conta.drop(drop2,axis=1,inplace=True)


# In[ ]:


### adjusting ri website
pivot_setor_cvm_cd_conta['Pagina_Web'] = pivot_setor_cvm_cd_conta['Pagina_Web'].astype(str)

conditions = [(pivot_setor_cvm_cd_conta['Pagina_Web'].str.startswith('http://')),(pivot_setor_cvm_cd_conta['Pagina_Web'].str.startswith('https://'))]
choices = [pivot_setor_cvm_cd_conta['Pagina_Web'],pivot_setor_cvm_cd_conta['Pagina_Web']]
pivot_setor_cvm_cd_conta['Pagina_Web'] = np.select(conditions,choices,('http://'+pivot_setor_cvm_cd_conta['Pagina_Web']))


# In[ ]:


pivot_setor_cvm_cd_conta.to_pickle("clean_data/pivoted_data/pivot_all_data.pkl")

#backup 
pivot_setor_cvm_cd_conta.to_pickle("BACKUPS/pivoted_data/pivot_all_data%s.pkl"%today)

