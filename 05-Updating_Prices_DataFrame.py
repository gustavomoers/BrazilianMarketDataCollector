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


import warnings
warnings.filterwarnings('ignore')


# # Adding price history, current price and calculating KPIs

# In[ ]:


## adding price history by month and year


hist_price = pd.read_csv('PRICES/monthly/todos_precos_montlhy_AV.csv', index_col=False,low_memory=False)

hist_price['date'] = pd.to_datetime(hist_price['date'])

hist_price['ano'] = hist_price['date'].dt.year
hist_price['mês'] = hist_price['date'].dt.month



lpp = hist_price.sort_values('date').groupby(['symbol','ano','mês']).tail(1)

lpp.drop(lpp.columns.difference(['date','5. adjusted close','symbol','ano','mês']), 
         axis=1,inplace=True)

lpp = lpp.drop(lpp[(lpp.ano == datetime.datetime.today().year) & (lpp["mês"] == datetime.datetime.today().month)].index)

pivall = pd.read_pickle('clean_data/pivoted_data/pivot_all_data.pkl')

pivall_hist_prices = pivall.merge(lpp, left_on=['CODIGO','ano','mês'], right_on=['symbol','ano','mês'],
                                  how='left')

pivall_hist_prices.to_pickle('clean_data/pivoted_data/pivot_all_data_hist_prices.pkl')

##backup

pivall_hist_prices.to_pickle('BACKUPS/pivoted_data/pivot_all_data_hist_prices%s.pkl'%today)


# In[ ]:


## Reading current price from yahoo finance

companies_data = pd.read_excel('info_brazilian_companies.xlsx')

item_list = companies_data['CODIGO'].to_list()



## getting currente prices

current_prices = {}
for item in item_list:
    try:
        item_1 = item.lower()
        current_prices.update({item: Ticker('%s.sa'%item_1).price[item_1+'.sa']['regularMarketPrice']})
    except:
        continue

companies_curr_price = pd.DataFrame.from_dict(current_prices, orient='index', columns=['current_price'])
companies_curr_price['current_price'] = np.select([companies_curr_price['current_price']== {}],[0],companies_curr_price['current_price'])
companies_curr_price['current_price'] = companies_curr_price['current_price'].astype('float64')
companies_curr_price.to_pickle('PRICES/current_prices.pkl')

##backup

companies_curr_price.to_pickle('BACKUPS/price/current_prices%s.pkl'%today)


# In[ ]:


### adding current prices to dataframe on TTM row
df_data = pd.read_pickle('clean_data/pivoted_data/pivot_all_data_hist_prices.pkl')
companies_curr_price = pd.read_pickle('PRICES/current_prices.pkl')
companies_curr_price['TTM'] = 'TTM' 


companies_curr_price.reset_index(inplace=True)

df_data = df_data.merge(companies_curr_price,left_on=['CODIGO','LABEL'],right_on=['index','TTM'],how='outer')




df_data['Preço'] = np.nan_to_num(df_data['5. adjusted close'])+np.nan_to_num(df_data['current_price'])

df_data.drop(['current_price','symbol','5. adjusted close','TTM','date'],axis=1,inplace=True)


df_data.to_pickle('clean_data/pivoted_data/pivot_alldata_with_curr_price.pkl')

##backup

df_data.to_pickle('BACKUPS/pivoted_data/pivot_alldata_with_curr_price%s.csv'%today)


# In[ ]:


# Calculating KPIs

pivot_alldata = pd.read_pickle('clean_data/pivoted_data/pivot_alldata_with_curr_price.pkl')




# Proventos    
pivot_alldata['Proventos'] = np.nan_to_num(pivot_alldata['Dividendos'])+np.nan_to_num(pivot_alldata['Juros sobre o Capital Próprio'])
pivot_alldata['Payout'] = (pivot_alldata['Proventos'])/(pivot_alldata['Lucro Atribuído a Controladores'])
pivot_alldata['Proventos por ação'] = (pivot_alldata['Proventos'])/(pivot_alldata['Total Ações'])  




# operations
pivot_alldata['Margem EBITDA'] = pivot_alldata['EBITDA']/pivot_alldata['Receita Líquida']
pivot_alldata['Margem EBIT'] = pivot_alldata['EBIT']/pivot_alldata['Receita Líquida']
pivot_alldata['Margem líquida'] = (pivot_alldata['Lucro Atribuído a Controladores'])/(pivot_alldata['Receita Líquida'])
pivot_alldata['Margem bruta'] = (pivot_alldata['Resultado Bruto'])/(pivot_alldata['Receita Líquida'])
pivot_alldata['Taxa efetiva de imposto'] = np.abs(pivot_alldata['Imposto']/pivot_alldata['EBIT'])
pivot_alldata['EBIT(1-t)'] = pivot_alldata['EBIT']*(1-pivot_alldata['Taxa efetiva de imposto'])




# divida e patrimonio
pivot_alldata['Dívida Bruta'] = pivot_alldata['Dívida curto prazo']+pivot_alldata['Dívida longo prazo']
pivot_alldata['Disponibilidade'] = pivot_alldata['Caixa e Equivalentes de Caixa']+pivot_alldata['Aplicações Financeiras']
pivot_alldata['Dívida Líquida'] = pivot_alldata['Dívida Bruta']-pivot_alldata['Disponibilidade']
pivot_alldata['Dív_Líq/EBIT'] = pivot_alldata['Dívida Líquida']/pivot_alldata['EBIT']
pivot_alldata['Dív_Líq/PL'] = (pivot_alldata['Dívida Líquida'])/(pivot_alldata['Patrimônio Líquido Consolidado'])
pivot_alldata['D/E'] = (pivot_alldata['Dívida Bruta'])/(pivot_alldata['Patrimônio Líquido Consolidado'])
pivot_alldata['Capital Investido'] = (pivot_alldata['Dívida Bruta'])+(pivot_alldata['Patrimônio Líquido Consolidado'])
pivot_alldata['Dívida/Capital_Inv'] = (pivot_alldata['Dívida Bruta'])/(pivot_alldata['Capital Investido'])
pivot_alldata['Patrimônio/Capital_Inv'] = (pivot_alldata['Patrimônio Líquido Consolidado'])/(pivot_alldata['Capital Investido'])
pivot_alldata['Endividamento Financeiro'] = (pivot_alldata['Dívida Bruta'])/(pivot_alldata['Ativo Total'])
pivot_alldata['Endividamento Financeiro Curto Prazo'] = (pivot_alldata['Dívida curto prazo'])/(pivot_alldata['Ativo Total'])
pivot_alldata['Dív_Líq/ebitda'] = (pivot_alldata['Dívida Líquida'])/(pivot_alldata['EBITDA'])  
pivot_alldata['Dívida Líquida Anterior'] = pivot_alldata.groupby(['CODIGO','tipo_resultado','trimestre'])['Dívida Líquida'].shift()
pivot_alldata['PL Con Anterior'] = pivot_alldata.groupby(['CODIGO','tipo_resultado','trimestre'])['Patrimônio Líquido Consolidado'].shift()
pivot_alldata['PL + Dív_Líq_Ant'] = (pivot_alldata['PL Con Anterior'])+(pivot_alldata['Dívida Líquida Anterior'])  
pivot_alldata['PL/Ativos'] = (pivot_alldata['Patrimônio Líquido Consolidado'])/(pivot_alldata['Ativo Total'])
pivot_alldata['PL + Dív_Líq'] = (pivot_alldata['Patrimônio Líquido Consolidado'])+(pivot_alldata['Dívida Líquida']) 



# returns
pivot_alldata['ROE'] = (pivot_alldata['Lucro Atribuído a Controladores'])/(pivot_alldata['PL Con Anterior'])
pivot_alldata['Porcentagem Retida'] = 1-(pivot_alldata['Payout'])  
pivot_alldata['Ativo Total Anterior'] = pivot_alldata.groupby(['CODIGO','tipo_resultado','trimestre'])['Ativo Total'].shift()
pivot_alldata['ROA'] = (pivot_alldata['EBIT(1-t)'])/(pivot_alldata['Ativo Total Anterior'])        
pivot_alldata['ROIC']=pivot_alldata['EBIT(1-t)']/pivot_alldata['PL + Dív_Líq_Ant']


## ajustando indicadores onde patrimonio liquido é negativo
pivot_alldata['ROE'] = np.select([pivot_alldata['PL Con Anterior'] < 0],[np.nan],pivot_alldata['ROE'])
pivot_alldata['ROIC'] = np.select([pivot_alldata['PL Con Anterior'] < 0],[np.nan],pivot_alldata['ROIC'])

# ativos e passivos 
pivot_alldata['Passivo/Ativo'] = (pivot_alldata['Passivo Circulante']+pivot_alldata['Passivo Não Circulante'])/(pivot_alldata['Ativo Total'])  
pivot_alldata['Líquidez Corrente'] = (pivot_alldata['Ativo Circulante'])/(pivot_alldata['Passivo Circulante'])  
pivot_alldata['Giro dos Ativos'] = (pivot_alldata['Receita Líquida'])/(pivot_alldata['Ativo Total'])  


# capex
pivot_alldata['Depreciação'] = np.abs(pivot_alldata['Depreciação, Amortização e Exaustão'])

pivot_alldata['Capex'] = np.abs(pivot_alldata['Capex'])

pivot_alldata['Capex'] = np.select([pivot_alldata['Capex'].isna()],
                                   [np.abs(pivot_alldata['Caixa Líquido Atividades de Investimento'])],
                                   pivot_alldata['Capex'])

pivot_alldata['Capex Líquido'] = np.abs(pivot_alldata['Capex'])-(pivot_alldata['Depreciação'])
pivot_alldata['Ativo_Circ non-cash'] = (pivot_alldata['Ativo Circulante']-pivot_alldata['Caixa e Equivalentes de Caixa']-pivot_alldata['Aplicações Financeiras'])
pivot_alldata['Passivo_Circ non-cash'] = (pivot_alldata['Passivo Circulante']-pivot_alldata['Dívida curto prazo'])
pivot_alldata['Capital de Giro non-cash'] = (pivot_alldata['Ativo_Circ non-cash']-pivot_alldata['Passivo_Circ non-cash'])
pivot_alldata['Capital de Giro non-cash anterior'] = pivot_alldata.groupby(['CODIGO','tipo_resultado','trimestre'])['Capital de Giro non-cash'].shift()
pivot_alldata['Variação Capital de Giro'] = (pivot_alldata['Capital de Giro non-cash']-pivot_alldata['Capital de Giro non-cash anterior'])                                      
pivot_alldata['Capital_Giro/revenues'] = (pivot_alldata['Capital de Giro non-cash']/pivot_alldata['Receita Líquida'])                                       
pivot_alldata['Patrimônio Reinvestido'] = (pivot_alldata['Capex Líquido'])+(pivot_alldata['Variação Capital de Giro'])
pivot_alldata['Porcentagem Reinvestida'] = (pivot_alldata['Patrimônio Reinvestido'])/(pivot_alldata['EBIT(1-t)'])
pivot_alldata['Receitas/Capital_Inv'] = (pivot_alldata['Receita Líquida'])/(pivot_alldata['PL + Dív_Líq'])
pivot_alldata['Capex/Receita'] = np.abs(pivot_alldata['Capex'])/pivot_alldata['Receita Líquida']
pivot_alldata['Capex/Depreciação'] = np.abs(pivot_alldata['Capex'])/pivot_alldata['Depreciação']
pivot_alldata['Capex Líquido/Receita'] = pivot_alldata['Capex Líquido']/pivot_alldata['Receita Líquida']
pivot_alldata['Capex Líquido/EBIT(1-t)'] = pivot_alldata['Capex Líquido']/pivot_alldata['EBIT(1-t)']


# pivotando preços units, on, pn e calculando market cap hist

prices_pivoted = pivot_alldata.pivot_table(columns=['CLASSE'], index=['Codigo_CVM','DT_FIM_EXERC'],values='Preço')

pivot_alldata = pivot_alldata.merge(prices_pivoted, left_on=['Codigo_CVM','DT_FIM_EXERC'], right_index=True,how='outer',)

units = {'TIET11': 5,
         'ALUP11': 3,
         'BIDI11': 3,
         'BPAC11': 3,
         'ENGI11': 5,
         'KLBN11': 5,
         'RNEW11': 3,
         'SAPR11': 5,
         'SANB11': 2,
         'SULA11': 3,
         'TAEE11': 3}


qnt_units = pd.DataFrame.from_dict(units, orient='index',columns=['Qtde de ações UN'])

pivot_alldata = pivot_alldata.merge(qnt_units, left_on=['CODIGO'], right_index=True, how='outer')

pivot_alldata['Qtde de ações UN'] = pivot_alldata['Qtde de ações UN'].fillna(value=1)

qnt_mv_value = {'TIET': 5,
         'ALUP': 3,
         'BIDI': 3,
         'BPAC': 3,
         'ENGI': 5,
         'KLBN': 5,
         'RNEW': 3,
         'SAPR': 5,
         'SANB': 2,
         'SULA': 3,
         'TAEE': 3}

qnt_mv = pd.DataFrame.from_dict(qnt_mv_value, orient='index',columns=['Qtde de ações UNITS'])

pivot_alldata = pivot_alldata.merge(qnt_mv, left_on=['CDO_STRIP'], right_index=True, how='outer')



conditions = [pivot_alldata['UNT N2'] > 0]

choices = [((pivot_alldata['UNT N2']/pivot_alldata['Qtde de ações UNITS'])*pivot_alldata['Total Ações'])]

pivot_alldata['Valor de Mercado'] = np.select(conditions, choices, default=((np.nan_to_num(pivot_alldata['ON'])*np.nan_to_num(pivot_alldata['Total ON'])) +
                                                                       (np.nan_to_num(pivot_alldata['PN'])*np.nan_to_num(pivot_alldata['Total PN'])) +
                                                                       (np.nan_to_num(pivot_alldata['PNA'])*np.nan_to_num(pivot_alldata['Total PN'])) +
                                                                       (np.nan_to_num(pivot_alldata['PNB'])*np.nan_to_num(pivot_alldata['Total PN'])) +
                                                                       (np.nan_to_num(pivot_alldata['PNC'])*np.nan_to_num(pivot_alldata['Total PN'])) +
                                                                       (np.nan_to_num(pivot_alldata['PND'])*np.nan_to_num(pivot_alldata['Total PN'])) +
                                                                       (np.nan_to_num(pivot_alldata['PNE'])*np.nan_to_num(pivot_alldata['Total PN'])) +
                                                                       (np.nan_to_num(pivot_alldata['PNF'])*np.nan_to_num(pivot_alldata['Total PN']))))



conditions = [pivot_alldata['UNT'] > 0]

choices = [((pivot_alldata['UNT']/pivot_alldata['Qtde de ações UNITS'])*pivot_alldata['Total Ações'])]

pivot_alldata['Valor de Mercado'] = np.select(conditions, choices, pivot_alldata['Valor de Mercado'])



# calculando indicadores historicos que dependem do market cap

pivot_alldata['Valor de Firma'] = pivot_alldata['Valor de Mercado']+(pivot_alldata['Dívida Líquida'])
pivot_alldata['EV/EBITDA'] = pivot_alldata['Valor de Firma']/(pivot_alldata['EBITDA'])
pivot_alldata['EV/EBIT'] = pivot_alldata['Valor de Firma']/(pivot_alldata['EBIT'])




# dependentes do share outstanding
pivot_alldata['LPA'] = (pivot_alldata['Qtde de ações UN']*pivot_alldata['Lucro Atribuído a Controladores'])/(pivot_alldata['Total Ações'])
pivot_alldata['VPA'] = (pivot_alldata['Qtde de ações UN']*pivot_alldata['Patrimônio Líquido Consolidado'])/(pivot_alldata['Total Ações'])

# indicadores de preço históricos

pivot_alldata['P/L'] = pivot_alldata['Preço']/pivot_alldata['LPA']
pivot_alldata['P/VPA'] = pivot_alldata['Preço']/pivot_alldata['VPA']
pivot_alldata['P/EBITDA'] = (pivot_alldata['Preço']*pivot_alldata['Total Ações'])/(pivot_alldata['Qtde de ações UN']*pivot_alldata['EBITDA'])
pivot_alldata['P/EBIT'] = (pivot_alldata['Preço']*pivot_alldata['Total Ações'])/(pivot_alldata['Qtde de ações UN']*pivot_alldata['EBIT'])
pivot_alldata['P/Ativo'] = (pivot_alldata['Preço']*pivot_alldata['Total Ações'])/(pivot_alldata['Qtde de ações UN']*pivot_alldata['Ativo Total'])
pivot_alldata['P/Cap_Giro'] = (pivot_alldata['Preço']*pivot_alldata['Total Ações'])/(pivot_alldata['Qtde de ações UN']*(pivot_alldata['Ativo Circulante']-pivot_alldata['Passivo Circulante']))
pivot_alldata['P/Ativo_Circ_Líq'] = (pivot_alldata['Preço']*pivot_alldata['Total Ações'])/(pivot_alldata['Qtde de ações UN']*(pivot_alldata['Ativo Circulante']-pivot_alldata['Passivo Total']))



# classficiação small/large caps

conditions = [(pivot_alldata['Valor de Mercado'] == 0),(pivot_alldata['Valor de Mercado'] < 50000000), ((pivot_alldata['Valor de Mercado'] >= 50000000) & (pivot_alldata['Valor de Mercado'] < 300000000)),
             ((pivot_alldata['Valor de Mercado'] >= 300000000) & (pivot_alldata['Valor de Mercado'] < 2000000000)), ((pivot_alldata['Valor de Mercado'] >= 2000000000) & (pivot_alldata['Valor de Mercado'] < 10000000000)),
             ((pivot_alldata['Valor de Mercado'] >= 10000000000) & (pivot_alldata['Valor de Mercado'] < 200000000000)), (pivot_alldata['Valor de Mercado'] >= 200000000000)]

choices = [np.nan,'Nano Cap','Micro Cap','Small Cap','Mid Cap','Large Cap','Mega Cap']

pivot_alldata['Classificação Capitalização'] = np.select(conditions, choices, default=np.nan)



# expected growths
pivot_alldata['Crescimento esperado LPA'] = (pivot_alldata['Porcentagem Retida'])*(pivot_alldata['ROE'])     
pivot_alldata['Crescimento esperado EBIT'] = (pivot_alldata['Porcentagem Reinvestida'])*(pivot_alldata['ROIC'])    
pivot_alldata['Crescimento EBIT'] = pivot_alldata.groupby(['CODIGO','tipo_resultado','trimestre'])['EBIT'].pct_change()
pivot_alldata['Crescimento Margem EBIT'] = pivot_alldata.groupby(['CODIGO','tipo_resultado','trimestre'])['Margem EBIT'].pct_change()
pivot_alldata['Crescimento LPA'] = pivot_alldata.groupby(['CODIGO','tipo_resultado','trimestre'])['LPA'].pct_change()
pivot_alldata['Crescimento Receita'] = pivot_alldata.groupby(['CODIGO','tipo_resultado','trimestre'])['Receita Líquida'].pct_change()
    


    

pivot_alldata = pivot_alldata.replace([np.inf, -np.inf], np.nan)


pivot_alldata.to_pickle('clean_data/pivoted_data/pivot_com_indicadores.pkl')

##backup

pivot_alldata.to_pickle('BACKUPS/pivoted_data/pivot_com_indicadores%s.csv'%today)


# In[ ]:


## adding dividends and calculating dividend yield

alldiv = pd.read_pickle('clean_data/dividends/proventos_b3.pkl')
alldiv['Data COM'] = pd.to_datetime(alldiv['Data COM'])

dv_year = alldiv.drop(alldiv.columns.difference(['Codigo_CVM','CLASSE','ano','Valor ajustado']),axis=1)

dv_year = dv_year.groupby(['Codigo_CVM','CLASSE','ano'],as_index=False).sum()

dv_year.rename({"Valor ajustado":"Proventos no Período","ano":"label"},axis=1,inplace=True)

dv_ttm = alldiv.drop(alldiv.columns.difference(['Codigo_CVM','CLASSE','Data COM','Valor ajustado']),axis=1)

dv_ttm = dv_ttm.set_index('Data COM')

dv_ttm = dv_ttm.loc[one_year_ago:today]

dv_ttm = dv_ttm.groupby(['Codigo_CVM','CLASSE'],as_index=False).sum()

dv_ttm.rename({'Valor ajustado':"Proventos no Período"},axis=1,inplace=True)

dv_ttm['label'] = 'TTM'

dv_all = pd.concat([dv_year,dv_ttm])

df = pd.read_pickle('clean_data/pivoted_data/pivot_com_indicadores.pkl')

df['Class div'] = np.select([((df['CLASSE']=='UNT') | (df['CLASSE']=='UNT N2'))],
                           ['UNT'],df['CLASSE'])

dv_all['label'] = dv_all['label'].astype(str)

df['LABEL'] = df['LABEL'].astype(str)

df_all = df.merge(dv_all,left_on=['Codigo_CVM','Class div','LABEL'],right_on=['Codigo_CVM','CLASSE','label'],
                 how='left')


df_all['Dividend Yield'] = df_all['Proventos no Período']/df_all['Preço']

df_all.drop(['CLASSE_y','Class div', 'label'], axis=1,inplace=True)

df_all = df_all.dropna(subset=['Nome_Empresarial'])

df_all.to_pickle('clean_data/final_data/df_principal.pkl')

##backup

df_all.to_pickle('BACKUPS/final_data/df_principal%s.pkl'%today)


# # Valuation KPIs

# In[ ]:


df = pd.read_pickle("clean_data/final_data/df_principal.pkl")


# In[ ]:


ttm = df.loc[df['LABEL'] == 'TTM']


# In[ ]:


damodaran_table = pd.read_excel('VALUATION/damodaran_table.xlsx')


# In[ ]:


# function to get spread and calculate cost of debt for company
def cost_of_debt(brazil_risk_free_rate, interest_coverage_ratio):
    if interest_coverage_ratio > 12.5:
        #Rating is AAA
        credit_spread = 0.0063
    if (interest_coverage_ratio > 9.5) & (interest_coverage_ratio <= 12.5):
        #Rating is AA
        credit_spread = 0.0078
    if (interest_coverage_ratio > 7.5) & (interest_coverage_ratio <= 9.5):
        # Rating is A+
        credit_spread = 0.0098
    if (interest_coverage_ratio > 6) & (interest_coverage_ratio <= 7.5):
        #Rating is A
        credit_spread = 0.0108
    if (interest_coverage_ratio > 4.5) & (interest_coverage_ratio <= 6):
        # Rating is A-
        credit_spread = 0.0122
    if (interest_coverage_ratio > 4) & (interest_coverage_ratio <= 4.5):
        #Rating is BBB
        credit_spread = 0.0156
    if (interest_coverage_ratio == 4):
        # Rating is BB+
        credit_spread = 0.02
    if (interest_coverage_ratio > 3) & (interest_coverage_ratio < 4):
        #Rating is BB
        credit_spread = 0.0240
    if (interest_coverage_ratio > 2.5) & (interest_coverage_ratio <= 3):
        # Rating is B+
        credit_spread = 0.0351
    if (interest_coverage_ratio > 2) & (interest_coverage_ratio <= 2.5):
        #Rating is B
        credit_spread = 0.0421
    if (interest_coverage_ratio > 1.5) & (interest_coverage_ratio <= 2):
        # Rating is B-
        credit_spread = 0.0515
    if (interest_coverage_ratio > 1.25) & (interest_coverage_ratio <= 2):
        #Rating is CCC
        credit_spread = 0.0820
    if (interest_coverage_ratio > 0.8) & (interest_coverage_ratio <= 1.25):
        #Rating is CC
        credit_spread = 0.0864
    if (interest_coverage_ratio > 0.5) & (interest_coverage_ratio <= 0.8):
        #Rating is C
        credit_spread = 0.1134
    if interest_coverage_ratio <= 0.5:
        #Rating is D
        credit_spread = 0.1512

    cd = brazil_risk_free_rate + credit_spread

    return cd


# In[ ]:


def busca_titulos_tesouro_direto():
    url = 'https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv'
    df = pd.read_csv(url, sep=';', decimal=',')
    df['Data Vencimento'] = pd.to_datetime(
        df['Data Vencimento'], dayfirst=True)
    df['Data Base'] = pd.to_datetime(df['Data Base'], dayfirst=True)
    multi_indice = pd.MultiIndex.from_frame(df.iloc[:, :3])
    df = df.set_index(multi_indice).iloc[:, 3:]
    return df


# ## BRAZILIAN BOND 10 YEAR

# In[ ]:


titulos = busca_titulos_tesouro_direto()

# getting 10 year brazilian bond

pre2031 = titulos.loc[('Tesouro Prefixado com Juros Semestrais', '2031-01-01')]
bond = pre2031.iloc[-1]['Taxa Compra Manha']/100
hoje = datetime.date.today()
hoje_em_texto = '{}/{}/{}'.format(hoje.day, hoje.month, hoje.year)
ontem = hoje - datetime.timedelta(days=4)
ontem_em_texto = '{}/{}/{}'.format(ontem.day, ontem.month, ontem.year)
#dez_ano = inv.get_bond_historical_data('Brazil 10Y', from_date=ontem_em_texto, to_date=hoje_em_texto)
#brazil_10y_bond = dez_ano.iloc[-1]['Close']/100


# ## ERP, RISK FREE RATE

# In[ ]:


# getting spread, risk free rate and erp

brazil_default_spread = (damodaran_table.loc[damodaran_table['Country'] == 'Brazil']['Rating-based Default Spread'].values[0])/100
brazil_risk_free_rate = bond-brazil_default_spread
brazil_erp = (damodaran_table.loc[damodaran_table['Country'] == 'Brazil']['Total Equity Risk Premium'].values[0])/100


# ## BETA

# In[ ]:


### beta from yahoo

companies = ttm['CODIGO'].unique()
ttm['beta'] = np.nan

for x in companies:
    try:
        y = x.lower()
        beta = Ticker(y+'.sa').key_stats[y+'.sa']['beta']
        ttm['beta'] = np.select([ttm['CODIGO'] == x],[beta],ttm['beta'])
    except:
        continue

### SE ERRO beta médio do setor

med = ttm.drop_duplicates('Nome_Empresarial',keep='first')

mean = med.groupby('SEGMENTO')['beta'].mean()

ttm['beta'] = np.select([ttm['beta'].isna()],[mean[ttm['SEGMENTO']]],ttm['beta'])

### SE ERRO, beta médio global do damodaran

mean_global = pd.read_excel('VALUATION/global_means_damodaran2021.xlsx')

beta_glob = mean_global.groupby('Industry name')['Equity (Levered) Beta'].mean()

ttm['beta'] = np.select([ttm['beta'].isna()],[beta_glob[ttm['DAMODARAN_GROUP']]],
                        ttm['beta'])




# ## COST OF EQUITY

# In[ ]:


#cost_of_equity = brazil_risk_free_rate + beta*brazil_erp

ttm['Cost of Equity'] = brazil_risk_free_rate+ttm['beta']*brazil_erp


# ## COST OF CAPITAL

# In[ ]:


### INTEREST COVERAGE RATIO

ttm['IC ratio'] = ttm['EBIT']/np.abs(ttm['Juros Pagos'])

## you need to fin a better way to calculate the IC ratio
ttm['IC ratio'] = np.select([ttm['IC ratio'].isna()],[3.5],ttm['IC ratio'] )

### after tax cost of debt

# after tax cost of debt

ttm['Cost of Debt'] = ttm.apply(lambda row: cost_of_debt(brazil_risk_free_rate,row['IC ratio']), axis=1)


ttm['After Taxes Cost of Debt'] = ttm['Cost of Debt']*(1-0.34)  ## brazilian marginal tax rate 

## COST OF CAPITAL

ttm['Cost of Capital'] = (ttm['After Taxes Cost of Debt'] * ttm['Dívida/Capital_Inv'])+                        (ttm['Cost of Equity'] * ttm['Patrimônio/Capital_Inv'])


# ## EXPECTED GROWTH REVENUE

# In[ ]:


rev = pd.read_pickle("clean_data/final_data/df_principal.pkl")

rev1 = rev.loc[(rev['LABEL'] == 'TTM') | (rev['tipo_resultado'] == 'anual')]

companies = rev1['CODIGO'].unique()

grw = {}
for x in companies:
    try:
        rev2 = rev1.loc[rev1['CODIGO'] == x].drop_duplicates("Crescimento Receita")
        growth = rev2['Crescimento Receita'][-5:].dropna().to_list()
        growths = np.array(growth)
        weights = []
        for y in range(len(growths)):
            weights.append(10**y)
        growth_rate_mean = np.average(growths, weights=weights)
        growth_rate_std_dev = growths.std()
        grw[x] = growth_rate_mean
    except:
        continue


# In[ ]:


media_crescimento_5anos = pd.DataFrame.from_dict(grw,orient='index',columns=['Média Crescimento Receitas 5 anos'])


# In[ ]:


ttm = ttm.merge(media_crescimento_5anos,right_index=True, left_on=['CODIGO'])


# In[ ]:





# ## 5-years average reinvested capital 

# In[ ]:


rev = pd.read_pickle("clean_data/final_data/df_principal.pkl")

rev1 = rev.loc[(rev['LABEL'] == 'TTM') | (rev['tipo_resultado'] == 'anual')]

companies = rev1['CODIGO'].unique()

pctr = {}
for x in companies:
    try:
        rev2 = rev1.loc[rev1['CODIGO'] == x].drop_duplicates("Porcentagem Reinvestida")
        growth = rev2['Porcentagem Reinvestida'][-10:].dropna().to_list()
        growth = [item for item in growth if item >= 0 and item < 1]
        pctr_reinv = np.array(growth)
        
        media_porcent_reinv = np.average(pctr_reinv)

        pctr[x] = media_porcent_reinv
    except:
        continue


# In[ ]:


media_pctr_5anos = pd.DataFrame.from_dict(pctr,orient='index',columns=['Média Porcetagem Reivenstida 5 anos'])

ttm = ttm.merge(media_pctr_5anos,right_index=True, left_on=['CODIGO'])


# In[ ]:


med = ttm.drop_duplicates('Nome_Empresarial',keep='first')

mean = med.groupby('SEGMENTO')['Média Porcetagem Reivenstida 5 anos'].mean()

ttm['Média Porcetagem Reivenstida 5 anos'] = np.select([ttm['Média Porcetagem Reivenstida 5 anos'].isna()],[mean[ttm['SEGMENTO']]],ttm['Média Porcetagem Reivenstida 5 anos'])


# ## CAGR Revenues 5-year

# In[ ]:


rev = pd.read_pickle("clean_data/final_data/df_principal.pkl")

rev1 = rev.loc[(rev['LABEL'] == 'TTM') | (rev['tipo_resultado'] == 'anual')]

companies = rev1['CODIGO'].unique()

cagr = {}
for x in companies:
    try:
        rev2 = rev1.loc[rev1['CODIGO'] == x].drop_duplicates("Receita Líquida")


        rec = rev2[["LABEL","Receita Líquida"]][-6:].dropna()

        first = rec.head(1)["Receita Líquida"].item()
        first_yr = rec.head(1)["LABEL"].item()

        last = rec.tail(1)["Receita Líquida"].item()
        last_yr = rec.tail(1)["LABEL"].item()

        num_yrs = int(last_year)-int(first_yr)

        cagr_receita = np.power(last / first,(1 / num_yrs)) - 1

        cagr[x] = cagr_receita
    except:
        continue


# In[ ]:


cagr_receita_5anos = pd.DataFrame.from_dict(cagr,orient='index',columns=['CAGR Receita 5 anos'])

ttm = ttm.merge(cagr_receita_5anos,right_index=True, left_on=['CODIGO'])


# ## CAGR Earnings 5-years

# In[ ]:


rev = pd.read_pickle("clean_data/final_data/df_principal.pkl")

rev1 = rev.loc[(rev['LABEL'] == 'TTM') | (rev['tipo_resultado'] == 'anual')]

companies = rev1['CODIGO'].unique()

cagr_lr = {}
for x in companies:
    try:
        rev2 = rev1.loc[rev1['CODIGO'] == x].drop_duplicates("Lucro Líquido")


        rec = rev2[["LABEL","Lucro Líquido"]][-6:].dropna()

        first = rec.head(1)["Lucro Líquido"].item()
        first_yr = rec.head(1)["LABEL"].item()

        last = rec.tail(1)["Lucro Líquido"].item()
        last_yr = rec.tail(1)["LABEL"].item()

        num_yrs = int(last_year)-int(first_yr)

        cagr_lucro = np.power(last / first,(1 / num_yrs)) - 1

        cagr_lr[x] = cagr_lucro
    except:
        continue


# In[ ]:


cagr_lucro_5anos = pd.DataFrame.from_dict(cagr_lr,orient='index',columns=['CAGR Lucro 5 anos'])

ttm = ttm.merge(cagr_lucro_5anos,right_index=True, left_on=['CODIGO'])


# In[ ]:


ttm.sort_values(['CLASSE_x'],inplace=True)

ttm.drop_duplicates('Nome_Empresarial',keep='first',inplace=True)

ttm.to_pickle('clean_data/final_data/last_results_alldata.pkl')

##backup
ttm.to_pickle('BACKUPS/last_results/last_results_alldata%s.pkl'%today)


# # KPIs averages calculation

# In[ ]:


last_results = pd.read_pickle('clean_data/final_data/last_results_alldata.pkl')


# In[ ]:


indicadores = ['Margem EBIT',
             'Margem líquida',
             'Margem bruta',
             'Taxa efetiva de imposto',
             'Dív_Líq/EBIT',
             'Dív_Líq/PL',
             'D/E',
             'Dívida/Capital_Inv',
             'Patrimônio/Capital_Inv',
             'Endividamento Financeiro',
             'Endividamento Financeiro Curto Prazo',
             'Dív_Líq/ebitda',
             'PL/Ativos',
             'ROE',
             'Porcentagem Retida',
             'ROA',
             'ROIC',
             'Passivo/Ativo',
             'Líquidez Corrente',
             'Giro dos Ativos',
             'Capital_Giro/revenues',
             'Porcentagem Reinvestida',
             'Receitas/Capital_Inv',
             'Capex/Receita',
             'Capex/Depreciação',
             'Capex Líquido/Receita',
             'Capex Líquido/EBIT(1-t)',
             'Crescimento esperado LPA',
             'Crescimento esperado EBIT',
             'Crescimento EBIT',
             'Crescimento Margem EBIT',
             'Crescimento LPA',
             'Crescimento Receita',
             'LPA',
             'VPA',
             'P/L',
             'P/VPA',
             'P/EBITDA',
             'P/EBIT',
             'P/Ativo',
             'P/Cap_Giro',
             'P/Ativo_Circ_Líq',
              'Payout','Dividend Yield','EV/EBIT','EV/EBITDA',
              'beta','Média Crescimento Receitas 5 anos','Cost of Capital','Cost of Debt',
              'IC ratio','Cost of Equity','CAGR Receita 5 anos','Média Porcetagem Reivenstida 5 anos',
              'CAGR Lucro 5 anos']


# In[ ]:


def remove_outlier(df_in, col_name):
    if len(df_in) > 5:   
        q1 = df_in[col_name].quantile(0.15)
        q3 = df_in[col_name].quantile(0.85)
        iqr = q3-q1 #Interquartile range
        fence_low  = q1-1.5*iqr
        fence_high = q3+1.5*iqr
        df_out = df_in.loc[(df_in[col_name] > fence_low) & (df_in[col_name] < fence_high) & (df_in[col_name] != 0)]
        return df_out
    else:
        return df_in


# In[ ]:


### calculating market average
mean_indicators = last_results.drop(last_results.columns.difference(['CODIGO','SETOR',
                                                                     'SEGMENTO','SUBSETOR',
                                                                     'Classificação Capitalização',
                                                                     'Margem EBITDA']),axis=1)
mean_indicators.replace([-np.inf,np.inf],np.nan,inplace=True)

arq=remove_outlier(mean_indicators,'Margem EBITDA')

for item in indicadores:
    mean_indicators = last_results.drop(last_results.columns.difference(['CODIGO','SETOR',
                                                                         'SEGMENTO','SUBSETOR',
                                                                         'Classificação Capitalização',
                                                                         item]),axis=1)
    mean_indicators.replace([-np.inf,np.inf],np.nan,inplace=True)

    test = remove_outlier(mean_indicators,item)
    arq = arq.merge(test,how='outer')

medias_mercado_ttm = arq.mean()
medias_mercado_ttm.name = 'Média Mercado'

medias_mercado_ttm.to_pickle('clean_data/final_data/medias_mercado_ttm.pkl')
medias_mercado_ttm.to_pickle('BACKUPS/medias/medias_mercado_ttm%s.pkl'%today)


# In[ ]:


setores = last_results['SETOR'].unique()
subsetores = last_results['SUBSETOR'].unique()
segmento = last_results['SEGMENTO'].unique()
capitaliz = last_results['Classificação Capitalização'].unique()


# In[ ]:


### calculating sector averages
concat_setor = pd.DataFrame()
for setor in setores:
    alq = last_results.loc[(last_results['SETOR'] == setor)]
    mean_indicators = alq.drop(alq.columns.difference(['CODIGO','SETOR',
                                                                         'SEGMENTO','SUBSETOR',
                                                                         'Classificação Capitalização',
                                                                         'Margem EBITDA']),axis=1)
    mean_indicators.replace([-np.inf,np.inf],np.nan,inplace=True)

    arq=remove_outlier(mean_indicators,'Margem EBITDA')

    for item in indicadores:
        mean_indicators = alq.drop(alq.columns.difference(['CODIGO','SETOR',
                                                                             'SEGMENTO','SUBSETOR',
                                                                             'Classificação Capitalização',
                                                                             item]),axis=1)
        mean_indicators.replace([-np.inf,np.inf],np.nan,inplace=True)

        test = remove_outlier(mean_indicators,item)
        arq = arq.merge(test,how='outer')
        
    concat_setor = pd.concat([concat_setor,arq])

medias_setor = concat_setor.groupby('SETOR').mean()
medias_setor.to_pickle('clean_data/final_data/medias_setor.pkl')
medias_setor.to_pickle('BACKUPS/medias/medias_setor%s.pkl'%today)


# In[ ]:


### calculating subsector averages
concat_subsetor = pd.DataFrame()
for setor in subsetores:
    alq = last_results.loc[(last_results['SUBSETOR'] == setor)]
    mean_indicators = alq.drop(alq.columns.difference(['CODIGO','SETOR',
                                                                         'SEGMENTO','SUBSETOR',
                                                                         'Classificação Capitalização',
                                                                         'Margem EBITDA']),axis=1)
    mean_indicators.replace([-np.inf,np.inf],np.nan,inplace=True)

    arq=remove_outlier(mean_indicators,'Margem EBITDA')

    for item in indicadores:
        mean_indicators = alq.drop(alq.columns.difference(['CODIGO','SETOR',
                                                                             'SEGMENTO','SUBSETOR',
                                                                             'Classificação Capitalização',
                                                                             item]),axis=1)
        mean_indicators.replace([-np.inf,np.inf],np.nan,inplace=True)

        test = remove_outlier(mean_indicators,item)
        arq = arq.merge(test,how='outer')
        
    concat_subsetor = pd.concat([concat_subsetor,arq])

medias_subsetor = concat_subsetor.groupby('SUBSETOR').mean()
medias_subsetor.to_pickle('clean_data/final_data/medias_subsetor.pkl')
medias_subsetor.to_pickle('BACKUPS/medias/medias_subsetor%s.pkl'%today)


# In[ ]:


### calculating industry averages
concat_segmento = pd.DataFrame()
for setor in segmento:
    alq = last_results.loc[(last_results['SEGMENTO'] == setor)]
    mean_indicators = alq.drop(alq.columns.difference(['CODIGO','SETOR',
                                                                         'SEGMENTO','SUBSETOR',
                                                                         'Classificação Capitalização',
                                                                         'Margem EBITDA']),axis=1)
    mean_indicators.replace([-np.inf,np.inf],np.nan,inplace=True)

    arq=remove_outlier(mean_indicators,'Margem EBITDA')

    for item in indicadores:
        mean_indicators = alq.drop(alq.columns.difference(['CODIGO','SETOR',
                                                                             'SEGMENTO','SUBSETOR',
                                                                             'Classificação Capitalização',
                                                                             item]),axis=1)
        mean_indicators.replace([-np.inf,np.inf],np.nan,inplace=True)

        test = remove_outlier(mean_indicators,item)
        arq = arq.merge(test,how='outer')
        
    concat_segmento = pd.concat([concat_segmento,arq])

medias_segmento = concat_segmento.groupby('SEGMENTO').mean()
medias_segmento.to_pickle('clean_data/final_data/medias_segmento.pkl')
medias_segmento.to_pickle('BACKUPS/medias/medias_segmento%s.pkl'%today)


# In[ ]:


### calculating capital segment averages
concat_capital = pd.DataFrame()
for setor in capitaliz:
    alq = last_results.loc[(last_results['Classificação Capitalização'] == setor)]
    mean_indicators = alq.drop(alq.columns.difference(['CODIGO','SETOR',
                                                                         'SEGMENTO','SUBSETOR',
                                                                         'Classificação Capitalização',
                                                                         'Margem EBITDA']),axis=1)
    mean_indicators.replace([-np.inf,np.inf],np.nan,inplace=True)

    arq=remove_outlier(mean_indicators,'Margem EBITDA')

    for item in indicadores:
        mean_indicators = alq.drop(alq.columns.difference(['CODIGO','SETOR',
                                                                             'SEGMENTO','SUBSETOR',
                                                                             'Classificação Capitalização',
                                                                             item]),axis=1)
        mean_indicators.replace([-np.inf,np.inf],np.nan,inplace=True)

        test = remove_outlier(mean_indicators,item)
        arq = arq.merge(test,how='outer')
        
    concat_capital = pd.concat([concat_capital,arq])

medias_capital = concat_capital.groupby('Classificação Capitalização').mean()
medias_capital.to_pickle('clean_data/final_data/medias_capital.pkl')
medias_capital.to_pickle('BACKUPS/medias/medias_capital%s.pkl'%today)


