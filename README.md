# BrazilianMarketDataCollector
 
 The purpose of this collector is to gather brazilian companies financial data from CVM, B3, yahoo-query and AlphaVantage, clean it, assemble and create a dataframe with all available information on these companies to use as a data source to the [FinanceDash Project](https://github.com/gustavomoers/FinanceDash).

 
 Unfortunatly, 03-Raw_Data_B3.py is not working right now, this is because B3 changed their host website and discontinued their old one, thus is not working and I did not implement a scrapper for the new website yet. So keep in mind that the dividends and total shares data are outdated.

 The data is collected from:

- [CVM](http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/): financial results for brazilian companies
- [B3](http://bvmf.bmfbovespa.com.br/CapitalSocial/): OUTADATED ... dividends, total shares, events for brazilian companies
- [AlphaVantage](https://www.alphavantage.co/): price history from brazilian market
- [Yahoo-query API](https://pypi.org/project/yahooquery/): current prices and betas from brazilian market


 I recommend first to clone repository and start with the file 00-RUN_ONLY_FIRST_TIME---Downloading_all_data_CVM.ipynb, you only need to run this first time, since it will download all the data available from CVM.
 
 Then you should also run the get_prices_AV.ipynb file, which is on PRICES folder, this file will get the price history from AlphaVantage API, you should create your onw account and use your own API key for this. This free API has a limit of requests per minute, so it takes a while to download all data, but you should only run this file one time, I am looking into other APIs to improve this code.



 After this first time, when you need to update the CVM and B3 data, you can start by running the 01-Updating_Data_CVM.ipynb file and then run the other files sequentially.
 
 If you wish to update only the prices, you can run only the 05-Updating_Prices_DataFrame.ipynb
 
 After this you will find all dataframes on the clean_data/final_data/ folder:
 
 - last_results_alldata.pkl: collection of last TTM (trailing-twelve-month) results for all companies.
 - medias_mercado_ttm.pkl: last TTM KPIs averages for the whole brazilian market.
 - medias_setor.pkl: last TTM KPIs averages for the brazilian market sectors.
 - medias_subsetor.pkl: last TTM KPIs averages for the brazilian market subsectors.
 - medias_segmento.pkl: last TTM KPIs averages for the brazilian market industries.
 - medias_capital.pkl: last TTM KPIs averages for the capitalization segment.
 - proventos_b3.pkl: dividends, grouping and splitting data for brazilian market.
 - df_principal.pkl: dataframe contaning all data avaiable.
 
 The final dataframe (df_principal.pkl) contains 177 coluns from 355 brazilian companies, each row is one result identified by the column LABEL (for example, it can be 1T2021 which means it is the first quarter result for the year 2021 for this company, or it can be just 2021 which means it is the annual result. It also contains the TTM lable, which is the last traling twelve months results)
 
 The 117 columns are:
 
 - CNPJ_Companhia
- Nome_Empresarial
- Data_Constituicao
- Codigo_CVM
- Data_Registro_CVM
- Situacao_Registro_CVM
- Pais_Origem
- Pais_Custodia_Valores_Mobiliarios
- Setor_Atividade
- Descricao_Atividade
- Especie_Controle_Acionario
- Pagina_Web
- NAME_PREG
- CLASSE_x
- CODIGO
- SETOR_NAICS
- SETOR
- SUBSETOR
- SEGMENTO
- SEGMENTO_B3
- RAZAO
- CODIGOS
- COD1
- COD2
- COD3
- COD4
- COD5
- COD6
- COD7
- DENOM_SOCIAL
- DENOM_COMERC
- SETOR_ATIV
- CDNO
- CDO_STRIP
- DAMODARAN_GROUP
- CD_CVM_y
- LABEL
- Despesas/Receitas Operacionais
- Resultado Bruto
- Resultado Financeiro
- Patrimônio Líquido Consolidado
- Patrimônio Líquido
- Participação dos Acionistas Não Controladores
- Passivo Total
- Passivo Circulante
- Dívida curto prazo
- Passivo Não Circulante
- Dívida longo prazo
- Aplicações Financeiras
- Ativo Circulante
- Ativo Não Circulante
- Ativo Realizável a Longo Prazo
- Ativo Total
- Caixa e Equivalentes de Caixa
- Investimentos
- Aumento (Redução) de Caixa e Equivalentes
- Caixa Gerado nas Operações
- Caixa Líquido Atividades Operacionais
- Caixa Líquido Atividades de Financiamento
- Caixa Líquido Atividades de Investimento
- Capex
- Juros Pagos
- Saldo Final de Caixa e Equivalentes
- Saldo Inicial de Caixa e Equivalentes
- Variação Cambial s/ Caixa e Equivalentes
- Variações nos Ativos e Passivos
- Depreciação, Amortização e Exaustão
- Dividendos
- Juros sobre o Capital Próprio
- Receita Líquida
- Custos
- EBIT
- EBITDA
- Imposto
- Lucro Líquido
- Lucro Atribuído a não controladores
- Lucro Atribuído a Controladores
- tipo_resultado
- trimestre
- DT_FIM_EXERC
- mês
- ano
- Quantidade_Acionistas_PF
- Quantidade_Acionistas_PJ
- Quantidade_Acionistas_Investidores_Institucionais
- Percentual_Acoes_Ordinarias_Circulacao
- Percentual_Acoes_Preferenciais_Circulacao
- Percentual_Total_Acoes_Circulacao
- Total Ações
- Total ON
- Total PN
- index
- Preço
- Proventos
- Payout
- Proventos por ação
- Margem EBITDA
- Margem EBIT
- Margem líquida
- Margem bruta
- Taxa efetiva de imposto
- EBIT(1-t)
- Dívida Bruta
- Disponibilidade
- Dívida Líquida
- Dív_Líq/EBIT
- Dív_Líq/PL
- D/E
- Capital Investido
- Dívida/Capital_Inv
- Patrimônio/Capital_Inv
- Endividamento Financeiro
- Endividamento Financeiro Curto Prazo
- Dív_Líq/ebitda
- Dívida Líquida Anterior
- PL Con Anterior
- PL + Dív_Líq_Ant
- PL/Ativos
- PL + Dív_Líq
- ROE
- Porcentagem Retida
- Ativo Total Anterior
- ROA
- ROIC
- Passivo/Ativo
- Líquidez Corrente
- Giro dos Ativos
- Depreciação
- Capex Líquido
- Ativo_Circ non-cash
- Passivo_Circ non-cash
- Capital de Giro non-cash
- Capital de Giro non-cash anterior
- Variação Capital de Giro
- Capital_Giro/revenues
- Patrimônio Reinvestido
- Porcentagem Reinvestida
- Receitas/Capital_Inv
- Capex/Receita
- Capex/Depreciação
- Capex Líquido/Receita
- Capex Líquido/EBIT(1-t)
- ON
- PN
- PN Resg
- PNA
- PNB
- PNC
- PND
- PNE
- PNF
- UNT
- UNT N2
- Qtde de ações UN
- Qtde de ações UNITS
- Valor de Mercado
- Valor de Firma
- EV/EBITDA
- EV/EBIT
- LPA
- VPA
- P/L
- P/VPA
- P/EBITDA
- P/EBIT
- P/Ativo
- P/Cap_Giro
- P/Ativo_Circ_Líq
- Classificação Capitalização
- Crescimento esperado LPA
- Crescimento esperado EBIT
- Crescimento EBIT
- Crescimento Margem EBIT
- Crescimento LPA
- Crescimento Receita
- Proventos no Período
- Dividend Yield
 
 
 
 This is it, if you want to see an example of the final dataframe you can check the Final_DataFrame_example.xlsx file. If you have any questions please contact me, hope this helps you on your projects!
