import sys
import pandas as pd
import datetime as dt
from functions.Sample import SamplingSwaps
from functions.Interpolation import Interpol
import re
from dateutil import tz
import numpy as np
from itertools import compress, chain
from functions.Rounding import rd

pd.set_option('display.float_format', lambda x: '%.3f' % x)

class LocalSwaps():
    """Clase que contiene el metodo de formacion de la muestra swap local, para un swap en especifico entre IBR, IBRUVR y basis USDCO
    
    Args:
        self.swap (str): Nombre del Swap Local a calcularle la muestra
        self.trade_quotes (pd.DataFrame): Informacion de Trades y Quotes (Unicos, estrategias spreads y estrategias butterflys
        (esta ultima aun no es tenida en cuenta para la muestra)) sobre el swap especificado, enviada por los brokers
        self.dv01 (pd.DataFrame): Montos de DV01s del swap especificado, enviados por brokers
        self.hist_sample (pd.DataFrame): Muestra Swap Local del swap especificado, del dia habil anterior
        self.bid_ask_spread_hist (pd.DataFrame): Parametro vigente de bid/offer spread de la muestra swap especifica
        self.tenors (list): Tenors estandares del swap local
        self.tenors_sections (dict): Secciones de tenores enlistados, las cuales representan las secciones de interpolacion (Criterio 7)
        self.days (list): Dias al tenor de los tenores estandares del swap local
        self.closing_hour (str): Hora de cierre de la jornada de negociacion de swaps
        self.logger (logging.RootLogger): Logger con nivel predeterminado para la escritura de errores e informacion
        self.min_minutes (int): Minutos minimos previos al cierre de la jornada de negociacion para establecer un intervalo de convocatoria,
        el cual es utilizado en la creacion de la muestra swap
        self.active_minutes (int) = Minutos minimos activos de un quote para tenerse en cuenta dentro de la muestra swap
        self.dec(int) = Numero de decimales a redonder la muestra swap
    """
    def __init__(self, swap, trade_quotes, dv01, hist_sample, bid_ask_spread_hist, 
    tenors, tenors_sections, days, closing_hour, logger, min_minutes = 5, active_minutes = 5, dec = 6):
        self.swap = swap
        self.trade_quotes = trade_quotes
        self.dv01 = dv01
        self.hist_sample = hist_sample
        self.ba_spread = bid_ask_spread_hist
        self.tenors = tenors
        self.tenors_sections = tenors_sections
        self.days = days
        self.closing_hour = closing_hour
        self.logger = logger
        self.min_minutes = min_minutes
        self.active_minutes = active_minutes
        self.dec = dec
        self.repeated_results = ['trade_on_time_1', 'spread_on_time_2', 'spread_off_time_4','trade_off_time_6']
    def local_sample(self):
        """Se crea una variable categorica de los tenores del swap especifico.
        Se crean un dataframe con la informacion de tenores estandar (excluyendo el tenor ON), con el nombre de swap_today
        Se hayan el intervalo de relevancia (con las variables de entorno self.closing_hour y self.min_minutes)
        Se dividen tres dataframes entre negociaciones y puntas sobre plazos, estrategias spread y estrategias butterflys
        Se filtra la informacion de plazos por aquellas negociaciones que superan el monto mínimo determinado de DV01 de cada broker.
        Se realiza la seleccion de la muestra en base a los 8 criterios jerarquicos:
            i.  best_trades_on_time: Se seleccionan aquellas negociaciones/trades sobre plazos que se hayan realizado dentro del intervalo de relevancia. 
            De haber información, se calculan las tasas ponderadas por monto de cada tenor y se agregan a la muestra.
            ii.	best_spreads_on_time: Se seleccionan aquellas negociaciones/trades sobre estrategias cruzadas que se hayan realizado dentro del intervalo de relevancia. 
            En caso de existir negociaciones de spreads, cuyo tenor este presente en los seleccionados en el anterior punto,
            se procede a seleccionar la estrategia cruzada a considerar de la siguiente forma:
                1.	Se selecciona los plazos elegibles de este criterio.
                2.	Si hay mas de una estrategia cruzada para hallar un unico plazo, 
                se considera aquella estrategia con mas cotizaciones/negociaciones en el dia. 
                Si hay mas de una que cumple esto, se elige aquella estrategia con mayor nominal total en el dia. 
                De haber mas de una estrategia con el mismo nominal, se elige la estrategia cruzada con el tenor a menor plazo.
                Por cada nuevo plazo elegible, se identifica la posicion sobre el tenor negociaso de la estrategia y 
                a la tasa se le suma o se le resta el spread dependiendo de la posicion y se agrega a la muestra. 
            iii.	best_active_quotes: Se seleccionan los quotes tanto BID como ASK sobre un plazo cuyo tiempo de permanencia en pantalla sea mayor a self.active_minutes.
            Se aplica el minimo y el maximo, agrupado por tenor y tipo-orden y se seleccionan aquellos plazos con ambas puntas. 
            Se elige la mejores puntas, se aplica el filtro de bid_ask_spread y se agregan a la muestra.
            iv.	best_spreads_off_time: Se aplica, de manera similar al punto ii, una selección de aquellas negociaciones sobre estrategias cruzadas 
            para hallar los plazos no negociados hasta el momento, se agregan los elegibles a la muestra.
            v.	implicit_spread: Se seleccionan aquellas estrategias cruzadas no usadas previamente que hayan tenido puntas ASK o BID, 
            cuyo tiempo permanencia fuera mayor a self.active_minutes; se calcula el minimo y el maximo de las estrategias cruzadas,
            se seleccionan aquellas estrategias que obtengan valores de BID y ASK.
            Posteriormente, se extraen las estrategias cruzadas con 1 de sus 2 plazos ya anteriormente seleccionado. 
            Se haya el nivel mid de estas negociaciones y se identifican aquellos nodos nuevos a hallar y, de manera similar al punto ii, 
            se adiciona la estrategia cruzada al nodo ya seleccionado. De este tenor implicito, se haya su valor implicito bid y ask, 
            se filtran por ba_spread y se agrega a la muestra.
            vi.	best_trades_off_time: Se seleccionan aquellas trades sobre plazos restantes durante toda la jornada, 
            se haya la tasa ponderada por monto de los plazos aplicables para este punto y se agregan a la muestra. 
            vii.	interpolated_var: Se itera por cada seccion de tenores (self.swap_nodes_sections) que hayan tenido menos un plazo hayado en los puntos anteriores.
            Por cada tenor dentro de la seccion de los tenores:
                1.	Se continua si el tenor hace parte de los tenores previamente seleccionados dentro de la muestra.
                2.	Si el tenor es el minimo de la seccion y el tenor posterior ya fue seleccionado previamente, 
                se realiza la actualizacion del primero con la variacion diaria del ultimo para el mid, bid y ask.
                Se agrega a la muestra swap.
                3.	Si el tenor es el maximo de la seccion y el tenor previo ya fue seleccionado previamente. 
                Se realiza la actualizacion del primero con la variacion diaria del ultimo para el mid, bid y ask. 
                Se agrega a la muestra swap.
                4.	Si el tenor no esta en los extremos y el tenor anterior se calculo pero el posterior no, 
                se realiza la actualizacion del tenor con la variacion diaria del tenor anterior para el mid, bid y ask. 
                Se agrega a la muestra swap.
                5.	Si el tenor no esta en los extremos y el tenor posterior se calculo pero el anterior no, 
                se realiza la actualizacion del tenor con la variacion diaria del tenor posterior para el mid, bid y ask. 
                Se agrega a la muestra swap.
                6.	Si el tenor no esta en los extremos y tanto el anterior como el posterior fueron calculados previamente, 
                Se realiza una actualizacion diaria de ambos y se interpola de manera lineal estas variaciones, 
                la cual se aplicada al valor anterior del tenor en cuestion, para el mid, bid, ask. 
                Se agrega a la muestra swap.
            viii.	Para los tenores que no pudieron tener valor con los 7 criterios anteriores, 
            se seleccionan las puntas del dia anterior.
        Finalmente, se retorna la muestra selecionada .
        """
        err_mss = 'Se genero un error realizando la muestra swap '+ self.swap +' . Fallo linea: '
        try:
            self.logger.info('Inicio creacion muestra swap: '+ self.swap)
            self.logger.info('Creacion de secciones de tenores como variable categorica')
            tenors_cat =  pd.Categorical(self.tenors, categories= self.tenors,ordered = True)
            self.logger.info('Inicializacion swap_today vacio')
            swap_today = pd.DataFrame({'tenor':self.tenors})
            swap_today['instrumento'] = self.swap
            for i in ['mid', 'bid', 'ask', 'tipo-precio']:
                            swap_today[i] = np.nan             
            swap_today['dias'] = self.days
            self.logger.info('Escalamiento de la zona horaria a Nueva York')
            NY_time_today = dt.datetime.now(tz=tz.gettz('America/New_York')).replace(tzinfo=None)
            BOG_time_today = dt.datetime.now(tz=tz.gettz('America/Bogota')).replace(tzinfo=None)
            diff_time = round((NY_time_today-BOG_time_today).total_seconds()/(60*60),1)
            close_market = dt.datetime.strptime(self.closing_hour, "%H:%M:%S") + dt.timedelta(minutes = 60*diff_time)
            self.logger.info('Hora de cierre del mercado (NY time): '+ close_market.strftime('%H:%M:%S'))
            start_hour = close_market - dt.timedelta(minutes= self.min_minutes)
            self.logger.info('Hora de inicio de convocatoria de mercado (NY time): '+ start_hour.strftime('%H:%M:%S'))
            if self.swap!='USDCO':
                self.trade_quotes.precio /= 100
            self.trade_quotes['hora-inicial'] = pd.to_datetime(self.trade_quotes['hora-inicial'], format = '%H:%M:%S')
            self.trade_quotes['hora-final']= pd.to_datetime(self.trade_quotes['hora-final'], format = '%H:%M:%S')
            broker_info_spreads = self.trade_quotes.loc[self.trade_quotes['tenor'].str.contains('X')]
            broker_info_flys = self.trade_quotes.loc[self.trade_quotes['tenor'].str.contains('V')]
            broker_info = self.trade_quotes.drop(broker_info_spreads.index.to_list()+broker_info_flys.index.to_list())
            self.logger.info('Inicio filtros')
            self.logger.info('Informacion de limit spread: \n' + self.ba_spread.to_string())
            self.logger.info('Filtrado de negociaciones por tenores estandar')
            broker_info = broker_info[broker_info.tenor.isin(self.tenors)]
            broker_info_spreads = broker_info_spreads[broker_info_spreads.tenor.str.contains('|'.join(self.tenors))]
            self.logger.info('Filtrado de negociaciones por monto negociado vs 2k dv01')
            if self.dv01.empty:
                self.logger.info('No hay info de DV01 por ser intradia')
            else:
                self.logger.info('Informacion de DV01: \n' + self.dv01.to_string())
                if self.swap=='IBR':
                    broker_info.loc[broker_info.sistema=='ICAP','nominal']*= broker_info.loc[broker_info.sistema=='ICAP'].set_index(['sistema', 'tenor']).join(
                        self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values*0.0000005
                    broker_info.loc[broker_info.sistema=='GFI','nominal']*= broker_info.loc[broker_info.sistema=='GFI'].set_index(['sistema', 'tenor']).join(
                    self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values/2
                    broker_info.loc[broker_info.sistema=='TP','nominal']*= broker_info.loc[broker_info.sistema=='TP'].set_index(['sistema', 'tenor']).join(
                        self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values*0.0000005
                elif self.swap=='IBRUVR':
                    broker_info.loc[broker_info.sistema=='ICAP','nominal']*= broker_info.loc[broker_info.sistema=='ICAP'].set_index(['sistema', 'tenor']).join(
                        self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values*0.000001
                    broker_info.loc[broker_info.sistema=='GFI','nominal']*= broker_info.loc[broker_info.sistema=='GFI'].set_index(['sistema', 'tenor']).join(
                    self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values
                    broker_info.loc[broker_info.sistema=='TP','nominal']*= broker_info.loc[broker_info.sistema=='TP'].set_index(['sistema', 'tenor']).join(
                        self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values*0.000001
                else:
                    broker_info.loc[broker_info.sistema=='GFI','nominal']*= broker_info.loc[broker_info.sistema=='GFI'].set_index(['sistema', 'tenor']).join(
                        self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values/2

                broker_info.loc[broker_info.sistema=='TRADITION','nominal']*= 1000000
                broker_info['dv01-filter'] = broker_info.set_index(['sistema', 'tenor']).join(
                    self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values
                self.logger.info('Broker info sin filtro de DV01: \n' + broker_info.to_string())
                #broker_info.nominal = rd(broker_info.nominal, 0)
                broker_info = broker_info[broker_info.nominal>=broker_info['dv01-filter']]

            self.logger.info('Informacion del mercado de spreads plausible: \n' + broker_info_spreads.loc[:,
                                                                                                          ['sistema', 'hora-inicial', 'hora-final','tenor', 
                                                                                                           'nominal', 'tipo-orden', 'precio']].to_string())
            self.logger.info('Informacion del mercado directo de swaps plausible: \n' + broker_info.loc[:,
                                                                                                          ['sistema', 'hora-inicial', 'hora-final','tenor', 
                                                                                                           'nominal', 'tipo-orden', 'precio']].to_string())
            # Hacer Filtro Monto Tenores cruzados
            # First Criteria
            self.logger.info('1er criterio de la muestra: best_trades_on_time')
            best_trades_on_time = broker_info.loc[(broker_info['hora-final'] >= start_hour) \
                                                   & (broker_info['hora-final'] <= close_market) \
                                                       & (broker_info['tipo-orden'] == 'TRADE')]
            self.logger.info('trades on time \n' + best_trades_on_time.to_string())
            if best_trades_on_time.empty:
                best_data_on_time  = pd.DataFrame()
                self.logger.info('No hubo trades dentro de los ' + str(self.active_minutes)+
                                 ' minutos antes del cierre')
            else:
                best_data_on_time = best_trades_on_time.groupby(['tenor', 'tipo-orden']
                                                                    ).apply(SamplingSwaps().summary_swp)
            if not best_data_on_time.empty:
                self.logger.info('Tenores 1er Criterio: ' + str(best_data_on_time.reset_index(['tipo-orden']).index.tolist()))
                swp_trades_on_time = SamplingSwaps().match_warrant_today(best_data_on_time, swap_today, 'TRADE')
                swap_today.loc[swp_trades_on_time.index.to_list(), ['mid', 'bid', 'ask', 'tipo-precio']] = \
                                   np.array([swp_trades_on_time.weighted, swp_trades_on_time.weighted, 
                                             swp_trades_on_time.weighted,'trade_on_time_1'], dtype = 'object')
            # Second Criteria
            self.logger.info('2nd criterio de la muestra: best_spreads_on_time')
            brokers_matchs_spreads_on_time = broker_info_spreads.loc[(broker_info_spreads['hora-final'] >= start_hour) \
                                                   & (broker_info_spreads['hora-final'] <= close_market) \
                                                       & (broker_info_spreads['tipo-orden'] == 'TRADE')]
            self.logger.info('spreads on time \n' + brokers_matchs_spreads_on_time.to_string())
            if best_trades_on_time.empty or brokers_matchs_spreads_on_time.empty:
                self.logger.info('No hay informacion de trades cruzados o de trades dentro del horario de convocatoria')
                best_spreads_on_time = pd.DataFrame()
            else:
                on_time_tenors = list(set(best_trades_on_time.tenor.values.tolist()))
                check_tenor_in_spreads = brokers_matchs_spreads_on_time.tenor.str.contains('|'.join(on_time_tenors))
                spreads_on_time = brokers_matchs_spreads_on_time.loc[check_tenor_in_spreads]
                if spreads_on_time.empty:
                    self.logger.info('No hay informacion de trades sobre spreads que cumplan con el Criterio 2')
                    best_spreads_on_time = pd.DataFrame()
                else:
                    duplicated_tenor = {}
                    for i in on_time_tenors:
                        duplicated_tenor[i] = [k.replace('X','') for k in list(
                            chain.from_iterable([list(filter(lambda x: 'X' in x,re.split(i,j))) \
                                                 for j in spreads_on_time.tenor.unique().tolist() if i in j.split("X")]))]
                    posibles_tenors_to_search = list(set(list(chain.from_iterable(list(duplicated_tenor.values())))))
                    tenors_to_search = list(set(posibles_tenors_to_search).difference(set(on_time_tenors)))
                    self.logger.info('Tenores posibles en 2ndo Criterio: ' + str(tenors_to_search))
                    for ten in tenors_to_search:
                        ten_to_identify = [key for key, values in duplicated_tenor.items() if ten in values]
                        if len(ten_to_identify) > 1:
                            self.logger.info('Definicion del tenor spread a escoger para: ' + ten)
                            num_cross_tenor = [broker_info_spreads.loc[(broker_info_spreads.tenor.str.contains(ten_id)) & \
                                                                       (broker_info_spreads.tenor.str.contains(ten)
                                                                        )].shape[0] for ten_id in ten_to_identify]
                            if [max(num_cross_tenor) == num for num in num_cross_tenor].count(True)==1:
                                ten_not_to_drop = ten_to_identify[num_cross_tenor.index(max(num_cross_tenor))]
                                ten_to_drop = [tenor for tenor in ten_to_identify if tenor not in ten_not_to_drop]
                            else:
                                nom_cross_tenor = [broker_info_spreads.loc[(broker_info_spreads.tenor.str.contains(ten_id)) & \
                                                                       (broker_info_spreads.tenor.str.contains(ten)
                                                                        )].nominal.sum() for ten_id in ten_to_identify]
                                if [max(nom_cross_tenor) == nom for nom in nom_cross_tenor].count(True)==1:
                                    ten_not_to_drop = ten_to_identify[nom_cross_tenor.index(max(nom_cross_tenor))]
                                    ten_to_drop = [tenor for tenor in ten_to_identify if tenor not in ten_not_to_drop]
                                else:
                                    ten_not_to_drop = min([tenors_cat[tenors_cat == ten_id] for ten_id in ten_to_identify]
                                                          ).__array__()[0]
                                    ten_to_drop = [tenor for tenor in ten_to_identify if tenor not in ten_not_to_drop]
                            self.logger.info('Tenor: ' + ten + '. Se haya por medio de la estrategia spread: ' + str(ten_not_to_drop))
                            for ten_out in ten_to_drop:
                                spreads_on_time.drop(index = spreads_on_time.loc[(spreads_on_time.tenor.str.contains(ten_out)) & \
                                                                           (spreads_on_time.tenor.str.contains(ten)
                                                                            )].index, inplace = True)
                    best_spreads_on_time = pd.DataFrame()
                    for i in tenors_to_search:
                        spr_per_tenor = spreads_on_time.loc[spreads_on_time.tenor.str.contains(i)]
                        traded_tenor = [s for s in re.split(
                            i, spr_per_tenor.loc[: ,'tenor'].unique().tolist()[0]) if "X" in s][0].replace('X', '')
                        spr_per_tenor['traded_tenor'] =  traded_tenor
                        self.logger.info("Spread tenor "+i+ "\n" +spr_per_tenor.to_string())
                        spr_per_tenor['traded_price'] = best_data_on_time.xs(traded_tenor, level= 'tenor')['weighted'].values[0]
                        spr_per_tenor['pos_spr'] = spr_per_tenor.tenor.str.split('X').apply(lambda x: x.index(traded_tenor))
                        spr_per_tenor.rename(columns = {'precio': 'spread', 'tenor': 'cross_tenor'}, inplace = True)
                        spr_per_tenor['precio'] = np.where(spr_per_tenor.pos_spr == 0, 
                                                           spr_per_tenor.traded_price + spr_per_tenor.spread, 
                                                           spr_per_tenor.traded_price - spr_per_tenor.spread)
                        list_of_tenors = np.unique(spr_per_tenor.cross_tenor.str.split('X').values)
                        for j in list_of_tenors :
                            j[0], j[1] = j[1], j[0]
                        spr_per_tenor['tenor']  = [list_of_tenors[li][x] for li in range(
                            len(list_of_tenors)) for x in spr_per_tenor.pos_spr.values]
                        best_spreads_on_time = pd.concat([best_spreads_on_time, spr_per_tenor])
                    if not best_spreads_on_time.empty:
                        best_spreads_on_time.drop(columns= ['traded_price', 'pos_spr', 'spread', 'cross_tenor'], inplace = True)
                        best_spreads_on_time = best_spreads_on_time.groupby(['tenor', 'tipo-orden']
                                                                            ).apply(SamplingSwaps().summary_swp)
                    self.logger.info("Best spreads on time \n" + best_spreads_on_time.to_string())
            if not best_spreads_on_time.empty:
                self.logger.info('Tenores 2ndo Criterio: ' + str(best_spreads_on_time.reset_index(['tipo-orden']).index.tolist()))
                swp_trades_spreads_on_time = SamplingSwaps().match_warrant_today(best_spreads_on_time, swap_today, 'TRADE')
                swap_today.loc[swp_trades_spreads_on_time.index.to_list(), ['mid', 'bid', 'ask', 'tipo-precio']] = \
                                   np.array([swp_trades_spreads_on_time.weighted, swp_trades_spreads_on_time.weighted, 
                                             swp_trades_spreads_on_time.weighted,'spread_on_time_2'], dtype = 'object')
            # Third Criteria
            self.logger.info('3er criterio de la muestra: best_active_quotes')
            if not best_spreads_on_time.empty:
                best_quotes = (
                    broker_info[(~broker_info.tenor.isin(best_trades_on_time.tenor.unique().tolist())) & (~broker_info.tenor.isin(best_spreads_on_time.reset_index(['tipo-orden']).index.tolist()))]).loc[
                        (broker_info['hora-final'] - broker_info['hora-inicial'] >= dt.timedelta(minutes = self.active_minutes)) & \
                            (broker_info['hora-final'] >= close_market) & \
                                (broker_info['hora-final'] - start_hour >= dt.timedelta(minutes = self.active_minutes)) & \
                                    (broker_info['tipo-orden'].isin(['BID', 'ASK']))]
            else:
                best_quotes = (broker_info[~broker_info.tenor.isin(best_trades_on_time.tenor.unique().tolist())]).loc[(broker_info['hora-final'] - \
                                                     broker_info['hora-inicial'] >= dt.timedelta(minutes = self.active_minutes)) & (broker_info['hora-final'] >= close_market) & (broker_info['tipo-orden'].isin(['BID', 'ASK']))]
                self.logger.info('best quotes \n' + best_quotes.to_string())
            try:
                best_quotes.drop(best_quotes.loc[best_quotes.index.isin(
                    best_spreads_on_time.reset_index(['tipo-orden']).index.values.tolist())].index, axis = 0, inplace = True)
            except:
                pass
            
            if not best_quotes.empty:
                best_quotes_data = best_quotes.groupby(['tenor', 'tipo-orden']).apply(SamplingSwaps().summary_swp)
                self.logger.info("Best quotes data \n" + best_quotes_data.to_string())
                best_ask_and_bid = []
                for index, row in (best_quotes_data.reset_index(level= ['tipo-orden']).groupby(
                        level = ['tenor'], observed = True)):
                    if any(row["tipo-orden"].str.contains('ASK')) & any(row["tipo-orden"].str.contains('BID')):
                        best_ask_and_bid.append(index)
                if best_ask_and_bid==[]:
                    self.logger.info('No hubo informacion de mejores puntas para el 3er crietrio: Criterio 3')
                    best_active_quote = pd.DataFrame()
                else:
                    self.logger.info('Tenores posibles en 3er Criterio: ' + str(best_ask_and_bid))
                    best_data_ba_on_time = (best_quotes_data.reset_index(
                        level= ('tipo-orden')).loc[best_ask_and_bid]).set_index('tipo-orden', append = True)
                    best_active_quote = (best_data_ba_on_time.reset_index(
                        level= ('tipo-orden')).groupby(level = 'tenor').apply(
                            lambda x: np.mean(np.array([x.reset_index(drop = False)[x.reset_index(
                                drop = False)['tipo-orden'] =='ASK']['min'].values,x.reset_index(
                                    drop = True)[x.reset_index(drop = True)['tipo-orden'] =='BID']['max'].values])))).to_frame()
                    best_active_ask = best_data_ba_on_time.reset_index(
                        level= ('tipo-orden')).groupby(level = 'tenor').apply(
                            lambda y: y.reset_index(drop = False)[y.reset_index(drop = False)['tipo-orden'] =='ASK']['min'])
        
                    best_active_bid = best_data_ba_on_time.reset_index(
                        level= ('tipo-orden')).groupby(level = 'tenor').apply(
                            lambda y: y.reset_index(drop = False)[y.reset_index(drop = False)['tipo-orden'] =='BID']['max'])
                    
                    best_active_quote.rename(columns = {0:'weighted'}, inplace = True)
                    best_active_ask.rename(columns = {0:'weighted'}, inplace = True)
                    best_active_bid.rename(columns = {1:'weighted'}, inplace = True)
                    for i in [best_active_quote, best_active_ask, best_active_bid]:
                        i['tipo-orden'] = 'TRADE'
                    best_active_quote = best_active_quote.reset_index(level = 'tenor').set_index(['tenor', 'tipo-orden'])
                    best_active_ask = best_active_ask.reset_index(level = 'tenor').set_index(['tenor', 'tipo-orden'])
                    best_active_bid = best_active_bid.reset_index(level = 'tenor').set_index(['tenor', 'tipo-orden'])
                    check_ba_filter = pd.concat([best_active_ask.reset_index('tipo-orden', drop = True).rename(columns = {'weighted':'ask'}), 
                                                 best_active_bid.reset_index('tipo-orden', drop = True).rename(columns = {'weighted':'bid'})], 
                                                axis = 1)
                    check_ba_filter = check_ba_filter.join(self.ba_spread, how = 'left')
                    check_ba_filter.loc[:,'ba_spread'] = check_ba_filter.ask-check_ba_filter.bid
                    self.logger.info('Filtro ba_spread. Criterio 3 \n' + check_ba_filter.to_string())
                    best_active_quote = (best_active_quote.reset_index('tipo-orden')[(check_ba_filter.ba_spread>=0) & (check_ba_filter.ba_spread<=(check_ba_filter.loc[:,0]+0.00000001))]).set_index('tipo-orden', append = True)
                    best_active_ask = best_active_ask.loc[best_active_quote.index.tolist()]
                    best_active_bid = best_active_bid.loc[best_active_quote.index.tolist()]
                    self.logger.info('Best active ask \n' + best_active_ask.to_string())
                    self.logger.info('Best active bid \n' + best_active_bid.to_string())
                    if not best_active_quote.empty:
                        self.logger.info('Tenores 3er Criterio: '+ str(best_active_quote.reset_index('tipo-orden').index.to_list()))
                        swp_quotes_on_time = SamplingSwaps().match_warrant_today(best_active_quote, swap_today, 'TRADE')
                        swp_quotes_on_time['bid'] = swp_quotes_on_time.loc[:,['tenor', 'bid', 'ask']].set_index('tenor').join(best_active_bid.reset_index('tipo-orden'), how ='left').weighted.values
                        swp_quotes_on_time['ask'] = swp_quotes_on_time.loc[:,['tenor', 'bid', 'ask']].set_index('tenor').join(best_active_ask.reset_index('tipo-orden'), how ='left').weighted.values
                        swap_today.loc[swp_quotes_on_time.index.to_list(), ['mid', 'bid', 'ask', 'tipo-precio']] = \
                            np.array([swp_quotes_on_time.weighted, 
                                      swp_quotes_on_time.bid, swp_quotes_on_time.ask,'active_quote_3'], dtype = 'object')
            else:
                self.logger.info('No hubo trades por fuera del horario de cierre que superaran' +str(self.active_minutes) + \
                                 'min activos. No info de Criterio 3')
                best_active_quote = pd.DataFrame()

            # Fourth Criteria
            self.logger.info('4to criterio de la muestra: best_spreads_off_time')
            one_to_third_tenors_list = [best_data_on_time,best_spreads_on_time, best_active_quote]
            one_to_third_tenors = list(chain.from_iterable(
                [list(set(x.index.get_level_values(level = 'tenor').tolist())) for x in one_to_third_tenors_list if not x.empty]))
            self.logger.info('Tenors 1-3 \n' +str(one_to_third_tenors))
            brokers_matchs_spreads_off_time = broker_info_spreads.drop(brokers_matchs_spreads_on_time.index.to_list(), 
                                                             axis = 0).loc[(broker_info_spreads['tipo-orden'] =='TRADE')]
            if brokers_matchs_spreads_off_time.empty or all(x.empty for x in one_to_third_tenors_list):
                if brokers_matchs_spreads_off_time.empty:
                    self.logger.info('No hubo suficientes trades sobre estrategias spread para calcular el 4to criterio')
                if all(x.empty for x in one_to_third_tenors_list):
                    self.logger.info('No hay tenores que se seleccionaron a traves de los 3 anteriores criterios')
                best_spreads_off_time = pd.DataFrame()
                off_time_tenors =[]
            else:
                check_tenor_in_spreads = brokers_matchs_spreads_off_time.tenor.str.contains('|'.join(one_to_third_tenors))
                spreads_off_time = brokers_matchs_spreads_off_time.loc[check_tenor_in_spreads]
                spreads_off_time['new_spread'] = [False if x[0] in one_to_third_tenors and x[1] in one_to_third_tenors else True for x in spreads_off_time.tenor.str.split('X').values]
                spreads_off_time = spreads_off_time.loc[spreads_off_time['new_spread'] == True]
                self.logger.info('spread off time \n'+ spreads_off_time.to_string())
                if spreads_off_time.empty:
                    self.logger.info('No hay informacion de trades sobre spreads con algun tenor negociado que ya haya sido seleccionada en los 3 anteriores criterios')
                    best_spreads_off_time = pd.DataFrame()
                    off_time_tenors =[]
                else:
                    duplicated_tenor = {}
                    for i in one_to_third_tenors:
                        duplicated_tenor[i] = [k.replace('X','') for k in list(
                            chain.from_iterable([list(filter(lambda x: 'X' in x,re.split(i,j))) \
                                                 for j in spreads_off_time.tenor.unique().tolist() if i in j.split("X")]))]
                    for og_ten, dup_ten in duplicated_tenor.items():
                        if dup_ten!=[]:
                            for l in dup_ten:                                
                                if (l+'X'+ og_ten not in spreads_off_time.tenor.unique()) and (og_ten+'X'+l not in spreads_off_time.tenor.unique()):
                                    duplicated_tenor[og_ten].remove(l)
                    posibles_tenors_to_search = list(set(list(chain.from_iterable(list(duplicated_tenor.values())))))
                    tenors_to_search = list(set(posibles_tenors_to_search).difference(set(one_to_third_tenors)))
                    self.logger.info('Tenores posibles en 4to Criterio: ' + str(tenors_to_search))
                    for ten in tenors_to_search:
                        ten_to_identify = [key for key, values in duplicated_tenor.items() if ten in values]
                        if len(ten_to_identify) > 1:
                            self.logger.info('Definicion del tenor spread a escoger para: ' + ten)
                            num_cross_tenor = [broker_info_spreads.loc[(broker_info_spreads.tenor.str.contains(ten_id)) & \
                                                                       (broker_info_spreads.tenor.str.contains(ten))].shape[0] for ten_id in ten_to_identify]
                            if [max(num_cross_tenor) == num for num in num_cross_tenor].count(True)==1:
                                ten_not_to_drop = ten_to_identify[num_cross_tenor.index(max(num_cross_tenor))]
                                ten_to_drop = [tenor for tenor in ten_to_identify if tenor not in ten_not_to_drop]
                            else:
                                nom_cross_tenor = [broker_info_spreads.loc[(broker_info_spreads.tenor.str.contains(ten_id)) & \
                                                                       (broker_info_spreads.tenor.str.contains(ten))].nominal.sum() for ten_id in ten_to_identify]
                                if [max(nom_cross_tenor) == nom for nom in nom_cross_tenor].count(True)==1:
                                    ten_not_to_drop = ten_to_identify[nom_cross_tenor.index(max(nom_cross_tenor))]
                                    ten_to_drop = [tenor for tenor in ten_to_identify if tenor not in ten_not_to_drop]
                                else:
                                    ten_not_to_drop = min([tenors_cat[tenors_cat == ten_id] for ten_id in ten_to_identify]).__array__()[0]
                                    ten_to_drop = [tenor for tenor in ten_to_identify if tenor not in ten_not_to_drop]
                            self.logger.info('Tenor: ' + ten + '. Se haya por medio de la estrategia spread: ' + str(ten_not_to_drop))
                            for ten_out in ten_to_drop:
                                spreads_off_time.drop(index = spreads_off_time.loc[(spreads_off_time.tenor.str.contains(ten_out)) & \
                                                                           (spreads_off_time.tenor.str.contains(ten))].index, inplace = True)
                    brokers_matchs_spreads_off_time_df = pd.DataFrame()
                    off_time_tenors = list(set([x[0] if x[0] not in one_to_third_tenors else x[1] for x in spreads_off_time['tenor'].str.split('X')]))
                    for i in off_time_tenors:
                        spr_per_tenor = spreads_off_time.loc[spreads_off_time.tenor.str.contains(i)]
                        traded_tenor = [s for s in re.split(
                            i, spr_per_tenor.loc[: ,'tenor'].unique().tolist()[0]) if "X" in s][0].replace('X', '')
                        spr_per_tenor['traded_tenor'] =  traded_tenor
                        check_pos_tenors_list = [x for x in range(
                            len(one_to_third_tenors_list)) if not one_to_third_tenors_list[x].empty and traded_tenor in one_to_third_tenors_list[x].index.get_level_values(level = 'tenor').tolist()]
                        spr_per_tenor['traded_price'] = one_to_third_tenors_list[check_pos_tenors_list[0]].xs(traded_tenor, level= 'tenor')['weighted'].values[0]
                        spr_per_tenor['pos_spr'] = spr_per_tenor.tenor.str.split('X').apply(lambda x: x.index(traded_tenor))
                        spr_per_tenor.rename(columns = {'precio': 'spread', 'tenor': 'cross_tenor'}, inplace = True)
                        spr_per_tenor['precio'] = np.where(spr_per_tenor.pos_spr == 0, 
                                                           spr_per_tenor.traded_price + spr_per_tenor.spread, 
                                                           spr_per_tenor.traded_price - spr_per_tenor.spread)
                        list_of_tenors = np.unique(spr_per_tenor.cross_tenor.str.split('X').values)
                        for j in list_of_tenors :
                            j[0], j[1] = j[1], j[0]
                        spr_per_tenor['tenor']  = [list_of_tenors[li][x] for li in range(
                            len(list_of_tenors)) for x in spr_per_tenor.pos_spr.values]
                        brokers_matchs_spreads_off_time_df = pd.concat([brokers_matchs_spreads_off_time_df , spr_per_tenor])
                    brokers_matchs_spreads_off_time_df.drop(columns= ['new_spread', 'traded_price', 'pos_spr', 
                                                                      'spread', 'cross_tenor'], inplace = True)
                    best_spreads_off_time = brokers_matchs_spreads_off_time_df.groupby(
                        ['tenor', 'tipo-orden']).apply(SamplingSwaps().summary_swp)
                if not best_spreads_off_time.empty:
                    self.logger.info('Tenores 4to Criterio: ' + str(best_spreads_off_time.reset_index(['tipo-orden']).index.tolist()))
                    swp_trades_spreads_off_time = SamplingSwaps().match_warrant_today(best_spreads_off_time, swap_today, 'TRADE')
                    swap_today.loc[swp_trades_spreads_off_time.index.to_list(), ['mid', 'bid', 'ask', 'tipo-precio']] = \
                                       np.array([swp_trades_spreads_off_time.weighted, swp_trades_spreads_off_time.weighted, 
                                                 swp_trades_spreads_off_time.weighted,'spread_off_time_4'], dtype = 'object')
            # Fifth Criteria
            self.logger.info('5to criterio de la muestra: implicit_spread')
            one_to_fourth_tenors = one_to_third_tenors + off_time_tenors 
            brokers_possible_quotes_spreads = broker_info_spreads.drop(
                brokers_matchs_spreads_on_time.index.to_list() + brokers_matchs_spreads_off_time.index.to_list(), 
                axis = 0).loc[(broker_info_spreads['hora-final'] - broker_info_spreads['hora-inicial'] >= dt.timedelta(minutes = 5)) & \
                              (broker_info_spreads['hora-final'] >= close_market) & (broker_info_spreads['tipo-orden'].isin(['BID', 'ASK']))]
            fifth_criteria_tenors =[]
            if not brokers_possible_quotes_spreads.empty:
                brokers_quotes_spreads = brokers_possible_quotes_spreads.groupby(
                                                         ['tenor', 'tipo-orden']).apply(SamplingSwaps().summary_swp)
                best_ask_and_bid_spreads = []
                for index, row in (brokers_quotes_spreads.reset_index(level= ['tipo-orden']).groupby(
                        level = ['tenor'], observed = True)):
                    if any(row["tipo-orden"].str.contains('ASK')) & any(row["tipo-orden"].str.contains('BID')):
                        best_ask_and_bid_spreads.append(index)
                for cross in best_ask_and_bid_spreads.copy():
                    if all(j in one_to_fourth_tenors for j in cross.split('X')) or not any(j in one_to_fourth_tenors for j in cross.split('X')):
                        best_ask_and_bid_spreads.pop(best_ask_and_bid_spreads.index(cross))                        
                self.logger.info('Cross tenor implicitos: '+ str(best_ask_and_bid_spreads))
                best_data_ba_spread = (brokers_quotes_spreads.reset_index(
                    level= ('tipo-orden')).loc[best_ask_and_bid_spreads]).set_index('tipo-orden', append = True)
                if not best_data_ba_spread.empty:
                    self.logger.info('Tenores cruzados aplicables como implicitos: ' +  str(best_data_ba_spread.reset_index(['tipo-orden']).index.unique().tolist()))
                    best_active_spreads = (best_data_ba_spread.reset_index(
                        level= ('tipo-orden')).groupby(level = 'tenor').apply(
                            lambda x: np.mean(np.array([x.reset_index(
                                drop = False)[x.reset_index(drop = False)['tipo-orden'] =='ASK']['min'].values,
                                x.reset_index(drop = True)[x.reset_index(
                                    drop = True)['tipo-orden'] =='BID']['max'].values])))).to_frame()
                    best_active_spreads.rename(columns = {0:'weighted'}, inplace = True)
                    for key,val in best_active_spreads.iterrows():
                        traded_tenor = list(compress(key.split('X'), [j in one_to_fourth_tenors for j in key.split('X')]))[0]
                        rate_per_tenor =  swap_today.loc[swap_today.tenor ==traded_tenor]
                        traded_price = rate_per_tenor.mid.values[0]
                        pos_rate = key.split('X').index(traded_tenor)
                        precio = np.where(pos_rate == 0,  traded_price + val.weighted, traded_price - val.weighted)
                        list_of_tenors = key.split('X')
                        list_of_tenors[0], list_of_tenors[1] = list_of_tenors[1], list_of_tenors[0]
                        new_tenor = list_of_tenors[pos_rate]
                        percent = self.ba_spread[self.ba_spread.index == new_tenor]
                        fifth_criteria_tenors.append(new_tenor)
                        swap_today.loc[swap_today.tenor == new_tenor, ['mid', 'bid', 'ask', 'tipo-precio']] = \
                            np.array([precio, precio-percent*0.5, precio+percent*0.5, 'implicit_cross_quotes_5'], dtype='object')         
            # Sixth criteria
            self.logger.info('6to criterio de la muestra: best_trades_off_time')
            one_to_fifth_tenors = one_to_fourth_tenors +  fifth_criteria_tenors
            broker_matchs_off_time = broker_info.drop(
                best_trades_on_time.index.to_list(), axis = 0).loc[(broker_info['tipo-orden'] =='TRADE') & \
                                                                   (~broker_info['tenor'].isin(one_to_fifth_tenors))]
            if broker_matchs_off_time.empty:
                self.logger.info('No hubo trades elegibles para el 6to Criterio de la muestra')
                best_trades_off_time = pd.DataFrame()
                sixth_criteria_tenors =[]
            else:    
                best_trades_off_time = broker_matchs_off_time.groupby(['tenor', 'tipo-orden']
                                                                   ).apply(SamplingSwaps().summary_swp)
                sixth_criteria_tenors = best_trades_off_time.index.get_level_values('tenor').values.tolist()
            if not best_trades_off_time.empty:
                self.logger.info('Tenores 6to Criterio: '+ str(sixth_criteria_tenors))
                swp_trades_off_time = SamplingSwaps().match_warrant_today(best_trades_off_time, swap_today, 'TRADE')
                swap_today.loc[swp_trades_off_time.index.to_list(), ['mid', 'bid', 'ask', 'tipo-precio']] = \
                                   np.array([swp_trades_off_time.weighted, swp_trades_off_time.weighted, 
                                             swp_trades_off_time.weighted,'trade_off_time_6'], dtype = 'object')
            # Seventh Criteria
            self.logger.info('7mo criterio de la muestra: intepolated_var')
            one_to_sixth_tenors = one_to_fifth_tenors + sixth_criteria_tenors
            self.logger.info('Inicio interpolacion de variaciones por segmentos')
            for seg in self.tenors_sections:
                self.logger.info('Segmento: ' + str(seg))
                if any(tenor in seg for tenor in  one_to_sixth_tenors):
                    segmented_tenors = tenors_cat[(tenors_cat >= seg[0]) & (tenors_cat<= seg[-1])]
                    count = 0                    
                    while count != len(segmented_tenors):
                        min_pos = np.nonzero(np.in1d(np.array(segmented_tenors), np.array(one_to_sixth_tenors)))[0].min()
                        max_pos = np.nonzero(np.in1d(np.array(segmented_tenors), np.array(one_to_sixth_tenors)))[0].max()
                        if segmented_tenors[count] in one_to_sixth_tenors:
                            pass
                        elif count < min_pos and count < max_pos:
                            self.logger.info(segmented_tenors[count] + ' es el minimo Tenor del seg: ' + str(seg) + '. Aplicable a extrapolacion de la variacion del tenor: ' + segmented_tenors[min_pos])
                            today_values_next_tenor = swap_today.loc[swap_today.tenor == segmented_tenors[min_pos], 'mid'].values
                            yest_values_next_tenor = self.hist_sample.loc[self.hist_sample.tenor == segmented_tenors[min_pos], 'mid'].values
                            var = (today_values_next_tenor - yest_values_next_tenor)
                            percent = self.ba_spread[self.ba_spread.index == segmented_tenors[count]][0].values
                            changed_values_1 = self.hist_sample.loc[self.hist_sample.tenor == segmented_tenors[count], 'mid'].values + var
                            changed_values = np.append(np.append(changed_values_1 ,changed_values_1-percent*0.5),changed_values_1+percent*0.5)                                
                            swap_today.loc[swap_today.tenor == segmented_tenors[count], ['mid','bid', 'ask', 'tipo-precio']] = \
                                np.asarray(np.append(changed_values,'intepolated_var_7'), dtype = 'object')
                        elif (count > min_pos) and (count > max_pos):                       
                            self.logger.info(segmented_tenors[count] + ' es el maximo Tenor del seg: ' + str(seg) + '. Aplicable a extrapolacion de la variacion del tenor: ' + segmented_tenors[max_pos])
                            today_values_prev_tenor = swap_today.loc[swap_today.tenor == segmented_tenors[max_pos], 'mid'].values
                            yest_values_prev_tenor = self.hist_sample.loc[self.hist_sample.tenor == segmented_tenors[max_pos], 'mid'].values
                            var = (today_values_prev_tenor - yest_values_prev_tenor)
                            percent = self.ba_spread[self.ba_spread.index == segmented_tenors[count]][0].values
                            changed_values_1 = self.hist_sample.loc[self.hist_sample.tenor == segmented_tenors[count], 'mid'].values + var
                            changed_values = np.append(np.append(changed_values_1 ,changed_values_1-percent*0.5),changed_values_1+percent*0.5)
                            swap_today.loc[swap_today.tenor == segmented_tenors[count], ['mid','bid', 'ask', 'tipo-precio']] = \
                                np.asarray(np.append(changed_values,'intepolated_var_7'), dtype = 'object')
                        elif (count > min_pos) and (count < max_pos):
                            min_tenor = segmented_tenors[segmented_tenors<segmented_tenors[count]][np.nonzero(
                                np.in1d(np.array(segmented_tenors[segmented_tenors<segmented_tenors[count]]), 
                                        np.array(one_to_sixth_tenors)))[0].max()]
                            max_tenor = segmented_tenors[segmented_tenors>segmented_tenors[count]][np.nonzero(
                                np.in1d(np.array(segmented_tenors[segmented_tenors>segmented_tenors[count]]),
                                        np.array(one_to_sixth_tenors)))[0].min()]
                            self.logger.info('Interpolacion lineal de variaciones entre tenores adyacentes del tenor: ' + segmented_tenors[count])
                            today_values_next_tenor =  swap_today.loc[swap_today.tenor == max_tenor, 'mid'].values
                            today_values_next_tenor_day = swap_today.loc[swap_today.tenor == max_tenor, 
                                                                         ['dias']].values.flatten()[0]
                            today_values_prev_tenor = swap_today.loc[swap_today.tenor == min_tenor, 'mid'].values
                            today_values_prev_tenor_day = swap_today.loc[swap_today.tenor == min_tenor, 
                                                                         ['dias']].values.flatten()[0]                
                            yest_values_next_tenor = self.hist_sample.loc[self.hist_sample.tenor == max_tenor, 'mid'].values.flatten()[0]
                            yest_values_perv_tenor = self.hist_sample.loc[self.hist_sample.tenor == min_tenor, 'mid'].values.flatten()[0]
                            var_next_tenor = (today_values_next_tenor - yest_values_next_tenor).flatten()
                            var_prev_tenor = (today_values_prev_tenor - yest_values_perv_tenor).flatten()
                            node_day = swap_today.loc[swap_today.tenor == segmented_tenors[count], ['dias']].values.flatten()[0]
                            interpolated_values = Interpol(np.array([today_values_prev_tenor_day, today_values_next_tenor_day]),
                                np.append(var_next_tenor,var_prev_tenor),
                                np.array([node_day])).method('linear_interpol')
                            percent = self.ba_spread[self.ba_spread.index == segmented_tenors[count]][0].values
                            changed_values_1 = self.hist_sample.loc[self.hist_sample.tenor == segmented_tenors[count], 'mid'].values + interpolated_values
                            changed_values = np.append(np.append(changed_values_1 ,changed_values_1-percent*0.5),changed_values_1+percent*0.5)
                            swap_today.loc[swap_today.tenor == segmented_tenors[count], 
                                           ['mid','bid', 'ask', 'tipo-precio']] = \
                                 np.asarray(np.append(changed_values,'intepolated_var_7'), dtype = 'object')

                        count+=1
            # Eigth Criteria
            self.logger.info('8vo criterio de la muestra: hist_price')
            prev_info_ind = swap_today.loc[swap_today.mid.isna() & swap_today.bid.isna()
                                                      & swap_today.ask.isna()].set_index(['tenor']).index
            self.logger.info('Tenores 8vo Criterio: ' + str(prev_info_ind.tolist()))
            prev_info = self.hist_sample.set_index(['tenor']).loc[prev_info_ind, ['mid', 'bid', 'ask']].reset_index(drop = True)
            swap_today_null = swap_today.loc[swap_today.mid.isna() & swap_today.bid.isna() & swap_today.ask.isna() ,
                          ['mid', 'bid', 'ask', 'tipo-precio']]
            self.logger.info('swap_today_null:\n%s',swap_today_null)
            prev_info_prices = np.array([prev_info.mid, prev_info.bid, prev_info.ask,'hist_price_8'], dtype = 'object')
            self.logger.info('prev_info_prices:\n%s',prev_info_prices)
            swap_today.loc[swap_today.mid.isna() & swap_today.bid.isna() & swap_today.ask.isna() ,
                          ['mid', 'bid', 'ask', 'tipo-precio']] = \
                np.array([prev_info.mid, prev_info.bid, prev_info.ask,'hist_price_8'], dtype = 'object')
            ba_spread_trades = swap_today.set_index('tenor').join(self.ba_spread, how='left')
            ba_spread_trades  = ba_spread_trades.loc[ba_spread_trades['tipo-precio'].isin(self.repeated_results)].reset_index()
            self.logger.info('Aplicacion de ba_spreads a trades del dia: ' + str(ba_spread_trades.tenor.tolist()))
            swap_today.loc[swap_today['tipo-precio'].isin(self.repeated_results), 'bid'] = ba_spread_trades.mid.astype(float).values-0.5*ba_spread_trades[0].values
            swap_today.loc[swap_today['tipo-precio'].isin(self.repeated_results), 'ask'] = ba_spread_trades.mid.astype(float).values+0.5*ba_spread_trades[0].values
            self.logger.info('Redondeo de valores a ' + str(self.dec)+ ' decimales')
            swap_today.mid = rd(swap_today.mid.astype(float).values, self.dec)
            swap_today.bid = rd(swap_today.bid.astype(float).values, self.dec)
            swap_today.ask = rd(swap_today.ask.astype(float).values, self.dec)
        except AttributeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            self.logger.error(err_mss+ error_line)
            self.logger.error(e)
            sys.exit(1)
        except ValueError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            self.logger.error(err_mss+ error_line)
            self.logger.error(e)
            sys.exit(1)
        except NameError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            self.logger.error(err_mss+ error_line)
            self.logger.error(e)
            sys.exit(1)
        except TypeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            self.logger.error(err_mss+ error_line)
            self.logger.error(e)
            sys.exit(1)
        except IndexError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            self.logger.error(err_mss+ error_line)
            self.logger.error(e)
            sys.exit(1)
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            self.logger.error(err_mss+ error_line)
            self.logger.error(e)
            sys.exit(1)
        self.logger.info('Fin de la creación de la muestra swap: '+ self.swap)
        self.logger.info(str(swap_today))
        return swap_today
    
    def bid_ask_spread_builder(self, last_bid_ask_spread_hist,intraday):
        """Creacion del bid ask spread diario aplicable a la muestra swap especifica.
        Se trae la informacion sobre las negociaciones del swap respectivo en el dia de valoracion. 
        Se aplica el tercer criterio jerarquico de la muestra swap para hallar directamente las mejores puntas de compra y venta agredibles en el dia.
        Se eliguen los bid ask spread percibido de mercado de cada tenor y para aquellos valores que no encuentran bid ask spread dada la falta de negociaciones, 
        Se eliguen los bid ask spread del dia anterior.
        Args:
            last_bid_ask_spread_hist (pd.DataFrame): Bid ask spread del dia habil anterior
        Returns:
            bid_ask_spread_today (pd.DataFrame): Bid ask spread del dia de valoracion
        """
        err_mss = 'Se genero al crear el bid_ask spread de la muestra swap '+ self.swap +' . Fallo linea: '
        try:
            if not intraday:
                
                self.logger.info('Inicio creacion bid_ask_spread con spread de quotes de mercado, curva : '+ self.swap)
                self.logger.info('Inicializacion bid_ask_spread vacio')
                bid_ask_spread_today = pd.DataFrame({'tenor':self.tenors})
                bid_ask_spread_today['instrumento'] = self.swap
                for i in ['bid_ask_spread', 'bid', 'ask']:
                     bid_ask_spread_today[i] = np.nan
                NY_time_today = dt.datetime.now(tz=tz.gettz('America/New_York')).replace(tzinfo=None)
                BOG_time_today = dt.datetime.now(tz=tz.gettz('America/Bogota')).replace(tzinfo=None)
                diff_time = round((NY_time_today-BOG_time_today).total_seconds()/(60*60),1)
                close_market = dt.datetime.strptime(self.closing_hour, "%H:%M:%S") + dt.timedelta(minutes = 60*diff_time)
                start_hour = close_market - dt.timedelta(minutes= self.min_minutes)
                self.trade_quotes['hora-inicial'] = pd.to_datetime(self.trade_quotes['hora-inicial'], format = '%H:%M:%S')
                self.trade_quotes['hora-final']= pd.to_datetime(self.trade_quotes['hora-final'], format = '%H:%M:%S')
                broker_info_spreads = self.trade_quotes.loc[self.trade_quotes['tenor'].str.contains('X')]
                broker_info_flys = self.trade_quotes.loc[self.trade_quotes['tenor'].str.contains('V')]
                broker_info = self.trade_quotes.drop(broker_info_spreads.index.to_list()+broker_info_flys.index.to_list())
                broker_info = broker_info[broker_info.tenor.isin(self.tenors)]
                
                if self.swap=='IBR':
                    broker_info.loc[broker_info.sistema=='ICAP','nominal']*= broker_info.loc[broker_info.sistema=='ICAP'].set_index(['sistema', 'tenor']).join(
                        self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values*0.0000005
                    broker_info.loc[broker_info.sistema=='GFI','nominal']*= broker_info.loc[broker_info.sistema=='GFI'].set_index(['sistema', 'tenor']).join(
                    self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values/2
                    broker_info.loc[broker_info.sistema=='TP','nominal']*= broker_info.loc[broker_info.sistema=='TP'].set_index(['sistema', 'tenor']).join(
                        self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values*0.0000005
                elif self.swap=='IBRUVR':
                    broker_info.loc[broker_info.sistema=='ICAP','nominal']*= broker_info.loc[broker_info.sistema=='ICAP'].set_index(['sistema', 'tenor']).join(
                        self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values*0.000001
                    broker_info.loc[broker_info.sistema=='GFI','nominal']*= broker_info.loc[broker_info.sistema=='GFI'].set_index(['sistema', 'tenor']).join(
                    self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values
                    broker_info.loc[broker_info.sistema=='TP','nominal']*= broker_info.loc[broker_info.sistema=='TP'].set_index(['sistema', 'tenor']).join(
                        self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values*0.000001
                else:
                    broker_info.loc[broker_info.sistema=='GFI','nominal']*= broker_info.loc[broker_info.sistema=='GFI'].set_index(['sistema', 'tenor']).join(
                        self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values/2

                broker_info.loc[broker_info.sistema=='TRADITION','nominal']*= 1000000
                broker_info['dv01-filter'] = broker_info.set_index(['sistema', 'tenor']).join(
                    self.dv01.set_index(['sistema', 'tenor']), on=['sistema', 'tenor'], lsuffix='x')['dv01'].values
                broker_info.nominal = rd(broker_info.nominal, 0)
                broker_info = broker_info[broker_info.nominal>=broker_info['dv01-filter']]
                # Tercer Criterio de la muestra
                self.logger.info('3er criterio de la muestra: best_active_quotes - ba_spread')
                best_quotes = broker_info.loc[
                    (broker_info['hora-final'] - broker_info['hora-inicial'] >= dt.timedelta(minutes = self.active_minutes)) & \
                        (broker_info['hora-final'] >= close_market) & \
                            (broker_info['hora-final'] - start_hour >= dt.timedelta(minutes = self.active_minutes)) & \
                                (broker_info['tipo-orden'].isin(['BID', 'ASK']))]
                if not best_quotes.empty:
                    self.logger.info(best_quotes.to_string())
                    best_quotes_data = best_quotes.groupby(['tenor', 'tipo-orden']).apply(SamplingSwaps().summary_swp)
                    best_ask_and_bid = []
                    for index, row in (best_quotes_data.reset_index(level= ['tipo-orden']).groupby(
                            level = ['tenor'], observed = True)):
                        if any(row["tipo-orden"].str.contains('ASK')) & any(row["tipo-orden"].str.contains('BID')):
                            best_ask_and_bid.append(index)
                    if best_ask_and_bid==[]:
                        self.logger.info('No hubo informacion de mejores puntas para el 3er crietrio: Criterio 3')
                        best_active_quote = pd.DataFrame()
                    else:
                        self.logger.info('Tenores posibles en 3er Criterio: ' + str(best_ask_and_bid))
                        best_data_ba_on_time = (best_quotes_data.reset_index(
                            level= ('tipo-orden')).loc[best_ask_and_bid]).set_index('tipo-orden', append = True)
                        best_active_quote = (best_data_ba_on_time.reset_index(
                            level= ('tipo-orden')).groupby(level = 'tenor').apply(
                                lambda x: np.mean(np.array([x.reset_index(drop = False)[x.reset_index(
                                    drop = False)['tipo-orden'] =='ASK']['min'].values,x.reset_index(
                                        drop = True)[x.reset_index(drop = True)['tipo-orden'] =='BID']['max'].values])))).to_frame()
                        best_active_ask = best_data_ba_on_time.reset_index(
                            level= ('tipo-orden')).groupby(level = 'tenor').apply(
                                lambda y: y.reset_index(drop = False)[y.reset_index(drop = False)['tipo-orden'] =='ASK']['min'])
            
                        best_active_bid = best_data_ba_on_time.reset_index(
                            level= ('tipo-orden')).groupby(level = 'tenor').apply(
                                lambda y: y.reset_index(drop = False)[y.reset_index(drop = False)['tipo-orden'] =='BID']['max'])
                        best_active_quote.rename(columns = {0:'weighted'}, inplace = True)
                        best_active_ask.rename(columns = {0:'weighted'}, inplace = True)
                        best_active_bid.rename(columns = {1:'weighted'}, inplace = True)
                        for i in [best_active_quote, best_active_ask, best_active_bid]:
                            i['tipo-orden'] = 'TRADE'
                        best_active_quote = best_active_quote.reset_index(level = 'tenor').set_index(['tenor', 'tipo-orden'])
                        best_active_ask = best_active_ask.reset_index(level = 'tenor').set_index(['tenor', 'tipo-orden'])
                        best_active_bid = best_active_bid.reset_index(level = 'tenor').set_index(['tenor', 'tipo-orden'])
                        self.logger.info('Filtro ba_spread. Criterio 3')
                        check_ba_filter = pd.concat([best_active_ask.reset_index('tipo-orden', drop = True).rename(columns = {'weighted':'ask'}), 
                                                     best_active_bid.reset_index('tipo-orden', drop = True).rename(columns = {'weighted':'bid'})], 
                                                    axis = 1)
                        check_ba_filter = check_ba_filter.join(self.ba_spread, how = 'left')
                        check_ba_filter.loc[:,'ba_spread'] = check_ba_filter.ask-check_ba_filter.bid
                        self.logger.info(check_ba_filter.to_string())
                        best_active_quote = (best_active_quote.reset_index('tipo-orden')[check_ba_filter.ba_spread>=0]).set_index('tipo-orden', append = True)
                        self.logger.info(best_active_quote.to_string())
                        best_active_ask = best_active_ask.loc[best_active_quote.index.tolist()]
                        best_active_bid = best_active_bid.loc[best_active_quote.index.tolist()]
                        if not best_active_quote.empty:
                            self.logger.info('Tenores 3er Criterio: '+ str(best_active_quote.reset_index('tipo-orden').index.to_list()))
                            swp_quotes_on_time = SamplingSwaps().match_warrant_today(best_active_quote, bid_ask_spread_today, 'TRADE')
                            self.logger.info(swp_quotes_on_time.to_string())
                            swp_quotes_on_time['bid'] = swp_quotes_on_time.loc[:,['tenor', 'bid', 'ask']].set_index('tenor').join(best_active_bid.reset_index('tipo-orden'), how ='left').weighted.values
                            swp_quotes_on_time['ask'] = swp_quotes_on_time.loc[:,['tenor', 'bid', 'ask']].set_index('tenor').join(best_active_ask.reset_index('tipo-orden'), how ='left').weighted.values
                            swp_quotes_on_time['bid_ask_spread'] = swp_quotes_on_time['ask'] - swp_quotes_on_time['bid']
                            self.logger.info(swp_quotes_on_time.to_string())
                            bid_ask_spread_today.loc[swp_quotes_on_time.index.to_list(), 'bid_ask_spread'] = \
                                swp_quotes_on_time.bid_ask_spread
                else:
                    self.logger.info('No hubo trades por fuera del horario de cierre que superaran' +str(self.active_minutes) + \
                                     'min activos. No info de Criterio 3')
                no_ba_spread_today = bid_ask_spread_today.loc[bid_ask_spread_today.bid_ask_spread.isna(), 'tenor'].values.tolist()
                self.logger.info("Seleccion spreads dia anterior para " + str(no_ba_spread_today))
                prev_info = last_bid_ask_spread_hist.loc[last_bid_ask_spread_hist.tenor.isin(no_ba_spread_today), ['tenor','bid-ask-spread']]
                prev_info.rename(columns = {'bid-ask-spread':'bid_ask_spread'}, inplace = True)
                self.logger.info("Union spreads del dia con spreads del dia anterior")
                bid_ask_spread_today=pd.merge(bid_ask_spread_today,prev_info, how="left", on="tenor")
                bid_ask_spread_today['bid_ask_spread'] = bid_ask_spread_today['bid_ask_spread_x'].fillna(bid_ask_spread_today['bid_ask_spread_y'])
                bid_ask_spread_today.drop(['bid_ask_spread_x', 'bid_ask_spread_y'], axis=1,inplace=True)
                #bid_ask_spread_today.loc[bid_ask_spread_today.tenor.isin(no_ba_spread_today), 'bid_ask_spread'] = prev_info['bid-ask-spread'].values
                bid_ask_spread_today.drop(columns = ['ask', 'bid'], inplace = True)
                bid_ask_spread_today.bid_ask_spread = rd(bid_ask_spread_today.bid_ask_spread.astype(float).values, self.dec)
            else:
                self.logger.info('Inicio creacion bid_ask_spread con spread de quotes de mercado, curva : '+ self.swap)
                self.logger.info('Inicializacion bid_ask_spread vacio')
                bid_ask_spread_today = pd.DataFrame({'tenor':self.tenors})
                bid_ask_spread_today['instrumento'] = self.swap
                for i in ['bid_ask_spread', 'bid', 'ask']:
                     bid_ask_spread_today[i] = np.nan
                NY_time_today = dt.datetime.now(tz=tz.gettz('America/New_York')).replace(tzinfo=None)
                BOG_time_today = dt.datetime.now(tz=tz.gettz('America/Bogota')).replace(tzinfo=None)
                diff_time = round((NY_time_today-BOG_time_today).total_seconds()/(60*60),1)
                close_market = dt.datetime.strptime(self.closing_hour, "%H:%M:%S") + dt.timedelta(minutes = 60*diff_time)
                start_hour = close_market - dt.timedelta(minutes= self.min_minutes)
                self.trade_quotes['hora-inicial'] = pd.to_datetime(self.trade_quotes['hora-inicial'], format = '%H:%M:%S')
                self.trade_quotes['hora-final']= pd.to_datetime(self.trade_quotes['hora-final'], format = '%H:%M:%S')
                broker_info_spreads = self.trade_quotes.loc[self.trade_quotes['tenor'].str.contains('X')]
                broker_info_flys = self.trade_quotes.loc[self.trade_quotes['tenor'].str.contains('V')]
                broker_info = self.trade_quotes.drop(broker_info_spreads.index.to_list()+broker_info_flys.index.to_list())
                broker_info = broker_info[broker_info.tenor.isin(self.tenors)]

                # Tercer Criterio de la muestra
                self.logger.info('3er criterio de la muestra: best_active_quotes - ba_spread')
                best_quotes = broker_info.loc[
                    (broker_info['hora-final'] - broker_info['hora-inicial'] >= dt.timedelta(minutes = self.active_minutes)) & \
                        (broker_info['hora-final'] >= close_market) & \
                            (broker_info['hora-final'] - start_hour >= dt.timedelta(minutes = self.active_minutes)) & \
                                (broker_info['tipo-orden'].isin(['BID', 'ASK']))]
                if not best_quotes.empty:
                    self.logger.info(best_quotes.to_string())
                    best_quotes_data = best_quotes.groupby(['tenor', 'tipo-orden']).apply(SamplingSwaps().summary_swp)
                    best_ask_and_bid = []
                    for index, row in (best_quotes_data.reset_index(level= ['tipo-orden']).groupby(
                            level = ['tenor'], observed = True)):
                        if any(row["tipo-orden"].str.contains('ASK')) & any(row["tipo-orden"].str.contains('BID')):
                            best_ask_and_bid.append(index)
                    if best_ask_and_bid==[]:
                        self.logger.info('No hubo informacion de mejores puntas para el 3er crietrio: Criterio 3')
                        best_active_quote = pd.DataFrame()
                    else:
                        self.logger.info('Tenores posibles en 3er Criterio: ' + str(best_ask_and_bid))
                        best_data_ba_on_time = (best_quotes_data.reset_index(
                            level= ('tipo-orden')).loc[best_ask_and_bid]).set_index('tipo-orden', append = True)
                        best_active_quote = (best_data_ba_on_time.reset_index(
                            level= ('tipo-orden')).groupby(level = 'tenor').apply(
                                lambda x: np.mean(np.array([x.reset_index(drop = False)[x.reset_index(
                                    drop = False)['tipo-orden'] =='ASK']['min'].values,x.reset_index(
                                        drop = True)[x.reset_index(drop = True)['tipo-orden'] =='BID']['max'].values])))).to_frame()
                        best_active_ask = best_data_ba_on_time.reset_index(
                            level= ('tipo-orden')).groupby(level = 'tenor').apply(
                                lambda y: y.reset_index(drop = False)[y.reset_index(drop = False)['tipo-orden'] =='ASK']['min'])
            
                        best_active_bid = best_data_ba_on_time.reset_index(
                            level= ('tipo-orden')).groupby(level = 'tenor').apply(
                                lambda y: y.reset_index(drop = False)[y.reset_index(drop = False)['tipo-orden'] =='BID']['max'])
                        best_active_quote.rename(columns = {0:'weighted'}, inplace = True)
                        best_active_ask.rename(columns = {0:'weighted'}, inplace = True)
                        best_active_bid.rename(columns = {1:'weighted'}, inplace = True)
                        for i in [best_active_quote, best_active_ask, best_active_bid]:
                            i['tipo-orden'] = 'TRADE'
                        best_active_quote = best_active_quote.reset_index(level = 'tenor').set_index(['tenor', 'tipo-orden'])
                        best_active_ask = best_active_ask.reset_index(level = 'tenor').set_index(['tenor', 'tipo-orden'])
                        best_active_bid = best_active_bid.reset_index(level = 'tenor').set_index(['tenor', 'tipo-orden'])
                        self.logger.info('Filtro ba_spread. Criterio 3')
                        check_ba_filter = pd.concat([best_active_ask.reset_index('tipo-orden', drop = True).rename(columns = {'weighted':'ask'}), 
                                                     best_active_bid.reset_index('tipo-orden', drop = True).rename(columns = {'weighted':'bid'})], 
                                                    axis = 1)
                        check_ba_filter = check_ba_filter.join(self.ba_spread, how = 'left')
                        check_ba_filter.loc[:,'ba_spread'] = check_ba_filter.ask-check_ba_filter.bid
                        self.logger.info(check_ba_filter.to_string())
                        best_active_quote = (best_active_quote.reset_index('tipo-orden')[check_ba_filter.ba_spread>=0]).set_index('tipo-orden', append = True)
                        self.logger.info(best_active_quote.to_string())
                        best_active_ask = best_active_ask.loc[best_active_quote.index.tolist()]
                        best_active_bid = best_active_bid.loc[best_active_quote.index.tolist()]
                        if not best_active_quote.empty:
                            self.logger.info('Tenores 3er Criterio: '+ str(best_active_quote.reset_index('tipo-orden').index.to_list()))
                            swp_quotes_on_time = SamplingSwaps().match_warrant_today(best_active_quote, bid_ask_spread_today, 'TRADE')
                            self.logger.info(swp_quotes_on_time.to_string())
                            swp_quotes_on_time['bid'] = swp_quotes_on_time.loc[:,['tenor', 'bid', 'ask']].set_index('tenor').join(best_active_bid.reset_index('tipo-orden'), how ='left').weighted.values
                            swp_quotes_on_time['ask'] = swp_quotes_on_time.loc[:,['tenor', 'bid', 'ask']].set_index('tenor').join(best_active_ask.reset_index('tipo-orden'), how ='left').weighted.values
                            swp_quotes_on_time['bid_ask_spread'] = swp_quotes_on_time['ask'] - swp_quotes_on_time['bid']
                            self.logger.info(swp_quotes_on_time.to_string())
                            bid_ask_spread_today.loc[swp_quotes_on_time.index.to_list(), 'bid_ask_spread'] = \
                                swp_quotes_on_time.bid_ask_spread
                else:
                    self.logger.info('No hubo trades por fuera del horario de cierre que superaran' +str(self.active_minutes) + \
                                     'min activos. No info de Criterio 3')
                no_ba_spread_today = bid_ask_spread_today.loc[bid_ask_spread_today.bid_ask_spread.isna(), 'tenor'].values.tolist()
                self.logger.info("Seleccion spreads dia anterior para " + str(no_ba_spread_today))
                prev_info = last_bid_ask_spread_hist.loc[last_bid_ask_spread_hist.tenor.isin(no_ba_spread_today), ['tenor','bid-ask-spread']]
                prev_info.rename(columns = {'bid-ask-spread':'bid_ask_spread'}, inplace = True)
                self.logger.info("Union spreads del dia con spreads del dia anterior")
                bid_ask_spread_today=pd.merge(bid_ask_spread_today,prev_info, how="left", on="tenor")
                bid_ask_spread_today['bid_ask_spread'] = bid_ask_spread_today['bid_ask_spread_x'].fillna(bid_ask_spread_today['bid_ask_spread_y'])
                bid_ask_spread_today.drop(['bid_ask_spread_x', 'bid_ask_spread_y'], axis=1,inplace=True)
                bid_ask_spread_today.drop(columns = ['ask', 'bid'], inplace = True)
                bid_ask_spread_today.bid_ask_spread = rd(bid_ask_spread_today.bid_ask_spread.astype(float).values, self.dec)
                
        except AttributeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            self.logger.error(err_mss + error_line)
            self.logger.error(e)
            sys.exit(1)
        except ValueError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            self.logger.error(err_mss + error_line)
            self.logger.error(e)
            sys.exit(1)
        except NameError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            self.logger.error(err_mss + error_line)
            self.logger.error(e)
            sys.exit(1)
        except TypeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            self.logger.error(err_mss + error_line)
            self.logger.error(e)
            sys.exit(1)
        except IndexError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            self.logger.error(err_mss + error_line)
            self.logger.error(e)
            sys.exit(1)
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            self.logger.error(err_mss + error_line)
            self.logger.error(e)
            sys.exit(1)
        self.logger.info('Fin de la creación del bid ask spread: '+ self.swap)
        return bid_ask_spread_today