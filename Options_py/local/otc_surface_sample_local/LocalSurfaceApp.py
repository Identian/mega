import pandas as pd
import datetime as dt
import sys
import numpy as np
from dateutil import tz
from functions.Sample import SamplingOptions

class LocalOptions():
    """Clase que contiene el metodo de formacion de la muestra de la superficie de volatilidad en estrategias para la tasa de cambio USDCOP
    
    Args:
        self.val_date (dt.date) :  fecha de valoracion
        self.trades_quotes (pd.DataFrame) : Informacion de Trades y Qoutes de volatilidades USDCOP enviadas por los brokers
        self.hist_sample (pd.DataFrame): Historico de la superficie USDCOP en estrategias
        self.tenors (list): tenores estandar de la superficie de volatilidad
        self.strategies (list): Estrategias estandar de la superficie de volatilidad
        self.logger (logging.RootLogger): Logger del proceso
        self.closing_hour (str): Hora de cierre estandar de jornada de negociaciones 
        self.min_amount (int): Cantidad minima operada para tener en consideracion dentro de la muestra de la superficie USDCOP
        self.quotes_window (int): Ventana de negociacion (en minutos) para considerar trades-quotes a tiempo (on time)
    """
    def __init__(self, s_val_date, trades_quotes, hist_sample, tenors, jump_tenors, strategies, logger, closing_hour = '13:00:00',min_amount = 1e6, quotes_window = 20):
        self.s_val_date = s_val_date
        self.trades_quotes = trades_quotes
        self.hist_sample = hist_sample
        self.tenors = tenors
        self.jump_tenors = jump_tenors
        self.strategies = strategies
        self.logger = logger
        self.closing_hour = closing_hour
        self.min_amount = min_amount
        self.quotes_window = quotes_window #minutes
    
    def USDCOP_strategy_surface_sample(self):
        """Creacion de la muestra de superficie de volailidad USDCOP para un dia de valoracion especifico.
        El codig realiza:
            1) Se crea un pd.DataFrame vacio de combinaciones estretagias-tenores
            2) Se halla la hora de cierre del mercado con horario de NY
            3) Se filtra la informacion de quotes-trades por monto y por estrategias y tenores estandar
            4) Se identifican los trades que estan a tiempo dentro de la ventana especificada en la emtodologia de la creacion de la muestra
            5) Se identifican los trades del dia que estan fuera de la ventana
            6) Se identifican aquellos quotes/puntas unificadas (bid y ask) que hayan durado mas de 20 min en pantalla
            7) De manera jerarquica se selecciona para la muestra los valores de volatilidad por combinacion estrategia-tenor:
                1- La volatilidad ponderada por monto de aquellos trades que hacen parte del punto 4)
                2- La volatilidad ponderada por monto de aquellos trades que hacen parte del punto 5)
                3- Promedio aritmético entre los mejores quotes/puntas unificadas de compra y venta que hacen parte del punto 6)
                4- Para el resto de combinacion estrategia-tenor, se elegirá el valor de volatilidad del dia anterior
        Return:
            op_usdcop_tenors (pd.DataFrame) : DataFrame con la informacion de los tenores estandar
            opc_today (pd.DataFrame) : DataFrame con la muestra de superficie de volatilidad para el dia de valoracion
        """
        err_mss = 'Se genero un error realizando la muestra de la superficie USDCOP. Fallo linea: '
        try:
            self.logger.info('Crecion muestra superficie volatilidad USDCOP')
            op_usdcop_tenors = pd.DataFrame({'Tenor':self.tenors})
            op_usdcop_strategies = pd.DataFrame({'Strategy':self.strategies})
            op_dict = {'Strategy': self.strategies, 'Tenor':self.tenors}
            #opc_today = pd.DataFrame(index=pd.MultiIndex.from_product(op_dict.values(), names = op_dict.keys())).reset_index()
            opc_today = pd.merge(op_usdcop_strategies,op_usdcop_tenors,'cross')
            opc_today['Date'] = self.s_val_date
            opc_today  = opc_today.reindex(columns=['Date','Tenor', 'Strategy'])
            for i in ['MID', 'BID', 'ASK', 'price_type']:
                opc_today[i] = np.nan
            closing_hour = dt.datetime.strptime(self.closing_hour, "%H:%M:%S")
            start_hour = closing_hour - dt.timedelta(minutes= self.quotes_window)
            broker_info = self.trades_quotes
            if not broker_info.empty:    
                broker_info.loc[:,'monto']*= 1000000
            self.logger.info('Aplicacion de filtros de la muestra')
            self.logger.info('Filtro Monto')
            broker_info = broker_info[broker_info.monto >= self.min_amount]
            self.logger.info('Filtro tenores estandar')
            broker_info = broker_info[broker_info.tenor.isin(op_usdcop_tenors.Tenor.values)]
            self.logger.info('Filtro estrategias estandar')
            broker_info = broker_info[broker_info.estrategia.isin(op_usdcop_strategies.Strategy.values)]
            broker_info.reset_index(inplace = True, drop = True)
            broker_info['hora-fin-operacion'] = pd.to_datetime(broker_info['hora-fin-operacion'], format = '%H:%M:%S')
            broker_info['hora-inicio-operacion'] = pd.to_datetime(broker_info['hora-inicio-operacion'], format = '%H:%M:%S')
            self.logger.info('Informacion aplicable para la muestra de opciones local USDCOP: \n ' +
                             broker_info.to_string())
            #1er Criterio: Trades a tiempo
            self.logger.info('1er Criterio de la muestra: trades on time')
            brokers_matchs_on_time = broker_info.loc[(broker_info['hora-fin-operacion'] >= start_hour )\
                                                             & (broker_info['hora-fin-operacion'] <= closing_hour) \
                                                                 & (broker_info['orden']  == 'TRADE')]
            if brokers_matchs_on_time.empty:
                self.logger.info('No hay informacion aplciable para el 1er criterio de la muestra')
                best_data_on_time  = pd.DataFrame()
            else:
                best_data_on_time = brokers_matchs_on_time.groupby(['estrategia', 'tenor', 'orden']
                                                                    ).apply(SamplingOptions().summary_opt)
                self.logger.info('Tenor/Estrategia criterio 1: '+ str(best_data_on_time.index.tolist()))
            # Trades fuera de tiempo
            self.logger.info('2ndo criterio de la muestra: trades off time')
            brokers_match_out_time = (broker_info.drop(brokers_matchs_on_time.index.to_list(), 
                                                               axis=0)).loc[(broker_info['hora-fin-operacion']<start_hour) \
                                                                            | (broker_info['hora-fin-operacion']>closing_hour) \
                                                                            & (broker_info['orden'] == 'TRADE')]
            if brokers_match_out_time.empty:
                self.logger.info('No hay trades aplicables al segundo criterio')
                best_data_out_time  = pd.DataFrame()
            else:
                best_data_out_time = brokers_match_out_time.groupby(['estrategia', 'tenor', 'orden']
                                                                     ).apply(SamplingOptions().summary_opt)
                self.logger.info('Tenor/Estrategia criterio 2: ' + str(best_data_out_time.index.tolist()))
            # Quotes activos
            self.logger.info('3er criterio de la muestra: active quotes')
            best_data = broker_info.loc[(broker_info['hora-fin-operacion'] - \
                                         broker_info['hora-inicio-operacion'] >= dt.timedelta(minutes = self.quotes_window)) & (broker_info['hora-fin-operacion'] >= closing_hour)]
            if best_data.empty:
                self.logger.info('No hubo puntas considerables para el tercer criterio de la muestra')
                best_data_ba =pd.DataFrame()
            else:
                best_data = best_data.groupby(['estrategia', 'tenor', 'orden']).apply(SamplingOptions().summary_opt)
                self.logger.info('Posible combinacion Tenor /Estrategia aplicable al tercer criterio: '+ str(best_data.reset_index('orden').index.tolist()))
                non_best_quotes = list(set(best_data.index.to_list()) & set(best_data_on_time.index.to_list() + best_data_out_time.index.to_list()))
                best_data.drop(non_best_quotes, axis=0, inplace = True)
                best_ask_and_bid = []
                for index, row in (best_data.reset_index(level= ('orden')).groupby(
                        level = ['estrategia', 'tenor'], observed = True)):
                    if any(row["orden"].str.contains('ASK')) & any(row["orden"].str.contains('BID')):
                        best_ask_and_bid.append(index)        
                best_data_ba = (best_data.reset_index(level= ('orden')).loc[best_ask_and_bid]).set_index('orden', append = True)
                self.logger.info('Tenor/Estrategia criterio 3: ' + str(best_data_ba.reset_index('orden').index.unique().tolist()))
            # Criterios Jerarquicos 
            # 1) Trades horario representativo
            if not best_data_on_time.empty:
                opc_trades_on_time = SamplingOptions().match_warrant_today(best_data_on_time, opc_today, 'TRADE')
                opc_today.loc[opc_trades_on_time.index.to_list(), ['MID', 'price_type']] = \
                    np.array([opc_trades_on_time.weighted,'trade_on_time_1'], dtype = 'object')
            # 2) Trades fuera del horario representativo
            if not best_data_out_time.empty:
                opc_trades_out_time = SamplingOptions().match_warrant_today(best_data_out_time, opc_today, 'TRADE')
                opc_today.loc[opc_trades_out_time.index.to_list(), ['MID', 'price_type']] = \
                    opc_trades_out_time.weighted,'trade_out_time_2'
            opc_today.BID.where(opc_today.MID.isna(), opc_today.MID, inplace = True)
            opc_today.ASK.where(opc_today.MID.isna(), opc_today.MID, inplace = True)

            # 3) Quotes horario representativo
            if not best_data_ba.empty:
                # Bid
                bid_on_time = best_data_ba.xs('BID', level = 'orden', drop_level = False)
                opc_bid_on_time =  SamplingOptions().match_warrant_today(bid_on_time, opc_today, 'BID')
                opc_today.loc[opc_bid_on_time.index.to_list(), ['BID']] = opc_bid_on_time['max']
                
                # Ask
                ask_on_time =  best_data_ba.xs('ASK', level = 'orden', drop_level = False)
                opc_ask_on_time =  SamplingOptions().match_warrant_today(ask_on_time, opc_today, 'ASK')
                opc_today.loc[opc_ask_on_time.index.to_list(), ['ASK']] = opc_ask_on_time['min']
                #Mid- Average
                opc_today.loc[opc_ask_on_time.index.to_list() , ['MID','price_type']] = \
                    opc_today.loc[opc_ask_on_time.index.to_list(),['BID', 'ASK']].mean(axis = 1), 'quotes_on_time_3'
            # 4) Informacion del dia anterior
            self.logger.info('4to criterio de la muestra: hist_price')
            prev_info_ind = opc_today.loc[opc_today.MID.isna() & opc_today.BID.isna()
                                          & opc_today.ASK.isna()].drop(
                                              columns = 'Date').set_index(['Strategy', 'Tenor']).index
            self.logger.info('Tenores 4to Criterio: ' + str(prev_info_ind.tolist()))
            prev_info = self.hist_sample.drop(columns = 'fecha-valoracion').set_index(
                ['estrategia', 'tenor']).loc[prev_info_ind, ['mid', 'bid', 'ask']].reset_index(drop = True)
            opc_today.loc[opc_today.MID.isna() & opc_today.BID.isna() & opc_today.ASK.isna(),
                          ['MID', 'BID', 'ASK', 'price_type']] = \
                np.array([prev_info.mid, prev_info.bid, prev_info.ask,'hist_price_4'], dtype = 'object')
            self.logger.info('Sacar la informacion de los tenores que se actualizan por medio del saltos: ' + str(self.jump_tenors))
            opc_today_jumps = opc_today.loc[opc_today.Tenor.isin(self.jump_tenors)]
            opc_today.drop(index = opc_today_jumps.index, inplace = True)
            op_usdcop_tenors.drop(op_usdcop_tenors.loc[op_usdcop_tenors.Tenor.isin(self.jump_tenors)].index, inplace = True)
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
        self.logger.info('Exito generacion muestra superficie de volatilidad USDCOP')
        return op_usdcop_tenors, opc_today, opc_today_jumps