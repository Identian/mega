import sys
import pandas as pd
import numpy as np
import logging
from common.DateUtils import DateUtils, CalendarHandler
from functions.Interpolation import Interpol
from functions.Rounding import rd
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class SamplingOptions():
    """Clase con los metodos generales para la creacion de a la muestra de superficie de volatilidad
    """
    def summary_opt(self, data):
        """Funcionales utiles para la conformacion de la muestra de superficie de volatilidad:
            volatilidad ponderada por monto, valor maximo y valor minimo de una seleccion de trades-quotes
            
        Args:
            data (pd.DataFrame): DataFrame de trades y/o quotes para aplicar los funcionales
        Return:
            res (pd.DataFrame): DataFrame con los resultados de los funcionales
        """
        try:
            result = {
                'weighted': sum(data['precio'] * data['monto'])/sum(data['monto']),
                'max': data['precio'].max(),
                'min': data['precio'].min()
            }
            res = pd.Series(result)
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error('Se genero un error calculando estadisticas de la muestra de opciones. Fallo linea: ' + error_line)
            logger.error(e)
            raise Exception(500)        
        return res
    def match_warrant_today(self, data, opc_today, warrant):
        """Seleccion de filas conformes con los criterios de datos y tipo de orden
        para insertar en la posicion especifica de la supercicie de volatilidad
        
        Args:
            data (pd.DataFrame): DataFrame con multiples indices que contiene los resultados de uno de los 3 primeros criterios jerarquicos 
            aplicados a la muestra de superficie de volatilidad
            opc_today (pd.DataFrame): Muestra de superficie de volatilidad
            warrant (str): Tipo de orden por la cual filtrar
        Return:
            
        """
        try:
            data_warrant = data.iloc[data.index.get_level_values('orden') == warrant]
            opc_warrant_data = opc_today.join(data_warrant.reset_index(level = ['orden']), on=['Strategy', 'Tenor'],how='left')
            pos_warrant = opc_warrant_data.loc[opc_warrant_data.orden == warrant].index.to_list()
            rows = opc_warrant_data.iloc[pos_warrant]
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error('Se genero un error con el match de ordenes. Fallo linea: '+error_line)
            logger.error(e)
            raise Exception(500)
        return  rows
  
class DayCount():
    """Clase que realiza los metodos de conteos de dias especificos dentro de la muestra swaps locales (IBR, IBRUVR, USDCO).
    """
    def local_fwd_daycount(self, val_date, calendar, tenors_short, tenors_long,  starting_day_convention ='same_day_starting',business_day_convention_short_1 = 'following', business_day_convention_short_2 = 'previous', bussines_day_convention_long = 'modified_following'):
        """Conteo de dias de la superficie USDCOP
        Se halla el dia efectivo de las negociaciones de sobre opciones USDCOP en Colombia, 
        Se encuentra las fechas de cada tenor a tener en cuenta desde la fecha efectiva, con una convencion de inicio de dia y de dia tipo de busqueda de dia bursatil
        A todas estas fechas, se les resta dos dias efectivos y se hayan los dias al plazo del tenor como la resta entre estas fechas y la fecha de valoracion
        Para el nodo ON, se haya el siguiente dia habil al dia de valoracion y se realiza la respectiva resta de fechas
        
        Args:
            val_date (dt.date): Fecha de valoracion
            calendar (list): Listado de fechas (dt.date) de un calendario de dias habiles (Colombia)
            tenors (list): Listado de tenores a los cuales se les hallara la fecha y los dias tenor
            starting_day_convetion (str): Parametro de convecion de inio del conteo
            business_day_convetion (str): Parametro de convecion de busqueda de dias bursatiles
        Return:
            days (list): Listado de dias al tenor para los items de la variable tenors
            on_day (int): Dia ON
        """
        err_mss = 'Se genero un error en el calculo del conteo de dias de la superficie USDCOP. Fallo linea: ' 
        try:
            # Creacion de plazo de dias corto plazo
            if val_date in calendar:    
                spot_date = val_date
            else:
                spot_date = DateUtils().add_business_days(val_date, -1, calendar)
            tenors_short_dates_next = [DateUtils().tenor_date(
                spot_date, x, calendar, starting_day_convention, business_day_convention_short_1) for x in tenors_short]
            tenors_short_dates_prev = [DateUtils().tenor_date(
                spot_date, x, calendar, starting_day_convention, business_day_convention_short_2) for x in tenors_short]
            tenor_short_lengths = [DateUtils().tenor_specs(x)['length'] for x in tenors_short]
            tenor_short_distance_next = [x.days for x in (np.array(tenors_short_dates_next) - spot_date)]
            tenor_short_distance_prev = [x.days for x in (np.array(tenors_short_dates_prev) - spot_date)]
            short_election = np.array([tenor_short_distance_next,tenor_short_distance_prev]).transpose()
            short_election_pos = [0 for x in range(len(tenor_short_lengths))]
            for i in range(len(tenor_short_lengths)):
                distance = abs((short_election[i]- tenor_short_lengths[i]))
                if all(j == 0 for j in distance):
                    short_election_pos[i] = 0
                else:
                    short_election_pos[i] = np.where(distance == np.take(distance, distance.min()))[0][0]
            final_short_days = [short_election[x][short_election_pos[x]] for x in range(len(tenor_short_lengths))]
            # Creacion de plazo de dias largo plazo
            trade_date = DateUtils().add_business_days(spot_date, 2, calendar)
            if trade_date in CalendarHandler(val_date, logger).get_last_months_calendar(calendar):
                spot_date_long = DateUtils().end_of_month_business_day_convention(trade_date, calendar)
                tenor_dates = [DateUtils().tenor_date(spot_date_long, x, calendar, starting_day_convention, bussines_day_convention_long) for x in tenors_long]
            else:
                tenor_dates = [DateUtils().tenor_date(trade_date, x, calendar, starting_day_convention, bussines_day_convention_long) for x in tenors_long]
            final_date = np.array([DateUtils().add_business_days(x, -2, calendar) for x in tenor_dates])
            final_long_days = [x.days for x in (final_date - val_date)]
            on_day = (DateUtils().add_business_days(val_date, 1, calendar) - val_date).days
            
        except AttributeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            sys.exit(1)
        except ValueError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            sys.exit(1)
        except NameError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            sys.exit(1)
        except TypeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            sys.exit(1)
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            sys.exit(1)
        return on_day, final_short_days+final_long_days
    
    def add_on_to_deltas(self, opc_d, on_day, days, dec = 6, tenor = 'ON' ):
        """Realizar la extrapolacion del tenorON para una supercifie de volatilidad en deltas
        Interpola de manera linear los valores para el dia ON por cada delta e inserta esta informacion en la superficie en deltas
        Esta informacion la inserta como una nueva fila en el dataframe de la supeficie de volatilidad en deltas
        Args:
            opc_d (pd.DataFrame): Superficie de Volatilidad en Deltas sin el nodo ON
            on_day (int): Dia ON
            days (list): Dias al plazo de los tenores de la superficie de volarilidad
            tenor (str): Tenor a añadir para la columna de tenores en opc_d
        """
        err_mss = 'Se genero un error agregando el nodo ON a la superficie USDCOP de deltas. Fallo linea: '
        try:
            opc_d['dias'] = days
            on_values = [(Interpol(opc_d.dias.values, opc_d[opc_d.columns[x]].values, on_day).method('linear_interpol')).item() for x in range(1, opc_d.shape[1]-1)]
            on_values.insert(0, tenor)
            on_values.insert(len(on_values)+1, on_day)
            opc_d.loc[len(opc_d.index)] = on_values
            opc_d.sort_values('dias', inplace = True)
            opc_d.loc[:,~opc_d.columns.isin(['tenor', 'dias'])] = rd(opc_d.loc[:,~opc_d.columns.isin(['tenor', 'dias'])], dec)
        except AttributeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            sys.exit(1)
        except ValueError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line) 
            logger.error(e)
            sys.exit(1)
        except NameError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            sys.exit(1)
        except TypeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            sys.exit(1)
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line) 
            logger.error(e)
            sys.exit(1)
        return opc_d

class SurfaceTransformation():
    """Clase que contiene las transformaciones de la superficie de volartildiad
    """
    def surface_to_deltas(self, opc_today, tenors, dec=5):
        """Convertir la superficie de opciones desde estrategias a deltas 
        (teniendo en consideracion que las estrategias estandar son At The Money (ATM) - Risk Reversal (RR) y Butterfly (BF))
        Realiza los calculos de conversion desde estrategias a deltas para cada delta estandar (90d, 75d, 50d, 25d 10d)
        y unifica esta informacion (tanto de bid como de ask) dentro de un mismo dataframe de salida con los tenores entregados
        
        Args:
            opc_today (pd.DataFrame): Superficie de volatilidad en estrategias
            tenors (list): Tenores estandar de la superficie de volatilidad
        Return:
            opc_d (pd.DataFrame): Superficie de volatilidad en niveles de deltas estandar
        """
        err_mss = 'Se genero un error convirtiendo la superficie de estrategias a deltas. Fallo linea: ' 
        try:
            call_90d = 1 / 100 * (opc_today.loc[opc_today.Strategy == "ATM", ["BID", "ASK"]].values + \
                                  opc_today.loc[opc_today.Strategy == "10BF", ["BID", "ASK"]].values - \
                                      (0.5 * opc_today.loc[opc_today.Strategy == "10RR", ["BID", "ASK"]].values))
            
            call_75d = 1 / 100 * (opc_today.loc[opc_today.Strategy == "ATM", ["BID", "ASK"]].values +\
                           opc_today.loc[opc_today.Strategy == "25BF", ["BID", "ASK"]].values - \
                           (0.5 * opc_today.loc[opc_today.Strategy == "25RR", ["BID", "ASK"]].values))
                
            call_50d = 1 / 100 * (opc_today.loc[opc_today.Strategy == "ATM", ["BID", "ASK"]].values)
            
            call_25d = 1 / 100 * (opc_today.loc[opc_today.Strategy == "ATM", ["BID", "ASK"]].values + \
                           opc_today.loc[opc_today.Strategy == "25BF", ["BID", "ASK"]].values + \
                           (0.5 * opc_today.loc[opc_today.Strategy == "25RR", ["BID", "ASK"]].values))
            
            call_10d =  1 / 100 * (opc_today.loc[opc_today.Strategy == "ATM", ["BID", "ASK"]].values + \
                           opc_today.loc[opc_today.Strategy == "10BF", ["BID", "ASK"]].values + \
                           (0.5 * opc_today.loc[opc_today.Strategy == "10RR", ["BID", "ASK"]].values))
            
            opc_d = pd.DataFrame(np.hstack([tenors.values, call_90d,call_75d, call_50d, call_25d,  call_10d]),
                                        columns = ["tenor","90D_Bid", "90D_Ask", "75D_Bid",	"75D_Ask",	"50D_Bid",	
                                                   "50D_Ask",	"25D_Bid",	"25D_Ask",	"10D_Bid",	"10D_Ask"])
            opc_d.loc[:,~opc_d.columns.isin(['tenor'])] = rd(opc_d.loc[:,~opc_d.columns.isin(['tenor'])], dec)
        except ValueError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            raise ValueError(204)
        except NameError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            raise NameError(404)
        except TypeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            raise TypeError(406)
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            raise Exception(500)
        return opc_d
    
    def surface_to_strategies(self, opc_d, tenors, val_date_string, opc_today = np.nan, dec = 5, add_on = True):
        """Conversion de la superficie de volatilidad desde deltas a estrategias.
        Se realiza el calculo de conversion de una superficie desde deltas a estrategias
        Se habilita la opcion de solo convertir el valor del nodo ON y agregarlo a una superficie de estrategias anterior
        
        Args:
            opc_d (pd.DataFrame): Superficie de volatilidad en deltas
            opc_today (dpd.DataFrame): Superficie de volatilidad en estregias (sin el nodo ON)
            tenors (list): Tenores estandar
            val_date_string (str): Fecha de valoracion en str con formato "YYYY-MM-DD"
            add_on (bool): Booleano para elegir añadir el nodo ON a una superficie de estrategias ya establecida
        Return:
            opc_today (pd.DataFrame): Superficie de volatilidad en estregias
        """
        err_mss = 'Se genero un error convirtiendo la superficie de deltas a estrategias. Fallo linea: '
        try:
            atm =  100 *(opc_d.loc[opc_d.tenor.isin(tenors) ,['50D_Bid','50D_Ask']].values).flatten()
            rr_25 = 100 * (opc_d.loc[opc_d.tenor.isin(tenors), ['25D_Bid','25D_Ask']].values - \
                           opc_d.loc[opc_d.tenor.isin(tenors), ['75D_Bid','75D_Ask']].values).flatten()
            rr_10 = 100 * (opc_d.loc[opc_d.tenor.isin(tenors), ['10D_Bid','10D_Ask']].values - \
                           opc_d.loc[opc_d.tenor.isin(tenors), ['90D_Bid','90D_Ask']].values).flatten()
            bf_25 = 100 * (0.5 *(opc_d.loc[opc_d.tenor.isin(tenors), ['75D_Bid','75D_Ask']].values + \
                                 opc_d.loc[opc_d.tenor.isin(tenors), ['25D_Bid','25D_Ask']].values) - \
                           opc_d.loc[opc_d.tenor.isin(tenors), ['50D_Bid','50D_Ask']].values).flatten()
            bf_10 = 100 * (0.5*(opc_d.loc[opc_d.tenor.isin(tenors), ['90D_Bid','90D_Ask']].values + \
                                opc_d.loc[opc_d.tenor.isin(tenors), ['10D_Bid','10D_Ask']].values) - \
                           opc_d.loc[opc_d.tenor.isin(tenors), ['50D_Bid','50D_Ask']].values).flatten()
            if add_on:

                opc_today = pd.concat([
                    pd.DataFrame([{'Date':val_date_string, 'Tenor': tenors[0],
                                   'Strategy': 'ATM', 'MID': np.nan, 
                                   'BID' : atm[0], 'ASK': atm[1], 'price_type': np.nan}]),
                                    opc_today.loc[opc_today.Strategy == 'ATM'],
                                    pd.DataFrame([{'Date':val_date_string, 'Tenor': tenors[0],
                                     'Strategy': '25RR', 'MID': np.nan, 
                                     'BID' : rr_25[0], 'ASK': rr_25[1], 'price_type': np.nan}]),
                                    opc_today.loc[opc_today.Strategy == '25RR'],
                                    pd.DataFrame([{'Date':val_date_string, 'Tenor': tenors[0],
                                     'Strategy': '10RR', 'MID': np.nan, 
                                     'BID' : rr_10[0], 'ASK': rr_10[1], 'price_type': np.nan}]),
                                    opc_today.loc[opc_today.Strategy == '10RR'],
                                    pd.DataFrame([{'Date':val_date_string, 'Tenor': tenors[0],
                                     'Strategy': '25BF', 'MID': np.nan, 
                                     'BID' : bf_25[0], 'ASK': bf_25[1], 'price_type': np.nan}]),
                                    opc_today.loc[opc_today.Strategy == '25BF'],
                                    pd.DataFrame([{'Date':val_date_string, 'Tenor': tenors[0],
                                     'Strategy': '10BF', 'MID': np.nan, 
                                     'BID' : bf_10[0], 'ASK': bf_10[1], 'price_type': np.nan}]),
                                    opc_today.loc[opc_today.Strategy == '10BF']], ignore_index=True)
                opc_today.loc[opc_today.MID.isna(), ['MID','price_type']] = \
                    np.array([opc_today.loc[opc_today.MID.isna(),['BID', 'ASK']].mean(axis = 1), 
                              'hist_price'], dtype = 'object')
                opc_today.loc[:,~opc_today.columns.isin(
                    ['Date', 'Tenor', 'Strategy', 'price_type'])] = rd(
                        opc_today.loc[:,~opc_today.columns.isin(['Date', 'Tenor', 'Strategy', 'price_type'])], dec)
            else:
                logger.error('No se ha implementado el ajuste de una superficie de deltas a estrategias general')
                raise NotImplementedError()
        except ValueError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            raise ValueError(204)
        except NameError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            raise NameError(404)
        except TypeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            raise TypeError(406)
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            raise Exception(500)
        return opc_today
        
    def surface_jumps(self, opc_today_d, opc_today_jumps , opc_yest, jump_tenors, spread_jumps, tenor_jump = '2Y'):
        """Realiza el salto en deltas de la superficie USDCOP como la agregacion del spread.
        Cuando se encuentran negociaciones en los nodos de largo plazo, se actualzian los spreads con esta nueva informacion.
        """
        err_mss = 'Fallo la actualizacion de los saltos sobre la supercicie de volatilidad para el LP. Fallo linea: '
        try:
            d_opc_values = opc_today_d.loc[opc_today_d.tenor==tenor_jump]
            j_tenors = pd.DataFrame({'Tenor':jump_tenors})
            for tenor in jump_tenors:
                spread_jump = spread_jumps.loc[spread_jumps.tenor == tenor]
                today_surface_tenor = opc_today_jumps.loc[(opc_today_jumps.Tenor == tenor)]
                if any(today_surface_tenor.price_type != 'hist_price_4'):
                    logger.info('Se actualiza el spread desde el tenor ' + tenor_jump + ' para el de ' + tenor)
                    tenor_value = j_tenors.loc[j_tenors.Tenor == tenor]
                    delta_tenor = self.surface_to_deltas(today_surface_tenor, tenor_value)
                    new_spread_jump = (delta_tenor.drop(columns = 'tenor').values - d_opc_values.drop(columns = 'tenor').values).flatten()
                    logger.info('Actualizacion Salto')
                    spread_jumps_mid = np.array([0.5* (new_spread_jump[0]+new_spread_jump[1]),
                                                     0.5* (new_spread_jump[2]+new_spread_jump[3]),
                                                     0.5* (new_spread_jump[4]+new_spread_jump[5]),
                                                     0.5* (new_spread_jump[6]+new_spread_jump[7]),
                                                     0.5* (new_spread_jump[8]+new_spread_jump[9])])
                    spread_jumps.loc[spread_jumps.tenor == tenor, spread_jumps.columns != 'tenor'] = spread_jumps_mid
                else:
                    logger.info('No hubo actualizacion de saltos para ' + tenor)
                    delta_tenor = pd.DataFrame([[tenor] + (d_opc_values[['90D_Bid', '90D_Ask']].values + spread_jump['d90_str'].values).flatten().tolist() + \
                                                   (d_opc_values[['75D_Bid', '75D_Ask']].values + spread_jump['d75_str'].values).flatten().tolist() + \
                                                   (d_opc_values[['50D_Bid', '50D_Ask']].values + spread_jump['d50_str'].values).flatten().tolist() + \
                                                   (d_opc_values[['25D_Bid', '25D_Ask']].values + spread_jump['d25_str'].values).flatten().tolist() + \
                                                   (d_opc_values[['10D_Bid', '10D_Ask']].values + spread_jump['d10_str'].values).flatten().tolist()],
                                              columns = opc_today_d.columns)
                opc_today_d = pd.concat([opc_today_d, delta_tenor], ignore_index = True)
        except ValueError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            raise ValueError(204)
        except NameError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            raise NameError(404)
        except TypeError as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            raise TypeError(406)
        except Exception as e:
            error_line = str(sys.exc_info()[-1].tb_lineno)
            logger.error(err_mss + error_line)
            logger.error(e)
            raise Exception(500)
        return opc_today_d, spread_jumps