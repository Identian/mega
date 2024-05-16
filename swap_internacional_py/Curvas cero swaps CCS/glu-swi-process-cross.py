"""El siguiente script contiene clases que permiten la construcción
de las curvas ccs: USDPEN, USDCLP, EURUSD a partir de la metodología
"""
import boto3
import json
import logging
from datetime import datetime

import pandas as pd
import sqlalchemy as sa

from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_aws import get_params, get_secret
from precia_utils.precia_exceptions import PlataformError
from precia_utils import precia_sftp
from swap_inter_ccs.otc_functions import swap_functions
from common_library_email_report.ReportEmail import ReportEmail


logger = setup_logging(logging.INFO)
parameter_store_name = "/ps-otc-lambda-reports"


class ExtratorDataDb:
    """Representa la extracción e inserción de información de las bases de datos"""
    def __init__(self, url_db: str, valuation_date: str) -> None:
        self.connection = None
        self.valuation_date = valuation_date
        self.url_db = url_db


    def connect_db(self):
        """Genera la conexión a base de datos"""
        try:
            self.connection = sa.create_engine(self.url_db).connect()
            logger.info("Se conecto correctamente a la base de datos")
        except Exception as e:
            logger.error(create_log_msg("Fallo la conexión a la base de datos"))
            raise PlataformError("Hubo un error en la conexión a base de datos: " + str(e))


    def disconnect_db(self):
        """Cierra la conexión a la base de datos"""
        if self.connection is not None:
            self.connection.close()
            self.connection = None


    def get_data_fwd_inter_daily(self, curve: str):
        """Obtiene la información de Forward Inter Diaria"""
        query_fwd_inter_daily = sa.sql.text("""
            SELECT days_fwd, mid_fwd FROM pub_otc_fwd_inter_daily
            WHERE instrument_fwd = :curve
            AND valuation_date = :valuation_date
            ORDER BY days_fwd ASC
        """)
        query_params = {
            "curve": curve,
            "valuation_date": self.valuation_date
        }
        error_message = "No se pudo traer la info del Forward Inter Diaria "
        try:
            df_fwd_inter_daily = pd.read_sql(query_fwd_inter_daily, self.connection, params=query_params)
            logger.debug(df_fwd_inter_daily)
            logger.info(f"Se obtuvo la información de Forward Inter Diaria de la curva {curve} exitosamente")
            if df_fwd_inter_daily.empty:
                update_report_process("Fallido", error_message, "Empty Data")
                raise ValueError(f"No hay datos de Forward Inter Diaria: {self.valuation_date}")
            return df_fwd_inter_daily
        except Exception as e:
            logger.error(create_log_msg(error_message +":"+curve + str(e)))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError(error_message+": "+curve +"\n" +str(e))
            
    
    def get_data_fwd_inter_nodes(self, curve: str):
        """Obtiene la información de Forward Inter Nodos"""
        query_fwd_inter_nodes = sa.sql.text("""
            SELECT days_fwd AS days, tenor_fwd AS tenor 
            FROM pub_otc_forwards_inter_points_nodes
            WHERE instrument_fwd = :curve
            AND valuation_date = :valuation_date
            ORDER BY days_fwd ASC
        """)
        query_params = {
            "curve": curve,
            "valuation_date": self.valuation_date
        }
        error_message = "No se pudo traer la info del Forward Inter Nodos "
        try:
            df_fwd_inter_nodes = pd.read_sql(query_fwd_inter_nodes, self.connection, params=query_params)
            logger.debug(df_fwd_inter_nodes)
            logger.info(f"Se obtuvo la información de Forward Inter Nodos de la curva {curve} exitosamente")
            if df_fwd_inter_nodes.empty:
                update_report_process("Fallido", error_message, "Empty Data")
                raise ValueError(f"No hay datos de Forward Inter Nodos: {self.valuation_date}")
            return df_fwd_inter_nodes
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+curve+str(e)))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError (f"No se pudo traer la info del Forward nodos {curve}" + str(e))


    def get_data_swap_inter(self, curve: str):
        """Obtiene la información de Swap Inter insumo"""
        query_swap_inter = sa.sql.text("""
            SELECT tenor, mid_price AS rate, bid_price as bid, ask_price as ask 
            FROM prc_otc_inter_swap
            WHERE curve = :curve
            AND valuation_date = :valuation_date
            AND status_info = :status_info
            ORDER BY days ASC
        """)
        query_params = {
            "curve": curve,
            "status_info": 1,
            "valuation_date": self.valuation_date
        }
        error_message = "No se pudo traer la info del Swap"
        try:
            df_swap_inter = pd.read_sql(query_swap_inter, self.connection, params=query_params)
            logger.debug(df_swap_inter)
            logger.info(f"Se obtuvo la información del Swap Inter insumo: {curve}")
            if df_swap_inter.empty:
                update_report_process("Fallido", error_message, "Empty Data")
                raise ValueError(f"No hay datos de Swap Inter insumo: {self.valuation_date}")
            return df_swap_inter
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+curve+str(e)))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError (f"No se pudo traer la info insumo del Swap {curve}" + str(e))

    
    def get_data_swap_inter_cc_daliy(self, curve: str):
        """Obtiene la información de las curvas Swap Cero Cupón OIS diaria"""
        query_swap_inter_cc_daliy = sa.sql.text("""
            SELECT days, rate FROM pub_otc_inter_swap_cc_daily
            WHERE curve = :curve
            AND valuation_date = :valuation_date
            AND status_info = :status_info
            ORDER BY days ASC
        """)
        query_params = {
            "curve": curve,
            "status_info": 1,
            "valuation_date": self.valuation_date
        }
        error_message = "No se pudo traer la info del del Swap Cero Cupón OIS diaria "
        try:
            df_swap_inter_cc_daliy = pd.read_sql(query_swap_inter_cc_daliy, self.connection, params=query_params)
            logger.debug(df_swap_inter_cc_daliy)
            logger.info(f"Se obtuvo la información del Swap Cero Cupón OIS diaria: {curve}")
            if df_swap_inter_cc_daliy.empty:
                update_report_process("Fallido", error_message, "Empty Data")
                raise ValueError(f"No hay datos de Swap Cero Cupón OIS diaria: {self.valuation_date}")
            return df_swap_inter_cc_daliy
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+curve+str(e)))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError(f"No se pudo traer la data del del Swap Cero Cupón OIS diaria: {curve}"+ str(e))


    def get_data_exchange_rate(self, curve: str):
        """Obtiene la tasa de cambio"""
        query_exchange_rate = sa.sql.text("""
            SELECT value_rates FROM pub_exchange_rate_parity
            WHERE id_precia = :curve
            AND valuation_date = :valuation_date
            AND status_info = :status_info
        """)
        query_params = {
            "curve": curve,
            "status_info": 1,
            "valuation_date": self.valuation_date
        }
        error_message = "No fue posible extraer la información de la tasa"
        try:
            df_exchange_rate = pd.read_sql(query_exchange_rate, self.connection, params=query_params)
            logger.debug(df_exchange_rate)
            logger.info(f"Se obtuvo la información de la tasa: {curve}")
            if df_exchange_rate.empty:
                update_report_process("Fallido", error_message, "Empty Data")
                raise ValueError(f"No hay datos la tasa de cambio: {self.valuation_date}")
            return df_exchange_rate
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+curve))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError(f"No fue posible extraer la información de la tasa {curve}: "+ str(e))


    def get_data_swaps_characteristics(self, curve: str):
        """Trae las características de la curva swap"""
        query_swaps_characteristics = sa.sql.text("""
            SELECT swap_curve, swap_name, daycount_convention, buyer_leg_rate, seller_leg_rate, 
            buyer_leg_currency, buyer_leg_frequency_payment, seller_leg_frequency_payment, 
            start_type, on_the_run_tenors, bullet_tenors, business_day_convention, business_day_calendar
            buyer_leg_projection_curve, seller_leg_projection_curve, buyer_leg_discount_curve, 
            seller_leg_discount_curve, colateral
            FROM precia_utils_swi_swap_characteristics
            WHERE swap_curve = :curve
        """)
        query_params = {
            "curve": curve
        }
        error_message = "No se pudo traer la info de las características de la curva swap"
        try:
            df_swaps_characteristics = pd.read_sql(query_swaps_characteristics, self.connection, params=query_params)
            logger.debug(df_swaps_characteristics)
            logger.info(f"Se obtuvo la información de las características de la curva swap: {curve}")
            return df_swaps_characteristics
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+curve))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError(f"No fue posible extraer la información de las caracteristicas de la curva {curve}: "+ str(e))
    

    def get_data_curves_characteristics(self, curve: str):
        """Trae las características de interpolación de la curva"""
        query_curves_characteristics = sa.sql.text("""
            SELECT curve_name, interpolation_method, interpolation_nodes
            FROM precia_utils_swi_curves_characteristics
            WHERE curve_name = :curve
        """)
        query_params = {
            "curve": curve
        }
        error_message = "No se pudo traer la info de las características de interpolación de la curva"
        try:
            df_curves_characteristics = pd.read_sql(query_curves_characteristics, self.connection, params=query_params)
            logger.debug(df_curves_characteristics)
            logger.info(f"Se obtuvo la información de las características de interpolación de la curva {curve}")
            return df_curves_characteristics
        except Exception as e:
            logger.error(create_log_msg(error_message+":"+curve))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError(f"No fue posible extraer la info de las caracteristicas de interpolación de la curva {curve}: "+ str(e))


    def get_data_calendars(self, calendar_swap: list):
        query_calendars = (
            "SELECT dates_calendar "
            + "FROM precia_utils_calendars "
            + "WHERE "+calendar_swap[0]+" = 1 "
            + "AND "+calendar_swap[1]+" = 1 "
        )
        error_message = "No se pudo traer la info de los calendarios"
        try:
            df_calendars = pd.read_sql(query_calendars, self.connection)
            logger.debug(df_calendars)
            logger.info("Se obtuvo la información de los calendarios")
            return df_calendars
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible extraer la informacion de los calendarios: "+ str(e))


    def get_data_interest_rates_characteristics(self):
        """Obtiene las caracteristicas generales de las tasas de interes"""
        query_interest_rates_characteristics = sa.sql.text("""
            SELECT interest_rate_name, daycount_convention, currency, tenor, start_type,
            business_day_convention, business_day_calendar
            FROM precia_utils_swi_interest_rates_characteristics
        """)
        error_message = "No se pudo traer la info de las caracteristicas generales de las tasas de interes"
        try:
            df_interest_rates_characteristics = pd.read_sql(query_interest_rates_characteristics, self.connection)
            logger.debug(df_interest_rates_characteristics)
            logger.info("Se obtuvo la información de precia_utils_curve_characteristics")
            return df_interest_rates_characteristics
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible extraer la informacion de las caracteristicas generales de las tasas de interes"+ str(e))


    def insert_data_db(self, df_swap_info: pd.DataFrame, pub_table: str):
        """Inserta la informacion calculada durante el proceso"""
        error_message = "Falló la inserción de la información en la base de datos"
        try:
            df_swap_info.to_sql(pub_table, con=self.connection, if_exists="append", index=False)
            logger.info("Finaliza la inserción a la base de datos exitosamente")
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible insertar la información en la base de datos: "+ str(e))
    

    def disable_previous_info(self, pub_table: str, valuation_date: str, swap_curve: str):
        """Actualiza el status de la información se encuentra en la base de datos
        pasando de 1 a 0
        """
        error_message = "Falló la actualización del estado de la información"
        try:
            update_query = (
                "UPDATE "+ pub_table + ""
                + " SET status_info= 0"
                + " WHERE curve = '"+ swap_curve + "'"
                + " AND valuation_date = '"+ valuation_date + "'"
            )
            self.connection.execute(update_query)
            logger.info("Se ha actualizado el estado de la informacion correctamente")
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible actualizar el estado de la informacion en base de datos: "+ str(e))
            
            
    def disable_previous_info_process(self, status_table: str, valuation_date: str, product: str, input_name: str):
        """Actualiza el status de la información se encuentra en la base de datos
        pasando de 1 a 0
        """
        error_message = "Falló la actualización del estado de la información"
        try:
            update_query = (
                "UPDATE "+ status_table + ""
                + " SET last_status= 0"
                + " WHERE product = '"+ product + "'"
                + " AND input_name = '"+ input_name + "'"
                + " AND valuation_date ='"+ valuation_date +"'"
            )
            self.connection.execute(update_query)
            logger.info("Se ha actualizado el estado de la informacion correctamente")
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("No fue posible actualizar el estado de la informacion en base de datos: ", str(e))


class Loader:
    """Representa la carga de la informacion de las curvas en base de datos y en los archivos"""

    def __init__(self, data_connection_sftp: dict, route_sftp: str) -> None:
        self.data_conection_sftp = data_connection_sftp
        self.route_sftp = route_sftp

    def connect_sftp(self):
        """Genera la conexión al SFTP"""
        error_message = "Fallo la conexión al SFTP"
        try:
            self.client = precia_sftp.connect_to_sftp(self.data_conection_sftp)
            self.sftp_connect = self.client.open_sftp()
            logger.info("Se conecto correctamente al SFTP")
        except Exception as e:
            logger.error(create_log_msg(error_message))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError("Hubo un error en la conexión al SFTP: " + str(e))
    
    def disconnect_sftp(self):
        """Cierra la conexión al SFTP"""
        if self.sftp_connect is not None:
            self.sftp_connect.close()
            self.sftp_connect = None
        
    def load_files_to_sftp(self, df_file: pd.DataFrame, file_name: str, columns_file: list):
        """Genera y carga los archivos en el SFTP de Swap Inter CCS"""
        logger.info("Comenzando a generar el archivo %s en el sftp", file_name)
        error_message = "No se pudo generar el archivo en el SFTP"
        try:
            with self.sftp_connect.open(self.route_sftp + file_name, "w") as f:
                try:
                    f.write(df_file.to_csv(index=False, sep=" ", line_terminator='\r\n', header=False, columns=columns_file))
                except Exception as e:
                    logger.error(create_log_msg(f"Fallo la escritura del archivo: {file_name}"))
                    raise PlataformError("No fue posible la escritura del archivo: "+ str(e))

        except Exception as e:
            logger.error(create_log_msg(error_message+": "+file_name))
            update_report_process("Fallido", error_message, str(e))
            raise PlataformError(f"No fue posible generar el archivo {file_name} en el SFTP: "+ str(e))


class ETL:
    """Representa la orquestación de la ETL"""
    def __init__(self, url_publish_otc: str, url_sirius: str, url_process_otc: str, url_precia_utils: str) -> None:
        self.url_sirius = url_sirius
        self.url_publish_otc = url_publish_otc
        self.url_process_otc = url_process_otc
        self.url_precia_utils = url_precia_utils
        curve_param = get_params(["CURVE"])
        self.curve = curve_param["CURVE"]
        valuation_date_param = get_params(["VALUATION_DATE"])
        self.valuation_date = valuation_date_param["VALUATION_DATE"]
        self.report_process = {
          "input_id": "SwapCC_"+self.curve,
          "output_id":"SwapCC_"+self.curve,
          "process": "Derivados OTC",
          "product": "Swap Internacional",
          "stage": "Metodologia",
          "status": "",
          "aws_resource": "glue-p-swi-process-cross",
          "type": "archivo",
          "description": "",
          "technical_description": "",
          "valuation_date": self.valuation_date
        }


    def extract_data(self):
        """Orquesta la extracción de la informacion de la clase ExtratorDataDb"""
        logger.info("Comienza la extraccion de informacion de base de datos...")
        self.calendar_curve_swap = {
            "USDPEN": ["peruvian_calendar", "federal_reserve_calendar"],
            "USDCLP": ["chilean_calendar", "federal_reserve_calendar"],
            "EURUSD": ["target_calendar", "federal_reserve_calendar"],
            "USDJPY": ["japanese_calendar", "federal_reserve_calendar"]
        }
        try:
            self.db_handler_utils = ExtratorDataDb(self.url_precia_utils, self.valuation_date)
            self.db_handler_utils.connect_db()
            self.df_swaps_characteristics = self.db_handler_utils.get_data_swaps_characteristics("SwapCC_"+self.curve)
            self.df_curves_characteristics = self.db_handler_utils.get_data_curves_characteristics("SwapCC_"+self.curve)
            self.df_interest_rates_characteristics = self.db_handler_utils.get_data_interest_rates_characteristics()
            if self.curve in self.calendar_curve_swap.keys():
                self.df_calendar_dates = self.db_handler_utils.get_data_calendars(self.calendar_curve_swap[self.curve])
            self.db_handler_utils.disconnect_db()
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            self.send_error_email(e.error_message)
            raise PlataformError(e.error_message)
        try:
            self.db_handler_pub = ExtratorDataDb(self.url_publish_otc, self.valuation_date)
            self.db_handler_pub.connect_db()
            self.df_fwd_inter_daily = self.db_handler_pub.get_data_fwd_inter_daily(self.curve)
            self.df_fwd_inter_nodes = self.db_handler_pub.get_data_fwd_inter_nodes(self.curve)
            self.df_swap_inter_cc_usdois = self.db_handler_pub.get_data_swap_inter_cc_daliy("SwapCC_USDOIS")
            if self.curve == "USDCLP":
                self.df_swapcc_camara_daliy = self.db_handler_pub.get_data_swap_inter_cc_daliy("SwapCC_Camara")
            elif self.curve == "EURUSD":
                self.df_swapcc_estr_daliy = self.db_handler_pub.get_data_swap_inter_cc_daliy("SwapCC_EUROIS")
            elif self.curve == "USDJPY":
                self.df_swapcc_jpyois_daliy = self.db_handler_pub.get_data_swap_inter_cc_daliy("SwapCC_JPYOIS")
            self.db_handler_pub.disconnect_db()
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            self.send_error_email(e.error_message)
            # self.report_process["status"] = "Fallido"
            # self.report_process["description"] = e.error_message
            # self.report_process["technical_description"] = e.error_message
            # launch_lambda(lambda_name, self.report_process)
            raise PlataformError(e.error_message)

        try:
            self.db_handler_rates = ExtratorDataDb(self.url_sirius, self.valuation_date)
            self.db_handler_rates.connect_db()
            self.df_exchange_rate = self.db_handler_rates.get_data_exchange_rate(self.curve)
            self.db_handler_rates.disconnect_db()
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            self.send_error_email(e.error_message)
            raise PlataformError(e.error_message)
        try:
            self.db_handler_prc = ExtratorDataDb(self.url_process_otc, self.valuation_date)
            self.db_handler_prc.connect_db()
            if self.curve == "EURUSD":
                self.df_swap_inter = self.db_handler_prc.get_data_swap_inter("Swap_EURUS")
            elif self.curve == "USDJPY":
                self.df_swap_inter = self.db_handler_prc.get_data_swap_inter("Swap_USDJPY")
            else:
                self.df_swap_inter = self.db_handler_prc.get_data_swap_inter("Swap_"+self.curve.replace("USD", ""))
            self.db_handler_prc.disconnect_db()   
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            self.send_error_email(e.error_message)
            raise PlataformError(e.error_message)


    def transform_data(self):
        """Orquesta la construcción de las curvas ccs"""
        trade_date = datetime.strptime(self.valuation_date,'%Y-%m-%d').date()
        swap_class = swap_functions(self.df_swaps_characteristics, self.df_curves_characteristics, self.df_interest_rates_characteristics, self.df_calendar_dates["dates_calendar"].astype(str), logger)
        
        if self.curve == 'USDPEN':
            try:
                self.swapcc_nodos, self.swapcc_diaria = swap_class.ccs_curve("SwapCC_USDPEN", trade_date, self.df_swap_inter, self.df_fwd_inter_daily, 
                                                                                        self.df_exchange_rate, self.df_swap_inter_cc_usdois["rate"].values,
                                                                                        self.df_swap_inter_cc_usdois["rate"].values, self.df_fwd_inter_nodes)
                self.curva_par = swap_class.ccs_par_curve("SwapCC_USDPEN", trade_date, self.df_swap_inter, self.swapcc_nodos)
            except PlataformError as e:
                logger.error(create_log_msg(e.error_message))
                self.send_error_email(e.error_message)
                update_report_process("Fallido", e.error_message, str(e))
                raise PlataformError(e.error_message)
        elif self.curve == 'USDCLP':
            try:
                self.swapcc_nodos, self.swapcc_diaria = swap_class.basis_curve("SwapCC_USDCLP", trade_date, self.df_swap_inter, self.df_fwd_inter_daily,
                                                                                        self.df_exchange_rate, self.df_swapcc_camara_daliy["rate"].values, self.df_swap_inter_cc_usdois["rate"].values, 
                                                                                        self.df_swap_inter_cc_usdois["rate"].values, self.df_fwd_inter_nodes)
                self.curva_par = swap_class.ccs_par_curve("SwapCC_USDCLP", trade_date, self.df_swap_inter, self.swapcc_nodos)
                self.curva_par[['mid', 'bid', 'ask']] *= 10000
            except PlataformError as e:
                logger.error(create_log_msg(e.error_message))
                self.send_error_email(e.error_message)
                update_report_process("Fallido", e.error_message, str(e))
                raise PlataformError(e.error_message)
        elif self.curve == 'EURUSD':
            try:
                self.df_fwd_inter_daily["mid_fwd"] = self.df_fwd_inter_daily["mid_fwd"]/10000
                self.swapcc_nodos, self.swapcc_diaria = swap_class.basis_curve("SwapCC_EURUSD",trade_date, self.df_swap_inter, self.df_fwd_inter_daily, 
                                                                                            self.df_exchange_rate, self.df_swapcc_estr_daliy["rate"].values, self.df_swap_inter_cc_usdois["rate"].values, 
                                                                                            self.df_swap_inter_cc_usdois["rate"].values, self.df_fwd_inter_nodes, base_cur="EUR")
                logger.info(self.swapcc_nodos)
                logger.info(self.df_swap_inter)
                self.curva_par = swap_class.ccs_par_curve("SwapCC_EURUSD", trade_date, self.df_swap_inter, self.swapcc_nodos)
                self.curva_par[['mid', 'bid', 'ask']] *= 10000
            except PlataformError as e:
                logger.error(create_log_msg(e.error_message))
                self.send_error_email(e.error_message)
                update_report_process("Fallido", e.error_message, str(e))
                raise PlataformError(e.error_message)
            
        elif self.curve == 'USDJPY':
            try:
                self.df_fwd_inter_daily["mid_fwd"] = self.df_fwd_inter_daily["mid_fwd"]/100
                self.swapcc_nodos, self.swapcc_diaria = swap_class.basis_curve("SwapCC_USDJPY", trade_date, self.df_swap_inter, self.df_fwd_inter_daily,
                                                                                        self.df_exchange_rate, self.df_swapcc_jpyois_daliy["rate"].values, self.df_swap_inter_cc_usdois["rate"].values, 
                                                                                        self.df_swap_inter_cc_usdois["rate"].values, self.df_fwd_inter_nodes)
                self.curva_par = swap_class.ccs_par_curve("SwapCC_USDJPY", trade_date, self.df_swap_inter, self.swapcc_nodos)
                self.curva_par[['mid', 'bid', 'ask']] *= 10000
            except PlataformError as e:
                logger.error(create_log_msg(e.error_message))
                self.send_error_email(e.error_message)
                update_report_process("Fallido", e.error_message, str(e))
                raise PlataformError(e.error_message)

        # Columnas adicionales para inserción en base de datos
        self.swapcc_diaria["valuation_date"] = self.valuation_date
        self.swapcc_diaria["curve"] = "SwapCC_"+self.curve
        self.swapcc_nodos["valuation_date"] = self.valuation_date
        self.swapcc_nodos["curve"] = "SwapCC_"+self.curve
        self.curva_par["valuation_date"] = self.valuation_date
        self.curva_par["curve"] = "Swap_"+self.curve

        # Redondear los df nodos y diarios a 10 decimales y la curva par a 5
        self.swapcc_nodos = self.swapcc_nodos.round(decimals=10)
        self.swapcc_diaria = self.swapcc_diaria.round(decimals=10)
        self.curva_par = self.curva_par.round(decimals=5)


    def load_info(self, data_connection_sftp, route_swap):
        """Orquesta la generación de archivos y la inserción a la base de datos de Swap Inter - CCS"""
        report_cross = {
            'product': ["Swap Inter Cross"],
            'input_name':["SwapCC_"+self.curve],
            'status_process':["successful"],
            'valuation_date':[self.valuation_date]
        }
        
        logger.info(f"Comienza la generación de archivos y la inserción a bd para la curva: {self.curve}")
        sufix_nodos_name = "_Nodos_fecha.txt".replace("fecha", self.valuation_date.replace("-", ""))
        sufix_diaria_name = "_Diaria_fecha.txt".replace("fecha", self.valuation_date.replace("-", ""))
        loader = Loader(data_connection_sftp, route_swap)
        file_name_diaria = "SwapCC_" + self.curve + sufix_diaria_name
        file_name_nodos = "SwapCC_" + self.curve + sufix_nodos_name
        file_name_curva_par = "Swap_" + self.curve + sufix_nodos_name
        if self.curve == "EURUSD":
            file_name_curva_par = "Basis_" + self.curve + sufix_nodos_name
        # Generación de los archivos
        try:
            loader.connect_sftp()
            loader.load_files_to_sftp(self.swapcc_diaria, file_name_diaria, columns_file=['days', 'rate'])
            loader.load_files_to_sftp(self.swapcc_nodos, file_name_nodos, columns_file=['days', 'rate'])
            loader.load_files_to_sftp(self.curva_par, file_name_curva_par, columns_file=['days', 'mid', 'bid', 'ask'])
            loader.disconnect_sftp()
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            self.send_error_email(e.error_message)
            raise PlataformError(e.error_message)
        finally:
            loader.disconnect_sftp()
        # Inserción en base de datos
        try:
            self.db_loader_pub = ExtratorDataDb(self.url_publish_otc, self.valuation_date)
            self.db_loader_pub.connect_db()
            self.db_loader_pub.disable_previous_info("pub_otc_inter_swap_cross_daily", self.valuation_date, "SwapCC_"+self.curve)
            self.db_loader_pub.insert_data_db(self.swapcc_diaria, "pub_otc_inter_swap_cross_daily")
            self.db_loader_pub.disable_previous_info("pub_otc_inter_swap_cross_points_nodes", self.valuation_date, "SwapCC_"+self.curve)
            self.db_loader_pub.insert_data_db(self.swapcc_nodos, "pub_otc_inter_swap_cross_points_nodes")
            self.curva_par.rename(columns={'mid':'mid_price','bid':'bid_price','ask':'ask_price'}, inplace=True)
            self.db_loader_pub.disable_previous_info("pub_otc_inter_swap_cross_curva_par", self.valuation_date, "Swap_"+self.curve)
            self.db_loader_pub.insert_data_db(self.curva_par, "pub_otc_inter_swap_cross_curva_par")
            self.db_loader_pub.disconnect_db()
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            self.send_error_email(e.error_message)
            raise PlataformError(e.error_message)
        logger.info("Finaliza la creación de los archivos y la inserción en la base de datos exitosamente")
        # Reporte del proceso de Cross
        df_report_cross = pd.DataFrame(report_cross)
        self.db_loader_utils = ExtratorDataDb(self.url_precia_utils, self.valuation_date)
        self.db_loader_utils.connect_db()
        self.db_loader_utils.disable_previous_info_process("precia_utils_swi_status_cross_dependencies", self.valuation_date, "Swap Inter Cross", "SwapCC_"+self.curve)
        self.db_loader_utils.insert_data_db(df_report_cross, "precia_utils_swi_status_cross_dependencies")
        self.db_loader_utils.disconnect_db()
        update_report_process("Exitoso", "Proceso Finalizado", "")

    
    def send_error_email(self, error_msg: str):
        """Envía los correos cuando ocurra un error en el proceso"""
        params_key = ["SMTP_SECRET"]
        params_glue = get_params(params_key)
        secret_smtp = params_glue["SMTP_SECRET"]
        key_secret_smtp = get_secret(secret_smtp)
        data_connection_smtp = {
            "server": key_secret_smtp["smtp_server"],
            "port": key_secret_smtp["smtp_port"],
            "user": key_secret_smtp["smtp_user"],
            "password": key_secret_smtp["smtp_password"],
        }
        mail_from = key_secret_smtp["mail_from"]
        mail_to = key_secret_smtp["mail_to"]
        subject = f"Megatron: ERROR al procesar Swap Inter CCS, curva: {self.curve}"
        body = f"""
Cordial Saludo.

Durante el procesamiento de la curva cross swap: {self.curve} se presento el siguiente error en glue.

ERROR: 
{error_msg}

Puede encontrar el log para mas detalles de las siguientes maneras:

    - Ir al servicio AWS Glue desde la consola AWS, dar clic en 'Jobs'(ubicado en el menu izquierdo), dar clic en el job informado en este correo, dar clic en la pestaña 'Runs' (ubicado en la parte superior), y buscar la ejecucion asociada a este mensaje por instrumento, fecha de valoracion y error comunicado, para ver el log busque el apartado 'Cloudwatch logs' y de clic en el link 'Output logs'.

    - Ir al servicio de CloudWatch en la consola AWS, dar clic en 'Grupos de registro' (ubicado en el menu izquierdo), y filtrar por '/aws-glue/python-jobs/output' e identificar la secuencia de registro por la hora de envio de este correo electronico.
Megatron.

Enviado por el servicio automático de notificaciones de Precia PPV S.A. en AWS
        """
        try:
            email = ReportEmail(subject, body)
            smtp_connection = email.connect_to_smtp(data_connection_smtp)
            message = email.create_mail_base(mail_from, mail_to)
            email.send_email(smtp_connection, message)
        except PlataformError as e:
            logger.error(create_log_msg(e.error_message))
            self.send_error_email(e.error_message)
            raise PlataformError(e.error_message)


def launch_lambda(lambda_name: str, payload: dict):
    """Lanza una ejecucion de lambda indicada

    Args:
        lambda_name (str): Nombre de la lambda a ejecutar
        payload (dict): Evento json para enviar a la lambda
    """
    try:
        lambda_client = boto3.client("lambda")
        lambda_response = lambda_client.invoke(
            FunctionName=lambda_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        logger.info("lambda_response:\n%s", lambda_response["Payload"].read().decode())
    except (Exception,) as lbd_exc:
        raise_msg = f"Fallo el lanzamiento de la ejecucion de la lambda: {lambda_name}"
        logger.error(create_log_msg(raise_msg))
        raise PlataformError(raise_msg) from lbd_exc
        

def get_parameter_from_ssm(parameter_name):
    """
    Obtiene un parámetro de Amazon Systems Manager (SSM) Parameter Store.

    Parámetros:
        parameter_name (str): El nombre del parámetro que se desea obtener.

    Retorna:
        str: El valor del parámetro almacenado en SSM Parameter Store.

    Excepciones:
        ssm_client.exceptions.ParameterNotFound: Si el parámetro no existe en SSM Parameter Store.
        Exception: Si ocurre un error inesperado al obtener el parámetro desde SSM Parameter Store.
    """
    ssm_client = boto3.client('ssm')
    try:
        response = ssm_client.get_parameter(
            Name=parameter_name,
            WithDecryption=True
        )
        return response['Parameter']['Value']
    except ssm_client.exceptions.ParameterNotFound:
        logger.error(f"El parámetro '{parameter_name}' no existe en Parameter Store.")
    except Exception as e:
        logger.error(f"Error al obtener el parámetro '{parameter_name}' desde r Parameter Store: {e}")
        
        
def update_report_process(status, description, technical_description):
    """
    Estructura el reporte de estado del proceso y luego llama la funcion que invoca la lambda
    
    Parameters:
        status (str): Estado del proceso
        description (str): Descripcion del proceso
        technical_description (str): Descripcion técnica del proceso (llegado el caso haya fallado el proceso)
        
    Returns:
        None
    """
    lambda_name = get_parameter_from_ssm(parameter_store_name)
    curve_param = get_params(["CURVE"])
    curve = curve_param["CURVE"]
    valuation_date_param = get_params(["VALUATION_DATE"])
    valuation_date = valuation_date_param["VALUATION_DATE"]
    report_process = {
          "input_id": "SwapCC_"+curve,
          "output_id":["SwapCC_"+curve],
          "process": "Derivados OTC",
          "product": "swp_inter",
          "stage": "Metodologia",
          "status": "",
          "aws_resource": "glue-p-swi-process-cross",
          "type": "archivo",
          "description": "",
          "technical_description": "",
          "valuation_date": valuation_date
        }
    report_process["status"]=status
    report_process["description"]=description
    report_process["technical_description"]=technical_description
    launch_lambda(lambda_name, report_process)
    logger.info("Se envia el reporte de estado del proceso")


def main():
    params_key = [
        "DB_SECRET",
        "CURVE",
        "VALUATION_DATE",
        "DATANFS_SECRET"
    ]
    params_glue = get_params(params_key)
    secret_db = params_glue["DB_SECRET"]
    key_secret_db = get_secret(secret_db)

    url_publish_otc = key_secret_db["conn_string_aurora_publish"]
    schema_publish_otc = key_secret_db["schema_aurora_publish"]
    url_db_publish_aurora = url_publish_otc+schema_publish_otc

    url_publish_rates = key_secret_db["conn_string_sirius"]
    schema_publish_rates = key_secret_db["schema_sirius_publish"]
    url_db_publish_sirius = url_publish_rates+schema_publish_rates

    url_process_otc = key_secret_db["conn_string_aurora_process"]
    schema_process_otc = key_secret_db["schema_aurora_process"]
    url_db_process_otc = url_process_otc+schema_process_otc

    url_sources_otc = key_secret_db["conn_string_aurora_sources"]
    schema_precia_utils_otc = key_secret_db["schema_aurora_precia_utils"]
    url_db_precia_utils_otc = url_sources_otc+schema_precia_utils_otc

    secret_sftp = params_glue["DATANFS_SECRET"]
    key_secret_sftp = get_secret(secret_sftp)
    data_connection_sftp = {
        "host": key_secret_sftp["sftp_host"],
        "port": key_secret_sftp["sftp_port"],
        "username": key_secret_sftp["sftp_user"],
        "password": key_secret_sftp["sftp_password"],
    }
    route_swap = key_secret_sftp["route_swap"]
    
    etl =  ETL(url_db_publish_aurora, url_db_publish_sirius, url_db_process_otc, url_db_precia_utils_otc)
    etl.extract_data()
    etl.transform_data()
    etl.load_info(data_connection_sftp, route_swap)


if __name__ == "__main__":
    main() 