"""Este módulo se encarga de gestionar los reportes de los procesos necesarios para Swap Inter Cross,
verificar que esten completos para cada curva y lanzar el proceso para Cross"""

from itertools import product
import json
import logging
import sys

import boto3
import pandas as pd
import sqlalchemy as sa

from precia_utils.precia_logger import setup_logging, create_log_msg
from precia_utils.precia_aws import get_params, get_secret
from precia_utils.precia_exceptions import PlataformError

logger = setup_logging(logging.INFO)


class DbHandler:
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


    def disable_previous_info(self, status_table: str, valuation_date: str, product: str, input_name: str):
        """Actualiza el status de la información se encuentra en la base de datos
        pasando de 1 a 0
        """
        try:
            update_query = (
                "UPDATE "+ status_table + ""
                + " SET last_status= 0"
                + " WHERE product = '"+ product + "'"
                + " AND input_name IN ("+ input_name + ")"
                + " AND valuation_date IN ("+ valuation_date + ")"
            )
            self.connection.execute(update_query)
            logger.info("Se ha actualizado el estado de la informacion correctamente")
        except Exception as e:
            logger.error(create_log_msg("Falló la actualización del estado de la información"))
            raise PlataformError("No fue posible actualizar el estado de la informacion en base de datos: ", str(e))


    def insert_data_db(self, df_info_process: pd.DataFrame, status_table: str):
        """Inserta la informacion de los procesos reportados"""
        try:
            df_info_process.to_sql(status_table, con=self.connection, if_exists="append", index=False)
            logger.info("Finaliza la inserción a la base de datos exitosamente")
        except Exception as e:
            logger.error(create_log_msg("Falló la inserción de la información en la base de datos"))
            raise PlataformError("No fue posible insertar la información en la base de datos: "+ str(e))
        

    def check_dependencies_cross(self, process: str):
        """Obtiene el numero de los procesos reportados en base de datos"""
        query_dependencies = sa.sql.text(""
            +"SELECT COUNT(input_name) as amount_processes FROM precia_utils_swi_status_cross_dependencies "
            +" WHERE product IN ('Rates parity', 'Forward Inter', 'Swap Inter')"
            +" AND input_name IN ("+process+")"
            +" AND valuation_date = :valuation_date "
            +" AND last_status = :last_status "
        "")
        query_params = {
            "last_status": 1,
            "valuation_date": self.valuation_date
        }
        try:
            reported_processes = pd.read_sql(query_dependencies, self.connection, params=query_params)
            number_processes = reported_processes.at[0, 'amount_processes']
            logger.info("Se obtuvo la información de los procesos reportados para cross")
            return number_processes
        except Exception as e:
            logger.error(create_log_msg("Fallo la extracción de la informacion de la base de datos"))
            raise PlataformError ("No se pudo traer la info de base de datos: " + str(e))


def generate_df(product_name: str, valuation_date: list, input_name:list):
    """Genera el dataframe con la informacion a insertar de los procesos"""
    processes_and_dates = list(product(input_name, valuation_date))
    df_report_process = pd.DataFrame(processes_and_dates, columns=["input_name", "valuation_date"])
    df_report_process["product"] = product_name
    df_report_process["status_process"] = "successful"
    return df_report_process


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


def main():
    params_key = [
        "DB_SECRET",
        "INPUT_NAME",
        "LAMBDA_TRIGGER_PROCESS",
        "PRODUCT",
        "VALUATION_DATE",
        "LAMBDA_TRIGGER_TIIESOFR",
        "LAMBDA_TRIGGER_UFCAMARA",
        "LAMBDA_TRIGGER_CLPCOLATERAL"
    ]
    params_glue = get_params(params_key)
    secret_db = params_glue["DB_SECRET"]
    key_secret_db = get_secret(secret_db)

    url_sources_otc = key_secret_db["conn_string_aurora_sources"]
    schema_precia_utils_otc = key_secret_db["schema_aurora_precia_utils"]
    url_db_precia_utils_otc = url_sources_otc+schema_precia_utils_otc

    product_name = params_glue["PRODUCT"]
    valuation_date = list(params_glue["VALUATION_DATE"].split(","))
    input_name = list(params_glue["INPUT_NAME"].split(","))
    valuation_dates_str = ",".join(["'{}'".format(element) for element in valuation_date])
    input_names = ",".join(["'{}'".format(element) for element in input_name])
    dependencies_usdclp = "'USDCLP', 'SwapCC_Camara', 'SwapCC_USDOIS', 'Swap_CLP'"
    dependencies_usdpen = "'USDPEN', 'SwapCC_USDOIS', 'Swap_PEN'"
    dependencies_eurusd = "'EURUSD', 'SwapCC_EUROIS', 'SwapCC_USDOIS', 'Swap_EURUS'"
    dependencies_usdjpy = "'USDJPY', 'Swap_USDJPY', 'SwapCC_JPYOIS', 'SwapCC_USDOIS'"
    dependencies_usdmxn = "'USDMXN', 'Swap_TIIESOFR', 'Swap_TIIE', 'SwapCC_USDOIS'"
    dependencies_clfclp = "'CLFCLP', 'Swap_UFCAMARA', 'SwapCC_Camara'"
    dependencies_usdclp_colateral = "'USDCLP', 'Swap_CLP', 'Swap_Camara', 'SwapCC_USDOIS'"
    
    usdclp_list = [element.strip("' ") for element in dependencies_usdclp.split(',')]
    usdpen_list = [element.strip("' ") for element in dependencies_usdpen.split(',')]
    eurusd_list = [element.strip("' ") for element in dependencies_eurusd.split(',')]
    usdjpy_list = [element.strip("' ") for element in dependencies_usdjpy.split(',')]
    usdmxn_list = [element.strip("' ") for element in dependencies_usdmxn.split(',')]
    clfclp_list = [element.strip("' ") for element in dependencies_clfclp.split(',')]
    usdclp_colateral_list = [element.strip("' ") for element in dependencies_usdclp_colateral.split(',')]
    product_list = ["Swap Inter", "Forward Inter", "Rates parity"]
    name_lambda_trigger_process = params_glue["LAMBDA_TRIGGER_PROCESS"]
    name_lambda_trigger_tiiesofr = params_glue["LAMBDA_TRIGGER_TIIESOFR"]
    name_lambda_trigger_ufcamara = params_glue["LAMBDA_TRIGGER_UFCAMARA"]
    name_lambda_trigger_clpcolateral = params_glue["LAMBDA_TRIGGER_CLPCOLATERAL"]

    payload_cross = {
        "VALUATION_DATE":valuation_date[0],
        "CURVE":""
    }

    is_product = product_name in product_list
    is_cross_clp = any(element in usdclp_list for element in input_name)
    is_cross_pen = any(element in usdpen_list for element in input_name)
    is_cross_eurusd = any(element in eurusd_list for element in input_name)
    is_cross_usdjpy = any(element in usdjpy_list for element in input_name)
    is_cross_usdmxn = any(element in usdmxn_list for element in input_name)
    is_cross_clfclp = any(element in clfclp_list for element in input_name)
    is_cross_usdclp_colateral_list = any(element in usdclp_colateral_list for element in input_name)

    df_report_process = generate_df(product_name, valuation_date, input_name)

    # Inserta los procesos reportados
    db_handler_sources = DbHandler(url_db_precia_utils_otc, valuation_date[0])
    db_handler_sources.connect_db()
    db_handler_sources.disable_previous_info("precia_utils_swi_status_cross_dependencies", valuation_dates_str, product_name, input_names)
    db_handler_sources.insert_data_db(df_report_process,"precia_utils_swi_status_cross_dependencies")

    if is_product:
        message_reports = "\n o se ha lanzado el proceso cross anteriormente para esta curva"
        if  is_cross_clp:
            reports_usdclp = db_handler_sources.check_dependencies_cross(dependencies_usdclp)
            if reports_usdclp == 5:
                payload_cross["CURVE"] = "USDCLP"
                launch_lambda(lambda_name=name_lambda_trigger_process, payload=payload_cross)
                logger.info("Se lanza el glue para el proceso del calculo de la curva cross: USDCLP")
            else:
                logger.info(f"No se han reportado todas las dependencias para USDCLP: {reports_usdclp}"+message_reports)

        if is_cross_pen:
            reports_usdpen = db_handler_sources.check_dependencies_cross(dependencies_usdpen)
            if reports_usdpen == 4:
                payload_cross["CURVE"] = "USDPEN"
                launch_lambda(lambda_name=name_lambda_trigger_process, payload=payload_cross)
                logger.info("Se lanza el glue para el proceso del calculo de la curva cross: USDPEN")
            else:
                logger.info(f"No se han reportado todas las dependencias para USDPEN: {reports_usdpen}"+message_reports)

        if is_cross_eurusd:
            reports_eurusd = db_handler_sources.check_dependencies_cross(dependencies_eurusd)
            if reports_eurusd == 5:
                payload_cross["CURVE"] = "EURUSD"
                launch_lambda(lambda_name=name_lambda_trigger_process, payload=payload_cross)
                logger.info("Se lanza el glue para el proceso del calculo de la curva cross: EURUSD")
            else:
                logger.info(f"No se han reportado todas las dependencias para EURUSD: {reports_eurusd}"+message_reports)
                
        if is_cross_usdjpy:
            reports_usdjpy = db_handler_sources.check_dependencies_cross(dependencies_usdjpy)
            if reports_usdjpy == 5:
                payload_cross["CURVE"] = "USDJPY"
                launch_lambda(lambda_name=name_lambda_trigger_process, payload=payload_cross)
                logger.info("Se lanza el glue para el proceso del calculo de la curva cross: USDJPY")
            else:
                logger.info(f"No se han reportado todas las dependencias para USDJPY: {reports_usdjpy}"+message_reports)
                
        if is_cross_usdmxn:
            reports_usdmxn = db_handler_sources.check_dependencies_cross(dependencies_usdmxn)
            if reports_usdmxn == 5:
                payload_cross["CURVE"] = "USDMXN"
                launch_lambda(lambda_name=name_lambda_trigger_tiiesofr, payload=payload_cross)
                logger.info("Se lanza el glue para el proceso del calculo de la curva cross: USDMXN")
            else:
                logger.info(f"No se han reportado todas las dependencias para USDMXN: {reports_usdmxn}"+message_reports)
                
        if is_cross_clfclp:
            reports_clfclp = db_handler_sources.check_dependencies_cross(dependencies_clfclp)
            if reports_clfclp == 3:
                payload_cross["CURVE"] = "CLFCLP"
                launch_lambda(lambda_name=name_lambda_trigger_ufcamara, payload=payload_cross)
                logger.info("Se lanza el glue para el proceso del calculo de la curva cross: CLFCLP")
            else:
                logger.info(f"No se han reportado todas las dependencias para CLFCLP: {reports_clfclp}"+message_reports)
                
        if is_cross_usdclp_colateral_list:
            reports_usdclp_colateral = db_handler_sources.check_dependencies_cross(dependencies_usdclp_colateral)
            if reports_usdclp_colateral == 5:
                payload_cross["CURVE"] = "USDCLP"
                launch_lambda(lambda_name=name_lambda_trigger_clpcolateral, payload=payload_cross)
                logger.info("Se lanza el glue para el proceso del calculo de la curva cross: USDCLP")
            else:
                logger.info(f"No se han reportado todas las dependencias para USDCLP: {reports_usdclp_colateral}"+message_reports)

        db_handler_sources.disconnect_db()
    else:
        logger.info("El producto reportado no es dependencia para Swap Inter Cross")

if __name__ == "__main__":
    main()