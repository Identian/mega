"""
Modulo para collectar informacion financiera del vendor Bloomberg
- Se conecta al servicio de Bloomberg por medio de la libreria requests.
- Solicita los datos conforme los tickets y campos en los parametros.
- Almacena los datos entregados por el servicio en bucket S3 y ruta de los parametros.
"""

from base64 import b64decode
import datetime
import logging
from json import loads as json_loads, dumps as json_dumps
import os
from sys import argv, exc_info, stdout
import time
import uuid
from urllib.parse import urljoin


import s3fs
import requests
from bwslib.bws_auth import Credentials, BWSAdapter, handle_response
from bwslib.sseclient import SSEClient
from awsglue.utils import getResolvedOptions
from boto3 import client as aws_client


PARAMS = {
    "INSTRUMENT",
    "PRODUCT",
    "OUTPUT_BUCKET_URI",
    "REQUEST_FIELDS",
    "REQUEST_IDENTIFIERS",
    "REQUEST_TEMPLATE",
    "BLOOMBERG_SECRET",
    "JOB_NAME",
    "OUTPUT_ID",
    "PROCESS",
}
ERROR_MSG_LOG_FORMAT = "{}. Fallo en linea: {}. Excepcion({}): {}."
PRECIA_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"
)
LAMBDA_REPORT_PS = "/ps-otc-lambda-reports"


# --------------------------------------------------------------------------------------------------
# CONFIGURACIÓN DEL SISTEMA DE LOGS
def setup_logging(log_level):
    """
    Configura el sistema de registro de logs.
    """
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
        file_handler = logging.StreamHandler(stdout)
    file_handler.setFormatter(logging.Formatter(PRECIA_LOG_FORMAT))
    logger.addHandler(file_handler)
    logger.setLevel(log_level)
    return logger


def create_log_msg(log_msg: str) -> str:
    """
    Aplica el formato adecuado al mensaje log_msg, incluyendo información sobre excepciones.
    """
    exception_type, exception_value, exception_traceback = exc_info()
    if not exception_type:
        return f"{log_msg}."
    error_line = exception_traceback.tb_lineno
    return ERROR_MSG_LOG_FORMAT.format(
        log_msg, error_line, exception_type.__name__, exception_value
    )


precia_logger = setup_logging(logging.INFO)


# --------------------------------------------------------------------------------------------------
# PRECIA_UTILS_EXCEPTIONS
class BaseError(Exception):
    """Exception personalizada para la capa precia_utils"""


class PlataformError(BaseError):
    """
    Clase heredada de BaseError que permite etiquetar los errores causados por
    errores del sistema identificados
    """

    def __init__(
        self,
        error_message="La plataforma presenta un error, ver el log para mas detalles",
    ):
        self.error_message = error_message
        super().__init__(self.error_message)

    def __str__(self):
        return str(self.error_message)


# --------------------------------------------------------------------------------------------------
def get_params(parameter_list) -> dict:
    """Obtiene los parametros de entrada del glue

    Parameters:
        parameter_list (list): Lista de parametros

    Returns:
        dict: Valor de los parametros
    """
    try:
        precia_logger.info("Obteniendo parametros del glue job ...")
        params = getResolvedOptions(argv, parameter_list)
        precia_logger.info("Todos los parametros fueron encontrados")
        return params
    except Exception as sec_exc:
        error_msg = (
            f"No se encontraron todos los parametros solicitados: {parameter_list}"
        )
        precia_logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from sec_exc


# --------------------------------------------------------------------------------------------------
def get_secret(secret_name: str) -> dict:
    """
    Obtiene secretos almacenados en el servicio Secrets Manager de AWS.

    Parameters:
        secret_name (str): Nombre del secreto en el servicio AWS.

    Returns:
        dict: Secreto con la informacion desplegada en Secrets Manager AWS.
    """
    try:
        precia_logger.info('Intentando obtener secreto: "%s" ...', secret_name)
        cliente_secrets_manager = aws_client("secretsmanager")
        secret_data = cliente_secrets_manager.get_secret_value(SecretId=secret_name)
        if "SecretString" in secret_data:
            secret_str = secret_data["SecretString"]
        else:
            secret_str = b64decode(secret_data["SecretBinary"])
        precia_logger.info("Se obtuvo el secreto.")
        return json_loads(secret_str)
    except (Exception,) as sec_exc:
        error_msg = f'Fallo al obtener el secreto "{secret_name}"'
        precia_logger.error(create_log_msg(error_msg))
        raise PlataformError(error_msg) from sec_exc


# --------------------------------------------------------------------------------------------------
# COLECTOR
class BloombergService(object):
    """
    Colector bloomberg para la descarga de archivos .csv en AWS bucket S3 desde HAPI
    """
    def __init__(self, bloomberg_secret, params) -> None:
        self.bloomberg_secret = bloomberg_secret
        self.bucket_uri = params["OUTPUT_BUCKET_URI"]
        self.glue_params = params
        self.host = bloomberg_secret.get("host")
        self.sse_url = "/eap/notifications/sse"
        self.catalogs_url = "/eap/catalogs/"
        self.universe_url = "universes/"
        self.fieldlist_url = "fieldLists/"
        self.trigger_url = "/eap/catalogs/bbg/triggers/oneshot/"
        self.request_url = "requests/"
        self.file_extension = ".csv"

    def create_session(self):
        """
        Crea sesion para la obtencion del archivo .csv con datos financieros
        """
        try:
            session = None
            credentials = Credentials.from_dict(
                json_loads(self.bloomberg_secret.get("credential_content"))
            )
            adapter = BWSAdapter(credentials)
            session = requests.Session()
            session.mount("https://", adapter)
        except requests.exceptions.HTTPError as err:
            precia_logger.error(err)
        return session

    def create_sse_session(self, session):
        """
        Crea el cliente de la sesion
        """
        sse_cliente = None
        try:
            sse_cliente = SSEClient(urljoin(self.host, self.sse_url), session)
        except requests.exceptions.HTTPError as err:
            precia_logger.error(err)
        return sse_cliente

    def load_catalog_id(self, session):
        """
        Carga el catalogo de la consulta en HAPI Bloomberg
        """
        catalog_id = None
        catalogs_url = urljoin(self.host, self.catalogs_url)
        catalogs_response = session.get(catalogs_url)
        handle_response(catalogs_response)
        catalogs = catalogs_response.json()["contains"]
        for catalog in catalogs:
            if catalog["subscriptionType"] == "scheduled":
                catalog_id = catalog["identifier"]
        if catalog_id is None:
            precia_logger.error(
                "No se ha encontrado un catalogo programado para %r",
                catalogs_response.json()["contains"],
            )
            raise RuntimeError("Scheduled catalog not found")
        precia_logger.info("El tipo de dato de catalogo ID es -> %s", type(catalog_id))
        return catalog_id

    def create_universe_payload(self, id_postfix, glue_params):
        """
        Crea el "universo" en Bloomberg HAPI, agrega los identificadores al payload
        """
        universe_id = "u" + id_postfix
        identifiers = glue_params["REQUEST_IDENTIFIERS"].split(",")
        universe_payload = {
            "@type": "Universe",
            "identifier": universe_id,
            "title": glue_params.get("PRODUCT"),
            "description": "Recoleccion desde la solucion Megatron",
            "contains": [
                (
                    {
                        "@type": "Identifier",
                        "identifierType": "TICKER",
                        "fieldOverrides": [
                            {
                                "@type": "FieldOverride",
                                "mnemonic": "FWD_CURVE_QUOTE_FORMAT",
                                "override": "POINTS",
                            }
                        ],
                        "identifierValue": identifier,
                    }
                )
                for identifier in identifiers
            ],
        }
        return universe_payload

    def create_fieldlist_payload(self, id_postfix, glue_params):
        """
        Carga la parte del payload que contiene los campos a consultar en Blomberg HAPI
        """
        fieldlist_id = "f" + id_postfix
        fields = glue_params["REQUEST_FIELDS"].split(",")
        fieldlist_payload = {
            "@type": "DataFieldList",
            "identifier": fieldlist_id,
            "title": glue_params.get("INSTRUMENT"),
            "description": "Necessary parameters for price calculation",
            "contains": [({"cleanName": field}) for field in fields],
        }
        return fieldlist_payload

    def get_service_data(self):
        """
        Obtiene los datos financieros desde Bloomberg HAPI en forma de archivo csv 
        """
        session = self.create_session()
        sse_session = self.create_sse_session(session=session)
        catalog_id = self.load_catalog_id(session=session)
        account_url = urljoin(self.host, f"/eap/catalogs/{catalog_id}/")

        unique_id = str(uuid.uuid1())[:6]
        id_postfix = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S") + unique_id
        precia_logger.info("Posfijo de la solicitud: %s", id_postfix)

        universe_payload = self.create_universe_payload(id_postfix, self.glue_params)

        universes_url = urljoin(account_url, self.universe_url)
        response = session.post(universes_url, json=universe_payload)

        handle_response(response)
        universe_location = response.headers["Location"]
        universe_url = urljoin(self.host, universe_location)
        precia_logger.info(
            "El universo se creo correctamente en el endpoint %s", universe_url
        )
        session.get(universe_url)
        # Se crea la lista de campos o atributos de los objetos que se van a consultar
        fieldlist_payload = self.create_fieldlist_payload(id_postfix, self.glue_params)
        fieldlists_url = urljoin(account_url, self.fieldlist_url)
        response = session.post(fieldlists_url, json=fieldlist_payload)
        # Se verifica que la creación de los fields para la consulta y se guarda su endpoint.
        if response.status_code != requests.codes["created"]:
            precia_logger.error("Codigo de estado inesperado: %s", response.status_code)
            raise RuntimeError("Unexpected response")

        fieldlist_location = response.headers["Location"]
        fieldlist_url = urljoin(self.host, fieldlist_location)
        precia_logger.info("Field list successfully created at %s", fieldlist_url)
        session.get(fieldlist_url)
        # Espera una respuesta max de 15min
        trigger_url = urljoin(self.host, self.trigger_url)
        session.get(trigger_url)

        # Se hace el llamado de la información.
        request_id = "r" + id_postfix
        request_payload = self.create_request_payload(
            request_id=request_id,
            universe_url=universe_url,
            fieldlist_url=fieldlist_url,
            trigger_url=trigger_url,
        )
        precia_logger.info("Payload: %s", request_payload)
        requests_url = urljoin(account_url, self.request_url)
        response = session.post(requests_url, json=request_payload)
        # Se verifica si se creo correctamente la solicitud.
        handle_response(response)
        precia_logger.info("La respuesta a la solicitud es: %s", response)
        request_location = response.headers["Location"]
        request_url = urljoin(self.host, request_location)

        precia_logger.info(
            "El recurso %s ha sido creado correctamente en %s", request_id, request_url
        )
        session.get(request_url)

        # Inicia la creación del receptor de eventos
        reply_timeout = datetime.timedelta(minutes=45)
        expiration_timestamp = datetime.datetime.utcnow() + reply_timeout
        while datetime.datetime.utcnow() < expiration_timestamp:
            # Leemos un nuevo evento
            event = sse_session.read_event()
            if event.is_heartbeat():
                precia_logger.info("Recibiendo eventos, esperando el adecuado.")
                continue

            precia_logger.info(
                "Evento de notificación de entrega de respuesta recibida: %s", event
            )
            event_data = json_loads(event.data)

            try:
                distribution = event_data["generated"]
                reply_url = distribution["@id"]
                distribution_id = distribution["identifier"]
                catalog = distribution["snapshot"]["dataset"]["catalog"]
                reply_catalog_id = catalog["identifier"]
            except KeyError:
                precia_logger.info("Recibido otro tipo de evento, sigue esperando")
            else:
                is_required_reply = f"{request_id}.csv" == distribution_id
                is_same_catalog = reply_catalog_id == catalog_id

                if not is_required_reply or not is_same_catalog:
                    precia_logger.info("Se produjo otra entrega. Continúe esperando.")
                    continue
                file_name = (
                    "bloomberg_"
                    + self.glue_params.get("PRODUCT")
                    + "_"
                    + self.glue_params.get("INSTRUMENT")
                    + "_"
                    + time.strftime("%Y-%m-%d")
                    + self.file_extension
                )
                output_file_path = os.path.join(self.bucket_uri, file_name)

            precia_logger.info("Nombre del archivo: %s", output_file_path)
            # Se genera una respuesta en la carpeta de descargas.
            headers = {"Accept-Encoding": "gzip"}
            self.save_output_file(session, reply_url, output_file_path, headers=headers)
            precia_logger.info("Archivo descargado")
            break
        else:
            precia_logger.info(
                "Repuesta no recibida en el tiempo esperado. A la espera de nuevos eventos"
            )

    def save_output_file(
        self, session_, url_, out_path, chunk_size=2048, stream=True, headers=None
    ):
        """
        Function to download the data to an output directory.

        This function allows user to specify the output location of this download
        and works for a single endpoint.

        Add 'Accept-Encoding: gzip' header to reduce download time.
        Note that the vast majority of dataset files exceed 100MB in size,
        so compression will speed up downloading significantly.

        Set 'chunk_size' to a larger byte size to speed up download process on
        larger downloads.
        """
        headers = headers or {"Accept-Encoding": "gzip"}
        with session_.get(url_, stream=stream, headers=headers) as response_:
            response_.raise_for_status()
            fs = s3fs.S3FileSystem()
            with fs.open(out_path, "wb") as out_file:
                precia_logger.info("Loading file from: %s (can take a while) ...", url_)
                for chunk in response_.raw.stream(chunk_size, decode_content=True):
                    out_file.write(chunk)

                precia_logger.info(
                    "\tContent-Disposition: %s",
                    response_.headers["Content-Disposition"],
                )
                precia_logger.info(
                    "\tContent-Encoding: %s", response_.headers["Content-Encoding"]
                )
                precia_logger.info(
                    "\tContent-Length: %s bytes", response_.headers["Content-Length"]
                )
                precia_logger.info(
                    "\tContent-Type: %s", response_.headers["Content-Type"]
                )
                precia_logger.info("\tFile downloaded to: %s", out_path)
                return response_

    def create_request_payload(
        self, request_id, universe_url, fieldlist_url, trigger_url
    ):
        """
        Crea el payload para realizar la solicitud de datos a Bloomberg HAPI
        """
        return {
            "@type": "DataRequest",
            "identifier": request_id,
            "title": "Prices for valuation",
            "description": "Consulta desde la solucion Megatron",
            "universe": universe_url,
            "fieldList": fieldlist_url,
            "trigger": trigger_url,
            "formatting": {
                "@type": "MediaType",
                "outputMediaType": "text/csv",
            },
            "pricingSourceOptions": {
                "@type": "DataPricingSourceOptions",
                "prefer": {"mnemonic": "BGN"},
            },
            "terminalIdentity": {
                "@type": "BlpTerminalIdentity",
                "userNumber": self.bloomberg_secret.get("user_number"),
                "serialNumber": self.bloomberg_secret.get("serial_number"),
                "workStation": 0,
            },
        }


# --------------------------------------------------------------------------------------------------
# REPORTE
def launch_lambda(lambda_name: str, payload: dict):
    """Lanza una ejecucion de lambda indicada

    Args:
        lambda_name (str): Nombre de la lambda a ejecutar
        payload (dict): Evento json para enviar a la lambda
    """
    try:
        lambda_client = aws_client("lambda")
        lambda_response = lambda_client.invoke(
            FunctionName=lambda_name,
            InvocationType="RequestResponse",
            Payload=json_dumps(payload),
        )
        precia_logger.info(
            "lambda_response:\n%s", lambda_response["Payload"].read().decode()
        )
    except (Exception,) as launch_lambda_error:
        raise_msg = f"Fallo el lanzamiento de la ejecucion de la lambda: {lambda_name}"
        precia_logger.error(create_log_msg(raise_msg))
        raise PlataformError(raise_msg) from launch_lambda_error


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
    ssm_client = aws_client("ssm")
    try:
        response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
        return response["Parameter"]["Value"]
    except ssm_client.exceptions.ParameterNotFound as parameter_not_found:
        raise_msg = f"El parámetro '{parameter_name}' no existe en Parameter Store."
        precia_logger.error(create_log_msg(raise_msg))
        raise PlataformError(raise_msg) from parameter_not_found
    except (Exception,) as get_parameter_error:
        raise_msg = f"Error al obtener el parámetro '{parameter_name}' "
        raise_msg += f"desde Parameter Store: {get_parameter_error}"
        precia_logger.error(create_log_msg(raise_msg))
        raise PlataformError(raise_msg) from get_parameter_error


def report_status(status, glue_params, technical_description=""):
    """
    Estructura el reporte de estado del proceso y luego llama la funcion que invoca la lambda

    Parameters:
        status (str): Estado del proceso
        description (str): Descripcion del proceso
        technical_description (str): Descripcion técnica del proceso

    Returns:
        None
    """
    report = {
        "input_id": "bmg"
        + "_"
        + glue_params["PRODUCT"]
        + "_"
        + glue_params["INSTRUMENT"],
        "output_id": glue_params["OUTPUT_ID"].split(","),
        "process": glue_params["PROCESS"],
        "product": glue_params["PRODUCT"],
        "stage": "Recoleccion",
        "status": "Exitoso",
        "aws_resource": glue_params["JOB_NAME"],
        "type": "insumo",
        "description": "Proceso finalizado",
        "technical_description": "",
        "valuation_date": time.strftime("%Y-%m-%d"),
    }
    if status != "Exitoso":
        report["status"] = status
        report["description"] = "Glue detenido por error"
        report["technical_description"] = str(technical_description)
    lambda_report = get_parameter_from_ssm(LAMBDA_REPORT_PS)
    launch_lambda(lambda_report, report)
    precia_logger.info("Reporte de estado del proceso actualizado como %s", status)


# --------------------------------------------------------------------------------------------------
def main():
    """
    Funcion principal del colector
    """
    try:
        glue_params = get_params(PARAMS)
        bloomberg_secret = get_secret(glue_params["BLOOMBERG_SECRET"])
        bloomberg_hapi = BloombergService(bloomberg_secret, glue_params)
        bloomberg_hapi.get_service_data()
        report_status("Exitoso", glue_params)
    except (Exception,) as glue_error:
        report_status("Fallido", glue_params, technical_description=glue_error)
        precia_logger.error(create_log_msg("Error fatal, ejecucion detenida"))
        raise_msg = (
            "Fallo Recoleccion de "
            + glue_params["PROCESS"]
            + " "
            + glue_params["PRODUCT"]
            + " "
            + glue_params["INSTRUMENT"]
        )
        raise PlataformError(raise_msg) from glue_error


# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
