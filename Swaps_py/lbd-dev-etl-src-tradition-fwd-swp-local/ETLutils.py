#
# =============================================================
#
# Nombre: ETLutils.py
# Tipo: Modulo
#
# Autor:
#  - Ruben Antonio Parra Medrano
# Tecnología - Precia
#
# Ultima modificación: 05/05/2022
#
#
# Modulo que reune funciones usadas con frecuencias en el
# desarrollo de APIs con Lambdas AWS.
#
# Variables de entorno:
# Keys: DB_SRC_SCHEMA, SECRET_DB_REGION, SECRET_DB_NAME
#
# Requerimientos:
# boto3: libreria AWS para usar sus servicios desde Python
# sqlalchemy
#
# =============================================================
#

import logging

import sqlalchemy as sa

from precia_utils.precia_logger import create_log_msg
from precia_utils.precia_exceptions import PlataformError


logger = logging.getLogger()


def connect_to_db(secret):
    """
    Conecta a la base a partir del secreto
    :return: Engine sqlalchemy que permite ejecutar queries
    """
    error_msg = "No se pudo conectar a la base de datos"
    raise_msg = "No se logro conectar a la base de datos src."
    try:
        db_schema = secret["schema"]
        logger.info("Conectandose a la base de datos %s ...", db_schema)
        db_url = create_db_url(secret)
        sql_engine = sa.create_engine(db_url, connect_args={"connect_timeout": 2})
        db_connection = sql_engine.connect()
        logger.info("[ETLutils.connect_to_src_db] Conexion exitosa.")
        return db_connection
    except KeyError as key_exc:
        error_msg += ". El s"
        logger.error(create_log_msg(error_msg))
        raise PlataformError(raise_msg) from key_exc
    except sa.exc.SQLAlchemyError as sql_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError(raise_msg) from sql_exc
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise


def create_db_url(secret: dict) -> str:
    """
    Crea el URL de conexion de la base de datos usado por el metodo 'create_engine' de
    SQLAlchemy a partir del secreto recuperado por Secret Manager AWS, para su
    funcionamiento el secreto debe incluir 'schema' y en el 'engine' de conexion
    mysql+mysqlconnector

    Args:
        secret (dict): Secreto recuperado por Secret Manager AWS

    Raises:
        PlataformError: El secreto no tiene todas las llaves esperadas

    Returns:
        str: URL de conexion para 'create_engine'
    """
    error_msg = "No se pudo crear el URL de coneccion de la base de datos"
    raise_msg = "El secreto no tiene las llaves esperadas"
    try:
        logger.info("Creando URL de conexion a la base de datos ...")
        username = secret["username"]
        password = secret["password"]
        host = secret["host"]
        port = secret["port"]
        engine = secret["engine"]
        shema = secret["schema"]
        db_url = f"{engine}://{username}:{password}@{host}:{port}/{shema}"
        logger.info("URL creado correctamente")
        return db_url
    except KeyError as url_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError(raise_msg) from url_exc
