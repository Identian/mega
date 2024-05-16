"""
=============================================================

Nombre: precia_db.py
Tipo: Modulo

Autor:
    - Ruben Antonio Parra Medrano
Tecnología - Precia

Ultima modificación: 21/09/2022

Reune las funcionalidades para conectarse a una base de datos
usando SQLalchemy

=============================================================
"""

import logging

import sqlalchemy as sa

from precia_utils.precia_exceptions import PlataformError
from precia_utils.precia_logger import create_log_msg

logger = logging.getLogger()


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
        schema = secret["schema"]
        db_url = f"mysql+pymysql://{username}:{password}@{host}:{port}/{schema}"
        logger.info("URL creado correctamente")
        return db_url
    except KeyError as url_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError(raise_msg) from url_exc


def connect_db_by_secret(secret: dict, timeout: int = 2) -> sa.engine.Engine:
    """
    Se conecta a una base de datos usando un secreto almacenado en Secret
    Manager AWS. En el secreto se debe especificar el engine+driver bajo
    la llave 'engine', y el esquema bajo la llave 'schema'.

    Args:
        secret_name (str): Nombre del secreto de la base de datos en Secret
        Manager AWS
        secret_region (str): Region donde se ubica el secreto de la base de
        datos en Secret Manager AWS
        timeout (int, optional): Tiempo maximo de espera de conexion a la
        base de datos. Defaults to 2.

    Raises:
        PlataformError: El secreto no tiene las llaves esperadas
        PlataformError: El RDS nego la conexion

    Returns:
        sa.engine.Engine: Engine MySQL que hace de interfaz de conexion
        con la base de datos.
    """
    error_msg = "No se pudo conectar a la base de datos"
    raise_msg = "Fallo el intento de conexion a la base de datos"
    try:
        logger.info("Conectandose a la base de datos ...")
        logger.info("Esquema destino: %s", secret["schema"])
        db_url = create_db_url(secret)
        sql_engine = sa.create_engine(db_url, connect_args={"connect_timeout": timeout})
        db_connection = sql_engine.connect()
        logger.info("Conexion exitosa.")
        return db_connection
    except sa.exc.SQLAlchemyError as sql_exc:
        logger.error(create_log_msg(error_msg))
        raise PlataformError(raise_msg) from sql_exc
    except PlataformError:
        logger.error(create_log_msg(error_msg))
        raise
    except (Exception,):
        logger.error(create_log_msg(error_msg))
        raise
