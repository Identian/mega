"""
=============================================================

Nombre: precia_sftp.py
Tipo: Modulo

Autor:
    - Ruben Antonio Parra Medrano
Tecnología - Precia

Ultima modificación: 21/09/2022

Reune las funcionalidades para conectarse a un SFTP con la
libreria paramiko

=============================================================
"""

import logging

import paramiko

from precia_utils.precia_exceptions import PlataformError
from precia_utils.precia_logger import create_log_msg

logger = logging.getLogger()


def connect_to_sftp(secret: dict, timeout: int = 2) -> paramiko.SSHClient:
    """
    Se conecta a un SFTP usando las credenciales contenidas en 'secret'
    y la libreria 'paramiko'. Las credenciales 'secret' se almacenan en Secrets Manager

    :returns: Objeto paramiko que representa la sesion SFTP abierta en el servidor SSHClient
    """
    raise_msg = "No fue posible conectarse al SFTP destino"
    try:
        logger.info("Conectandose al SFTP ...")
        logger.info("Validando secreto del SFTP ...")
        sftp_host = secret["host"]
        sftp_port = secret["port"]
        sftp_username = secret["username"]
        sftp_password = secret["password"]
        logger.info("Secreto del SFTP tienen el formato esperado.")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        logger.info("Intentando conectarse a : %s, %s ...", sftp_host, sftp_port)
        client.connect(
            sftp_host,
            port=sftp_port,
            username=sftp_username,
            password=sftp_password,
            timeout=timeout,
        )
        logger.info("Conexion al SFTP exitosa.")
        return client
    except KeyError as key_exc:
        error_msg = "El secreto del SFTP no tiene el formato esperado"
        logger.error(create_log_msg(error_msg))
        raise PlataformError(raise_msg) from key_exc
    except (Exception,) as unknown_exc:
        error_msg = "Fallo el intento de conexion al SFTP"
        logger.error(create_log_msg(error_msg))
        raise PlataformError(raise_msg) from unknown_exc
