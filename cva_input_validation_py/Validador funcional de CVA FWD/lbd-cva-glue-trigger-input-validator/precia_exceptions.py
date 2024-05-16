"""
=============================================================

Nombre: precia_exceptions.py
Tipo: Modulo

Autor:
    - Ruben Antonio Parra Medrano
Tecnología - Precia

Ultima modificación: 21/09/2022

Reune las funcionalidades para el generar logs con formato en Precia,
Los formatos son los presentados en las variables ERROR_MSG_LOG_FORMAT
y PRECIA_LOG_FORMAT

=============================================================
"""


class BaseError(Exception):
    """Exception personalizada para la capa precia_utils"""


class UserError(BaseError):
    """
    Clase heredada de BaseError que permite etiquetar los errores causados
    por la informacion suministrada en el event
    """

    def __init__(
        self,
        error_message="El event no tienen la estructura y/o valores esperados",
    ):
        self.error_message = error_message
        super().__init__(self.error_message)

    def __str__(self):
        return str(self.error_message)


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


class DependencyError(BaseError):
    """
    Clase heredada de BaseError que permite etiquetar los errores asociados a dependencias
    datos creados por otros recursos necesarios para dar continuidad del proceso
    """

    def __init__(
        self,
        error_message="Existe una dependencia de datos de otros proceso no satisfecha",
    ):
        self.error_message = error_message
        super().__init__(self.error_message)

    def __str__(self):
        return str(self.error_message)
