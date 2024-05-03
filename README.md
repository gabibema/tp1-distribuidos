# tp1-distribuidos

## Alumnos

| Nombre                                                        | Padrón |
| ------------------------------------------------------------- | ------ |
| [Juan Manuel Diaz](https://github.com/Diaz-Manuel)            | 108183 |
| [Gabriel Bedoya](https://github.com/gabibema)                 | 107602 |


## Ejecución

Para la ejecución del sistema, se provee un Makefile que soporta los siguientes comandos:
* make docker-compose-up: permite levantar el sistema con el docker-compose-dev.yaml provisto en el repositorio
* make docker-compose-client: Permite ejecutar un cliente y conectarse al sistema mediante el gateway. El archivo yaml que utiliza es docker-compose-client.yaml
* make docker-compose-down-client: Finaliza la ejecución del cliente y elimina los containers creados.
* make docker-compose-down-all: Finaliza la ejecución tanto del sistema como del cliente y todos los containers asociados.

Para poder ejecutar el sistema correctamente, el cliente debe poseer el archivo de books y reviews provistos por amazon, los cuales se pueden obtener en: [Amazon Books Reviews](https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews)
Se mencionó anteriormente los archivos yaml tanto del cliente como del sistema, ambos archivos permiten configurar el entorno de ejecución, para el cliente lo más importante es dónde se encuentran los archivos a enviarse al gateway (los paths correspondientes), y el directorio donde se guardarán los outputs en respuesta del sistema a través del gateway. Además se provee un parámetro de configuración que es la cantidad de mensajes a enviar durante la transferencia de los archivos.
En el caso del sistema, el archivo yaml es mucho más extenso y se permite configurar aspectos más ligados a la arquitectura y por ejemplo queues/exchanges internos que maneja el sistema.
