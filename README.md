# TODO

core.autocrlf=input
user.name=Diego Castro
user.email=diegocastroviadero19@gmail.com


- publicar en github

- añadir filtro por columnas a las tablas
- registrarse en Datadis y probar que funciona
  - es un servicio transversal a todas las distribuidoras eléctricas que permite recuperar el consumo eléctrico (de esta forma no habría que descargarlo manualmente)
- tener un fichero de configuración
  - con propiedad para deshabilitar la recuperación de los precios
  - con propiedad para deshabilitar la recuperación de la generación eléctrica
  - con propiedad para indicar las coordenadas para recuperar la generación eléctrica
  - con propiedad para deshabilitar la recuperación del histórico del clima
- https://joint-research-centre.ec.europa.eu/pvgis-photovoltaic-geographical-information-system/pvgis-tools/monthly-radiation_en
  - para recuperar la radiación solar en un sitio
- https://worldweatheronline.com/developer/api/docs/historical-weather-api.aspx
  - para recuperar el histórico del clima en un sitio (ver si funciona y están los datos que interesan)

# DOC

Descargar [Spark](https://spark.apache.org/downloads.html) y descomprimirlo en una carpeta

Definir las siguientes variables de entorno en el fichero `.env`:

```
JAVA_HOME=C:\work\dev\langs\java\zulu11.48.21-ca-jdk11.0.11
PYTHONHOME=C:\work\dev\langs\python\python-3.9.12
SPARK_HOME=C:\work\dev\langs\spark\spark-3.2.1-bin-hadoop3.2
HADOOP_HOME=C:\work\dev\langs\spark\spark-3.2.1-bin-hadoop3.2
PYSPARK_PYTHON=C:\work\dev\langs\python\python-3.9.12\python.exe
ESIOS_TOKEN=<TOKEN>
PATH=%PATH%;%HADOOP_HOME%\bin
```

Descargar [Winutils](https://github.com/kontext-tech/winutils) (winutils.exe y hadoop.dll, en este caso, version 3.2) y meterlo en `HADOOP_HOME`

En `requirements.txt` meter:

```requirements.txt
pyspark==3.2.1
```

Para crear un `SparkContext`:

```python
from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)
```


# Links

[API E-SIOS](https://api.esios.ree.es)
[Plotly](https://plotly.com/javascript/plotlyjs-function-reference/#plotlynewplot)
[DataTables](https://datatables.net/manual/index)
[Fomantic UI](https://fomantic-ui.com)
[Jinja2](https://jinja.palletsprojects.com/en/3.1.x/templates/)
[Dataframe JS](https://gmousse.gitbooks.io/dataframe-js/content/doc/api/dataframe.html)


# Issues

Uso la versión 3.9 de Python, porque he tenido un problema con python 3.10, PySpark, Pandas, el PyCharm (2021.3.2) en modo debug.

Al crear un DataFrame de PySpark, a partir de otro DataFrame de Pandas, que ha sido creado leyendo de un fichero json, PySpark acaba llamando al método `to_records` del DataFrame de Pandas. Esto provoca un error de `NoneType is not callable`. Parece ser (por lo que he leído [aquí](https://www.reddit.com/r/learnpython/comments/shztvw/pandas_dfto_records_error_typeerror_nonetype/)) que esto se debe a un problema con Python 3.10 y al ejecutar en debug en PyCharm. Al pasar a Python 3.9.12, se soluciona el problema.