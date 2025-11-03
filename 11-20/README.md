# Notas de PySpark: Ejercicios 11-20

Este documento resume las funciones y conceptos clave de PySpark utilizados en los ejercicios 11 al 20 de este repositorio.

---

## Sesión y Lectura/Escritura (I/O)

Conceptos básicos para iniciar la sesión de Spark y mover datos.

* `SparkSession.builder.appName(...).getOrCreate()`
    * **Qué hace:** Es el punto de entrada para cualquier aplicación de Spark. Configura la sesión.
    * `appName(nombre)`: Asigna un nombre a tu trabajo/aplicación, que aparecerá en la UI de Spark.
    * `getOrCreate()`: Obtiene la `SparkSession` activa o, si no existe, crea una nueva.
    * **Usado en (Ej 20):** Para inicializar la variable `spark`.
    * **Ejemplo de sintaxis:**
    ```python
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("MiApp").getOrCreate()
    ```

* `spark.read.format(path, ...)`
    * **Qué hace:** Es la interfaz principal para leer datos de diversas fuentes (CSV, JSON, Parquet, etc.).
    * **Usado en (Ej 19):** Para leer archivos `.csv()`, `.json()` y `.parquet()`.
    * **Opciones Clave (para CSV):**
        * `header=True`: Indica que la primera fila del CSV es el encabezado.
        * `inferSchema=True`: Spark intentará adivinar automáticamente el tipo de dato de cada columna (costoso para archivos grandes, pero útil para ejercicios).
    * **Ejemplo de sintaxis:**
    ```python
    df_csv = spark.read.csv("ruta/al/archivo.csv", header=True, inferSchema=True)
    df_json = spark.read.json("ruta/al/archivo.json")
    df_parquet = spark.read.parquet("ruta/al/archivo.parquet")
    ```

* `df.write.mode(...).option(...).format(path)`
    * **Qué hace:** Es la interfaz principal para escribir un DataFrame a un sistema de archivos.
    * **Usado en (Ej 14):** Para guardar un DataFrame como un archivo CSV.
    * **Opciones Clave:**
        * `mode("overwrite")`: Sobrescribe el archivo si ya existe. Otras opciones son `"append"`, `"ignore"`, `"errorifexists"` (default).
        * `option("header", True)`: Escribe la fila de encabezado (nombres de columnas) en el archivo CSV.
    * **Ejemplo de sintaxis:**
    ```python
    # Escribe un CSV con encabezado, sobrescribiendo si existe
    df.write.mode("overwrite").option("header", True).csv("ruta/de/salida")
    ```

---

## Transformaciones de DataFrame

Operaciones que modifican la estructura o el orden del DataFrame.

* `df.withColumnRenamed(old_name, new_name)`
    * **Qué hace:** Renombra una columna existente. Es una transformación inmutable (devuelve un nuevo DataFrame). Se puede encadenar varias veces.
    * **Usado en (Ej 11):** Para cambiar nombres de columnas como "Open" a "Precio Inicial".
    * **Ejemplo de sintaxis:**
    ```python
    df_renombrado = df.withColumnRenamed("nombre_antiguo", "nombre_nuevo")
    ```

* `df.orderBy(col1, col2, ...)`
    * **Qué hace:** Ordena el DataFrame globalmente basado en una o más columnas. Es una transformación costosa (requiere un "shuffle").
    * **Usado en (Ej 13):** Para ordenar empleados por departamento y salario.
    * **Control de Dirección:** Se usa `.asc()` (ascendente, default) o `.desc()` (descendente) en el objeto Columna.
    * **Ejemplo de sintaxis:**
    ```python
    # Ordena por 'edad' ascendente y 'salario' descendente
    df.orderBy(F.col("edad").asc(), F.col("salario").desc())
    ```

---

## Optimización

Funciones para gestionar el paralelismo y rendimiento de Spark.

* `df.coalesce(n)`
    * **Qué hace:** Reduce el número de particiones del DataFrame a `n`.
    * **Por qué se usa:** Se usa principalmente justo antes de escribir (`.write`) para consolidar los datos en menos archivos. `df.coalesce(1)` es una forma común de forzar la escritura en un *único archivo* (útil para archivos de resultados pequeños).
    * **Diferencia con `repartition`:** `coalesce` es más eficiente para *reducir* particiones porque evita un "full shuffle" (mezcla completa de datos), simplemente combina particiones existentes.
    * **Usado en (Ej 14, 17):** Para reducir el número de particiones antes de escribir.
    * **Ejemplo de sintaxis:**
    ```python
    # Reduce las particiones a 5
    df_optimizado = df.coalesce(5)
    
    # Fuerza la salida a un solo archivo CSV
    df.coalesce(1).write.csv("mi_archivo_unico")
    ```

---

## Funciones de Columna

Funciones del módulo `pyspark.sql.functions` (importado como `F`) que operan sobre columnas.

* `F.concat(col1, col2, ...)`
    * **Qué hace:** Concatena (une) múltiples columnas de tipo `string`.
    * **Nota:** Si se desea concatenar con un string estático (un literal), se debe usar `F.lit()`.
    * **Usado en (Ej 12):** Para crear una columna "name-country" a partir de "Brand_Name" y "Country".
    * **Ejemplo de sintaxis:**
    ```python
    # Crea "Apellido, Nombre"
    df.withColumn("nombre_completo", F.concat(F.col("apellido"), F.lit(", "), F.col("nombre")))
    ```

---

## Funciones de Ventana (Window Functions)

Funciones que realizan cálculos sobre un grupo ("ventana") de filas.

* `F.rank().over(window_spec)`
    * **Qué hace:** Calcula el "ranking" de una fila dentro de su partición de ventana.
    * **Manejo de empates:** `rank()` asigna el mismo rango a valores idénticos y *deja un hueco* en la secuencia. (Ej: 1, 2, 2, 4).
    * **Alternativa:** `F.dense_rank()` hace lo mismo pero *no deja huecos* (Ej: 1, 2, 2, 3).
    * **Usado en (Ej 15):** Para clasificar a los empleados según el salario dentro de su departamento.
    * **Ejemplo de sintaxis:**
    ```python
    from pyspark.sql.window import Window
    
    # Especificación: Por cada 'depto', ordenar por 'salario' desc
    window_spec = Window.partitionBy("depto").orderBy(F.col("salario").desc())
    
    df.withColumn("ranking_salario", F.rank().over(window_spec))
    ```