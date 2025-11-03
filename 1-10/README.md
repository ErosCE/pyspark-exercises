# Notas de PySpark: Ejercicios 1-10

Este documento resume las funciones y conceptos clave de PySpark utilizados en los primeros 10 ejercicios de este repositorio. Sirve como un apunte rápido para referencia futura.

## Manipulación de Texto y Arrays

Funciones usadas para limpiar y transformar datos de tipo `string` o `array`.

* `F.split(columna, delimitador_regex)`
    * **Qué hace:** Divide una columna de tipo `string` en un `array` de strings, usando una expresión regular (regex) como delimitador.
    * **Usado en (Ej 1):** Para separar cada línea (`"value"`) en un array de palabras, usando el espacio (`"\\s+"`) como separador.
    * **Ejemplo de sintaxis:**
    ```python
    df.select(F.split(F.col("frase"), " ").alias("palabras"))
    ```

* `F.explode(columna_array)`
    * **Qué hace:** Transforma una columna que contiene un `array` en múltiples filas. Cada elemento del `array` se convierte en una fila separada.
    * **Usado en (Ej 1):** Para "desempaquetar" el `array` de palabras (generado por `split`) y que cada palabra ocupe su propia fila.
    * **Ejemplo de sintaxis:**
    ```python
    df.select(F.explode(F.col("mi_array")).alias("elemento_individual"))
    ```

* `F.regexp_replace(columna, patron, reemplazo)`
    * **Qué hace:** Limpia una columna de `string` reemplazando todas las subcadenas que coincidan con la expresión regular (`patron`) por un nuevo `string` (`reemplazo`).
    * **Usado en (Ej 1):** Para eliminar todos los caracteres que no son letras o números (como puntuación), reemplazándolos por un `string` vacío (`""`).
    * **Ejemplo de sintaxis:**
    ```python
    # Elimina todos los números del string
    df.select(F.regexp_replace(F.col("texto"), "[0-9]", "").alias("texto_sin_numeros"))
    ```

## Creación y Modificación de Columnas

* `df.withColumn(nuevo_nombre, expresion_col)`
    * **Qué hace:** Es la forma estándar de añadir una nueva columna a un DataFrame o reemplazar una existente.
    * **Usado en (Ej 6, 10):** Para crear tanto columnas con valores constantes como columnas derivadas de cálculos entre otras columnas.
    * **Ejemplo de sintaxis:**
    ```python
    df_nuevo = df.withColumn("salario_doble", F.col("salario") * 2)
    ```

* `F.lit(valor)`
    * **Qué hace:** Crea una columna con un valor literal (constante). Se usa dentro de `withColumn` cuando quieres que todas las filas tengan el mismo valor.
    * **Usado en (Ej 6):** Para añadir una columna donde cada fila contenía el mismo `string`.
    * **Ejemplo de sintaxis:**
    ```python
    df_nuevo = df.withColumn("pais", F.lit("PER"))
    ```

## Manejo de Nulos y Duplicados

* `df.dropna(subset=[...])`
    * **Qué hace:** Elimina filas que contienen valores nulos. Si se especifica el parámetro `subset`, solo revisará esas columnas específicas en busca de nulos.
    * **Usado en (Ej 5):** Para descartar filas donde la columna `'Age'` era nula.
    * **Ejemplo de sintaxis:**
    ```python
    # Elimina filas si 'email' o 'telefono' son nulos
    df_limpio = df.dropna(subset=["email", "telefono"])
    ```

* `df.dropDuplicates(subset=[...])`
    * **Qué hace:** Elimina filas duplicadas. Si se especifica un `subset`, considera "duplicadas" solo a las filas que tienen valores idénticos en ese conjunto de columnas.
    * **Usado en (Ej 3):** Como un método simple para eliminar duplicados basándose en las columnas `'Name'`, `'Department'` y `'Joining'`.
    * **Ejemplo de sintaxis:**
    ```python
    # Elimina duplicados basados en el 'id_usuario'
    df_unico = df.dropDuplicates(subset=["id_usuario"])
    ```

* `F.col(columna).isNull()` / `F.col(columna).isNotNull()`
    * **Qué hace:** Son las funciones correctas para filtrar filas donde un valor es nulo o no nulo.
    * **Usado en (Ej 9):** Para verificar qué clientes (`isNotNull`) tenían pedidos y cuáles no (`isNull`).
    * **Ejemplo de sintaxis:**
    ```python
    df_sin_pedidos = df.filter(F.col("id_pedido").isNull())
    ```

## Agregaciones

* `df.agg(agregacion_1, agregacion_2, ...)`
    * **Qué hace:** Permite ejecutar múltiples funciones de agregación (como `avg`, `sum`, `count`, `min`, `max`) al mismo tiempo, generalmente después de un `groupBy`.
    * **Usado en (Ej 2):** Para calcular el `avg('Salary')` y el `count('*')` por departamento en una sola operación.
    * **Ejemplo de sintaxis:**
    ```python
    df_resumen = df.groupBy("departamento").agg(
        F.avg("salario").alias("salario_promedio"),
        F.count("*").alias("num_empleados")
    )
    ```

## Funciones de Ventana (Window Functions)

Las funciones de ventana realizan cálculos sobre un "marco" o "ventana" de filas relacionadas con la fila actual. A diferencia de `groupBy`, no colapsan las filas.

* `Window.partitionBy(col1, ...)`
    * **Qué hace:** Define la "partición" o grupo de filas sobre el cual se aplicará la función. Es conceptualmente similar a un `groupBy` (ej. "agrupar por `Ticker`"), pero mantiene todas las filas.

* `Window.orderBy(col1, ...)`
    * **Qué hace:** Define el orden de las filas *dentro* de cada partición. Es crucial para funciones que dependen del orden, como `row_number()` o sumas acumuladas.

* `F.row_number().over(window_spec)`
    * **Qué hace:** Asigna un número secuencial único (1, 2, 3...) a cada fila dentro de su partición, siguiendo el orden definido en el `orderBy`.
    * **Usado en (Ej 3):** Para numerar los registros duplicados de un empleado y luego filtrar (quedarnos solo con `row_num == 1`) para la deduplicación.
    * **Ejemplo de sintaxis:**
    ```python
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy("departamento").orderBy(F.desc("salario"))
    
    df_ranking = df.withColumn("ranking", F.row_number().over(window_spec))
    ```

* `F.sum(col).over(window_spec)`
    * **Qué hace:** Calcula una agregación (en este caso, `sum`) sobre el marco de la ventana.
    * **Usado en (Ej 10):** Para calcular la suma acumulada del precio (`Open`) de cada `Ticker`.

* `.rowsBetween(Window.unboundedPreceding, Window.currentRow)`
    * **Qué hace:** Define explícitamente el "marco" de la ventana. `Window.unboundedPreceding` significa "desde la primera fila de la partición" y `Window.currentRow` (o `0`) significa "hasta la fila actual".
    * **Usado en (Ej 10):** Para definir el marco del `sum()` como un "total acumulado" (suma todo desde el inicio hasta la fila actual).
    * **Ejemplo de sintaxis:**
    ```python
    from pyspark.sql.window import Window
    
    # Define la ventana: para cada ticker, ordenado por fecha, 
    # mira desde la primera fila (unboundedPreceding) hasta la actual (0).
    window_spec = Window.partitionBy("ticker").orderBy("fecha") \
                      .rowsBetween(Window.unboundedPreceding, 0) # 0 es Window.currentRow
    
    df_acumulado = df.withColumn("total_acumulado", F.sum("valor").over(window_spec))
    ```