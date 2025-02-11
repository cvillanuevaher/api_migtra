from fastapi import FastAPI, Query
from databricks import sql
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import os
from datetime import datetime, date
from decimal import Decimal

# Cargar variables de entorno desde el archivo .env
load_dotenv()

app = FastAPI()

# Cargar variables de entorno desde Azure o .env
server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")

# Validar que las variables no sean None
if not server_hostname or not http_path or not access_token:
    raise ValueError("Faltan variables de entorno: Verifica DATABRICKS_SERVER_HOSTNAME, HTTP_PATH y ACCESS_TOKEN")

@app.get("/")
def read_root():
    return {"message": "¡Bienvenido a la API de Databricks! Usa /api/stock para obtener datos."}

@app.get("/api/stock")
def get_stock(
    fecha: str = Query(..., description="Fecha del movimiento (YYYY-MM-DD)"),
    codigos_centros: list[str] = Query(..., description="Lista de códigos de centros"),
    codigos_canchas: list[str] = Query(..., description="Lista de códigos de canchas")
):
    try:
        # Convertir las listas a cadenas separadas por comas
        codigos_centros_str = ", ".join([f"'{codigo}'" for codigo in codigos_centros])
        codigos_canchas_str = ", ".join([f"'{codigo}'" for codigo in codigos_canchas])

        # Consulta SQL parametrizada con la fecha y códigos proporcionados
        query = f"""
        SELECT 
            B.FECHA_MOVIMIENTO AS FECHA,
            C.NOMBRE AS centro,
            C.CODIGO AS cod_cancha,
            S.ID_SECTOR AS cod_sector,
            S.DESCRIPCION AS sector,
            PC.SIGLA AS producto,
            L.ESTADO_CALIDAD AS calidad,
            CONCAT(ENV.DESCRIPCION_CORTA, 
                   CASE WHEN ENV.COD_ENVASE = '16' THEN '' 
                   ELSE CONCAT(ENV.CAPACIDAD, ' ', ENV.UNIDAD_ENV) END) AS formato,
            SUM(CASE WHEN L.ALM_CODIGO = 4 THEN B.STOCK_FINAL / 1000 ELSE B.STOCK_FINAL END) AS stock
        FROM 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_tb_balance_dia_productos B
        JOIN 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_tb_lotes_inventario L ON B.ID_LOTE = L.ID_LOTE
        JOIN 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_tb_canchas C ON L.ALM_CODIGO = C.CODIGO
        JOIN 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_tb_ubicaciones U ON L.ALM_CODIGO = U.ALM_CODIGO AND L.ID_UBICACION = U.ID_UBICACION
        JOIN 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_tb_sectores S ON L.ALM_CODIGO = S.UBI_ALM_CODIGO AND L.ID_UBICACION = S.UBI_ID_UBICACION AND L.ID_SECTOR = S.ID_SECTOR
        JOIN 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_va_productos_canchas PC ON PC.COD_PRODUCTO = CAST(L.COD_PRODUCTO AS STRING)
        JOIN 
            `prd_medallion`.ds_bdanntp2_usr_dblink.sdp_tb_envases ENV ON ENV.COD_ENVASE = L.COD_ENVASE
        LEFT JOIN 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_tb_zonas_despacho ZD ON L.ALM_CODIGO = ZD.ALM_CODIGO AND L.ID_UBICACION = ZD.ID_UBICACION
        LEFT JOIN 
            `prd_medallion`.ds_bdanntp2_cancha_adm.sdp_no_sector_stock NSS ON CAST(L.ALM_CODIGO AS STRING) = NSS.COD_CANCHA AND L.ID_UBICACION = NSS.COD_UBI AND L.ID_SECTOR = NSS.COD_SEC
        WHERE 
            B.FECHA_MOVIMIENTO = DATE('{fecha}')  -- Usar la fecha proporcionada aquí
            AND L.ALM_CODIGO IN ({codigos_centros_str})  -- Filtrar por códigos de centros
            AND L.ID_UBICACION IN ({codigos_canchas_str})  -- Filtrar por códigos de canchas
            AND L.COD_PRODUCTO NOT IN (2220, 2308)
            AND UPPER(S.DESCRIPCION) NOT LIKE '%VIRTUAL%'
            AND ZD.ALM_CODIGO IS NULL
            AND NSS.COD_CANCHA IS NULL
        GROUP BY 
            B.FECHA_MOVIMIENTO,
            C.NOMBRE,
            C.CODIGO,
            S.ID_SECTOR,
            S.DESCRIPCION,
            PC.SIGLA,
            L.ESTADO_CALIDAD,
            CONCAT(ENV.DESCRIPCION_CORTA, 
                   CASE WHEN ENV.COD_ENVASE = '16' THEN '' 
                   ELSE CONCAT(ENV.CAPACIDAD, ' ', ENV.UNIDAD_ENV) END)
        HAVING 
            SUM(CASE WHEN L.ALM_CODIGO = 4 THEN B.STOCK_FINAL / 1000 ELSE B.STOCK_FINAL END) >= 0
        """

        # Ejecutar la consulta SQL en Databricks
        with sql.connect(server_hostname=server_hostname,
                         http_path=http_path,
                         access_token=access_token) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()

                # Obtener nombres de columnas y convertir resultados a una lista de diccionarios
                columns = [column[0] for column in cursor.description]
                objetos = []
                for row in result:
                    row_dict = dict(zip(columns, row))
                    # Convertir datetime o date a string ISO 8601 y Decimal a float
                    for key, value in row_dict.items():
                        if isinstance(value, (datetime, date)):
                            row_dict[key] = value.isoformat()  # Convertir a formato ISO 8601
                        elif isinstance(value, Decimal):
                            row_dict[key] = float(value)  # Convertir Decimal a float
                    objetos.append(row_dict)

        return JSONResponse(content=objetos)  # Devolver resultados en formato JSON
        
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/consume")
def get_consume(
    fecha: str = Query(..., description="Fecha del movimiento (DD-MM-YYYY)")
):
    try:
        # Definir la base de datos en una variable para parametrizarla, usando comillas invertidas
        catalog_ = "`prd_medallion`"
        schema_ = "ds_bdanntp2_cancha_adm"
        schema2_ = "ds_bdanntp2_usr_dblink"

        # Consulta SQL parametrizada para obtener información sobre lotes de inventario.
        query = f"""
        SELECT
            stli.NRO_INTERNO AS INTERNO,
            stli.CANTIDAD_REAL AS ACTUAL,
            stli.CANTIDAD_PRESUPUESTO AS ENTRADAS,
            ABS(stli.CANTIDAD_REAL - stli.CANTIDAD_PRESUPUESTO) AS SALIDAS,
            stu.DESCRIPCION AS CANCHA,
            sts.DESCRIPCION AS SECTOR,
            svpc.SIGLA AS PRODUCTO,
            date_format(stali.LIBERACION_LABORATORIO, 'yyyyMMddHHmm') AS OV,
            date_format(COALESCE(stli.FECHA_PRIMER_MOV, stli.FECHA_CREACION), 'dd-MM-yyyy') AS FechaEmisionLote,
            date_format(COALESCE(stli.FECHA_MODIFICACION, stli.FECHA_PRIMER_MOV, stli.FECHA_CREACION), 'dd-MM-yyyy') AS FechaUltimaModificacion
        FROM 
            {catalog_}.{schema_}.SDP_TB_LOTES_INVENTARIO stli
        INNER JOIN 
            {catalog_}.{schema_}.SDP_TB_GRUPOS_LOTES stgl ON stli.COD_GRUPO = stgl.COD_GRUPO
        INNER JOIN 
            {catalog_}.{schema_}.SDP_TB_ANEXO_LOTESINV stal ON stli.ID_LOTE = stal.ID_LOTE
        INNER JOIN 
            {catalog_}.{schema_}.SDP_TB_SECTORES sts ON stli.ALM_CODIGO = sts.UBI_ALM_CODIGO 
            AND stli.ID_UBICACION = sts.UBI_ID_UBICACION 
            AND stli.ID_SECTOR = sts.ID_SECTOR
        INNER JOIN 
            {catalog_}.{schema_}.SDP_TB_UBICACIONES stu ON stli.ALM_CODIGO = stu.ALM_CODIGO 
            AND stli.ID_UBICACION = stu.ID_UBICACION
        INNER JOIN 
            {catalog_}.{schema_}.SDP_VA_PRODUCTOS_CANCHAS svpc ON CAST(stli.COD_PRODUCTO AS STRING) = svpc.cod_producto
        INNER JOIN 
            {catalog_}.{schema2_}.SDP_TB_ENVASES ste ON stli.COD_ENVASE = ste.COD_ENVASE
        INNER JOIN 
            {catalog_}.{schema_}.SDP_TB_TIPO_CONTENEDORES sttc ON stli.COD_TIPO_CONTENEDOR = sttc.COD_TIPO
        LEFT JOIN 
            {catalog_}.{schema_}.SDP_TB_APROB_ESPECIALES stae ON stli.ID_LOTE = stae.NRO_LOTE_SQM
        INNER JOIN 
            {catalog_}.{schema_}.SDP_TB_PROPIETARIOS stp ON stli.RUT_PROPIETARIO = stp.RUT
        INNER JOIN 
            {catalog_}.{schema_}.CG_REF_CODES crc ON stli.ESTADO_CALIDAD = crc.RV_LOW_VALUE
        INNER JOIN 
            {catalog_}.{schema_}.SDP_TB_ANEXO_LOTESINV_II stali ON stli.ID_LOTE = stali.ID_LOTE
        INNER JOIN 
            {catalog_}.{schema_}.CG_REF_CODES crc2 ON crc2.RV_LOW_VALUE = stali.ESTADO_PLANTA
        INNER JOIN 
            {catalog_}.{schema_}.CG_REF_CODES crc3 ON crc3.RV_LOW_VALUE = stali.ESTADO_COMERCIAL
        WHERE 
            stli.ALM_CODIGO = 19 -- ulog
            AND crc2.RV_DOMAIN = 'SDP_TB_ANEXO_LOTESINV_II.ESTADO_PLANTA'
            AND crc3.RV_DOMAIN = 'SDP_TB_ANEXO_LOTESINV_II.ESTADO_COMERCIAL'
            AND TO_DATE(date_format(COALESCE(stli.FECHA_MODIFICACION, stli.FECHA_PRIMER_MOV, stli.FECHA_CREACION), 'dd-MM-yyyy'), 'dd-MM-yyyy') = '{fecha}' 
            AND crc.RV_DOMAIN = 'SDP_TB_LOTES_INVENTARIO.ESTADO_CALIDAD'
            AND NOT EXISTS (
                SELECT 1 
                FROM {catalog_}.{schema_}.SDP_TB_ZONAS_DESPACHO stzd 
                WHERE stzd.ALM_CODIGO = stli.ALM_CODIGO 
                AND stzd.ID_UBICACION = stli.ID_UBICACION
            )
            AND NOT EXISTS (
                SELECT 1 
                FROM {catalog_}.{schema_}.SDP_NO_SECTOR_STOCK snss 
                WHERE snss.cod_cancha = CAST(stli.ALM_CODIGO AS STRING)
                AND snss.COD_UBI = stli.ID_UBICACION 
                AND snss.COD_SEC = stli.ID_SECTOR
            )
        ORDER BY 
            NRO_INTERNO
        """

        # Ejecutar la consulta SQL en Databricks para obtener información sobre los consumos.
        with sql.connect(server_hostname=server_hostname,
                         http_path=http_path,
                         access_token=access_token) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                
                # Obtener nombres de columnas y convertir resultados a una lista de diccionarios.
                columns = [column[0] for column in cursor.description]
                objetos_consumo = []
                for row in result:
                    row_dict_consumo = dict(zip(columns, row))
                    # Convertir datetime o date a string ISO 8601 y Decimal a float.
                    for key, value in row_dict_consumo.items():
                        if isinstance(value, (datetime, date)):
                            row_dict_consumo[key] = value.isoformat()  # Convertir a formato ISO 8601.
                        elif isinstance(value, Decimal):
                            row_dict_consumo[key] = float(value)  # Convertir Decimal a float.
                    objetos_consumo.append(row_dict_consumo)

        return JSONResponse(content=objetos_consumo)  # Devolver resultados en formato JSON.

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/historico")
def get_consume(
    fecha_inicio: str = Query(..., description="Fecha inicio(DD-MM-YYYY)"),
    fecha_fin: str = Query(..., description="Fecha fin (DD-MM-YYYY)"),
    Id_planta: str = Query(..., description="ID planta")
):
    try:
        # Definir la base de datos en una variable para parametrizarla, usando comillas invertidas
        catalog_ = "`prd_medallion`"
        schema_ = "ds_bdanntp2_cancha_adm"
        schema2_ = "ds_bdanntp2_usr_dblink"

        # Consulta SQL parametrizada para obtener información sobre lotes de inventario.
        query = f"""
        SELECT 
            DATE_FORMAT(stlp.FECHA_PRODUCCION, 'dd-MM-yyyy') AS fecha,
            stp.DESCRIPCION AS planta,
            stlp.ID_LOTE AS nro,
            TRIM(SUBSTR(crc2.RV_MEANING, 1, 6)) AS estado,
            stli.ID_LOTE AS nro_sqm,
            CASE 
                WHEN stli.ALM_CODIGO = 11 THEN COALESCE(stli.NRO_INTERNO, CONCAT('Maxi ', stal.FIN_CONTENEDOR))
                ELSE stli.NRO_INTERNO 
            END AS nro_int,
            stlp.TURNO AS turno,
            svpc1.SIGLA AS agregado,
            stlp.CANTIDAD AS cantidad,
            INITCAP(ste.DESCRIPCION_CORTA) AS envase,
            svpc2.SIGLA AS produccion
        FROM 
            {catalog_}.{schema_}.SDP_TB_LOTES_PRODUCCION stlp
        INNER JOIN 
            {catalog_}.{schema_}.CG_REF_CODES crc1 ON stlp.TIPO = crc1.RV_LOW_VALUE
        INNER JOIN 
            {catalog_}.{schema_}.CG_REF_CODES crc2 ON stlp.ESTADO = crc2.RV_LOW_VALUE
        INNER JOIN 
            {catalog_}.{schema_}.SDP_VA_PRODUCTOS_CANCHAS svpc1 ON CAST(stlp.COD_PRODUCTO AS STRING) = svpc1.COD_PRODUCTO
        LEFT JOIN 
            {catalog_}.{schema_}.SDP_VA_PRODUCTOS_CANCHAS svpc2 ON CAST(stlp.PRODUCTO_DE_AGREGADO AS STRING) = svpc2.COD_PRODUCTO
        INNER JOIN 
            {catalog_}.{schema2_}.SDP_TB_ENVASES ste ON stlp.COD_ENVASE = ste.COD_ENVASE
        INNER JOIN 
            {catalog_}.{schema_}.CG_REF_CODES crc3 ON stlp.ESTADO_CALIDAD = crc3.RV_LOW_VALUE
        LEFT JOIN 
            {catalog_}.{schema_}.SDP_TB_LOTES_INVENTARIO stli ON stlp.ID_LOTE_INVENTARIO = stli.ID_LOTE
        LEFT JOIN 
            {catalog_}.{schema_}.SDP_TB_CANCHAS stc ON stli.ALM_CODIGO = stc.CODIGO
        LEFT JOIN 
            {catalog_}.{schema_}.SDP_TB_UBICACIONES stu ON stli.ALM_CODIGO = stu.ALM_CODIGO AND stli.ID_UBICACION = stu.ID_UBICACION
        LEFT JOIN 
            {catalog_}.{schema_}.SDP_TB_SECTORES sts ON stli.ALM_CODIGO = sts.UBI_ALM_CODIGO AND stli.ID_UBICACION = sts.UBI_ID_UBICACION AND stli.ID_SECTOR = sts.ID_SECTOR
        LEFT JOIN 
            {catalog_}.{schema_}.SDP_TB_ANEXO_LOTESINV stal ON stlp.ID_LOTE_INVENTARIO = stal.ID_LOTE
        INNER JOIN 
            {catalog_}.{schema_}.SDP_TB_PLANTAS stp ON stlp.ID_PLANTA = stp.ID_PLANTA
        WHERE 
            date_format(stlp.FECHA_PRODUCCION, 'dd-MM-yyyy') BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
            AND stlp.ID_PLANTA = '{Id_planta}'
            AND stlp.TIPO = 'I' 
            AND stlp.ESTADO <> 'A' 
            AND crc1.RV_DOMAIN = 'SDP_TB_LOTES_PRODUCCION.TIPO' 
            AND crc2.RV_DOMAIN = 'SDP_TB_LOTES_PRODUCCION.ESTADO' 
            AND crc3.RV_DOMAIN = 'SDP_TB_LOTES_PRODUCCION.ESTADO_CALIDAD'
        """

        # Ejecutar la consulta SQL en Databricks para obtener información sobre los consumos.
        with sql.connect(server_hostname=server_hostname,
                         http_path=http_path,
                         access_token=access_token) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                
                # Obtener nombres de columnas y convertir resultados a una lista de diccionarios.
                columns = [column[0] for column in cursor.description]
                objetos_consumo = []
                for row in result:
                    row_dict_consumo = dict(zip(columns, row))
                    # Convertir datetime o date a string ISO 8601 y Decimal a float.
                    for key, value in row_dict_consumo.items():
                        if isinstance(value, (datetime, date)):
                            row_dict_consumo[key] = value.isoformat()  # Convertir a formato ISO 8601.
                        elif isinstance(value, Decimal):
                            row_dict_consumo[key] = float(value)  # Convertir Decimal a float.
                    objetos_consumo.append(row_dict_consumo)

        return JSONResponse(content=objetos_consumo)  # Devolver resultados en formato JSON.

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)