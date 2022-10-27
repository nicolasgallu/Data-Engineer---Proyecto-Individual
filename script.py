
import pandas as pd
import numpy as np
import openpyxl 
import fastparquet
import sqlalchemy



productos = pd.read_parquet("Datasets relevamiento precios\producto.parquet",engine="auto") 
sucursales =pd.read_csv("Datasets relevamiento precios\sucursal.csv") 

file = pd.ExcelFile("Datasets relevamiento precios\precios_semanas_20200419_20200426.xlsx")
file.sheet_names 

precios_1 = pd.read_csv("Datasets relevamiento precios\precios_semana_20200413.csv",encoding="utf-16")
precios_2 = pd.read_json("Datasets relevamiento precios\precios_semana_20200503.json")
precios_3 = pd.read_csv("Datasets relevamiento precios\precios_semana_20200518.txt",sep="|")
precios_4 = pd.read_excel("Datasets relevamiento precios\precios_semanas_20200419_20200426.xlsx",sheet_name="precios_20200426_20200426")
precios_5 = pd.read_excel("Datasets relevamiento precios\precios_semanas_20200419_20200426.xlsx",sheet_name="precios_20200419_20200419")



def correccion_producto (tabla):
    tabla["producto_id"] = tabla["producto_id"].astype(str)
    tabla["producto_id"] = tabla["producto_id"].apply(lambda x: x.replace(".0","") if ".0" in x else x)
    tabla["producto_id"] = tabla["producto_id"].apply(lambda x: str(x).zfill(13) if len(str(x)) < 13 and str(x) !="nan" else x )
    tabla["producto_id"] = tabla["producto_id"].apply(lambda x: "sin registro" if x == "" or  x =="nan" else x )

def correccion_sucursal (tabla):
    tabla["sucursal_id"] = tabla["sucursal_id"].astype(str)
    tabla["sucursal_id"] = tabla["sucursal_id"].apply(lambda x: str(x).replace("00:00:00","") if ("00:00:00") in x else x )
    tabla["sucursal_id"] = tabla["sucursal_id"].apply(lambda x: "sin registro" if x == "" or  x == "nan" else x )

def limpieza_precios(tabla):
    tabla.precio = tabla.precio.apply(lambda x : 0 if str(x) == "nan" or x == "" else x)
    
def faltantes (tabla):
    sin_precio = len(tabla.precio[tabla.precio == 0]) 
    sin_sucursal_id = len(tabla.sucursal_id[tabla.sucursal_id == "sin registro"]) 
    sin_producto_id = len(tabla.producto_id[tabla.producto_id == "sin registro"]) 
    print (
           f"precios faltantes: {sin_precio} = {round(sin_precio*100/len(tabla),4)}%\n"
           f"sucursal_id faltantes: {sin_sucursal_id} = {round(sin_sucursal_id*100/len(tabla),4)}%\n" 
           f"producto_id faltantes: {sin_producto_id} = {round(sin_producto_id*100/len(tabla),4)}%")

def transformacion (*args):
     global precios_historico
     for tabla in [*args]:
          correccion_producto(tabla)
          correccion_sucursal(tabla)
          limpieza_precios(tabla)
          faltantes(tabla)
          print("-" * 40)
     
     precios_historico = pd.concat([*args],ignore_index=True)

def faltantes_sin_id (tabla):
    global Tabla_Aux
    Tabla_Aux = tabla[(tabla.producto_id== "sin registro") & (tabla.precio== 0)]
    tabla.drop(Tabla_Aux.index,inplace=True)



transformacion(precios_1,precios_2,precios_3,precios_4,precios_5) 

faltantes_sin_id(precios_historico) 




conexion= "mysql://root:drosblock@127.0.0.1:3306/nicolas"
engine = sqlalchemy.create_engine(conexion)


precios_historico.to_sql (name="ventas",con=conexion, if_exists="replace")
print("se cargaron los precios")
productos.to_sql (name="productos",con=conexion, if_exists="replace")
print("se cargaron los productos")
sucursales.to_sql (name="sucursales",con=conexion, if_exists="replace")
print("se cargaron las sucursales")

