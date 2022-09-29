import os
import csv
import sqlite3

import json
import requests
import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Crear el motor (engine) de la base de datos
engine = sqlalchemy.create_engine("sqlite:///meli.db")
base = declarative_base()

class Articulo(base):
    __tablename__ = "articulo"

    id = Column(String, primary_key=True)
    site_id = Column(String)
    title = Column(String)
    price = Column(Integer)
    currency_id = Column(String)
    initial_quantity = Column(Integer)
    available_quantity = Column(Integer)
    sold_quantity = Column(Integer)
    
    def __repr__(self):
        return f"Articulo: {self.name}"


def create_schema():
    base.metadata.drop_all(engine)

    # Crear las tablas
    base.metadata.create_all(engine)


def fill():
    print('Completemos la DB!')
    name_articulo = ''
    url = 'https://api.mercadolibre.com/items?ids='
    url1 = ''
    Session = sessionmaker(bind=engine)
    session = Session()

    with open('meli_technical_challenge_data.csv') as csvfile:
        data = list(csv.DictReader(csvfile))
    #En las líneas anteriores abro el archivo que se usará para recuperar la información de los artículos
    
    for k in data:
        name_articulo = k.get('site') + k.get('id')
        url1 = url + name_articulo
        response = requests.get(url1)
        json_data = response.json()
        for articulo in json_data:
            # En el bloque IF se realiza un control de los parámetros necesarios para guardar en la DB
            # si el dato no existe, la variable toma el valor None, para no romper el programa.
            if articulo['body'].get('id') != None:
                id = articulo['body']['id']
            else:
                id = None
            if articulo['body'].get('site_id') != None:
                site_id = articulo['body']['site_id']
            else:
                site_id = None
            if articulo['body'].get('title') != None:
                title = articulo['body']['title']
            else:
                title = None
            if articulo['body'].get('price') != None:
                price = articulo['body']['price']
            else:
                price = None
            if articulo['body'].get('currency_id') != None:
                currency_id = articulo['body']['currency_id']
            else:
                currency_id = None
            if articulo['body'].get('initial_quantity') != None:
                initial_quantity = articulo['body']['initial_quantity']
            else:
                initial_quantity = None
            if articulo['body'].get('available_quantity') != None:
                available_quantity = articulo['body']['available_quantity']
            else:
                available_quantity = None
            if articulo['body'].get('sold_quantity') != None:
                sold_quantity = articulo['body']['sold_quantity']
            else:
                sold_quantity = None
            articulo = Articulo(id = id, site_id = site_id, title = title, price = price, currency_id = currency_id, initial_quantity = initial_quantity, available_quantity = available_quantity, sold_quantity = sold_quantity)
            session.add(articulo)
            session.commit()



def fetch(id):
    Session = sessionmaker(bind=engine)
    session = Session()

    query = session.query(Articulo).filter(Articulo.id == id)
    if query != None:
        print(query)
    else:
        print('El artículo ingresado no existe en la DB')


if __name__ == "__main__":
  # Crear DB
  create_schema()

  # Completar la DB con el CSV
  fill()

  # Leer filas
  fetch('MLA845041373')
  fetch('MLA717159516')
