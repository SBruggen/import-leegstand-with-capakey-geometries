import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, MetaData, Table, Column, func, insert, inspect
from sqlalchemy import Integer, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from geoalchemy2 import Geometry
from shapely import wkt, wkb
from shapely.geometry import MultiPolygon
import json
import os

### Database connection

# Load database connection parameters from config file
loc_config = os.path.join('..', '..', 'data', 'config.json')
with open(loc_config, 'r') as f:
    db_params = json.load(f)

# Database connection parameters
db_name = db_params['dbname']
db_user = db_params['user']
db_password = db_params['password']
db_host = db_params['host']
db_port = db_params['port']

# Create a SQLAlchemy engine
conn_string = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
engine = create_engine(conn_string)

# Create a configured "Session" class
Session = sessionmaker(bind=engine)

# Create a session
session = Session()

# Define a base class for your models
Base = declarative_base()

### Load excel file

# path
excel_file_path = "U:/data/registers/leegstand en verwaarlozing/leegstand20231121.xlsx"

# Read Excel data into a pandas DataFrame
try:
    excel_data = pd.read_excel(excel_file_path)
    print("Excel file found and read")
except Exception as e:
    print(f"Error: {e}")


### Query to retrieve spatial data from public.percelen
try:
    percelen_query = "SELECT capakey, geometry FROM public.percelen"
    percelen_gdf = gpd.read_postgis(percelen_query, engine, geom_col='geometry')
    print("\npercelen imported to gdf:")
    print(percelen_gdf.head())
except:
    print(f"Error: {e}")


# Query to retrieve spatial data from editeren.leegstand_leegstandregister_24001
try:
    leegstand_query = "SELECT capakey, geometry FROM editeren.leegstand_leegstandregister_24001"
    leegstand_gdf = gpd.read_postgis(leegstand_query, engine, geom_col='geometry')
    print("\nleegstand data  imported to gdf:")
    print(leegstand_gdf.head())
except:
    print(f"Error: {e}")


### Add geodata to new leegstand data by joining with percelen based on capakey

# Merge spatial data with Excel data based on capakey
merged_data = pd.merge(excel_data, percelen_gdf, left_on='CapaKey', right_on='capakey', how='left')
merged_data = pd.merge(merged_data, leegstand_gdf, on='capakey', how='left', suffixes=('_percelen', '_leegstand'))

# Check for missing geometry data
missing_geometry_mask = (merged_data['geometry_percelen'].isna()) & (merged_data['geometry_leegstand'].isna())

# Separate rows with missing geometry data into a separate DataFrame
missing_geometry = merged_data[missing_geometry_mask]

# Print a message for missing geometry data
print("\nChecking for capakeys not in database.")
for capakey in missing_geometry['CapaKey']:
    print(f"No spatial data found for capakey: {capakey}")

# Remove rows with missing geometry data from the merged DataFrame
merged_data = merged_data[~missing_geometry_mask]

# Debugging: Print the merged dataframe to check its structure
print("\nmerged_data:")
print(merged_data.head())

# Use the 'geometry_percelen' column if available, otherwise use 'geometry_leegstand'
merged_data['Geometry'] = merged_data['geometry_percelen'].combine_first(merged_data['geometry_leegstand'])

# Drop unnecessary columns
merged_data = merged_data.drop(['geometry_percelen', 'geometry_leegstand', 'capakey'], axis=1)

# Debugging: Print the final dataframe to check its structure
print("\nFinal merged_data:")
print(merged_data.head())


### convert to geodataframe

# Convert DataFrame to GeoDataFrame
gdf = gpd.GeoDataFrame(merged_data, geometry='Geometry', crs="EPSG:31370")

# Convert GeoDataFrame columns to lower case
gdf.columns = [col.lower() for col in gdf.columns]

print(gdf.columns)

### Define table model

class YourTable(Base):
    __tablename__ = 'leegstand_test3'
    __table_args__ = {'schema': 'gemeentespecifiek'}  # Specify the schema here

    id = Column(Integer, primary_key=True)
    capaKey = Column('capakey', String)
    dossierPrefix = Column('dossierprefix', String)
    dossierType = Column('dossiertype', String)
    dossiernummer = Column('dossiernummer', String)
    internNummer = Column('internnummer', String)
    datumOpname = Column('datumopname', DateTime)
    vip_inventaristype = Column('vip-inventaristype', String)
    vip_status = Column('vip-status', String)
    vip_statuscode = Column('vip-statuscode', Integer)
    vip_typeonroerendgoed = Column('vip-typeonroerendgoed', String)
    postcode = Column('postcode', Integer)
    gemeente = Column('gemeente', String)
    straat = Column('straat', String)
    huisnummer = Column('huisnummer', String)
    busnummer = Column('busnummer', String)
    geometry = Column('geometry', Geometry(geometry_type='MULTIPOLYGON', srid=0))

# function to drop and recreate the table
def recreate_table(engine, table_class):
    table_class.__table__.drop(engine, checkfirst=True)
    Base.metadata.create_all(engine)
    print(f"Table '{table_class.__table_args__['schema']}.{table_class.__tablename__}' recreated.")

# Drop and recreate the table to ensure correct structure
recreate_table(engine, YourTable)

# function for checking table structure
def check_table_structure(engine, table_class):
    inspector = inspect(engine)
    schema = table_class.__table_args__['schema']
    table_name = table_class.__tablename__

    if inspector.has_table(table_name, schema=schema):
        print(f"Table '{schema}.{table_name}' exists. Validating columns...")
        
        existing_columns = inspector.get_columns(table_name, schema=schema)
        existing_column_names = {col['name'] for col in existing_columns}

        model_columns = {column.name for column in table_class.__table__.columns}

        # Find columns that are in the model but not in the existing table
        missing_in_existing = model_columns - existing_column_names
        # Find columns that are in the existing table but not in the model
        extra_in_existing = existing_column_names - model_columns

        if not missing_in_existing and not extra_in_existing:
            print("Column names match.")
        else:
            print("Column names do not match.")
            if missing_in_existing:
                print("Columns missing in existing table:", missing_in_existing)
            if extra_in_existing:
                print("Extra columns in existing table:", extra_in_existing)
    else:
        print(f"Table '{schema}.{table_name}' does not exist. Creating table...")
        Base.metadata.create_all(engine)

# Check and validate the table structure
check_table_structure(engine, YourTable)

### Add data from gdf

# function to add data from gdf
def insert_data_from_gdf(session, gdf, table_class):
    for idx, row in gdf.iterrows():
        geom = row['geometry']
        if isinstance(geom, str):
            # Convert WKT string to Shapely geometry object
            geom = wkt.loads(geom)
        elif not isinstance(geom, (MultiPolygon,)):
            raise ValueError(f"Expected MultiPolygon or WKT string, got {type(geom)}")

        # Ensure the geometry is a MultiPolygon
        if not isinstance(geom, MultiPolygon):
            geom = MultiPolygon([geom])

        # Convert Shapely geometry to WKB format
        wkb_geometry = geom.wkb

        # Create an insert statement
        stmt = insert(table_class).values(
            capakey=row['capakey'],
            dossierprefix=row['dossierprefix'],
            dossiertype=row['dossiertype'],
            dossiernummer=row.get('dossiernummer'),
            internnummer=row['internnummer'],
            datumopname=row['datumopname'],
            vip_inventaristype=row.get('vip-inventaristype'),
            vip_status=row.get('vip-status'),
            vip_statuscode=row.get('vip-statuscode'),
            vip_typeonroerendgoed=row.get('vip-typeonroerendgoed'),
            postcode=row['postcode'],
            gemeente=row['gemeente'],
            straat=row['straat'],
            huisnummer=row['huisnummer'],
            busnummer=row['busnummer'],
            geometry=func.ST_GeomFromWKB(wkb_geometry)
        )

        # Execute the insert statement
        session.execute(stmt)

    # Commit the transaction
    session.commit()


# Insert data from GeoDataFrame into the table
insert_data_from_gdf(session, gdf, YourTable)

# Close the session
session.close()
