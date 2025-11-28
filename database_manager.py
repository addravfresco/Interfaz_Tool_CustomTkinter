# database_manager.py
# Modulo para gestionar la conexión a SQL Server y la lectura del catálogo Excel.

import pyodbc
import pandas as pd

class DatabaseManager:
    """
    Clase para manejar la conexión a SQL Server y las operaciones de datos.
    Carga el catálogo de RFCs y Dependencias desde un archivo Excel al inicio.
    """
    
    def __init__(self, driver, server, database, username, password, catalogo_excel_path, encrypt, trust_server_certificate):
        
        # Cadena de conexión a SQL Server
        self.conn_str = (
            f'DRIVER={{{driver}}};' 
            f'SERVER={server};'
            f'DATABASE={database};' # Conexión inicial a SAT-Nomina
            f'UID={username};'
            f'PWD={password};'
            f'Encrypt={encrypt};'
            f'TrustServerCertificate={trust_server_certificate};'
        )
        self.conn = None
        self.catalogo_excel_path = catalogo_excel_path
        # Carga el Excel al iniciar
        self.catalogo_df = self._load_catalogo_excel() 

    def connect(self):
        """Intenta establecer la conexión con la DB."""
        if self.conn:
            return True
        try:
            self.conn = pyodbc.connect(self.conn_str)
            print("Conexión a la DB establecida con éxito.")
            return True
        except pyodbc.Error as ex:
            # Captura el estado SQL
            sqlstate = ex.args[0]
            print(f"Error de conexión a SQL Server. Código: {sqlstate}")
            return False

    def _load_catalogo_excel(self):
        """Carga el catálogo de RFCs y Dependencias desde un archivo Excel."""
        try:
            df = pd.read_excel(self.catalogo_excel_path)
            df = df.iloc[:, [0, 1]] 
            
            # Asignamos los nombres esperados
            df.columns = ['RFC', 'Dependencia'] 
            
            df = df.dropna()
            
            print(f"Catálogo de Excel cargado con {len(df)} registros.")
            return df
        except Exception as e:
            print(f"Error al cargar el archivo de Excel ({self.catalogo_excel_path}): {e}")
            return pd.DataFrame({'RFC': [], 'Dependencia': []})

    def get_dependencias_list(self):
        """Obtiene la lista única y ordenada de dependencias para el menú de la GUI."""
        if self.catalogo_df.empty:
            return []
        
        dependencias = self.catalogo_df['Dependencia'].unique().tolist()
        dependencias.sort()
        return dependencias
    
    def execute_query(self, tabla_principal, dependencia):
        """
        Consulta SQL Server usando la lista de RFCs de una Dependencia específica (WHERE IN)
        e incluye un JOIN a la base SIGER para obtener el nombre del receptor.
        """
        if not self.conn:
            return pd.DataFrame() 

        # 1. Obtener la lista de RFCs desde Pandas
        rfcs_filtrados = self.catalogo_df[self.catalogo_df['Dependencia'] == dependencia]['RFC'].tolist()
        
        if not rfcs_filtrados:
            return pd.DataFrame()
            
        # 2. Formatear la lista de RFCs para la cláusula IN de SQL
        rfcs_a_consultar = rfcs_filtrados[:1000] 
        # Aseguramos que cada RFC esté entre comillas simples
        rfcs_tuple = ', '.join([f"'{rfc}'" for rfc in rfcs_a_consultar])

        # 3. Construir la consulta SQL con el JOIN
        query = f"""
        SELECT
            T1.*,
            T2.NOMBRE   -- <<< Campo que extraemos de la base SIGER
        FROM
            {tabla_principal} AS T1
        INNER JOIN 
            SIGER.dbo.[Catalogo RFC 2024 Personas Fisicas-Concentrado-13032025] AS T2
        ON 
            T1.ReceptorRFC = T2.RFC COLLATE DATABASE_DEFAULT  -- <<< SOLUCIÓN DE CONFLICTO DE INTERCALACIÓN
        WHERE
            T1.EmisorRFC IN ({rfcs_tuple});
        """

        # 4. Ejecutar la consulta
        try:
            df_resultado = pd.read_sql(query, self.conn)
            # Agregamos la columna de Dependencia que usamos para el filtro
            df_resultado.insert(0, 'Dependencia', dependencia)
            return df_resultado
            
        except Exception as e:
            print(f"Error al ejecutar la consulta SQL: {e}")
            return pd.DataFrame()