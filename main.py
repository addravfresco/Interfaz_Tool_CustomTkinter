# main.py

# Aplicación GUI principal con CustomTkinter.

import customtkinter as ctk
from database_manager import DatabaseManager 
import tkinter.ttk as ttk 
from tkinter import messagebox
import pandas as pd
from tkinter import filedialog 
import configparser 
import os 
import numpy as np 

class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        # 1. Configuración de la Ventana
        self.title("Herramienta de Consulta SAT")
        self.geometry("1200x700")

        # --- CARGAR CONFIGURACIÓN DESDE config.ini ---
        config = configparser.ConfigParser(interpolation=None) 
        try:
            config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
            config.read(config_path)
        except Exception as e:
            messagebox.showerror("Error de Configuración", f"No se pudo leer config.ini: {e}")
            self.destroy()
            return

        # 2. Asignación de Variables desde config.ini
        try:
            sql_config = config['SQL_SERVER']
            paths_config = config['PATHS']

            # SQL Server Credentials
            self.db_driver = sql_config['DRIVER']
            self.db_server = sql_config['SERVER']
            self.db_database = sql_config['DATABASE']
            self.db_username = sql_config['USERNAME']
            self.db_password = sql_config['PASSWORD']

            # Driver 18 Security Parameters
            self.db_encrypt = sql_config['ENCRYPT']
            self.db_trust_cert = sql_config['TRUST_SERVER_CERTIFICATE']
            self.catalogo_excel_path = paths_config['CATALOGO_EXCEL_PATH']

        except KeyError as e:
            messagebox.showerror("Error de Configuración", f"Falta la clave {e} en config.ini. Verifique los nombres de sección y campo.")
            self.destroy()
            return

        # 3. Nombres de tablas en la DB
        self.tablas = {
            "Percepciones": "[dbo].[2024-AECF_0101_Anexo4-Detalle-Percepciones]",
            "Deducciones": "[dbo].[2024-AECF_0101_Anexo5-Detalle-Deducciones]"
        }

        # Inicializar el manejador de base de datos
        self.db_manager = DatabaseManager(
            self.db_driver, self.db_server, self.db_database, 
            self.db_username, self.db_password, self.catalogo_excel_path,
            self.db_encrypt, self.db_trust_cert 
        )

        self.current_data_df = pd.DataFrame()
        self.all_dependencias = [] 
        self.after_id = None # ID del temporizador para el despliegue automático (NUEVO)
        self.create_widgets()
        self.initialize_connection()

    def create_widgets(self):
        # Distribución de elementos (grid layout)
        self.grid_rowconfigure(1, weight=1) 
        self.grid_columnconfigure(0, weight=1)
        self.control_frame = ctk.CTkFrame(self)
        self.control_frame.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="ew")

        # Estado de Conexión
        self.status_label = ctk.CTkLabel(self.control_frame, text="Estado: Inicializando...", text_color="grey")
        self.status_label.pack(side="left", padx=10, pady=10)

        # Tabla (Percepciones/Deducciones)
        ctk.CTkLabel(self.control_frame, text="Tabla:").pack(side="left", padx=(30, 5))
        self.table_option = ctk.CTkOptionMenu(self.control_frame, values=list(self.tablas.keys()))
        self.table_option.pack(side="left", padx=10)

        # Dependencias - Combobox (Búsqueda/Autocompletado)
        ctk.CTkLabel(self.control_frame, text="Dependencia:").pack(side="left", padx=5)
        self.dependencia_frame = ctk.CTkFrame(self.control_frame, fg_color="transparent")
        self.dependencia_frame.pack(side="left", padx=10)

        # ttk.Combobox: permite escribir y autocompletar
        self.dependencia_combobox = ttk.Combobox(
            self.dependencia_frame, 
            values=["Cargando..."],
            width=50 
        )
        self.dependencia_combobox.pack(side="left") 

        # Tipo de Importe
        ctk.CTkLabel(self.control_frame, text="Importe:").pack(side="left", padx=5)
        self.importe_option = ctk.CTkOptionMenu(
            self.control_frame, 
            values=['PercepcionImporteGravado', 'PercepcionImporteExento'],
            width=200
        )
        self.importe_option.set('PercepcionImporteGravado') 
        self.importe_option.pack(side="left", padx=10)

        # Botones
        self.query_button = ctk.CTkButton(self.control_frame, text="Consultar", command=self.run_query)
        self.query_button.pack(side="left", padx=(30, 10))
        self.export_button = ctk.CTkButton(self.control_frame, text="Exportar a CSV/Excel", command=self.export_data, state="disabled")
        self.export_button.pack(side="left", padx=10)

        # Área de Visualización de Datos (Treeview)
        self.data_frame = ctk.CTkFrame(self)
        self.data_frame.grid(row=1, column=0, padx=20, pady=(10, 20), sticky="nsew")
        self.data_frame.grid_rowconfigure(0, weight=1)
        self.data_frame.grid_columnconfigure(0, weight=1)
        self.tree = ttk.Treeview(self.data_frame)
        self.tree.grid(row=0, column=0, sticky="nsew", padx=(0, 15), pady=(0, 15))

        # Scrollbars
        vsb = ctk.CTkScrollbar(self.data_frame, orientation="vertical", command=self.tree.yview)
        hsb = ctk.CTkScrollbar(self.data_frame, orientation="horizontal", command=self.tree.xview)
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

    def show_dropdown(self):
        """Genera el evento para mostrar la lista desplegable del Combobox."""
        if self.dependencia_combobox.get() and self.dependencia_combobox['values']:
             # Simula un clic en la flecha para abrir el desplegable
             self.dependencia_combobox.event_generate('<Down>')

    def initialize_connection(self):
        """Intenta conectar a la DB y cargar la lista de dependencias."""
        is_db_connected = self.db_manager.connect()
        dependencias = self.db_manager.get_dependencias_list()

        if is_db_connected:
            if dependencias:
                self.status_label.configure(text="Estado: Conectado a SQL y Catálogo OK", text_color="green") 
                self.all_dependencias = dependencias 
                self.dependencia_combobox['values'] = dependencias
                self.dependencia_combobox.set("") # Campo vacío al inicio
                # Enlazar el evento de tecleado para el autocompletado
                self.dependencia_combobox.bind('<KeyRelease>', self.filter_dependencias)
            else:
                self.status_label.configure(text="Estado: Conectado (Error de Catálogo)", text_color="orange")
                self.dependencia_combobox['values'] = ["Error: Verificar Excel"]
                self.dependencia_combobox.set("Error: Verificar Excel")
        else:
            self.status_label.configure(text="Estado: ERROR DE CONEXIÓN SQL", text_color="red")

    def filter_dependencias(self, event):
        """Filtra la lista de dependencias y programa el despliegue automático si el usuario se detiene."""

        # 1. Cancelar cualquier temporizador de despliegue anterior
        if self.after_id:
            self.after_cancel(self.after_id)
            self.after_id = None

        text_ingresado = self.dependencia_combobox.get().lower()

        if not text_ingresado:
            self.dependencia_combobox['values'] = self.all_dependencias
        else:
            # Filtrar las coincidencias (insensible a mayúsculas/minúsculas)
            filtered_list = [
                d for d in self.all_dependencias 
                if text_ingresado in d.lower() 
            ]

            self.dependencia_combobox['values'] = filtered_list
            # 2. Programar el despliegue del menú después de 300ms de inactividad
            if filtered_list and len(text_ingresado) > 0:
                self.after_id = self.after(300, self.show_dropdown) # <--- Programación del despliegue

            # Si se presiona Enter, forzamos la consulta
            if event.keysym == 'Return':
                 self.run_query()

    def _pivot_data(self, df):
        """Transforma el DataFrame de formato largo a formato ancho (crosstab), incluyendo UUID, NOMBRE y usando el importe seleccionado."""
        if df.empty:
            return pd.DataFrame()

        importe_columna = self.importe_option.get()
        try:
            df[importe_columna] = pd.to_numeric(df[importe_columna], errors='coerce')
            
            # ÍNDICES ACTUALIZADOS: Incluye la columna NOMBRE
            pivot_df = pd.pivot_table(
                df, 
                values=importe_columna, 
                index=['Dependencia', 'EmisorRFC', 'ReceptorRFC', 'UUID', 'NOMBRE'], # <<< CAMBIO AQUÍ
                columns='PercepcionClave', 
                aggfunc=np.sum, 
                fill_value=0 
            ).reset_index()

            # La suma horizontal empieza a partir de la columna 5, ya que ahora tenemos 5 columnas de índice (0-4)
            pivot_df['Total General'] = pivot_df.iloc[:, 5:].sum(axis=1) 
            pivot_df.columns.name = None
            return pivot_df

        except KeyError as e:
            messagebox.showerror("Error de Transformación", f"El DataFrame de la DB no contiene la columna necesaria: {e}. Verifique las columnas retornadas.")
            return pd.DataFrame()
        except Exception as e:
            messagebox.showerror("Error de Pivoteo", f"Ocurrió un error al pivotar los datos: {e}")
            return pd.DataFrame()

    def run_query(self):
        """Ejecuta la consulta, pivotea y muestra el resultado en el Treeview."""
        tabla_db = self.tablas.get(self.table_option.get())
        dependencia_seleccionada = self.dependencia_combobox.get() 
        if not dependencia_seleccionada or "Error" in dependencia_seleccionada or not self.db_manager.conn:
            messagebox.showwarning("Advertencia", "Seleccione o escriba una dependencia válida y verifique la conexión a SQL.")
            return
        self.query_button.configure(text="Consultando...", state="disabled")
        self.update() 
        try:
            # 1. OBTENER DATOS BRUTOS
            df_bruto = self.db_manager.execute_query(tabla_db, dependencia_seleccionada)
            if df_bruto.empty:
                messagebox.showwarning("Sin Resultados", f"No se encontraron registros para '{dependencia_seleccionada}'.")
                self.current_data_df = pd.DataFrame()
                self.show_data_in_treeview(self.current_data_df)
                self.export_button.configure(state="disabled")
                return

            # 2. PIVOTAR Y TRANSFORMAR
            df_transformado = self._pivot_data(df_bruto)
            self.current_data_df = df_transformado 
            if not df_transformado.empty:
                self.show_data_in_treeview(df_transformado)
                messagebox.showinfo("Consulta Exitosa", f"Se encontraron {len(df_transformado)} registros consolidados para '{dependencia_seleccionada}'.")
                self.export_button.configure(state="normal")
            else:
                self.current_data_df = pd.DataFrame()
                self.show_data_in_treeview(self.current_data_df) 
                self.export_button.configure(state="disabled")
                messagebox.showwarning("Fallo de Transformación", "Los datos brutos se obtuvieron, pero falló la transformación (pivoteo).")

        except Exception as e:
            messagebox.showerror("Error de Consulta", f"Ocurrió un error inesperado al consultar la DB: {e}")

        finally:
            self.query_button.configure(text="Consultar", state="normal")

    def show_data_in_treeview(self, df):
        """Limpia el Treeview y lo llena con los datos del DataFrame."""
        for item in self.tree.get_children():
            self.tree.delete(item)
        if df.empty:
            self.tree.configure(columns=())
            return

        columns = list(df.columns)
        self.tree["columns"] = columns
        self.tree["show"] = "headings" 

        for col in columns:
            self.tree.heading(col, text=col)
            # Damos ancho a las columnas clave, dando prioridad a NOMBRE
            if col in ['Dependencia', 'EmisorRFC', 'ReceptorRFC', 'UUID', 'Total General']:
                width_val = 150
            elif col == 'NOMBRE': # <<< ANCHO MAYOR PARA EL NOMBRE COMPLETO
                width_val = 250    
            else:
                width_val = 100
            
            self.tree.column(col, width=width_val, anchor="w")

        for _, row in df.iterrows():
            self.tree.insert("", "end", values=list(row))

    def export_data(self):
        """Exporta los datos actuales a un archivo CSV o Excel."""
        if self.current_data_df.empty:
            messagebox.showwarning("Advertencia", "No hay datos para exportar.")
            return

        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx")],
            title="Guardar Datos Consultados"
        )

        if file_path:
            try:
                if file_path.endswith('.xlsx'):
                    self.current_data_df.to_excel(file_path, index=False)
                else:
                    self.current_data_df.to_csv(file_path, index=False, encoding='utf-8')

                messagebox.showinfo("Exportación Exitosa", f"Datos exportados correctamente a:\n{file_path}")
            except ImportError:
                messagebox.showerror("Error de Librería", "Para exportar a Excel (.xlsx), ejecute: pip install openpyxl")
            except Exception as e:
                messagebox.showerror("Error de Exportación", f"Ocurrió un error al guardar el archivo:\n{e}")

if __name__ == "__main__":
    ctk.set_appearance_mode("System") 
    ctk.set_default_color_theme("blue") 
    app = App()
    app.mainloop()