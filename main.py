import sys
import psycopg2
import psycopg2.extras
import psycopg2.errorcodes

def print_generic_error(e): print(f"Error: {e.pgcode} - {e.pgerror}")

def connect_db():
     """
     Se conecta a la BD predeterminada del usuario (DNS vacio)
     :return: La conexión con la BD (o sale del programa de no conseguirla)
     """
     try:
        conn = psycopg2.connect("")
        conn.autocommit = False
        return conn
     except psycopg2.Error as e:
         print("Error de conexión")
         sys.exit(1)
         
         
def disconnect_db(conn):
    """
    Se desconecta de la BD. Hace antes un commit de la transacción activa.
    :param conn: La conexión aberta a la BD
    :return: Nada
    """
    conn.commit()
    conn.close()
    

def anadir_libro(conn):
    """
    Añade un libro a la base de datos. Pide al usuario los datos necesarios.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """
    with conn.cursor() as cur:
        try:
            print("+--------------+")
            print("| Añadir libro |")
            print("+--------------+")
            
            stitulo = input("Titulo: ")
            titulo = None if stitulo == "" else stitulo 
            
            sautor = input("Autor: ")
            autor = None if sautor == "" else sautor
            
            sanio_publicacion = input("Año de publicación: ")
            anio_publicacion = None if sanio_publicacion == "" else int(sanio_publicacion)
            
            sisbn = input("ISBN: ")
            isbn = None if sisbn == "" else sisbn
            
            ssinopsis = input("Sinopsis: ")
            sinopsis = None if ssinopsis == "" else ssinopsis
        
            sid_categoria = input("Id de categoria: ")
            id_categoria = None if sid_categoria == "" else int(sid_categoria)
            
            cur.execute("""
                INSERT INTO Libro (titulo, autor, anioPublicacion, isbn, sinopsis, idCategoria) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """, 
            (titulo, autor, anio_publicacion, isbn, sinopsis, id_categoria))
            
            conn.commit()
            print("Libro añadido correctamente.")
            
        except ValueError as e:
            print("Error: El año de publicación debe ser un número entero.")
            
        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                print("Error: ISBN ya existe en otro libro.")
            elif e.pgcode == psycopg2.errorcodes.FOREIGN_KEY_VIOLATION:
                print(f"Error: Categoria especificada no existe.")
            elif e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                print("Error: El titulo es obligatorio.")
            elif e.pgcode == psycopg2.errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                if e.diag.column_name == "titulo":
                    print("Error: El titulo es demasiado largo.")
                elif e.diag.column_name == "autor":
                    print("Error: El nombre de autor es demasiado largo.")
                elif e.diag.column_name == "isbn":
                    print("Error: El ISBN es demasiado largo.")
                elif e.diag.column_name == "sinopsis":
                    print("Error: La sinopsis es demasiado larga.")
            else:
                print_generic_error(e) 
            conn.rollback()