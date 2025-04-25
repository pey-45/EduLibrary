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
            try:
                anio_publicacion = None if sanio_publicacion == "" else int(sanio_publicacion)
            except ValueError:
                print("Error: El año de publicación debe ser un número entero.")
                return
            
            
            sisbn = input("ISBN: ")
            isbn = None if sisbn == "" else sisbn
            
            ssinopsis = input("Sinopsis: ")
            sinopsis = None if ssinopsis == "" else ssinopsis
        
            sid_categoria = input("Id de categoria: ")
            try:
                id_categoria = None if sid_categoria == "" else int(sid_categoria)
            except ValueError:
                print("Error: El id de categoría debe ser un número entero.")
                return
            
            cur.execute("""
                INSERT INTO Libro (titulo, autor, anioPublicacion, isbn, sinopsis, idCategoria) 
                VALUES (%s, %s, %s, %s, %s, %s)
                """, 
                (titulo, autor, anio_publicacion, isbn, sinopsis, id_categoria))
            
            conn.commit()
            print("Libro añadido correctamente.")
            
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
            
            
def buscar_libros(conn):
    """
    Busca libros en la base de datos. Pide al usuario palabras clave, autor y/o categorías.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """
    with conn.cursor() as cur:
        try:
            print("+--------------+")
            print("| Buscar libro |")
            print("+--------------+")
            
            stitulo = input("Titulo: ")
            titulo = None if stitulo == "" else stitulo 
            
            sautor = input("Autor: ")
            autor = None if sautor == "" else sautor
            
            sanio_publicacion = input("Año de publicación: ")
            try:
                anio_publicacion = None if sanio_publicacion == "" else int(sanio_publicacion)
            except ValueError:
                print("Error: El año de publicación debe ser un número entero.")
                return
            
            sisbn = input("ISBN: ")
            isbn = None if sisbn == "" else sisbn
            
            ssinopsis = input("Sinopsis: ")
            sinopsis = None if ssinopsis == "" else sinopsis
        
            sid_categoria = input("Id de categoria: ")
            try:
                id_categoria = None if sid_categoria == "" else int(sid_categoria)
            except ValueError:
                print("Error: El id de categoría debe ser un número entero.")
                return
            
            cur.execute("""
                SELECT * FROM Libro 
                WHERE (%s IS NULL OR titulo ILIKE %s) AND 
                      (%s IS NULL OR autor ILIKE %s) AND 
                      (%s IS NULL OR anioPublicacion = %s) AND 
                      (%s IS NULL OR isbn ILIKE %s) AND 
                      (%s IS NULL OR sinopsis ILIKE %s) AND 
                      (%s IS NULL OR idCategoria = %s)
                """, 
                (titulo, f"%{titulo}%" if titulo is not None else None, autor, f"%{autor}%" if autor is not None else None, anio_publicacion, anio_publicacion, 
                 isbn, f"%{isbn}%" if isbn is not None else None, sinopsis, f"%{sinopsis}%" if sinopsis is not None else None, id_categoria, id_categoria))
            
            libros = cur.fetchall()
            conn.commit()
            
            if len(libros) == 0:
                print("No se han encontrado libros.")
                return
            
            print(f"Se han encontrado {len(libros)} libros.")
            
            for libro in libros:
                print(f"ID: {libro[0]}")
                print(f"Titulo: {libro[1]}")
                print(f"Autor: {libro[2]}")
                print(f"Año de publicación: {libro[3]}")
                print(f"ISBN: {libro[4]}")
                print(f"Sinopsis: {libro[5]}")
                print(f"Id categoria: {libro[6]}")
        
        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()
            
            