import sys
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.errorcodes
from psycopg2.errorcodes import SERIALIZATION_FAILURE


def print_generic_error(e): print(f"\nError: {getattr(e, 'pgcode', 'Unknown')} - {getattr(e, 'pgerror', 'Unknown error')}")

def press_enter_to_continue(): input("\nPresione enter para continuar...")

def perror(e_str): print(f"\nError: {e_str.strip()}")

def pmessage(s_str): print(f"\n{s_str.strip()}")

def ptitle(s_str):
    s_str_strip = s_str.strip()
    s_str_strip_len = len(s_str_strip)
    print(f"\n\t+-{'-'*s_str_strip_len}-+\n\t| {s_str_strip} |\n\t+-{'-'*s_str_strip_len}-+\n")

def get_string(prompt):
    res = input(prompt).strip()
    return None if res == "" else res

def get_parsed(prompt, datatype):
    res = input(prompt)
    return None if res == "" else datatype(res)


def connect_db():
     """
     Establece una conexion con la base de datos predeterminada del usuario (usando DNS vacio).
     :return: La conexion establecida con la base de datos. Si no se puede establecer, el programa termina.
     """
     try:
        conn = psycopg2.connect(host='localhost', user='postgres', password='1234', dbname='biblioteca')
        conn.autocommit = False
        return conn
     except psycopg2.Error:
         print("Error de conexion")
         sys.exit(1)
         
         
def disconnect_db(conn):
    """
    Cierra la conexion con la base de datos, realizando antes un commit de la transaccion activa.
    :param conn: La conexion activa con la base de datos
    :return: None
    """
    try:
        conn.commit()
    except psycopg2.Error:
        conn.rollback()
    finally:
        conn.close()
    
# 1
def anadir_libro(conn):
    """
    Anade un nuevo libro a la biblioteca solicitando al usuario todos los datos necesarios.
    :param conn: La conexion activa con la base de datos
    :return: None
    """

    sql_sentence = """
        INSERT INTO 
            libro (titulo, autor, aniopublicacion, isbn, sinopsis, idcategoria)
        VALUES
            (%(t)s, %(a)s, %(ap)s, %(i)s, %(s)s, %(ic)s)
    """

    ptitle("Anadir libro")
    pmessage("*: Obligatorio\n")

    titulo = get_string("Titulo*: ")
    autor = get_string("Autor: ")
    try:
        anio_publicacion = get_parsed("Anio de publicacion: ", int)
    except ValueError:
        perror("El anio de publicacion debe ser un numero entero.")
        press_enter_to_continue()
        return
    isbn = get_string("ISBN: ")
    sinopsis = get_string("Sinopsis: ")
    try:
        id_categoria = get_parsed("Id de categoria: ", int)
    except ValueError:
        perror("El id de categoria debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                't': titulo,
                'a': autor,
                'ap': anio_publicacion,
                'i': isbn,
                's': sinopsis,
                'ic': id_categoria
            })
            
            conn.commit()
            pmessage("Libro anadido correctamente.")
            press_enter_to_continue()
            
        except psycopg2.Error as e:
            conn.rollback()
            if e.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                perror("El ISBN ya existe en otro libro.")
            elif e.pgcode == psycopg2.errorcodes.FOREIGN_KEY_VIOLATION:
                perror("La categoria especificada no existe.")
            elif e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                perror("El titulo es obligatorio.")
            elif e.pgcode == psycopg2.errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                if e.diag.column_name == "titulo":
                    perror("El titulo es demasiado largo.")
                elif e.diag.column_name == "autor":
                    perror("El nombre de autor es demasiado largo.")
                elif e.diag.column_name == "isbn":
                    perror("El ISBN es demasiado largo.")
                elif e.diag.column_name == "sinopsis":
                    perror("La sinopsis es demasiado larga.")
            else:
                print_generic_error(e)
            press_enter_to_continue()

            
# 2
def buscar_libros(conn):
    """
    Realiza una busqueda de libros en la biblioteca segun los criterios especificados por el usuario.
    :param conn: La conexion activa con la base de datos
    :return: None
    """

    sql_sentence = """
        SELECT 
            l.id,
            l.titulo,
            l.autor,
            l.aniopublicacion,
            l.isbn,
            c.nombre AS nombrecategoria,
            (
                SELECT precio
                FROM preciolibro
                WHERE idlibro = l.id
                ORDER BY fecha DESC
                limit 1 
            ) AS precioactual,
            NOT EXISTS (
                SELECT 1
                FROM prestamo p
                WHERE p.idlibro = l.id 
                AND p.fechadevolucion IS NULL
            ) AS disponible
        FROM
            libro l
        LEFT JOIN
            categoria c ON l.idcategoria = c.id
        WHERE
            (%(t)s IS NULL
            OR l.titulo ilike %(t)s)
            AND (%(a)s IS NULL
            OR l.autor ilike %(a)s)
            AND (%(ap)s IS NULL
            OR l.aniopublicacion = %(ap)s)
            AND (%(i)s IS NULL
            OR l.isbn ilike %(i)s)
            AND (%(ic)s IS NULL
            OR l.idcategoria = %(ic)s) 
    """

    ptitle("Buscar libros")
    pmessage("Los campos en blanco son ignorados\n")

    titulo = get_string("Titulo: ")
    autor = get_string("Autor: ")
    try:
        anio_publicacion = get_parsed("Anio de publicacion: ", int)
    except ValueError:
        perror("El anio de publicacion debe ser un numero entero.")
        press_enter_to_continue()
        return
    isbn = get_string("ISBN: ")
    try:
        id_categoria = get_string("Id de categoria: ")
    except ValueError:
        perror("El id de categoria debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence,{
                't': titulo,
                'a': autor,
                'ap': anio_publicacion,
                'i': isbn,
                'ic': id_categoria
            })
            
            libros = cur.fetchall()
            
            if len(libros) == 0:
                pmessage("No se han encontrado libros.")
                press_enter_to_continue()
                return
            
            pmessage(f"Se han encontrado {len(libros)} libros:")
            
            for libro in libros:
                print(f"\nID: {libro['id']}")
                print(f"Titulo: {libro['titulo']}")
                print(f"Autor: {libro['autor']}")
                print(f"Anio de publicacion: {libro['aniopublicacion']}")
                print(f"ISBN: {libro['isbn']}")
                print(f"Categoria: {libro['nombrecategoria']}")
                print(f"Precio actual (€): {libro['precioactual'] if libro['precioactual'] is not None else 'Sin precio registrado'}")
                print(f"Disponibilidad: {'Disponible' if libro['disponible'] else 'No disponible'}")

            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            print_generic_error(e)
            press_enter_to_continue()

# 3
def consultar_libro(conn):
    """
    Muestra toda la informacion disponible de un libro especifico segun su ID.
    :param conn: La conexion activa con la base de datos
    :return: None
    """

    sql_sentence = """
        SELECT 
            l.titulo,
            l.autor,
            l.aniopublicacion,
            l.isbn,
            l.sinopsis,
            l.idcategoria,
            c.nombre AS nombrecategoria,
            (
                SELECT precio
                FROM preciolibro
                WHERE idlibro = l.id
                ORDER BY fecha DESC
                limit 1 
            ) AS precioactual,
            NOT EXISTS (
                SELECT 1
                FROM prestamo p
                WHERE p.idlibro = l.id
                AND p.fechadevolucion IS NULL
            ) AS disponible
        FROM
            libro l
        LEFT JOIN
            categoria c ON l.idcategoria = c.id
        WHERE
            l.id = %(i)s
    """

    ptitle("Consultar libro")

    try:
        id_libro = get_parsed("Id del libro: ", int)
    except ValueError:
        perror("El id del libro debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_libro
            })
            
            libro = cur.fetchone()
            
            if libro is None:
                perror("No se ha encontrado el libro.")
                press_enter_to_continue()
                return
            
            print(f"\nId: {id_libro}")
            print(f"Titulo: {libro['titulo']}")
            print(f"Autor: {libro['autor']}")
            print(f"Anio de publicacion: {libro['aniopublicacion']}")
            print(f"ISBN: {libro['isbn']}")
            print(f"Sinopsis: {libro['sinopsis']}")
            print(f"Id de categoria: {libro['idcategoria']}")
            print(f"Categoria: {libro['nombrecategoria']}")
            print(f"Precio actual (€): {libro['precioactual'] if libro['precioactual'] is not None else 'Sin precio registrado'}")
            print(f"Disponibilidad: {'Disponible' if libro['disponible'] else 'No disponible'}")

            press_enter_to_continue()
        
        except psycopg2.Error as e:
            conn.rollback()
            print_generic_error(e)
            press_enter_to_continue()

# 4
def modificar_libro(conn):
    """
    Permite actualizar la informacion de un libro existente en la biblioteca.
    :param conn: La conexion activa con la base de datos
    :return: None
    """

    sql_update_titulo = """
        UPDATE
            libro
        SET 
            titulo = %(t)s
        WHERE 
            id = %(i)s
    """

    sql_update_autor = """
        UPDATE
            libro
        SET 
            autor = %(a)s
        WHERE 
            id = %(i)s
    """

    sql_update_anio_publicacion = """
        UPDATE
            libro
        SET     
            aniopublicacion = %(ap)s
        WHERE 
            id = %(i)s
    """

    sql_update_isbn = """
        UPDATE
            libro
        SET 
            isbn = %(isbn)s
        WHERE 
            id = %(i)s
    """

    sql_update_sinopsis = """
        UPDATE
            libro
        SET
            sinopsis = %(s)s
        WHERE 
            id = %(i)s
    """

    sql_update_categoria = """
        UPDATE
            libro
        SET 
            idcategoria = %(ic)s
        WHERE 
            id = %(i)s
    """

    sql_update_precio = """
        INSERT INTO 
            preciolibro (idlibro, precio)
        VALUES 
            (%(i)s, %(p)s) 
    """

    ptitle("Modificar libro")
    pmessage("Los campos en blanco son ignorados\n")

    try:
        id_libro = get_parsed("Id del libro: ", int)
    except ValueError:
        perror("El id del libro debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
    with conn.cursor() as cur:
        try:
            if input("Modificar titulo? (s/n): ").strip().lower() == "s":
                titulo = get_string("Nuevo titulo: ")

                cur.execute(sql_update_titulo, {
                    't': titulo,
                    'i': id_libro
                })

            if input("Modificar autor? (s/n): ").strip().lower() == "s":
                autor = get_string("Nuevo autor: ")

                cur.execute(sql_update_autor, {
                    'a': autor,
                    'i': id_libro
                })

            if input("Modificar anio de publicacion? (s/n):").strip().lower() == "s":
                try:
                    anio_publicacion = get_parsed("Nuevo anio de publicacion: ", int)
                except ValueError:
                    perror("El anio de publicacion debe ser un numero entero.")
                    press_enter_to_continue()
                    return

                cur.execute(sql_update_anio_publicacion, {
                    'ap': anio_publicacion,
                    'i': id_libro
                })

            if input("Modificar ISBN? (s/n): ").strip().lower() == "s":
                isbn = get_string("Nuevo ISBN: ")

                cur.execute(sql_update_isbn, {
                    'isbn': isbn,
                    'i': id_libro
                })

            if input("Modificar sinopsis? (s/n): ").strip().lower() == "s":
                sinopsis = get_string("Nueva sinopsis: ")

                cur.execute(sql_update_sinopsis, {
                    's': sinopsis,
                    'i': id_libro
                })

            if input("Modificar categoria? (s/n): ").strip().lower() == "s":
                try:
                    id_categoria = get_parsed("Nuevo id de categoria: ", int)
                except ValueError:
                    perror("El id de categoria debe ser un numero entero.")
                    press_enter_to_continue()
                    return

                cur.execute(sql_update_categoria, {
                    'ic': id_categoria,
                    'i': id_libro
                })

            if input("Modificar precio? (s/n): ").strip().lower() == "s":
                try:
                    precio = get_parsed("Nuevo precio (€): ", float)
                except ValueError:
                    perror("El precio debe ser un numero real.")
                    press_enter_to_continue()
                    return

                cur.execute(sql_update_precio, {
                    'i': id_libro,
                    'p': precio
                })

            conn.commit()
            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            if e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                perror("El titulo no puede ser nulo.")
            elif e.pgcode == psycopg2.errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                if e.diag.column_name == "titulo":
                    perror("El titulo es demasiado largo.")
                elif e.diag.column_name == "autor":
                    perror("El nombre de autor es demasiado largo.")
                elif e.diag.column_name == "isbn":
                    perror("El ISBN es demasiado largo.")
                elif e.diag.column_name == "sinopsis":
                    perror("La sinopsis es demasiado larga.")
            elif e.pgcode == psycopg2.errorcodes.FOREIGN_KEY_VIOLATION:
                if e.diag.column_name == "idcategoria":
                    perror("La categoria especificada no existe.")
                elif e.diag.column_name == "idlibro":
                    perror("El libro especificado no existe.")
            elif e.pgcode == psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE:
                perror("El precio es demasiado alto.")
            elif e.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE:
                perror("Otro usuario ha modificado el libro mientras editabas.")
                if input("Volver a intentarlo? (s/n): ").strip().lower() == "s":
                    modificar_libro(conn)
                    return
                else:
                    pmessage("No se pudo modificar el libro.")
            else:
                print_generic_error(e)
            press_enter_to_continue()

# 5
def eliminar_libro(conn):
    """
    Elimina permanentemente un libro de la biblioteca segun su ID.
    :param conn: La conexion activa con la base de datos
    :return: None
    """

    sql_sentence = """
        DELETE
        FROM 
            libro
        WHERE 
            id = %(i)s
    """

    ptitle("Eliminar libro")

    try:
        id_libro = get_parsed("Id del libro: ", int)
    except ValueError:
        perror("El id del libro debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_libro
            })

            if cur.rowcount == 0:
                conn.rollback()
                perror("El libro con id {id_libro} no existe.")
            else:
                conn.commit()
                pmessage("Libro eliminado correctamente.")

            press_enter_to_continue()
            
        except psycopg2.Error as e:
            conn.rollback()
            print_generic_error(e)
            press_enter_to_continue()

# 6
def actualizar_precio(conn):
    """
    Actualiza el precio de un libro, permitiendo especificar un nuevo valor o aplicar un porcentaje de variacion.
    :param conn: La conexion activa con la base de datos
    :return: None
    """

    sql_aumento_manual = """
        INSERT INTO
            preciolibro (idlibro, precio)
        VALUES  
            (%(i)s, %(p)s)
    """

    sql_aumento_porcentaje = """
        INSERT INTO 
            preciolibro (idlibro, precio)
        SELECT 
            %(i)s,
            precio * (1 + %(p)s / 100.0)
        FROM 
            preciolibro
        WHERE 
            idlibro = %(i)s
        ORDER BY 
            fecha DESC 
        limit 
            1
    """

    ptitle("Actualizar precio de libro")

    try:
        id_libro = get_parsed("Id del libro: ", int)
    except ValueError:
        perror("El id del libro debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor() as cur:
        try:
            if input("Aumentar precio con porcentaje? (s/n): ").lower() == "s":
                try:
                    porcentaje = get_parsed("Porcentaje de aumento/descuento (usar negativo para descuento): ", float)
                except ValueError:
                    perror("El porcentaje debe ser un numero real.")
                    press_enter_to_continue()
                    return

                cur.execute(sql_aumento_porcentaje, {
                    'i': id_libro,
                    'p': porcentaje
                })

            else:
                try:
                    precio = get_parsed("Nuevo precio (€): ", float)
                except ValueError:
                    perror("El precio debe ser un numero real.")
                    press_enter_to_continue()
                    return

                cur.execute(sql_aumento_manual, {
                    'i': id_libro,
                    'p': precio
                })

            conn.commit()
            pmessage("Precio actualizado correctamente.")
            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            if e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                perror("El precio no puede ser nulo.")
            elif e.pgcode == psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE:
                perror("El precio es demasiado alto.")
            elif e.pgcode == psycopg2.errorcodes.FOREIGN_KEY_VIOLATION:
                perror("El libro especificado no existe.")
            elif e.pgcode == psycopg2.errorcodes.CHECK_VIOLATION:
                perror("El precio debe ser un numero entero positivo.")
            press_enter_to_continue()

# 7
def ver_historial_precios(conn):
    """
    Muestra un listado cronologico de todos los precios que ha tenido un libro especifico.
    :param conn: La conexion activa con la base de datos
    :return: None
    """

    sql_sentence = """
        SELECT 
            precio,
            fecha
        FROM 
            preciolibro
        WHERE 
            idlibro = %(i)s
        ORDER BY
            fecha DESC
    """

    ptitle("Ver historial de precios de libro")

    try:
        id_libro = get_parsed("Id del libro: ", int)
    except ValueError:
        perror("El id del libro debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_libro,
            })

            precios = cur.fetchall()

            if not precios:
                perror("El libro no tiene un precio registrado.")
                return

            print()
            for precio in precios:
                print(f"{precio['fecha'].date()} -> {precio['precio']:.2f} €")

            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            print_generic_error(e)

# 8
def anadir_categoria(conn):
    """
    Anade una categoria a la base de datos. Pide al usuario los datos necesarios.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        INSERT INTO
            categoria (nombre, descripcion)
        VALUES 
            (%(n)s, %(d)s)
    """

    ptitle("Anadir categoria")
    pmessage("*: Obligatorio\n")

    nombre = get_string("Nombre*: ")
    descripcion = get_string("Descripcion*: ")

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'n': nombre,
                'd': descripcion,
            })

            conn.commit()
            pmessage("Categoria anadida correctamente.")
            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            if e.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                perror("Una categoria con ese nombre ya existe.")
            elif e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                if e.diag.column_name == "nombre":
                    perror("El nombre no puede ser nulo.")
                elif e.diag.column_name == "descripcion":
                    perror("La descripcion no puede ser nula.")
            elif e.pgcode == psycopg2.errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                if e.diag.column_name == "nombre":
                    perror("El nombre es demasiado largo.")
                elif e.diag.column_name == "descripcion":
                    perror("La descripcion es demasiado larga.")
            else:
                print_generic_error(e)
            press_enter_to_continue()

# 9
def modificar_categoria(conn):
    """
    Modifica los atributos de una categoria en la base de datos. Pide al usuario el id de la categoria y los atributos a modificar.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_update_nombre = """
        UPDATE 
            categoria 
        SET 
            nombre = %(n)s 
        WHERE 
            id = %(i)s
    """

    sql_update_descripcion = """
        UPDATE 
            categoria  
        SET 
            descripcion = %(d)s 
        WHERE 
            id = %(i)s
    """

    ptitle("Modificar categoria")
    pmessage("Los campos en blanco son ignorados\n")

    try:
        id_categoria = get_parsed("Id de la categoria: ", int)
    except ValueError:
        perror("El id de la categoria debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
    with conn.cursor() as cur:
        try:
            if input("Modificar nombre? (s/n): ").strip().lower() == "s":
                nombre = get_string("Nuevo nombre: ")

                cur.execute(sql_update_nombre, {
                    'n': nombre,
                    'i': id_categoria
                })

            if input("Modificar descripcion? (s/n): ").strip().lower() == "s":
                descripcion = get_string("Nueva descripcion: ")

                cur.execute(sql_update_descripcion, {
                    'd': descripcion,
                    'i': id_categoria
                })

            conn.commit()
            pmessage("Categoria modificada correctamente.")
            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            if e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                if e.diag.column_name == "nombre":
                    perror("El nombre no puede ser nulo.")
                elif e.diag.column_name == "descripcion":
                    perror("La descripcion no puede ser nula.")
            elif e.pgcode == psycopg2.errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                if e.diag.column_name == "nombre":
                    perror("El nombre es demasiado largo.")
                elif e.diag.column_name == "descripcion":
                    perror("La descripcion es demasiado larga.")
            elif e.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE:
                perror("Otro usuario ha modificado la categoria mientras editabas.")
                if input("Volver a intentarlo? (s/n): ").strip().lower() == "s":
                    modificar_categoria(conn)
                    return
                else:
                    pmessage("No se pudo modificar la categoria.")
            else:
                print_generic_error(e)
            press_enter_to_continue()

# 10
def eliminar_categoria(conn):
    """
    Elimina una categoria de la base de datos. Pide al usuario el id de la categoria.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        DELETE FROM
            categoria 
        WHERE
            id = %(i)s
    """

    ptitle("Eliminar categoria")

    try:
        id_categoria = get_parsed("Id de la categoria: ", int)
    except ValueError:
        perror("El id de la categoria debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_categoria
            })

            if cur.rowcount == 0:
                conn.rollback()
                pmessage(f"La categoria con id {id_categoria} no existe.")
            else:
                conn.commit()
                pmessage("Categoria eliminada correctamente.")

            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            print_generic_error(e)
            press_enter_to_continue()

# 11
def efectuar_prestamo(conn):
    """
    Anade un prestamo a la base de datos. Pide al usuario los datos necesarios.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        INSERT INTO 
            prestamo (fechaprestamo, comentarios, idlibro, idestudiante)
        VALUES 
            (NOW(), %(c)s, %(il)s, %(ie)s)
    """

    ptitle("Efectuar prestamo")
    pmessage("*: Obligatorio\n")

    comentarios = get_string("Comentarios: ")
    try:
        id_libro = get_parsed("Id del libro: ", int)
    except ValueError:
        perror("El id del libro debe ser un numero entero.")
        press_enter_to_continue()
        return
    try:
        id_estudiante = get_parsed("Id del estudiante: ", int)
    except ValueError:
        perror("El id del estudiante debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'c': comentarios,
                'il': id_libro,
                'ie': id_estudiante,
            })

            conn.commit()
            pmessage("Prestamo efectuado correctamente.")
            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            if e.pgcode == psycopg2.errorcodes.FOREIGN_KEY_VIOLATION:
                if e.diag.column_name == "idlibro":
                    perror("Libro especificado no existe.")
                elif e.diag.column_name == "idestudiante":
                    perror("Estudiante especificado no existe.")
            elif e.pgcode == psycopg2.errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                perror("El comentario es demasiado largo.")
            else:
                print_generic_error(e)
            press_enter_to_continue()

# 12
def ver_historial_prestamos_libro(conn):
    """
    Muestra el historial de prestamos de un libro. Pide al usuario el id del libro.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        SELECT 
            p.fechaprestamo, 
            p.fechadevolucion, 
            p.comentarios, 
            p.idestudiante,
            e.nombre AS nombreestudiante
        FROM 
            prestamo p
        JOIN 
            estudiante e ON e.id = p.idestudiante
        WHERE   
            p.idlibro = %(i)s
        ORDER BY
            p.fechaprestamo DESC
    """

    ptitle("Ver historial de prestamos de libro")

    try:
        id_libro = get_parsed("Id del libro: ", int)
    except ValueError:
        perror("El id del libro debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_libro,
            })

            prestamos = cur.fetchall()

            if not prestamos:
                perror("El libro no tiene prestamos registrados.")
                press_enter_to_continue()
                return

            for prestamo in prestamos:
                print(f"\nFecha de prestamo: {prestamo['fechaprestamo'].date()}")
                print(f"Fecha de devolucion: {prestamo['fechadevolucion'].date() if prestamo['fechadevolucion'] else 'No devuelto'}")
                print(f"Comentarios: {prestamo['comentarios']}")
                print(f"Id del estudiante: {prestamo['idestudiante']}")
                print(f"Estudiante: {prestamo['nombreestudiante']}")

            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            print_generic_error(e)
            press_enter_to_continue()

# 13
def ver_historial_prestamos_estudiante(conn):
    """
    Muestra el historial de prestamos de un estudiante. Pide al usuario el id del estudiante.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        SELECT 
            p.fechaprestamo, 
            p.fechadevolucion, 
            p.comentarios, 
            p.idlibro,
            l.titulo AS nombrelibro
        FROM 
            prestamo p
        JOIN 
            libro l ON l.id = p.idlibro
        WHERE   
            p.idestudiante = %(i)s
        ORDER BY
            p.fechaprestamo DESC
    """

    ptitle("Ver historial de prestamos de estudiante")

    try:
        id_estudiante = get_parsed("Id del estudiante: ", int)
    except ValueError:
        perror("El id del estudiante debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_estudiante,
            })

            prestamos = cur.fetchall()

            if not prestamos:
                perror("El estudiante no tiene prestamos registrados.")
                press_enter_to_continue()
                return

            for prestamo in prestamos:
                print(f"\nFecha de prestamo: {prestamo['fechaprestamo'].date()}")
                print(f"Fecha de devolucion: {prestamo['fechadevolucion'].date() if prestamo['fechadevolucion'] else 'No devuelto'}")
                print(f"Comentarios: {prestamo['comentarios']}")
                print(f"Id del libro: {prestamo['idlibro']}")
                print(f"Libro: {prestamo['nombrelibro']}")

            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            print_generic_error(e)
            press_enter_to_continue()

# 14
def consultar_prestamo(conn):
    """
    Consulta un prestamo en la base de datos. Pide al usuario el id del prestamo.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        SELECT 
            p.fechaprestamo, 
            p.fechadevolucion, 
            p.comentarios, 
            p.idlibro,
            l.titulo AS nombrelibro,
            p.idestudiante,
            e.nombre AS nombreestudiante
        FROM 
            prestamo p
        JOIN 
            libro l ON l.id = p.idlibro
        JOIN 
            estudiante e ON e.id = p.idestudiante
        WHERE 
            p.id = %(i)s
    """

    ptitle("Consultar prestamo")

    try:
        id_prestamo = get_parsed("Id del prestamo: ", int)
    except ValueError:
        perror("El id del prestamo debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_prestamo,
            })

            prestamo = cur.fetchone()

            if prestamo is None:
                perror("No se ha encontrado el prestamo.")
                press_enter_to_continue()
                return

            print(f"\nId: {id_prestamo}")
            print(f"Fecha del prestamo: {prestamo['fechaprestamo'].date()}")
            print(f"Fecha de devolucion: {prestamo['fechadevolucion'].date() if prestamo['fechadevolucion'] else 'No devuelto'}")
            print(f"Comentarios: {prestamo['comentarios']}")
            print(f"Id del libro: {prestamo['idlibro']}")
            print(f"Libro: {prestamo['nombrelibro']}")
            print(f"Id del estudiante: {prestamo['idestudiante']}")
            print(f"Estudiante: {prestamo['nombreestudiante']}")

        except psycopg2.Error as e:
            conn.rollback()
            print_generic_error(e)
            press_enter_to_continue()

# 15
def finalizar_prestamo(conn):
    """
    Finaliza un prestamo en la base de datos. Pide al usuario el id del prestamo.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        UPDATE 
            prestamo
        SET
            fechadevolucion = NOW()
        WHERE
            id = %(i)s
    """

    ptitle("Finalizar prestamo")

    try:
        id_prestamo = get_parsed("Id del prestamo: ", int)
    except ValueError:
        perror("El id del prestamo debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_prestamo,
            })

            if cur.rowcount == 0:
                conn.rollback()
                perror("No se ha encontrado el prestamo.")
                press_enter_to_continue()
                return

            conn.commit()
            pmessage("Prestamo finalizado correctamente.")

        except psycopg2.Error as e:
            conn.rollback()
            if e.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE:
                perror("Otro usuario ha modificado el prestamo mientras editabas.")
                if input("Volver a intentarlo? (s/n): ").strip().lower() == "s":
                    finalizar_prestamo(conn)
                    return
                else:
                    pmessage("No se pudo finalizar el prestamo.")
            else:
                print_generic_error(e)
            press_enter_to_continue()

# 16
def eliminar_prestamo(conn):
    """
    Elimina un prestamo de la base de datos. Pide al usuario el id del prestamo.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        DELETE FROM
            prestamo 
        WHERE 
            id = %(i)s
    """

    ptitle("Eliminar prestamo")

    try:
        id_prestamo = get_parsed("Id del prestamo: ", int)
    except ValueError:
        perror("El id del prestamo debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_prestamo,
            })

            if cur.rowcount == 0:
                conn.rollback()
                perror(f"El prestamo con id {id_prestamo} no existe.")
            else:
                conn.commit()
                pmessage("Prestamo eliminado correctamente.")

            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            print_generic_error(e)
            press_enter_to_continue()

# 17
def anadir_estudiante(conn):
    """
    Anade un estudiante a la base de datos. Pide al usuario los datos necesarios.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        INSERT INTO
            estudiante (nombre, apellidos, curso, email, telefono)
        VALUES 
            (%(n)s, %(a)s, %(c)s, %(e)s, %(t)s)
    """

    ptitle("Anadir estudiante")
    pmessage("*: Obligatorio\n")

    nombre = get_string("Nombre*: ")
    apellidos = get_string("Apellidos*: ")
    try:
        curso = get_parsed("Curso*: ", int)
    except ValueError:
        perror("El curso debe ser un numero entero.")
        press_enter_to_continue()
        return
    email = get_string("Email*: ")
    telefono = get_string("Telefono (+XX XXX XX XX XX): ")

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'n': nombre,
                'a': apellidos,
                'c': curso,
                'e': email,
                't': telefono,
            })

            conn.commit()
            pmessage("Estudiante anadido correctamente.")
            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            if e.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                if e.diag.column_name == "email":
                    perror("El email ya existe.")
                elif e.diag.column_name == "telefono":
                    perror("El telefono ya existe.")
            elif e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                if e.diag.column_name == "nombre":
                    perror("El nombre no puede ser nulo.")
                elif e.diag.column_name == "apellidos":
                    perror("Los apellidos no pueden ser nulos.")
                elif e.diag.column_name == "curso":
                    perror("El curso no puede ser nulo.")
                elif e.diag.column_name == "email":
                    perror("El email no puede ser nulo.")
            elif e.pgcode == psycopg2.errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                if e.diag.column_name == "nombre":
                    perror("El nombre es demasiado largo.")
                elif e.diag.column_name == "apellidos":
                    perror("Los apellidos son demasiado largos.")
                elif e.diag.column_name == "email":
                    perror("El email es demasiado largo.")
                elif e.diag.column_name == "telefono":
                    perror("El telefono es demasiado largo.")
            elif e.pgcode == psycopg2.errorcodes.CHECK_VIOLATION:
                perror("El curso debe ser un numero entero mayor que cero.")
            else:
                print_generic_error(e)
            press_enter_to_continue()

# 18
def consultar_estudiante(conn):
    """
    Consulta un estudiante en la base de datos. Pide al usuario el id del estudiante.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        SELECT 
            e.nombre,
            e.apellidos,
            e.curso,
            e.email,
            e.telefono,
            (
                SELECT count(*) 
                FROM prestamo p 
                WHERE p.idestudiante = e.id
                AND p.fechadevolucion IS NULL
            ) AS librosenposesion
        FROM
            estudiante e
        WHERE
             e.id = %(i)s
    """

    ptitle("Consultar estudiante")

    try:
        id_estudiante = get_parsed(f"Id del estudiante: ", int)
    except ValueError:
        perror("El id del estudiante debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_estudiante,
            })

            estudiante = cur.fetchone()

            if estudiante is None:
                perror("No se ha encontrado el estudiante.")
                press_enter_to_continue()
                return

            print(f"\nId: {id_estudiante}")
            print(f"Nombre: {estudiante['nombre']}")
            print(f"Apellidos: {estudiante['apellidos']}")
            print(f"Curso: {estudiante['curso']}")
            print(f"Email: {estudiante['email']}")
            print(f"Telefono: {estudiante['telefono']}")
            print(f"Libros en posesion: {estudiante['librosenposesion']}")

        except psycopg2.Error as e:
            conn.rollback()
            print_generic_error(e)
            press_enter_to_continue()

# 19
def aumentar_curso(conn):
    """
    Aumenta el curso de uno o varios estudiantes. Pide al usuario los datos necesarios.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        UPDATE
            estudiante
        SET 
            curso = curso + 1
        WHERE 
            id = %(i)s
    """

    ptitle("Aumentar curso de estudiantes")

    id_estudiantes = []
    while True:
        try:
            id_estudiante = get_parsed("Id del estudiante (o 'enter' para terminar): ", int)
            if id_estudiante is None:
                break
        except ValueError:
            perror("El id del estudiante debe ser un numero entero.")
            continue

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
    with conn.cursor() as cur:
        try:
            actualizados = 0

            print()
            for id_estudiante in id_estudiantes:
                cur.execute(sql_sentence, {
                    'i': id_estudiante,
                })

                if cur.rowcount == 1:
                    actualizados += 1
                    print(f"El curso de la estudiante con id {id_estudiante} ha sido actualizado correctamente.")
                else:
                    print(f"El estudiante con id {id_estudiante} no existe.")  # Continuamos igualmente

            conn.commit()
            pmessage(f"Curso aumentado correctamente a {actualizados} estudiantes.")
            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            if e.pgcode == psycopg2.errorcodes.CHECK_VIOLATION:
                perror("El curso debe ser un numero entero mayor que cero.")
            elif e.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE:
                perror("Otro usuario ha modificado el estudiante mientras editabas.")
                if input("Volver a intentarlo? (s/n): ").strip().lower() == "s":
                    aumentar_curso(conn)
                    return
                else:
                    pmessage("No se pudo aumentar el curso.")
            else:
                print_generic_error(e)
            press_enter_to_continue()

# 20
def modificar_estudiante(conn):
    """
    Modifica los atributos de un estudiante en la base de datos. Pide al usuario el id del estudiante y los atributos a modificar.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_update_nombre = """
        UPDATE 
            estudiante 
        SET 
            nombre = %(n)s 
        WHERE 
            id = %(i)s
    """

    sql_update_apellidos = """
        UPDATE 
            estudiante 
        SET 
            apellidos = %(a)s 
        WHERE 
            id = %(i)s
    """

    sql_update_curso = """
        UPDATE 
            estudiante 
        SET 
            curso = %(c)s 
        WHERE 
            id = %(i)s
    """

    sql_update_email = """
        UPDATE 
            estudiante 
        SET 
            email = %(e)s 
        WHERE 
            id = %(i)s
    """

    sql_update_telefono = """
        UPDATE 
            estudiante  
        SET 
            telefono = %(t)s 
        WHERE 
            id = %(i)s
    """

    ptitle("Modificar estudiante")
    pmessage("Los campos en blanco son ignorados\n")

    try:
        id_estudiante = get_parsed("Id del estudiante: ", int)
    except ValueError:
        perror("El id del estudiante debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
    with conn.cursor() as cur:
        try:
            if input("Modificar nombre? (s/n): ").strip().lower() == "s":
                nombre = get_string("Nuevo nombre: ")

                cur.execute(sql_update_nombre, {
                    'n': nombre,
                    'i': id_estudiante,
                })

            if input("Modificar apellidos? (s/n): ").strip().lower() == "s":
                apellidos = get_string("Nuevos apellidos: ")

                cur.execute(sql_update_apellidos, {
                    'a': apellidos,
                    'i': id_estudiante,
                })

            if input("Modificar curso? (s/n):").strip().lower() == "s":
                try:
                    curso = get_parsed("Nuevo curso: ", int)
                except ValueError:
                    perror("El curso debe ser un numero entero.")
                    return

                cur.execute(sql_update_curso, {
                    'c': curso,
                    'i': id_estudiante,
                })

            if input("Modificar email? (s/n): ").strip().lower() == "s":
                email = get_string("Nuevo email: ")

                cur.execute(sql_update_email, {
                    'e': email,
                    'i': id_estudiante,
                })

            if input("Modificar telefono? (s/n): ").strip().lower() == "s":
                telefono = get_string("Nuevo telefono: ")

                cur.execute(sql_update_telefono, {
                    't': telefono,
                    'i': id_estudiante,
                })

            conn.commit()
            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            if e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                if e.diag.column_name == "nombre":
                    perror("El nombre no puede ser nulo.")
                elif e.diag.column_name == "apellidos":
                    perror("Los apellidos no pueden ser nulos.")
                elif e.diag.column_name == "curso":
                    perror("El curso no puede ser nulo.")
                elif e.diag.column_name == "email":
                    perror("El email no puede ser nulo.")
            elif e.pgcode == psycopg2.errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                if e.diag.column_name == "nombre":
                    perror("El nombre es demasiado largo.")
                elif e.diag.column_name == "apellidos":
                    perror("Los apellidos son demasiado largos.")
                elif e.diag.column_name == "email":
                    perror("El email es demasiado largo.")
                elif e.diag.column_name == "telefono":
                    perror("El telefono es demasiado largo.")
            elif e.pgcode == psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE:
                perror("El curso es demasiado grande.")
            elif e.pgcode == psycopg2.errorcodes.CHECK_VIOLATION:
                perror("El curso debe ser un numero entero mayo que cero.")
            elif e.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE:
                perror("Otro usuario ha modificado el estudiante mientras editabas.")
                if input("Volver a intentarlo? (s/n): ").strip().lower() == "s":
                    modificar_estudiante(conn)
                    return
                else:
                    pmessage("No se pudo modificar el estudiante.")
            else:
                print_generic_error(e)
            press_enter_to_continue()

# 21
def eliminar_estudiante(conn):
    """
    Elimina un estudiante de la base de datos. Pide al usuario el id del estudiante.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        DELETE
        FROM 
            estudiante
        WHERE 
            id = %(i)s
    """

    ptitle("Eliminar estudiante")

    try:
        id_estudiante = get_parsed("Id del estudiante: ", int)
    except ValueError:
        perror("El id del estudiante debe ser un numero entero.")
        press_enter_to_continue()
        return

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_estudiante,
            })

            if cur.rowcount == 0:
                conn.rollback()
                perror(f"El estudiante con id {id_estudiante} no existe.")
            else:
                conn.commit()
                pmessage("Estudiante eliminado correctamente.")

            press_enter_to_continue()

        except psycopg2.Error as e:
            conn.rollback()
            print_generic_error(e)
            press_enter_to_continue()


def menu(conn):
    """
    Imprime un menu de opciones, solicita la opcion y ejecuta la funcion asociada.
    'q' para salir.
    """

    menu_text = f"""    [LIBROS]
    1 - Anadir libro        
    2 - Buscar libros por atributos    
    3 - Consultar detalles de libro
    4 - Modificar libro      
    5 - Eliminar libro   
     
    [PRECIOS DE LIBROS]  
    6 - Actualizar precio de libro
    7 - Ver historial de precios de libro
    
    [CATEGORIAS]
    8 - Anadir categoria
    9 - Modificar categoria
    10 - Eliminar categoria
    
    [PRESTAMOS]
    11 - Efectuar prestamo
    12 - Ver historial de prestamos de libro
    13 - Ver historial de prestamos de estudiante
    14 - Consultar prestamo
    15 - Finalizar prestamo
    16 - Eliminar prestamo
    
    [ESTUDIANTES]
    17 - Anadir estudiante
    18 - Consultar estudiante
    19 - Aumentar curso de estudiantes
    20 - Modificar estudiante
    21 - Eliminar estudiante
    
    q - Salir
    """
    
    while True:
        print(f"{'\n' * 32}")
        ptitle("MENU")
        print(menu_text)
        tecla = input("Opcion> ").strip()
        if tecla == 'q':
            print("\nHasta pronto!\n")
            break
        elif tecla == '1':
            anadir_libro(conn)
        elif tecla == '2':
            buscar_libros(conn)
        elif tecla == '3':
            consultar_libro(conn)
        elif tecla == '4':
            modificar_libro(conn)
        elif tecla == '5':
            eliminar_libro(conn)
        elif tecla == '6':
            actualizar_precio(conn)
        elif tecla == '7':
            ver_historial_precios(conn)
        elif tecla == '8':
            anadir_categoria(conn)
        elif tecla == '9':
            modificar_categoria(conn)
        elif tecla == '10':
            eliminar_categoria(conn)
        elif tecla == '11':
            efectuar_prestamo(conn)
        elif tecla == '12':
            ver_historial_prestamos_libro(conn)
        elif tecla == '13':
            ver_historial_prestamos_estudiante(conn)
        elif tecla == '14':
            consultar_prestamo(conn)
        elif tecla == '15':
            finalizar_prestamo(conn)
        elif tecla == '16':
            eliminar_prestamo(conn)
        elif tecla == '17':
            anadir_estudiante(conn)
        elif tecla == '18':
            consultar_estudiante(conn)
        elif tecla == '19':
            aumentar_curso(conn)
        elif tecla == '20':
            modificar_estudiante(conn)
        elif tecla == '21':
            eliminar_estudiante(conn)


def main():
    """
    Funcion principal. Conecta a la bd y ejecuta el menu.
    Cando sale del menu, desconecta da bd y termina el programa.
    """
    print('Conectando a PosgreSQL...')
    conn = connect_db()
    print('Conectado.')
    menu(conn)
    disconnect_db(conn)


if __name__ == '__main__':
    main()