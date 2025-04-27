import datetime
import sys
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.errorcodes

def print_generic_error(e): print(f"Error: {getattr(e, 'pgcode', 'Unknown')} - {getattr(e, 'pgerror', 'Unknown error')}")


def connect_db():
     """
     Se conecta a la BD predeterminada del usuario (DNS vacio)
     :return: La conexión con la BD (o sale del programa de no conseguirla)
     """
     try:
        conn = psycopg2.connect("")
        conn.autocommit = False
        return conn
     except psycopg2.Error:
         print("Error de conexión")
         sys.exit(1)
         
         
def disconnect_db(conn):
    """
    Se desconecta de la BD. Hace antes un commit de la transacción activa.
    :param conn: La conexión aberta a la BD
    :return: Nada
    """
    try:
        conn.commit()
    except psycopg2.Error:
        conn.rollback()
    finally:
        conn.close()
    

def anadir_libro(conn):
    """
    Añade un libro a la base de datos. Pide al usuario los datos necesarios.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED

    sql_sentence = """
        INSERT INTO 
            Libro (titulo, autor, anioPublicacion, isbn, sinopsis, idCategoria) 
        VALUES 
            (%(t)s, %(a)s, %(aP)s, %(i)s, %(s)s, %(iC)s)
    """


    print("+--------------+")
    print("| Añadir libro |")
    print("+--------------+")

    stitulo = input("Titulo: ").strip()
    titulo = None if stitulo == "" else stitulo

    sautor = input("Autor: ").strip()
    autor = None if sautor == "" else sautor

    sanio_publicacion = input("Año de publicación: ").strip()
    try:
        anio_publicacion = None if sanio_publicacion == "" else int(sanio_publicacion)
    except ValueError:
        print("Error: El año de publicación debe ser un número entero.")
        return

    sisbn = input("ISBN: ").strip()
    isbn = None if sisbn == "" else sisbn

    ssinopsis = input("Sinopsis: ").strip()
    sinopsis = None if ssinopsis == "" else ssinopsis

    sid_categoria = input("Id de categoria: ").strip()
    try:
        id_categoria = None if sid_categoria == "" else int(sid_categoria)
    except ValueError:
        print("Error: El id de categoría debe ser un número entero.")
        return

    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                't': titulo,
                'a': autor,
                'aP': anio_publicacion,
                'i': isbn,
                's': sinopsis,
                'iC': id_categoria
            })
            
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

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED

    sql_sentence = """
        SELECT 
            L.id, 
            L.titulo, 
            L.autor, 
            L.anioPublicacion, 
            L.isbn, 
            C.nombre AS nombreCategoria,
            (
                SELECT precio
                FROM PrecioLibro
                WHERE idLibro = L.id
                ORDER BY fecha DESC
                LIMIT 1 
            ) AS precioActual,
            NOT EXISTS (
                SELECT 1
                FROM Prestamo P
                WHERE P.idLibro = L.id
                AND P.fechaDevolucion IS NULL
            ) AS disponible
        FROM 
            Libro L
        LEFT JOIN 
            Categoria C ON L.idCategoria = C.id
        WHERE 
            (%(t)s IS NULL OR L.titulo ILIKE %(t0)s)
            AND (%(a)s IS NULL OR L.autor ILIKE %(a0)s)
            AND (%(aP)s IS NULL OR L.anioPublicacion = %(aP)s)
            AND (%(i)s IS NULL OR L.isbn ILIKE %(i0)s)
            AND (%(iC)s IS NULL OR L.idCategoria = %(iC)s)
    """

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

    sid_categoria = input("Id de categoria: ")
    try:
        id_categoria = None if sid_categoria == "" else int(sid_categoria)
    except ValueError:
        print("Error: El id de categoría debe ser un número entero.")
        return

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence,{
                't': titulo,
                't0': f"%{titulo}%" if titulo is not None else None,
                'a': autor,
                'a0': f"%{autor}%" if autor is not None else None,
                'aP': anio_publicacion,
                'i': isbn,
                'i0': f"%{isbn}%" if isbn is not None else None,
                'iC': id_categoria
            })
            
            libros = cur.fetchall()
            
            if len(libros) == 0:
                print("No se han encontrado libros.")
                return
            
            print(f"Se han encontrado {len(libros)} libros.")
            
            for libro in libros:
                print(f"ID: {libro['id']}")
                print(f"Titulo: {libro['titulo']}")
                print(f"Autor: {libro['autor']}")
                print(f"Año de publicación: {libro['anioPublicacion']}")
                print(f"ISBN: {libro['isbn']}")
                print(f"Categoria: {libro['nombreCategoria']}")
                print(f"Precio actual: {libro['precioActual'] if libro['precioActual'] is not None else 'Sin precio registrado'} €")
                print(f"Disponibilidad: {"Disponible" if libro['disponible'] else "No disponible"}")
        
        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()
            
            
def consultar_libro(conn):
    """
    Consulta un libro en la base de datos. Pide al usuario el id del libro.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED

    sql_sentence = """
        SELECT 
            L.titulo,
            L.autor,
            L.anioPublicacion,
            L.isbn,
            L.sinopsis,
            L.idCategoria,
            C.nombre AS nombreCategoria,
            (
                SELECT precio
                FROM PrecioLibro
                WHERE idLibro = L.id
                ORDER BY fecha DESC
                LIMIT 1 
            ) AS precioActual,
            NOT EXISTS (
                SELECT 1
                FROM Prestamo P
                WHERE P.idLibro = L.id
                AND P.fechaDevolucion IS NULL
            ) AS disponible
        FROM 
            Libro L
        LEFT JOIN
            Categoria C ON L.idCategoria = C.id
        WHERE
            %(i)s = L.id
    """

    print("+-----------------+")
    print("| Consultar libro |")
    print("+-----------------+")

    sid_libro = input("Id del libro: ")
    try:
        id_libro = int(sid_libro)
    except ValueError:
        print("Error: El id del libro debe ser un número entero.")
        return

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_libro
            })
            
            libro = cur.fetchone()
            
            if libro is None:
                print("No se ha encontrado el libro.")
                return
            
            print(f"Id: {id_libro}")
            print(f"Titulo: {libro['titulo']}")
            print(f"Autor: {libro['autor']}")
            print(f"Año de publicación: {libro['anioPublicacion']}")
            print(f"ISBN: {libro['isbn']}")
            print(f"Sinopsis: {libro['sinopsis']}")
            print(f"Id de categoria: {libro['idCategoria']}")
            print(f"Categoria: {libro['nombreCategoria']}")
            print(f"Precio actual: {libro['precioActual'] if libro['precioActual'] is not None else 'Sin precio registrado'} €")
            print(f"Disponibilidad: {"Disponible" if libro['disponible'] else "No disponible"}")
        
        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()


def modificar_libro(conn):
    """
    Modifica los atributos de un libro en la base de datos. Pide al usuario el id del libro y los atributos a modificar.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    sql_update_titulo = """
        UPDATE 
            Libro 
        SET 
            titulo = %(t)s 
        WHERE 
            id = %(i)s
    """

    sql_update_autor = """
        UPDATE 
            Libro 
        SET 
            autor = %(a)s 
        WHERE 
            id = %(i)s
    """

    sql_update_anio_publicacion = """
        UPDATE 
            Libro 
        SET 
            anioPublicacion = %(aP)s 
        WHERE 
            id = %(i)s
    """

    sql_update_isbn = """
        UPDATE 
            Libro 
        SET 
            isbn = %(isbn)s 
        WHERE 
            id = %(i)s
    """

    sql_update_sinopsis = """
        UPDATE 
            Libro 
        SET 
            sinopsis = %(s)s 
        WHERE 
            id = %(i)s
    """

    sql_update_categoria = """
        UPDATE
            Libro
        SET 
            idCategoria = %(iC)s
        WHERE 
            id = %(i)s
    """

    sql_update_precio = """
        INSERT INTO 
            PrecioLibro (idLibro, precio) 
        VALUES 
            (%(i)s, %(p)s) 
    """

    print("+-----------------+")
    print("| Modificar libro |")
    print("+-----------------+")

    sid_libro = input("Id del libro: ").strip()
    try:
        id_libro = int(sid_libro)
    except ValueError:
        print("Error: El id del libro debe ser un número entero.")
        return

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            if input("Modificar título? (s/n): ").strip().lower() == "s":
                stitulo = input("Nuevo titulo: ").strip()
                titulo = None if stitulo == "" else stitulo

                cur.execute(sql_update_titulo, {
                    't': titulo,
                    'i': id_libro
                })

            if input("Modificar autor? (s/n): ").strip().lower() == "s":
                sautor = input("Nuevo autor: ").strip()
                autor = None if sautor == "" else sautor

                cur.execute(sql_update_autor, {
                    'a': autor,
                    'i': id_libro
                })

            if input("Modificar año de publicacion? (s/n):").strip().lower() == "s":
                sanio_publicacion = input("Nuevo año de publicacion: ").strip()
                anio_publicacion = None if sanio_publicacion == "" else int(sanio_publicacion)

                cur.execute(sql_update_anio_publicacion, {
                    'aP': anio_publicacion,
                    'i': id_libro
                })

            if input("Modificar ISBN? (s/n): ").strip().lower() == "s":
                sisbn = input("Nuevo ISBN: ").strip()
                isbn = None if sisbn == "" else sisbn

                cur.execute(sql_update_isbn, {
                    'isbn': isbn,
                    'i': id_libro
                })

            if input("Modificar sinopsis? (s/n): ").strip().lower() == "s":
                ssinopsis = input("Nuevo sinopsis: ").strip()
                sinopsis = None if ssinopsis == "" else ssinopsis

                cur.execute(sql_update_sinopsis, {
                    's': sinopsis,
                    'i': id_libro
                })

            if input("Modificar categoria? (s/n): ").strip().lower() == "s":
                sid_categoria = input("Nuevo id de categoria: ").strip()
                id_categoria = None if sid_categoria == "" else int(sid_categoria)

                cur.execute(sql_update_categoria, {
                    'iC': id_categoria,
                    'i': id_libro
                })

            if input("Modificar precio? (s/n): ").strip().lower() == "s":
                sprecio = input("Nuevo precio: ").strip()
                precio = None if sprecio == "" else float(sprecio)

                cur.execute(sql_update_precio, {
                    'i': id_libro,
                    'p': precio
                })

        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                print("Error: El titulo no puede ser nulo.")
            elif e.pgcode == psycopg2.errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                if e.diag.column_name == "titulo":
                    print("Error: El titulo es demasiado largo.")
                elif e.diag.column_name == "autor":
                    print("Error: El nombre de autor es demasiado largo.")
                elif e.diag.column_name == "isbn":
                    print("Error: El ISBN es demasiado largo.")
                elif e.diag.column_name == "sinopsis":
                    print("Error: La sinopsis es demasiado larga.")
            elif e.pgcode == psycopg2.errorcodes.FOREIGN_KEY_VIOLATION:
                if e.diag.column_name == "idCategoria":
                    print(f"Error: Categoria especificada no existe.")
                elif e.diag.column_name == "idLibro":
                    print(f"Error: Libro especificado no existe.")
            elif e.pgcode == psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE:
                print(f"Error: el precio es demasiado alto.")
            else:
                print_generic_error(e)
            conn.rollback()


def eliminar_libro(conn):
    """
    Elimina un libro de la base de datos. Pide al usuario el id del libro.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED

    sql_sentence = """
        DELETE FROM 
            Libro 
        WHERE 
            id = %(i)s
    """

    print("+----------------+")
    print("| Eliminar libro |")
    print("+----------------+")

    sid_libro = input("Id del libro: ")
    try:
        id_libro = None if sid_libro == "" else int(sid_libro)
    except ValueError:
        print("Error: El id del libro debe ser un número entero.")
        return

    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_libro
            })

            conn.commit()

            if cur.rowcount == 0:
                print(f"El libro con id {id_libro} no existe.")
            else:
                print("Libro eliminado correctamente.")

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()


def actualizar_precio(conn):
    """
    Actualiza el precio de un libro en la base de datos. Pide al usuario el id del libro y el nuevo precio o un porcentaje de aumento/descuento.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    sql_aumento_manual = """
        INSERT INTO 
            PrecioLibro (idLibro, precio) 
        VALUES 
            (%(i)s, %(p)s)
    """

    sql_aumento_porcentaje = """
        INSERT INTO 
            PrecioLibro (idLibro, precio) 
        SELECT 
            %(i)s,
            precio * (1 + %(p)s / 100.0)
        FROM 
            PrecioLibro
        WHERE 
            idLibro = %(i)s
        ORDER BY 
            fecha DESC
        LIMIT 
            1
    """

    print("+----------------------------+")
    print("| Actualizar precio de libro |")
    print("+----------------------------+")


    sid_libro = input("Id del libro: ")
    try:
        id_libro = None if sid_libro == "" else int(sid_libro)
    except ValueError:
        print("Error: El id del librodebe ser un número entero.")
        return


    with conn.cursor() as cur:
        try:
            if input("Aumentar precio con porcentaje? (s/n): ").lower() == "s":
                sporcentaje = input("Porcentaje de aumento/descuento (usar negativo para descuento): ")
                porcentaje = None if sporcentaje == "" else float(sporcentaje)

                cur.execute(sql_aumento_porcentaje, {
                    'i': id_libro,
                    'p': porcentaje
                })

                if cur.fetchone() is None:
                    print("Error: El libro no tiene un precio registrado.")
                    return
            else:
                sprecio = input("Nuevo precio: ")
                precio = None if sprecio == "" else float(sprecio)

                cur.execute(sql_aumento_manual, {
                    'i': id_libro,
                    'p': precio
                })

            conn.commit()

        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                print("Error: El precio no puede ser nulo.")
            elif e.pgcode == psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE:
                print(f"Error: el precio es demasiado alto.")
            elif e.pgcode == psycopg2.errorcodes.FOREIGN_KEY_VIOLATION:
                print(f"Error: Libro especificado no existe.")
            else:
                print_generic_error(e)
            conn.rollback()


def ver_historial_precios(conn):
    """
    Muestra el historial de precios de un libro. Pide al usuario el id del libro.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        SELECT 
            precio,
            fecha
        FROM 
            PrecioLibro
        WHERE   
            idLibro = %(i)s
        ORDER BY 
            fecha DESC
    """

    print("+-----------------------------------+")
    print("| Ver historial de precios de libro |")
    print("+-----------------------------------+")

    sid_libro = input("Id del libro: ")
    try:
        id_libro = None if sid_libro == "" else int(sid_libro)
    except ValueError:
        print("Error: El id del librodebe ser un número entero.")
        return

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_libro,
            })

            precios = cur.fetchall()

            if not precios:
                print("Error: El libro no tiene un precio registrado.")
                return

            for precio in precios:
                print(f"{precio['fecha'].date()} -> {precio['precio']:.2f} €")

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()


def añadir_categoria(conn):
    """
    Añade una categoría a la base de datos. Pide al usuario los datos necesarios.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED

    sql_sentence = """
       INSERT INTO 
           Categoria (nombre, descripcion))
       VALUES 
           (%(n)s, %(d)s)
    """

    print("+------------------+")
    print("| Añadir categoria |")
    print("+------------------+")

    snombre = input("Nombre: ")
    nombre = None if snombre == "" else snombre

    sdescripcion = input("Descripción: ")
    descripcion = None if sdescripcion == "" else sdescripcion

    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'n': nombre,
                'd': descripcion,
            })

            conn.commit()
            print("Categoría añadida correctamente.")

        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                print("Error: Una categoría con ese nombre ya existe.")
            elif e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                if e.diag.column_name == "nombre":
                    print("Error: El nombre no puede ser nulo.")
                elif e.diag.column_name == "descripcion":
                    print("Error: La descripcion no puede ser nula.")
            elif e.pgcode == psycopg2.errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                if e.diag.column_name == "nombre":
                    print("Error: El nombre es demasiado largo.")
                elif e.diag.column_name == "descripcion":
                    print("Error: La descripcion es demasiado larga.")
            else:
                print_generic_error(e)
            conn.rollback()


def modificar_categoria(conn):
    """
    Modifica los atributos de una categoria en la base de datos. Pide al usuario el id de la categoria y los atributos a modificar.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    sql_update_nombre = """
        UPDATE 
            Categoria 
        SET 
            nombre = %(n)s 
        WHERE 
            id = %(i)s
    """

    sql_update_descripcion = """
        UPDATE 
            Categoria  
        SET 
            descripcion = %(d)s 
        WHERE 
            id = %(i)s
    """

    print("+---------------------+")
    print("| Modificar categoria |")
    print("+---------------------+")

    sid_categoria = input("Id de la categoria: ").strip()
    try:
        id_categoria = int(sid_categoria)
    except ValueError:
        print("Error: El id de la categoria debe ser un número entero.")
        return

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            if input("Modificar nombre? (s/n): ").strip().lower() == "s":
                snombre = input("Nuevo titulo: ").strip()
                nombre = None if snombre == "" else snombre

                cur.execute(sql_update_nombre, {
                    'n': nombre,
                    'i': id_categoria
                })

            if input("Modificar descripcion? (s/n): ").strip().lower() == "s":
                sdescripcion = input("Nuevo autor: ").strip()
                descripcion = None if sdescripcion == "" else sdescripcion

                cur.execute(sql_update_descripcion, {
                    'd': descripcion,
                    'i': id_categoria
                })

        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                if e.diag.column_name == "nombre":
                    print("Error: El nombre no puede ser nulo.")
                elif e.diag.column_name == "descripcion":
                    print("Error: La descripcion no puede ser nula.")
            elif e.pgcode == psycopg2.errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                if e.diag.column_name == "nombre":
                    print("Error: El nombre es demasiado largo.")
                elif e.diag.column_name == "descripcion":
                    print("Error: La descripcion es demasiado larga.")
            else:
                print_generic_error(e)
            conn.rollback()


def eliminar_libro(conn):
    """
    Elimina una categoria de la base de datos. Pide al usuario el id de la categoria.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED

    sql_sentence = """
        DELETE FROM 
            Categoria 
        WHERE 
            id = %(i)s
    """

    print("+--------------------+")
    print("| Eliminar categoria |")
    print("+--------------------+")

    sid_categoria = input("Id de la categoria: ")
    try:
        id_categoria = int(sid_categoria)
    except ValueError:
        print("Error: El id de la categoria debe ser un número entero.")
        return

    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_categoria
            })

            conn.commit()

            if cur.rowcount == 0:
                print(f"La categoria con id {id_categoria} no existe.")
            else:
                print("Categoria eliminada correctamente.")

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()


def efectuar_prestamo(conn):
    """
    Añade un prestamo a la base de datos. Pide al usuario los datos necesarios.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED

    sql_sentence = """
        INSERT INTO Prestamo (fechaPrestamo, comentarios, idLibro, idEstudiante)
        VALUES (NOW(), %(c)s, %(iL)s, %(iE)s)
    """

    print("+-------------------+")
    print("| Efectuar prestamo |")
    print("+-------------------+")

    scomentarios = input("Comentarios: ").strip()
    comentarios = None if scomentarios == "" else scomentarios

    sid_libro = input("Año de publicación: ").strip()
    try:
        id_libro = None if sid_libro == "" else int(sid_libro)
    except ValueError:
        print("Error: El id del libro debe ser un número entero.")
        return

    sid_estudiante = input("Año de publicación: ").strip()
    try:
        id_estudiante = None if sid_estudiante == "" else int(sid_estudiante)
    except ValueError:
        print("Error: El id del estudiante debe ser un número entero.")
        return

    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'c': comentarios,
                'iL': id_libro,
                'iE': id_estudiante,
            })

            conn.commit()
            print("Prestamo efectuado correctamente.")

        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.FOREIGN_KEY_VIOLATION:
                if e.diag.column_name == "idLibro":
                    print(f"Error: Libro especificado no existe.")
                elif e.diag.column_name == "idEstudiante":
                    print(f"Error: Estudiante especificado no existe.")
            elif e.pgcode == psycopg2.errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                print("Error: El comentario es demasiado largo.")
            else:
                print_generic_error(e)
            conn.rollback()


def ver_historial_prestamos_libro(conn):
    """
    Muestra el historial de prestamos de un libro. Pide al usuario el id del libro.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        SELECT 
            P.fechaPrestamo, 
            P.fechaDevolucion, 
            P.comentarios, 
            P.idEstudiante,
            E.nombre as nombreEstudiante
        FROM 
            Prestamo P
        JOIN 
            Estudiante E ON E.id = P.idEstudiante
        WHERE   
            P.idLibro = %(i)s
        ORDER BY 
            P.fechaPrestamo DESC
    """

    print("+-------------------------------------+")
    print("| Ver historial de prestamos de libro |")
    print("+-------------------------------------+")

    sid_libro = input("Id del libro: ")
    try:
        id_libro = None if sid_libro == "" else int(sid_libro)
    except ValueError:
        print("Error: El id del libro debe ser un número entero.")
        return

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_libro,
            })

            prestamos = cur.fetchall()

            if not prestamos:
                print("Error: El libro no tiene prestamos registrados.")
                return

            for prestamo in prestamos:
                print(f"Fecha de prestamo: {prestamo['fechaPrestamo'].date()}")
                print(f"Fecha de devolucion: {prestamo['fechaDevolucion'].date() if prestamo['fechaDevolucion'] else 'No devuelto'}")
                print(f"Comentarios: {prestamo['comentarios']}")
                print(f"Id del estudiante: {prestamo['idEstudiante']}")
                print(f"Estudiante: {prestamo['nombreEstudiante']}")

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()


def ver_historial_prestamos_estudiante(conn):
    """
    Muestra el historial de prestamos de un estudiante. Pide al usuario el id del estudiante.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        SELECT 
            P.fechaPrestamo, 
            P.fechaDevolucion, 
            P.comentarios, 
            P.idLibro,
            L.libro as nombreLibro
        FROM 
            Prestamo P
        JOIN 
            Libro L ON L.id = P.idLibro
        WHERE   
            P.idEstudiante = %(i)s
        ORDER BY 
            P.fechaPrestamo DESC
    """

    print("+------------------------------------------+")
    print("| Ver historial de prestamos de estudiante |")
    print("+------------------------------------------+")

    sid_estudiante = input("Id del estudiante: ")
    try:
        id_estudiante = None if sid_estudiante == "" else int(sid_estudiante)
    except ValueError:
        print("Error: El id del estudiante debe ser un número entero.")
        return

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_estudiante,
            })

            prestamos = cur.fetchall()

            if not prestamos:
                print("Error: El estudiante no tiene prestamos registrados.")
                return

            for prestamo in prestamos:
                print(f"Fecha de prestamo: {prestamo['fechaPrestamo'].date()}")
                print(f"Fecha de devolucion: {prestamo['fechaDevolucion'].date() if prestamo['fechaDevolucion'] else 'No devuelto'}")
                print(f"Comentarios: {prestamo['comentarios']}")
                print(f"Id del libro: {prestamo['idLibro']}")
                print(f"Libro: {prestamo['nombreLibro']}")

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()


def consultar_prestamo(conn):
    """
    Consulta un prestamo en la base de datos. Pide al usuario el id del prestamo.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED

    sql_sentence = """
        SELECT 
            P.fechaPrestamo, 
            P.fechaDevolucion, 
            P.comentarios, 
            P.idLibro,
            L.nombre AS nombreLibro,
            P.idEstudiante,
            E.nombre AS nombreEstudiante
        FROM 
            Prestamo P
        JOIN 
            Libro L ON L.id = P.idLibro
        JOIN 
            Estudiante E ON E.id = P.idEstudiante
        WHERE 
            P.id = %(i)s
    """

    print("+--------------------+")
    print("| Consultar prestamo |")
    print("+--------------------+")

    sid_prestamo = input("Id del prestamo: ")
    try:
        id_prestamo = int(sid_prestamo)
    except ValueError:
        print("Error: El id del prestamo debe ser un número entero.")
        return

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_prestamo,
            })

            prestamo = cur.fetchone()

            if prestamo is None:
                print("No se ha encontrado el prestamo.")
                return

            print(f"Id: {id_prestamo}")
            print(f"Fecha del prestamo: {prestamo['fechaPrestamo'].date()}")
            print(f"Fecha de devolucion: {prestamo['fechaDevolucion'].date() if prestamo['fechaDevolucion'] else 'No devuelto'}")
            print(f"Comentarios: {prestamo['comentarios']}")
            print(f"Id del libro: {prestamo['idLibro']}")
            print(f"Libro: {prestamo['nombreLibro']}")
            print(f"Id del estudiante: {prestamo['idEstudiante']}")
            print(f"Estudiante: {prestamo['nombreEstudiante']}")

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()


