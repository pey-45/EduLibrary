import sys
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.errorcodes

def print_generic_error(e): print(f"Error: {getattr(e, 'pgcode', 'Unknown')} - {getattr(e, 'pgerror', 'Unknown error')}")


def connect_db():
     """
     Establece una conexión con la base de datos predeterminada del usuario (usando DNS vacío).
     :return: La conexión establecida con la base de datos. Si no se puede establecer, el programa termina.
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
    Cierra la conexión con la base de datos, realizando antes un commit de la transacción activa.
    :param conn: La conexión activa con la base de datos
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
    Añade un nuevo libro a la biblioteca solicitando al usuario todos los datos necesarios.
    :param conn: La conexión activa con la base de datos
    :return: None
    """

    sql_sentence = """
        INSERT INTO 
            Libro (titulo, autor, anioPublicacion, isbn, sinopsis, idCategoria) 
        VALUES 
            (%(t)s, %(a)s, %(aP)s, %(i)s, %(s)s, %(iC)s)
    """


    print("+--------------+")
    print("| Anadir libro |")
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
            
# 2
def buscar_libros(conn):
    """
    Realiza una búsqueda de libros en la biblioteca según los criterios especificados por el usuario.
    :param conn: La conexión activa con la base de datos
    :return: None
    """

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
                print(f"Disponibilidad: {'Disponible' if libro['disponible'] else 'No disponible'}")
        
        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()
            
# 3
def consultar_libro(conn):
    """
    Muestra toda la información disponible de un libro específico según su ID.
    :param conn: La conexión activa con la base de datos
    :return: None
    """

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
            L.id = %(i)s
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
            print(f"Disponibilidad: {'Disponible' if libro['disponible'] else 'No disponible'}")
        
        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()

# 4
def modificar_libro(conn):
    """
    Permite actualizar la información de un libro existente en la biblioteca.
    :param conn: La conexión activa con la base de datos
    :return: None
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

            if input("Modificar anio de publicacion? (s/n):").strip().lower() == "s":
                sanio_publicacion = input("Nuevo anio de publicacion: ").strip()
                try:
                    anio_publicacion = None if sanio_publicacion == "" else int(sanio_publicacion)
                except ValueError:
                    print("Error: El anio de publicacion debe ser un número entero.")
                    return

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
                ssinopsis = input("Nueva sinopsis: ").strip()
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

            conn.commit()

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

# 5
def eliminar_libro(conn):
    """
    Elimina permanentemente un libro de la biblioteca según su ID.
    :param conn: La conexión activa con la base de datos
    :return: None
    """

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

            if cur.rowcount == 0:
                print(f"El libro con id {id_libro} no existe.")
                conn.rollback()
            else:
                print("Libro eliminado correctamente.")
                conn.commit()

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()

# 6
def actualizar_precio(conn):
    """
    Actualiza el precio de un libro, permitiendo especificar un nuevo valor o aplicar un porcentaje de variación.
    :param conn: La conexión activa con la base de datos
    :return: None
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

            else:
                sprecio = input("Nuevo precio: ")
                precio = None if sprecio == "" else float(sprecio)

                cur.execute(sql_aumento_manual, {
                    'i': id_libro,
                    'p': precio
                })

            print("Precio actualizado correctamente.")
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

# 7
def ver_historial_precios(conn):
    """
    Muestra un listado cronológico de todos los precios que ha tenido un libro específico.
    :param conn: La conexión activa con la base de datos
    :return: None
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

# 8
def añadir_categoria(conn):
    """
    Anade una categoria a la base de datos. Pide al usuario los datos necesarios.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    sql_sentence = """
       INSERT INTO 
           Categoria (nombre, descripcion)
       VALUES 
           (%(n)s, %(d)s)
    """

    print("+------------------+")
    print("| Anadir categoria |")
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

# 9
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
                snombre = input("Nuevo nombre: ").strip()
                nombre = None if snombre == "" else snombre

                cur.execute(sql_update_nombre, {
                    'n': nombre,
                    'i': id_categoria
                })

            if input("Modificar descripcion? (s/n): ").strip().lower() == "s":
                sdescripcion = input("Nueva descripcion: ").strip()
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

# 10
def eliminar_categoria(conn):
    """
    Elimina una categoria de la base de datos. Pide al usuario el id de la categoria.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

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

            if cur.rowcount == 0:
                print(f"La categoria con id {id_categoria} no existe.")
                conn.rollback()
            else:
                print("Categoria eliminada correctamente.")
                conn.commit()

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()

# 11
def efectuar_prestamo(conn):
    """
    Añade un prestamo a la base de datos. Pide al usuario los datos necesarios.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        INSERT INTO Prestamo (fechaPrestamo, comentarios, idLibro, idEstudiante)
        VALUES (NOW(), %(c)s, %(iL)s, %(iE)s)
    """

    print("+-------------------+")
    print("| Efectuar prestamo |")
    print("+-------------------+")

    scomentarios = input("Comentarios: ").strip()
    comentarios = None if scomentarios == "" else scomentarios

    sid_libro = input("Id del libro: ").strip()
    try:
        id_libro = None if sid_libro == "" else int(sid_libro)
    except ValueError:
        print("Error: El id del libro debe ser un número entero.")
        return

    sid_estudiante = input("Id del estudiante: ").strip()
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

# 12
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

# 13
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
            L.titulo as nombreLibro
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

# 14
def consultar_prestamo(conn):
    """
    Consulta un prestamo en la base de datos. Pide al usuario el id del prestamo.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        SELECT 
            P.fechaPrestamo, 
            P.fechaDevolucion, 
            P.comentarios, 
            P.idLibro,
            L.titulo AS nombreLibro,
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

# 15
def finalizar_prestamo(conn):
    """
    Finaliza un prestamo en la base de datos. Pide al usuario el id del prestamo.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    sql_sentence = """
        UPDATE 
            Prestamo
        SET
            fechaDevolucion = NOW()
        WHERE
            id = %(i)s
    """

    print("+--------------------+")
    print("| Finalizar prestamo |")
    print("+--------------------+")

    sid_prestamo = input("Id del prestamo: ")
    try:
        id_prestamo = int(sid_prestamo)
    except ValueError:
        print("Error: El id del prestamo debe ser un número entero.")
        return

    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_prestamo,
            })

            if cur.rowcount == 0:
                print("No se ha encontrado el préstamo.")
                conn.rollback()
                return

            conn.commit()
            print("Prestamo finalizado correctamente.")

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()

# 16
def eliminar_prestamo(conn):
    """
    Elimina un prestamo de la base de datos. Pide al usuario el id del prestamo.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        DELETE FROM 
            Prestamo 
        WHERE 
            id = %(i)s
    """

    print("+-------------------+")
    print("| Eliminar prestamo |")
    print("+-------------------+")

    sid_prestamo = input("Id del prestamo: ")
    try:
        id_prestamo = None if sid_prestamo == "" else int(sid_prestamo)
    except ValueError:
        print("Error: El id del prestamo debe ser un número entero.")
        return

    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_prestamo,
            })

            if cur.rowcount == 0:
                print(f"El prestamo con id {id_prestamo} no existe.")
                conn.rollback()
            else:
                print("Prestamo eliminado correctamente.")
                conn.commit()

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()

# 17
def anadir_estudiante(conn):
    """
    Añade un estudiante a la base de datos. Pide al usuario los datos necesarios.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    sql_sentence = """
       INSERT INTO Estudiante (nombre, apellidos, curso, email, telefono)
       VALUES (%(n)s, %(a)s, %(c)s, %(e)s, %(t)s)
    """

    print("+-------------------+")
    print("| Añadir estudiante |")
    print("+-------------------+")

    snombre = input("Nombre: ").strip()
    nombre = None if snombre == "" else snombre

    sapellidos = input("Apellidos: ").strip()
    apellidos = None if sapellidos == "" else sapellidos

    scurso = input("Curso: ").strip()
    try:
        curso = None if scurso == "" else int(scurso)
    except ValueError:
        print("Error: El curso debe ser un número entero.")
        return

    semail = input("Email: ").strip()
    email = None if semail == "" else semail

    stelefono = input("Telefono (+XX XXX XX XX XX): ").strip()
    telefono = None if stelefono == "" else stelefono

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
            print("Estudiante añadido correctamente.")

        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                if e.diag.column_name == "email":
                    print("Error: El email ya existe.")
                elif e.diag.column_name == "telefono":
                    print("Error: El telefono ya existe.")
            elif e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                if e.diag.column_name == "nombre":
                    print("Error: El nombre no puede ser nulo.")
                elif e.diag.column_name == "apellidos":
                    print("Error: Los apellidos no pueden ser nulos.")
                elif e.diag.column_name == "curso":
                    print("Error: El curso no puede ser nulo.")
                elif e.diag.column_name == "email":
                    print("Error: El email no puede ser nulo.")
            elif e.pgcode == psycopg2.errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                if e.diag.column_name == "nombre":
                    print("Error: El nombre es demasiado largo.")
                elif e.diag.column_name == "apellidos":
                    print("Error: Los apellidos son demasiado largos.")
                elif e.diag.column_name == "email":
                    print("Error: El email es demasiado largo.")
                elif e.diag.column_name == "telefono":
                    print("Error: El telefono es demasiado largo.")
            else:
                print_generic_error(e)
            conn.rollback()

# 18
def consultar_estudiante(conn):
    """
    Consulta un estudiante en la base de datos. Pide al usuario el id del estudiante.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        SELECT 
            E.nombre,
            E.apellidos,
            E.curso,
            E.email,
            E.telefono,
            (
                SELECT count(*) 
                FROM Prestamo P 
                WHERE P.idEstudiante = E.id
                AND P.fechaDevolucion IS NULL
            ) as librosEnPosesion
        FROM
            Estudiante E
        WHERE
             E.id = %(i)s
    """

    print("+----------------------+")
    print("| Consultar estudiante |")
    print("+----------------------+")

    sid_estudiante = input("Id del estudiante: ")
    try:
        id_estudiante = int(sid_estudiante)
    except ValueError:
        print("Error: El id del estudiante debe ser un número entero.")
        return

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_estudiante,
            })

            estudiante = cur.fetchone()

            if estudiante is None:
                print("No se ha encontrado el estudiante.")
                return

            print(f"Id: {id_estudiante}")
            print(f"Nombre: {estudiante['nombre']}")
            print(f"Apellidos: {estudiante['apellidos']}")
            print(f"Curso: {estudiante['curso']}")
            print(f"Email: {estudiante['email']}")
            print(f"Telefono: {estudiante['telefono']}")
            print(f"Libros en posesion: {estudiante['librosEnPosesion']}")

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()

# 19
def aumentar_curso(conn):
    """
    Aumenta el curso de uno o varios estudiantes. Pide al usuario los datos necesarios.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    sql_sentence = """
        UPDATE
            Estudiante
        SET 
            curso = curso + 1
        WHERE 
            id = %(i)s
    """

    print("+-------------------------------+")
    print("| Aumentar curso de estudiantes |")
    print("+-------------------------------+")


    id_estudiantes = []

    while True:
        sid_estudiante = input("Id del estudiante (o 'enter' para terminar): ").strip()
        if sid_estudiante == "":
            break
        try:
            id_estudiantes.append(int(sid_estudiante))
        except ValueError:
            print("El id del estudiante debe ser un número entero.")
            continue

    with conn.cursor() as cur:
        try:
            actualizados = 0

            for id_estudiante in id_estudiantes:
                cur.execute(sql_sentence, {
                    'i': id_estudiante,
                })

                if cur.rowcount == 1:
                    actualizados += 1
                else:
                    print(f"El estudiante con id {id_estudiante} no existe.")  # Continuamos igualmente

            print(f"Curso aumentado correctamente a {actualizados} estudiantes.")
            conn.commit()

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()

# 20
def modificar_estudiante(conn):
    """
    Modifica los atributos de un estudiante en la base de datos. Pide al usuario el id del estudiante y los atributos a modificar.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    sql_update_nombre = """
        UPDATE 
            Estudiante 
        SET 
            nombre = %(n)s 
        WHERE 
            id = %(i)s
    """

    sql_update_apellidos = """
        UPDATE 
            Estudiante 
        SET 
            apellidos = %(a)s 
        WHERE 
            id = %(i)s
    """

    sql_update_curso = """
        UPDATE 
            Estudiante 
        SET 
            curso = %(c)s 
        WHERE 
            id = %(i)s
    """

    sql_update_email = """
        UPDATE 
            Estudiante 
        SET 
            email = %(e)s 
        WHERE 
            id = %(i)s
    """

    sql_update_telefono = """
        UPDATE 
            Estudiante  
        SET 
            telefono = %(t)s 
        WHERE 
            id = %(i)s
    """

    print("+----------------------+")
    print("| Modificar estudiante |")
    print("+----------------------+")

    sid_estudiante = input("Id del estudiante: ").strip()
    try:
        id_estudiante = int(sid_estudiante)
    except ValueError:
        print("Error: El id del estudiante debe ser un número entero.")
        return

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            if input("Modificar nombre? (s/n): ").strip().lower() == "s":
                snombre = input("Nuevo nombre: ").strip()
                nombre = None if snombre == "" else snombre

                cur.execute(sql_update_nombre, {
                    'n': nombre,
                    'i': id_estudiante,
                })

            if input("Modificar apellidos? (s/n): ").strip().lower() == "s":
                sapellidos = input("Nuevos apellidos: ").strip()
                apellidos = None if sapellidos == "" else sapellidos

                cur.execute(sql_update_apellidos, {
                    'a': apellidos,
                    'i': id_estudiante,
                })

            if input("Modificar curso? (s/n):").strip().lower() == "s":
                scurso = input("Nuevo curso: ").strip()
                try:
                    curso = None if scurso == "" else int(scurso)
                except ValueError:
                    print("Error: El curso debe ser un número entero.")
                    return

                cur.execute(sql_update_curso, {
                    'c': curso,
                    'i': id_estudiante,
                })

            if input("Modificar email? (s/n): ").strip().lower() == "s":
                semail = input("Nuevo email: ").strip()
                email = None if semail == "" else semail

                cur.execute(sql_update_email, {
                    'e': email,
                    'i': id_estudiante,
                })

            if input("Modificar telefono? (s/n): ").strip().lower() == "s":
                stelefono = input("Nuevo telefono: ").strip()
                telefono = None if stelefono == "" else stelefono

                cur.execute(sql_update_telefono, {
                    't': telefono,
                    'i': id_estudiante,
                })

            conn.commit()

        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                if e.diag.column_name == "nombre":
                    print("Error: El nombre no puede ser nulo.")
                elif e.diag.column_name == "apellidos":
                    print("Error: Los apellidos no pueden ser nulos.")
                elif e.diag.column_name == "curso":
                    print("Error: El curso no puede ser nulo.")
                elif e.diag.column_name == "email":
                    print("Error: El email no puede ser nulo.")
            elif e.pgcode == psycopg2.errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                if e.diag.column_name == "nombre":
                    print("Error: El nombre es demasiado largo.")
                elif e.diag.column_name == "apellidos":
                    print("Error: Los apellidos son demasiado largos.")
                elif e.diag.column_name == "email":
                    print("Error: El email es demasiado largo.")
                elif e.diag.column_name == "telefono":
                    print("Error: El telefono es demasiado largo.")
            elif e.pgcode == psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE:
                print("Error: El curso es demasiado grande.")
            else:
                print_generic_error(e)
            conn.rollback()

# 21
def eliminar_estudiante(conn):
    """
    Elimina un estudiante de la base de datos. Pide al usuario el id del estudiante.
    :param conn: La conexión abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        DELETE FROM 
            Estudiante  
        WHERE 
            id = %(i)s
    """

    print("+---------------------+")
    print("| Eliminar estudiante |")
    print("+---------------------+")

    sid_estudiante = input("Id del estudiante: ")
    try:
        id_estudiante = int(sid_estudiante)
    except ValueError:
        print("Error: El id del estudiante debe ser un número entero.")
        return

    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_estudiante,
            })

            if cur.rowcount == 0:
                print(f"El estudiante con id {id_estudiante} no existe.")
                conn.rollback()
            else:
                print("Estudiante eliminado correctamente.")
                conn.commit()

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()


def menu(conn):
    """
    Imprime un menú de opciones, solicita la opción y ejecuta la función asociada.
    'q' para salir.
    """

    menu_text = """
                +------+
                | MENÚ |
                +------+
              
        [LIBROS]
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
        print(menu_text)
        tecla = input("Opcion> ")
        if tecla == 'q':
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
            añadir_categoria(conn)
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