import sys
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.errorcodes

def print_generic_error(e): print(f"Error: {getattr(e, 'pgcode', 'Unknown')} - {getattr(e, 'pgerror', 'Unknown error')}")


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
        insert into 
            libro (titulo, autor, aniopublicacion, isbn, sinopsis, idcategoria)
        values
            (%(t)s, %(a)s, %(ap)s, %(i)s, %(s)s, %(ic)s)
    """

    print("+--------------+")
    print("| Anadir libro |")
    print("+--------------+")

    stitulo = input("Titulo: ").strip()
    titulo = None if stitulo == "" else stitulo

    sautor = input("Autor: ").strip()
    autor = None if sautor == "" else sautor

    sanio_publicacion = input("Anio de publicacion: ").strip()
    try:
        anio_publicacion = None if sanio_publicacion == "" else int(sanio_publicacion)
    except ValueError:
        print("Error: El anio de publicacion debe ser un numero entero.")
        return

    sisbn = input("ISBN: ").strip()
    isbn = None if sisbn == "" else sisbn

    ssinopsis = input("Sinopsis: ").strip()
    sinopsis = None if ssinopsis == "" else ssinopsis

    sid_categoria = input("Id de categoria: ").strip()
    try:
        id_categoria = None if sid_categoria == "" else int(sid_categoria)
    except ValueError:
        print("Error: El id de categoria debe ser un numero entero.")
        return

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
            print("Libro anadido correctamente.")
            
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
    Realiza una busqueda de libros en la biblioteca segun los criterios especificados por el usuario.
    :param conn: La conexion activa con la base de datos
    :return: None
    """

    sql_sentence = """
        select 
            l.id,
            l.titulo,
            l.autor,
            l.aniopublicacion,
            l.isbn,
            c.nombre as nombrecategoria,
            (
                select precio
                from preciolibro
                where idlibro = l.id
                order by fecha desc
                limit 1 
            ) as precioactual,
            not exists (
                select 1
                from prestamo p
                where p.idlibro = l.id 
                and p.fechadevolucion is null
            ) as disponible
        from
            libro l
        left join
            categoria c on l.idcategoria = c.id
        where
            (%(t)s is null
            or l.titulo ilike %(t0)s)
            and (%(a)s is null
            or l.autor ilike %(a0)s)
            and (%(ap)s is null
            or l.aniopublicacion = %(ap)s)
            and (%(i)s is null
            or l.isbn ilike %(i0)s)
            and (%(ic)s is null
            or l.idcategoria = %(ic)s) 
    """

    print("+--------------+")
    print("| Buscar libro |")
    print("+--------------+")

    stitulo = input("Titulo: ")
    titulo = None if stitulo == "" else stitulo

    sautor = input("Autor: ")
    autor = None if sautor == "" else sautor

    sanio_publicacion = input("Anio de publicacion: ")
    try:
        anio_publicacion = None if sanio_publicacion == "" else int(sanio_publicacion)
    except ValueError:
        print("Error: El anio de publicacion debe ser un numero entero.")
        return

    sisbn = input("ISBN: ")
    isbn = None if sisbn == "" else sisbn

    sid_categoria = input("Id de categoria: ")
    try:
        id_categoria = None if sid_categoria == "" else int(sid_categoria)
    except ValueError:
        print("Error: El id de categoria debe ser un numero entero.")
        return

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(sql_sentence,{
                't': titulo,
                't0': f"%{titulo}%" if titulo is not None else None,
                'a': autor,
                'a0': f"%{autor}%" if autor is not None else None,
                'ap': anio_publicacion,
                'i': isbn,
                'i0': f"%{isbn}%" if isbn is not None else None,
                'ic': id_categoria
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
                print(f"Anio de publicacion: {libro['aniopublicacion']}")
                print(f"ISBN: {libro['isbn']}")
                print(f"Categoria: {libro['nombrecategoria']}")
                print(
                    f"Precio actual: {libro['precioactual'] if libro['precioactual'] is not None else 'Sin precio registrado'} €")
                print(f"Disponibilidad: {'Disponible' if libro['disponible'] else 'No disponible'}")
        
        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()
            
# 3
def consultar_libro(conn):
    """
    Muestra toda la informacion disponible de un libro especifico segun su ID.
    :param conn: La conexion activa con la base de datos
    :return: None
    """

    sql_sentence = """
        select 
            l.titulo,
            l.autor,
            l.aniopublicacion,
            l.isbn,
            l.sinopsis,
            l.idcategoria,
            c.nombre as nombrecategoria,
            (
                select precio
                from preciolibro
                where idlibro = l.id
                order by fecha desc
                limit 1 
            ) as precioactual,
            not exists (
                select 1
                from prestamo p
                where p.idlibro = l.id
                and p.fechadevolucion is null
            ) as disponible
        from
            libro l
        left join
            categoria c on l.idcategoria = c.id
        where
            l.id = %(i)s
    """

    print("+-----------------+")
    print("| Consultar libro |")
    print("+-----------------+")

    sid_libro = input("Id del libro: ")
    try:
        id_libro = int(sid_libro)
    except ValueError:
        print("Error: El id del libro debe ser un numero entero.")
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
            print(f"Anio de publicacion: {libro['aniopublicacion']}")
            print(f"ISBN: {libro['isbn']}")
            print(f"Sinopsis: {libro['sinopsis']}")
            print(f"Id de categoria: {libro['idcategoria']}")
            print(f"Categoria: {libro['nombrecategoria']}")
            print(
                f"Precio actual: {libro['precioactual'] if libro['precioactual'] is not None else 'Sin precio registrado'} €")
            print(f"Disponibilidad: {'Disponible' if libro['disponible'] else 'No disponible'}")
        
        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()

# 4
def modificar_libro(conn):
    """
    Permite actualizar la informacion de un libro existente en la biblioteca.
    :param conn: La conexion activa con la base de datos
    :return: None
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    sql_update_titulo = """
        update
            libro
        set 
            titulo = %(t)s
        where 
            id = %(i)s
    """

    sql_update_autor = """
        update
            libro
        set 
            autor = %(a)s
        where 
            id = %(i)s
    """

    sql_update_anio_publicacion = """
        update
            libro
        set     
            aniopublicacion = %(aP)s
        where 
            id = %(i)s
    """

    sql_update_isbn = """
        update
            libro
        set 
            isbn = %(isbn)s
        where 
            id = %(i)s
    """

    sql_update_sinopsis = """
        update
            libro
        set
            sinopsis = %(s)s
        where 
            id = %(i)s
    """

    sql_update_categoria = """
        update
            libro
        set 
            idcategoria = %(iC)s
        where 
            id = %(i)s
    """

    sql_update_precio = """
        insert into 
            preciolibro (idlibro, precio)
        values 
            (%(i)s, %(p)s) 
    """

    print("+-----------------+")
    print("| Modificar libro |")
    print("+-----------------+")

    sid_libro = input("Id del libro: ").strip()
    try:
        id_libro = int(sid_libro)
    except ValueError:
        print("Error: El id del libro debe ser un numero entero.")
        return

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            if input("Modificar titulo? (s/n): ").strip().lower() == "s":
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
                    print("Error: El anio de publicacion debe ser un numero entero.")
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
    Elimina permanentemente un libro de la biblioteca segun su ID.
    :param conn: La conexion activa con la base de datos
    :return: None
    """

    sql_sentence = """
        delete
        from 
            libro
        where 
            id = %(i)s
    """

    print("+----------------+")
    print("| Eliminar libro |")
    print("+----------------+")

    sid_libro = input("Id del libro: ")
    try:
        id_libro = None if sid_libro == "" else int(sid_libro)
    except ValueError:
        print("Error: El id del libro debe ser un numero entero.")
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
    Actualiza el precio de un libro, permitiendo especificar un nuevo valor o aplicar un porcentaje de variacion.
    :param conn: La conexion activa con la base de datos
    :return: None
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    sql_aumento_manual = """
        insert into 
            preciolibro (idlibro, precio)
        values  
            (%(i)s, %(p)s)
    """

    sql_aumento_porcentaje = """
        insert into 
            preciolibro (idlibro, precio)
        select 
            %(i)s,
            precio * (1 + %(p)s / 100.0)
        from 
            preciolibro
        where 
            idlibro = %(i)s
        order by 
            fecha desc 
        limit 
            1
    """

    print("+----------------------------+")
    print("| Actualizar precio de libro |")
    print("+----------------------------+")

    sid_libro = input("Id del libro: ")
    try:
        id_libro = None if sid_libro == "" else int(sid_libro)
    except ValueError:
        print("Error: El id del librodebe ser un numero entero.")
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
    Muestra un listado cronologico de todos los precios que ha tenido un libro especifico.
    :param conn: La conexion activa con la base de datos
    :return: None
    """

    sql_sentence = """
    select 
        precio,
        fecha
    from 
        preciolibro
    where 
        idlibro = %(i)s
    order by 
        fecha desc
    """

    print("+-----------------------------------+")
    print("| Ver historial de precios de libro |")
    print("+-----------------------------------+")

    sid_libro = input("Id del libro: ")
    try:
        id_libro = None if sid_libro == "" else int(sid_libro)
    except ValueError:
        print("Error: El id del librodebe ser un numero entero.")
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
def anadir_categoria(conn):
    """
    Anade una categoria a la base de datos. Pide al usuario los datos necesarios.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        insert into 
            categoria (nombre, descripcion)
        values 
            (%(n)s, %(d)s)
    """

    print("+------------------+")
    print("| Anadir categoria |")
    print("+------------------+")

    snombre = input("Nombre: ")
    nombre = None if snombre == "" else snombre

    sdescripcion = input("Descripcion: ")
    descripcion = None if sdescripcion == "" else sdescripcion

    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'n': nombre,
                'd': descripcion,
            })

            conn.commit()
            print("Categoria anadida correctamente.")

        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                print("Error: Una categoria con ese nombre ya existe.")
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
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    sql_update_nombre = """
        update 
            categoria 
        set 
            nombre = %(n)s 
        where 
            id = %(i)s
    """

    sql_update_descripcion = """
        update 
            categoria  
        set 
            descripcion = %(d)s 
        where 
            id = %(i)s
    """

    print("+---------------------+")
    print("| Modificar categoria |")
    print("+---------------------+")

    sid_categoria = input("Id de la categoria: ").strip()
    try:
        id_categoria = int(sid_categoria)
    except ValueError:
        print("Error: El id de la categoria debe ser un numero entero.")
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
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        delete from
            categoria 
        where
            id = %(i)s
    """

    print("+--------------------+")
    print("| Eliminar categoria |")
    print("+--------------------+")

    sid_categoria = input("Id de la categoria: ")
    try:
        id_categoria = int(sid_categoria)
    except ValueError:
        print("Error: El id de la categoria debe ser un numero entero.")
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
    Anade un prestamo a la base de datos. Pide al usuario los datos necesarios.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        insert into prestamo (fechaprestamo, comentarios, idlibro, idestudiante)
        values (now(), %(c)s, %(il)s, %(ie)s)
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
        print("Error: El id del libro debe ser un numero entero.")
        return

    sid_estudiante = input("Id del estudiante: ").strip()
    try:
        id_estudiante = None if sid_estudiante == "" else int(sid_estudiante)
    except ValueError:
        print("Error: El id del estudiante debe ser un numero entero.")
        return

    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'c': comentarios,
                'il': id_libro,
                'ie': id_estudiante,
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
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        select 
            p.fechaprestamo, 
            p.fechadevolucion, 
            p.comentarios, 
            p.idestudiante,
            e.nombre as nombreEstudiante
        from 
            prestamo p
        join 
            estudiante e on e.id = p.idestudiante
        where   
            p.idlibro = %(i)s
        order by 
            p.fechaprestamo desc
    """

    print("+-------------------------------------+")
    print("| Ver historial de prestamos de libro |")
    print("+-------------------------------------+")

    sid_libro = input("Id del libro: ")
    try:
        id_libro = None if sid_libro == "" else int(sid_libro)
    except ValueError:
        print("Error: El id del libro debe ser un numero entero.")
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
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        select 
            p.fechaprestamo, 
            p.fechadevolucion, 
            p.comentarios, 
            p.idlibro,
            l.titulo as nombrelibro
        from 
            prestamo p
        join 
            libro l on l.id = p.idlibro
        where   
            p.idestudiante = %(i)s
        order by 
            p.fechaprestamo desc
    """

    print("+------------------------------------------+")
    print("| Ver historial de prestamos de estudiante |")
    print("+------------------------------------------+")

    sid_estudiante = input("Id del estudiante: ")
    try:
        id_estudiante = None if sid_estudiante == "" else int(sid_estudiante)
    except ValueError:
        print("Error: El id del estudiante debe ser un numero entero.")
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
                print(f"Fecha de prestamo: {prestamo['fechaprestamo'].date()}")
                print(f"Fecha de devolucion: {prestamo['fechadevolucion'].date() if prestamo['fechadevolucion'] else 'No devuelto'}")
                print(f"Comentarios: {prestamo['comentarios']}")
                print(f"Id del libro: {prestamo['idlibro']}")
                print(f"Libro: {prestamo['nombrelibro']}")

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()

# 14
def consultar_prestamo(conn):
    """
    Consulta un prestamo en la base de datos. Pide al usuario el id del prestamo.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        select 
            p.fechaprestamo, 
            p.fechadevolucion, 
            p.comentarios, 
            p.idlibro,
            l.titulo as nombrelibro,
            p.idestudiante,
            e.nombre as nombreestudiante
        from 
            prestamo p
        join 
            libro l on l.id = p.idlibro
        join 
            estudiante e on e.id = p.idestudiante
        where 
            p.id = %(i)s
    """

    print("+--------------------+")
    print("| Consultar prestamo |")
    print("+--------------------+")

    sid_prestamo = input("Id del prestamo: ")
    try:
        id_prestamo = int(sid_prestamo)
    except ValueError:
        print("Error: El id del prestamo debe ser un numero entero.")
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
            print(f"Fecha del prestamo: {prestamo['fechaprestamo'].date()}")
            print(
                f"Fecha de devolucion: {prestamo['fechadevolucion'].date() if prestamo['fechadevolucion'] else 'No devuelto'}")
            print(f"Comentarios: {prestamo['comentarios']}")
            print(f"Id del libro: {prestamo['idlibro']}")
            print(f"Libro: {prestamo['nombrelibro']}")
            print(f"Id del estudiante: {prestamo['idestudiante']}")
            print(f"Estudiante: {prestamo['nombreestudiante']}")

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()

# 15
def finalizar_prestamo(conn):
    """
    Finaliza un prestamo en la base de datos. Pide al usuario el id del prestamo.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    sql_sentence = """
        update 
            prestamo
        set
            fechadevolucion = now()
        where
            id = %(i)s
    """

    print("+--------------------+")
    print("| Finalizar prestamo |")
    print("+--------------------+")

    sid_prestamo = input("Id del prestamo: ")
    try:
        id_prestamo = int(sid_prestamo)
    except ValueError:
        print("Error: El id del prestamo debe ser un numero entero.")
        return

    with conn.cursor() as cur:
        try:
            cur.execute(sql_sentence, {
                'i': id_prestamo,
            })

            if cur.rowcount == 0:
                print("No se ha encontrado el prestamo.")
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
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        delete from
            prestamo 
        where 
            id = %(i)s
    """

    print("+-------------------+")
    print("| Eliminar prestamo |")
    print("+-------------------+")

    sid_prestamo = input("Id del prestamo: ")
    try:
        id_prestamo = None if sid_prestamo == "" else int(sid_prestamo)
    except ValueError:
        print("Error: El id del prestamo debe ser un numero entero.")
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
    Anade un estudiante a la base de datos. Pide al usuario los datos necesarios.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        insert into estudiante (nombre, apellidos, curso, email, telefono)
        values (%(n)s, %(a)s, %(c)s, %(e)s, %(t)s)
    """

    print("+-------------------+")
    print("| Anadir estudiante |")
    print("+-------------------+")

    snombre = input("Nombre: ").strip()
    nombre = None if snombre == "" else snombre

    sapellidos = input("Apellidos: ").strip()
    apellidos = None if sapellidos == "" else sapellidos

    scurso = input("Curso: ").strip()
    try:
        curso = None if scurso == "" else int(scurso)
    except ValueError:
        print("Error: El curso debe ser un numero entero.")
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
            print("Estudiante anadido correctamente.")

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
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        select 
            e.nombre,
            e.apellidos,
            e.curso,
            e.email,
            e.telefono,
            (
                select count(*) 
                from prestamo p 
                where p.idestudiante = e.id
                and p.fechadevolucion is null
            ) as librosenposesion
        from
            estudiante e
        where
             e.id = %(i)s
    """

    print("+----------------------+")
    print("| Consultar estudiante |")
    print("+----------------------+")

    sid_estudiante = input("Id del estudiante: ")
    try:
        id_estudiante = int(sid_estudiante)
    except ValueError:
        print("Error: El id del estudiante debe ser un numero entero.")
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
            print(f"Libros en posesion: {estudiante['librosenposesion']}")

        except psycopg2.Error as e:
            print_generic_error(e)
            conn.rollback()

# 19
def aumentar_curso(conn):
    """
    Aumenta el curso de uno o varios estudiantes. Pide al usuario los datos necesarios.
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    sql_sentence = """
        update
            estudiante
        set 
            curso = curso + 1
        where 
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
            print("El id del estudiante debe ser un numero entero.")
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
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    sql_update_nombre = """
        update 
            estudiante 
        set 
            nombre = %(n)s 
        where 
            id = %(i)s
    """

    sql_update_apellidos = """
        update 
            estudiante 
        set 
            apellidos = %(a)s 
        where 
            id = %(i)s
    """

    sql_update_curso = """
        update 
            estudiante 
        set 
            curso = %(c)s 
        where 
            id = %(i)s
    """

    sql_update_email = """
        update 
            estudiante 
        set 
            email = %(e)s 
        where 
            id = %(i)s
    """

    sql_update_telefono = """
        update 
            estudiante  
        set 
            telefono = %(t)s 
        where 
            id = %(i)s
    """

    print("+----------------------+")
    print("| Modificar estudiante |")
    print("+----------------------+")

    sid_estudiante = input("Id del estudiante: ").strip()
    try:
        id_estudiante = int(sid_estudiante)
    except ValueError:
        print("Error: El id del estudiante debe ser un numero entero.")
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
                    print("Error: El curso debe ser un numero entero.")
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
    :param conn: La conexion abierta a la BD
    :return: Nada
    """

    sql_sentence = """
        delete
        from estudiante
        where id = %(i)s
    """

    print("+---------------------+")
    print("| Eliminar estudiante |")
    print("+---------------------+")

    sid_estudiante = input("Id del estudiante: ")
    try:
        id_estudiante = int(sid_estudiante)
    except ValueError:
        print("Error: El id del estudiante debe ser un numero entero.")
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
    Imprime un menu de opciones, solicita la opcion y ejecuta la funcion asociada.
    'q' para salir.
    """

    menu_text = """
                +------+
                | MENU |
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