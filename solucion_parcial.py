#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Solución PARCIAL do boletín de exercicios de BDA.
#
# Autor: Miguel Rodríguez Penabad (miguel.penabad@udc.es)

import sys
import psycopg2
import psycopg2.extras
import psycopg2.errorcodes


## ------------------------------------------------------------
def connect_db():
     """
     Conéctase á bd predeterminada do usuario (DSN baleiro)
     :return: A conexión coa BD (ou sae do programa de non conseguila)
     """
     try:
        conn = psycopg2.connect("")
        conn.autocommit = False
        return conn
     except psycopg2.Error as e:
         print("Erro de conexión")
         sys.exit(1)

## ------------------------------------------------------------
def disconnect_db(conn):
    """
    Desconéctase da BD. Fai antes un commit da transacción activa.
    :param conn: a conexión aberta á bd
    :return: Nada
    """
    conn.commit()
    conn.close()

## ------------------------------------------------------------
def create_table(conn):
    """
    Crea a táboa artigo (codart, nomart, prezoart)
    :param conn: a conexión aberta á bd
    :return: Nada
    """
    sql = """
            create table artigo(
                codart int constraint artigo_pkey primary key,
                nomart varchar(30) not null,
                prezoart numeric(5,2) constraint ch_art_prezo3_pos check (prezoart > 0)
            )
    """
    
    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql)
            conn.commit()
            print("Táboa artigo creada.")
        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
                print("A táboa artigo xa existe. Non se crea.")
            else: 
                print(f"Erro {e.pgcode}: {e.pgerror}")
            conn.rollback()

## ------------------------------------------------------------
def drop_table(conn):
    """
    Elimina a táboa artigo
    :param conn: a conexión aberta á bd
    :return: Nada
    """
    sql = """
            drop table artigo
    """
    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql)
            conn.commit()
            print("Táboa artigo eliminada.")
        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                print("A táboa artigo non existe. Non se borra.")
            else: 
                print(f"Erro {e.pgcode}: {e.pgerror}")
            conn.rollback()

## ------------------------------------------------------------
def add_row(conn):
    """
    Pide por teclado código, nome e prezo e inserta o artigo
    :param conn: a conexión aberta á bd
    :return: Nada
    """
    scod = input("Código: ")
    cod = None if scod == "" else int(scod)  
    snome = input("Nome: ")
    nome = None if snome == "" else snome
    sprezo = input("Prezo: ")
    prezo = None if sprezo == "" else float(sprezo)

    sql = """
        insert into artigo(codart, nomart, prezoart)
            values(%(c)s,%(n)s,%(p)s)
    """
    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql, {'c': cod, 'n': nome, 'p': prezo})
            conn.commit()
            print("Artigo engadido.")
        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                print(f"O artigo de código {cod} xa existe. Non se engade.")
            elif e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                if e.diag.column_name == "codart":
                    print("O código de artigo é obrigatorio")
                else:
                    print("O nome do artigo é obrigatorio") 
            elif e.pgcode == psycopg2.errorcodes.CHECK_VIOLATION:
                print("O prezo do artigo debe ser positivo")          
            else:
                print(f"Erro {e.pgcode}: {e.pgerror}")
            conn.rollback()

## ------------------------------------------------------------
def delete_row(conn):
    """
    Pide por teclado código dun artigo e elimínao
    :param conn: a conexión aberta á bd
    :return: Nada
    """
    scod = input("Código: ")
    cod = None if scod == "" else int(scod)  

    sql = """
            delete from artigo
            where codart = %s
        """
    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql, (cod,))
            conn.commit()
            if cursor.rowcount == 0:
                print(f"O artigo de código {cod} non existe.")
            else:
                print("Artigo eliminado.")
        except psycopg2.Error as e:
            print(f"Erro {e.pgcode}: {e.pgerror}")
            conn.rollback()


## ------------------------------------------------------------
def show_row(conn, control_tx=True):
    """
    Pide por teclado código dun artigo e mostra os seus detalles
    :param conn: a conexión aberta á bd
    :param control_tx: indica se debemos facer o control transaccional
    :return: O código do artigo, se existe. None noutro caso
    """
    scod = input("Código: ")
    cod = None if scod == "" else int(scod)  

    sql = """
            select nomart, prezoart
            from artigo
            where codart = %(c)s
        """
    if control_tx:
        conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED

    retval = None

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        try:
            cursor.execute(sql, {'c': cod})
            row = cursor.fetchone()
            if row:
                retval = cod
                prezo = row['prezoart'] if row['prezoart'] else "Descoñecido"
                print(f"Código: {cod} Nome: {row['nomart']}   Prezo: {prezo}")
            else:
                print(f"O artigo de código {cod} non existe")
            if control_tx:
                conn.commit()
        except psycopg2.Error as e:
            print(f"Erro {e.pgcode}: {e.pgerror}")
            if control_tx:
                conn.rollback()
        return retval
## ------------------------------------------------------------
def show_by_price(conn):
    """
    Pide un prezo por teclado e mostra os detalles dos artigos que son
    máis caros. Mostra tamén cantos artigos se atoparon.
    :param conn: a conexión aberta á bd
    :return: Nada
    """
    sprezo = input("Prezo (mostraremos artigos máis caros): ")
    prezo = None if sprezo == "" else float(sprezo)  

    sql = """
            select codart, nomart, prezoart
            from artigo
            where prezoart > %(p)s
        """
    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        try:
            cursor.execute(sql, {'p': prezo})
            rows = cursor.fetchall()
            for row in rows:
                prezo = row['prezoart'] if row['prezoart'] else "Descoñecido"
                print(f"Código: {row['codart']} Nome: {row['nomart']}   Prezo: {prezo}")
            
            print(f"Hai {cursor.rowcount} artigos que costan máis de {sprezo}.")

            conn.commit()
        except psycopg2.Error as e:
            print(f"Erro {e.pgcode}: {e.pgerror}")
            conn.rollback()




## ------------------------------------------------------------
def update_price(conn):
    """
    Pide por teclado código. Se o artigo existe, mostra o seu detalle,
    pide unha porcentaxe de incemento de prezo e actualiza o artigo.
    :param conn: a conexión aberta á bd
    :return: Nada
    """
    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    cod = show_row(conn, control_tx=False)

    if cod is None:
        conn.rollback()
        return

    sincremento = input("Incremento (%): ")
    incremento = None if sincremento == "" else float(sincremento)

    sql = """
        update artigo
        set prezoart = prezoart + prezoart * %(inc)s / 100.0
        where codart = %(c)s
    """
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql, {'c': cod, 'inc': incremento})

            input("PULSA ENTER")

            conn.commit()
            print("Artigo actualizado.")
        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE:
                print("O prezo do artigo foi modificado por outro usuario e non corresponde co prezo mostrado.")
                print("<< TODO: Implmentar a xestión de erro de xeito que o programa teña unha boa usabilidade >>")
            if e.pgcode == psycopg2.errorcodes.CHECK_VIOLATION:
                print("O prezo do artigo debe ser positivo")          
            else:
                print(f"Erro {e.pgcode}: {e.pgerror}")
            conn.rollback()


## ------------------------------------------------------------
def update_row(conn):
    """
    Pide por teclado código. Se o artigo existe, mostra o seu detalle,
    pide o novo nome e prezo e actualiza o artigo
    :param conn: a conexión aberta á bd
    :return: Nada
    """

    # Podería valorarse poñer o modo serializable no caso de que non se considerase
    # válido modificar un artigo que foi modificado por outra tx entre a execución
    # de show_row() e o update.
    # Nese caso, debería controlarse o erro SERIALIZATION_FAILURE.
    conn.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    cod = show_row(conn, control_tx=False)

    if cod is None:
        conn.rollback()
        return

    snome = input("Novo nome: ")
    nome = None if snome == "" else snome
    sprezo = input("Novo prezo: ")
    prezo = None if sprezo == "" else float(sprezo)


    sql = """
        update artigo
        set nomart = %(n)s, prezoart = %(p)s
        where codart = %(c)s
    """
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql, {'c': cod, 'n': nome, 'p': prezo})
            conn.commit()
            print("Artigo actualizado.")
        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION:
                print("O nome do artigo é obrigatorio") 
            elif e.pgcode == psycopg2.errorcodes.CHECK_VIOLATION:
                print("O prezo do artigo debe ser positivo")          
            else:
                print(f"Erro {e.pgcode}: {e.pgerror}")
            conn.rollback()



## ------------------------------------------------------------
def menu(conn):
    """
    Imprime un menú de opcións, solicita a opción e executa a función asociada.
    'q' para saír.
    """
    MENU_TEXT = """
      -- MENÚ --
1 - Crear táboa artigo  2 - Eliminar táboa      3 - Engadir artigo
4 - Borrar artigo       5 - Mostrar artigo      6 - Mostrar por prezo
7 - Actualizar prezo    8 - Actualizar artigo
q - Saír   
"""
    while True:
        print(MENU_TEXT)
        tecla = input('Opción> ')
        if tecla == 'q':
            break
        elif tecla == '1':
            create_table(conn)  
        elif tecla == '2':
            drop_table(conn)  
        elif tecla == '3':
            add_row(conn)  
        elif tecla == '4':
            delete_row(conn)  
        elif tecla == '5':
            show_row(conn)  
        elif tecla == '6':
            show_by_price(conn)  
        elif tecla == '7':
            update_price(conn)  
        elif tecla == '8':
            update_row(conn)  
            
            
## ------------------------------------------------------------
def main():
    """
    Función principal. Conecta á bd e executa o menú.
    Cando sae do menú, desconecta da bd e remata o programa
    """
    print('Conectando a PosgreSQL...')
    conn = connect_db()
    print('Conectado.')
    menu(conn)
    disconnect_db(conn)

## ------------------------------------------------------------

if __name__ == '__main__':
    main()
