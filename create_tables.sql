DROP TABLE prestamo, libro, estudiante, categoria CASCADE;

CREATE TABLE categoria (
    id BIGSERIAL,
    nombre VARCHAR(100) NOT NULL,
    descripcion VARCHAR(255) NOT NULL,
    CONSTRAINT CategoriaPK PRIMARY KEY (id),
    CONSTRAINT CategoriaNombreUQ UNIQUE (nombre)
);

CREATE TABLE estudiante (
    id BIGSERIAL,
    nombre VARCHAR(100) NOT NULL,
    apellidos VARCHAR(100) NOT NULL,
    curso SMALLINT NOT NULL,
    email VARCHAR(150) NOT NULL,
    telefono VARCHAR(20),
    CONSTRAINT EstudiantePK PRIMARY KEY (id),
    CONSTRAINT EstudianteEmailUQ UNIQUE (email),
    CONSTRAINT EstudianteTelefonoUQ UNIQUE (telefono)
);

CREATE TABLE libro (
    id BIGSERIAL,
    titulo VARCHAR(200) NOT NULL,
    autor VARCHAR(150),
    aniopublicacion SMALLINT,
    isbn VARCHAR(20),
    sinopsis TEXT,
    idcategoria BIGINT,
    CONSTRAINT LibroPK PRIMARY KEY (id),
    CONSTRAINT LibroISBNUQ UNIQUE (isbn),
    CONSTRAINT LibroCategoriaFK FOREIGN KEY (idcategoria)
        REFERENCES categoria(id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

CREATE TABLE preciolibro (
    id BIGSERIAL,
    precio FLOAT NOT NULL,
    fecha TIMESTAMP NOT NULL,
    idlibro BIGINT NOT NULL
        REFERENCES libro(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE prestamo (
    id BIGSERIAL,
    fechaprestamo TIMESTAMP NOT NULL,
    fechadevolucion TIMESTAMP,
    comentarios TEXT,
    idlibro BIGINT,
    idestudiante BIGINT,
    CONSTRAINT PrestamoPK PRIMARY KEY (id),
    CONSTRAINT PrestamoLibroFK FOREIGN KEY (idlibro)
        REFERENCES libro(id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,
    CONSTRAINT PrestamoEstudianteFK FOREIGN KEY (idestudiante)
        REFERENCES estudiante(id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);