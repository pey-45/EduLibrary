DROP TABLE Prestamo, Libro, Estudiante, Categoria CASCADE;

CREATE TABLE Categoria (
    id BIGSERIAL,
    nombre VARCHAR(100) NOT NULL,
    descripcion VARCHAR(255) NOT NULL,
    CONSTRAINT CategoriaPK PRIMARY KEY (id),
    CONSTRAINT CategoriaNombreUQ UNIQUE (nombre)
);

CREATE TABLE Estudiante (
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

CREATE TABLE Libro (
    id BIGSERIAL,
    titulo VARCHAR(200) NOT NULL,
    autor VARCHAR(150),
    anioPublicacion SMALLINT,
    isbn VARCHAR(20),
    sinopsis TEXT,
    idCategoria BIGINT,
    CONSTRAINT LibroPK PRIMARY KEY (id),
    CONSTRAINT LibroISBNUQ UNIQUE (isbn),
    CONSTRAINT LibroCategoriaFK FOREIGN KEY (idCategoria)
        REFERENCES Categoria(id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

CREATE TABLE PrecioLibro (
    id BIGSERIAL,
    precio FLOAT NOT NULL,
    fecha TIMESTAMP NOT NULL,
    idLibro BIGINT NOT NULL
        REFERENCES Libro(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE Prestamo (
    id BIGSERIAL,
    fechaPrestamo TIMESTAMP NOT NULL,
    fechaDevolucion TIMESTAMP,
    comentarios TEXT,
    idLibro BIGINT,
    idEstudiante BIGINT,
    CONSTRAINT PrestamoPK PRIMARY KEY (id),
    CONSTRAINT PrestamoLibroFK FOREIGN KEY (idLibro)
        REFERENCES Libro(id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,
    CONSTRAINT PrestamoEstudianteFK FOREIGN KEY (idEstudiante)
        REFERENCES Estudiante(id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);