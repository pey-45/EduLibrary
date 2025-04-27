-- Insertar categorías
INSERT INTO Categoria (id, nombre, descripcion) VALUES
('Ficción', 'Libros de ficción imaginativa'),
('Ciencia', 'Libros científicos y divulgativos'),
('Historia', 'Libros históricos'),
('Tecnología', 'Libros sobre avances tecnológicos'),
('Arte', 'Libros de arte y cultura');

-- Insertar libros
INSERT INTO Libro (id, titulo, autor, anioPublicacion, isbn, sinopsis, idCategoria) VALUES
('El señor de los anillos', 'J.R.R. Tolkien', 1954, '978-0261102385', 'Una aventura épica en la Tierra Media.', 1),
('Breves respuestas a grandes preguntas', 'Stephen Hawking', 2018, '978-0553176988', 'Últimas reflexiones del célebre físico.', 2),
('Sapiens', 'Yuval Noah Harari', 2011, '978-8499924211', 'Breve historia de la humanidad.', 3),
('Introducción a la inteligencia artificial', 'Stuart Russell', 2010, '978-8498879864', 'Libro clásico sobre IA.', 4),
('Historia del arte', 'E.H. Gombrich', 1950, '978-0714832470', 'Recorrido por la historia del arte.', 5);

-- Insertar precios para los libros
INSERT INTO PrecioLibro (id, precio, fecha, idLibro) VALUES
(19.99, NOW() - INTERVAL '100 days', 1),
(24.50, NOW() - INTERVAL '90 days', 2),
(17.00, NOW() - INTERVAL '80 days', 3),
(29.99, NOW() - INTERVAL '70 days', 4),
(22.75, NOW() - INTERVAL '60 days', 5);

-- Insertar estudiantes
INSERT INTO Estudiante (id, nombre, apellidos, curso, email, telefono) VALUES
('Ana', 'García López', 1, 'ana.garcia@email.com', '600111222'),
('Luis', 'Martínez Ruiz', 2, 'luis.martinez@email.com', '600222333'),
('Clara', 'Sánchez Pérez', 3, 'clara.sanchez@email.com', '600333444'),
('Pedro', 'Gómez Díaz', 4, 'pedro.gomez@email.com', '600444555'),
('Laura', 'Torres Vega', 2, 'laura.torres@email.com', '600555666');

-- Insertar préstamos
INSERT INTO Prestamo (id, fechaPrestamo, fechaDevolucion, comentarios, idLibro, idEstudiante) VALUES
(NOW() - INTERVAL '20 days', NULL, 'Lectura personal.', 1, 1),
(NOW() - INTERVAL '15 days', NOW() - INTERVAL '5 days', 'Para un proyecto.', 2, 2),
(NOW() - INTERVAL '10 days', NULL, 'Consulta bibliográfica.', 3, 3),
(NOW() - INTERVAL '8 days', NOW() - INTERVAL '1 days', 'Trabajo académico.', 4, 4),
(NOW() - INTERVAL '5 days', NULL, 'Lectura por placer.', 5, 5);
